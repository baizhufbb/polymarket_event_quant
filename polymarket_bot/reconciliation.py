from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue, SimpleQueue
from threading import Event, Thread

from .exchange import Exchange

# The venue's read index lags a fresh acceptance by up to about a second;
# one re-read separates that from an order that is genuinely gone.
MISSING_ORDER_RECHECK_SECONDS = 1.0


@dataclass(frozen=True)
class TrackedOrderSnapshot:
    order_id: str
    slug: str
    size: str


@dataclass(frozen=True)
class ReconciledOrder:
    snapshot: TrackedOrderSnapshot
    raw: dict | None
    error: str | None = None


@dataclass(frozen=True)
class ReconciliationUpdate:
    orders: tuple[ReconciledOrder, ...]
    batch_error: str | None = None


class ReconciliationWorker:
    """Fetch exchange order state without blocking the placement loop."""

    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        self._stop = Event()
        self._busy = Event()
        self._requests: Queue[tuple[TrackedOrderSnapshot, ...]] = Queue(maxsize=1)
        self._updates: SimpleQueue[ReconciliationUpdate] = SimpleQueue()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("reconciliation worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-order-reconciliation",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=20.0)

    def submit(self, rows: list) -> bool:
        if self._busy.is_set():
            return False
        snapshots = tuple(
            TrackedOrderSnapshot(
                order_id=str(row["order_id"]),
                slug=str(row["slug"]),
                size=str(row["size"]),
            )
            for row in rows
            if row["status"] != "simulated"
        )
        self._busy.set()
        self._requests.put_nowait(snapshots)
        return True

    def drain(self) -> list[ReconciliationUpdate]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                snapshots = self._requests.get(timeout=0.2)
            except Empty:
                continue
            try:
                update = self._fetch(snapshots)
            except Exception as exc:
                update = ReconciliationUpdate(
                    (),
                    batch_error=f"{type(exc).__name__}: {exc}",
                )
            finally:
                self._busy.clear()
            self._updates.put(update)

    def _fetch(
        self, snapshots: tuple[TrackedOrderSnapshot, ...]
    ) -> ReconciliationUpdate:
        if not snapshots:
            return ReconciliationUpdate(())
        try:
            open_by_id = {}
            for raw in self.exchange.open_orders():
                order_id = raw.get("id") or raw.get("orderID") or raw.get("orderId")
                if order_id:
                    open_by_id[str(order_id)] = raw
        except Exception as exc:
            return ReconciliationUpdate(
                (),
                batch_error=f"{type(exc).__name__}: {exc}",
            )

        results = []
        missing = []
        for snapshot in snapshots:
            try:
                raw = open_by_id.get(snapshot.order_id)
                if raw is None:
                    raw = self.exchange.get_order(snapshot.order_id)
                if raw is None:
                    missing.append(snapshot)
                    continue
                if not isinstance(raw, dict):
                    raise ValueError("exchange returned no order payload")
                results.append(ReconciledOrder(snapshot=snapshot, raw=raw))
            except Exception as exc:
                results.append(
                    ReconciledOrder(
                        snapshot=snapshot,
                        raw=None,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
        # One pause for the whole round, interruptible by stop(): a worker
        # that is shutting down retires nothing on a single missed read.
        if missing and not self._stop.wait(MISSING_ORDER_RECHECK_SECONDS):
            for snapshot in missing:
                try:
                    raw = self.exchange.get_order(snapshot.order_id)
                    if raw is None:
                        raw = {
                            "id": snapshot.order_id,
                            "status": "terminal_unknown",
                            "size_matched": "0",
                        }
                    if not isinstance(raw, dict):
                        raise ValueError("exchange returned no order payload")
                    results.append(ReconciledOrder(snapshot=snapshot, raw=raw))
                except Exception as exc:
                    results.append(
                        ReconciledOrder(
                            snapshot=snapshot,
                            raw=None,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
        return ReconciliationUpdate(tuple(results))
