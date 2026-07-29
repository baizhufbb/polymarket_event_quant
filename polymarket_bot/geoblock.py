from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, SimpleQueue
from threading import Event, Thread
from typing import Protocol


API_UNRESTRICTED_FRONTEND_ONLY_COUNTRIES = frozenset({"IE", "JP", "MT", "NL"})


def is_api_trading_blocked(result: dict) -> bool:
    if result.get("blocked") is not True:
        return False
    country = str(result.get("country") or "").upper()
    return country not in API_UNRESTRICTED_FRONTEND_ONLY_COUNTRIES


class GeoblockExchange(Protocol):
    def geoblock(self) -> dict: ...


@dataclass(frozen=True)
class GeoblockUpdate:
    available: bool
    blocked: bool
    changed: bool
    recovered: bool
    result: dict | None = None
    error: str | None = None


class GeoblockWorker:
    def __init__(
        self,
        exchange: GeoblockExchange,
        *,
        interval_seconds: float,
        retry_seconds: float,
    ):
        self.exchange = exchange
        self.interval_seconds = interval_seconds
        self.retry_seconds = retry_seconds
        self._stop = Event()
        self._healthy = Event()
        self._attempted = False
        self._updates: SimpleQueue[GeoblockUpdate] = SimpleQueue()
        self._thread: Thread | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("geoblock worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-geoblock",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def drain(self) -> list[GeoblockUpdate]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def run_once(self) -> GeoblockUpdate:
        was_healthy = self._healthy.is_set()
        attempted_before = self._attempted
        self._attempted = True
        try:
            result = self.exchange.geoblock()
        except Exception as exc:
            self._healthy.clear()
            update = GeoblockUpdate(
                available=False,
                blocked=False,
                changed=not attempted_before or was_healthy,
                recovered=False,
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            blocked = is_api_trading_blocked(result)
            if blocked:
                self._healthy.clear()
            else:
                self._healthy.set()
            update = GeoblockUpdate(
                available=True,
                blocked=blocked,
                changed=not attempted_before or was_healthy == blocked,
                recovered=attempted_before and not was_healthy and not blocked,
                result=result,
            )
        if update.changed or update.blocked:
            self._updates.put(update)
        return update

    def _run(self) -> None:
        while not self._stop.is_set():
            update = self.run_once()
            delay = (
                self.interval_seconds if update.available else self.retry_seconds
            )
            self._stop.wait(delay)
