from __future__ import annotations

import time
from dataclasses import dataclass
from queue import Empty, SimpleQueue
from threading import Event, Thread
from typing import Protocol


class HeartbeatExchange(Protocol):
    def heartbeat(self) -> dict: ...


@dataclass(frozen=True)
class HeartbeatUpdate:
    success: bool
    changed: bool
    recovered: bool
    error: str | None = None


class HeartbeatWorker:
    def __init__(self, exchange: HeartbeatExchange, interval_seconds: float):
        self.exchange = exchange
        self.interval_seconds = interval_seconds
        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[HeartbeatUpdate] = SimpleQueue()
        self._attempted = False
        self._thread: Thread | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("heartbeat worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(6.0, self.interval_seconds + 1.0))

    def drain(self) -> list[HeartbeatUpdate]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def run_once(self) -> HeartbeatUpdate:
        was_healthy = self._healthy.is_set()
        attempted_before = self._attempted
        self._attempted = True
        try:
            self.exchange.heartbeat()
        except Exception as exc:
            self._healthy.clear()
            update = HeartbeatUpdate(
                success=False,
                changed=not attempted_before or was_healthy,
                recovered=False,
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            self._healthy.set()
            update = HeartbeatUpdate(
                success=True,
                changed=not attempted_before or not was_healthy,
                recovered=attempted_before and not was_healthy,
            )
        self._updates.put(update)
        return update

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            self.run_once()
            remaining = max(0.0, self.interval_seconds - (time.monotonic() - started))
            if self._stop.wait(remaining):
                return
