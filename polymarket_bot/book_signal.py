from __future__ import annotations

import json
import time
from threading import Event, Thread

from polymarket import PRODUCTION
from websockets.sync.client import connect


PING_INTERVAL_SECONDS = 10.0
RECONNECT_DELAY_SECONDS = 0.5
RECV_TIMEOUT_SECONDS = 1.0


def payload_mentions_assets(payload: object, asset_ids: frozenset[str]) -> bool:
    """True when a market-feed payload references one of the asset ids."""
    events = payload if isinstance(payload, list) else [payload]
    for event in events:
        if not isinstance(event, dict):
            continue
        if str(event.get("asset_id") or "") in asset_ids:
            return True
        for change in event.get("price_changes") or []:
            if (
                isinstance(change, dict)
                and str(change.get("asset_id") or "") in asset_ids
            ):
                return True
    return False


class BookOpenSignal:
    """Watch the public market feed for the first event of a market.

    The submission budget is rate limited, so the bot must not probe the
    order endpoint while the book is still closed. The first market-feed
    message that references either outcome token is the earliest public
    evidence that the engine has opened the book; entries burst from that
    moment with an untouched budget.
    """

    def __init__(
        self,
        up_token_id: str,
        down_token_id: str,
        *,
        url: str | None = None,
        connect_fn=connect,
    ):
        self._asset_ids = frozenset({up_token_id, down_token_id})
        self._url = url or PRODUCTION.clob_market_ws_url
        self._connect = connect_fn
        self._open = Event()
        self._stop = Event()
        self.signal_ts_ms: int | None = None
        self.error: str | None = None
        self._thread = Thread(
            target=self._run, name="book-open-signal", daemon=True
        )
        self._thread.start()

    def wait(self, timeout: float) -> bool:
        return self._open.wait(timeout)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5.0)

    def _run(self) -> None:
        while not self._stop.is_set() and not self._open.is_set():
            try:
                self._watch()
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                self._stop.wait(RECONNECT_DELAY_SECONDS)

    def _watch(self) -> None:
        with self._connect(self._url, open_timeout=10, close_timeout=5) as socket:
            socket.send(
                json.dumps(
                    {"assets_ids": sorted(self._asset_ids), "type": "market"},
                    separators=(",", ":"),
                )
            )
            next_ping = time.monotonic() + PING_INTERVAL_SECONDS
            while not self._stop.is_set():
                if time.monotonic() >= next_ping:
                    socket.send("PING")
                    next_ping = time.monotonic() + PING_INTERVAL_SECONDS
                try:
                    raw = socket.recv(timeout=RECV_TIMEOUT_SECONDS)
                except TimeoutError:
                    continue
                if raw == "PONG":
                    continue
                try:
                    payload = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if payload_mentions_assets(payload, self._asset_ids):
                    self.signal_ts_ms = time.time_ns() // 1_000_000
                    self._open.set()
                    return
