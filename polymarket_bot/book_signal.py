from __future__ import annotations

import json
import time
from threading import Event, Thread

import requests
from polymarket import PRODUCTION
from websockets.sync.client import connect


CLOB_BOOKS = "https://clob.polymarket.com/books"
PING_INTERVAL_SECONDS = 10.0
RECONNECT_DELAY_SECONDS = 0.5
RECV_TIMEOUT_SECONDS = 1.0
DEFAULT_REST_POLL_MS = 25.0


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


def books_mention_assets(payload: object, asset_ids: frozenset[str]) -> bool:
    """True when the public books endpoint already lists one of the books."""
    if not isinstance(payload, list):
        return False
    return any(
        isinstance(book, dict) and str(book.get("asset_id") or "") in asset_ids
        for book in payload
    )


class BookOpenSignal:
    """Fire on the earliest public evidence that a market's book exists.

    Two detectors race: the market WebSocket feed and a REST poll of the
    public books endpoint. Live measurements show orders are accepted a few
    hundred milliseconds before the feed publishes its first event, so the
    REST poll usually rings first. Neither detector touches the rate-limited
    order endpoint, so the submission budget stays untouched until the burst.
    """

    def __init__(
        self,
        up_token_id: str,
        down_token_id: str,
        *,
        url: str | None = None,
        connect_fn=connect,
        rest_poll_ms: float | None = DEFAULT_REST_POLL_MS,
        books_fn=None,
    ):
        self._asset_ids = frozenset({up_token_id, down_token_id})
        self._url = url or PRODUCTION.clob_market_ws_url
        self._connect = connect_fn
        self._rest_poll_s = (
            rest_poll_ms / 1000.0 if rest_poll_ms is not None else None
        )
        self._books_fn = books_fn
        self._open = Event()
        self._stop = Event()
        self.signal_ts_ms: int | None = None
        self.signal_source: str | None = None
        self.error: str | None = None
        self._threads = [
            Thread(target=self._run_feed, name="book-signal-feed", daemon=True)
        ]
        if self._rest_poll_s is not None:
            self._threads.append(
                Thread(target=self._run_books, name="book-signal-rest", daemon=True)
            )
        for thread in self._threads:
            thread.start()

    def wait(self, timeout: float) -> bool:
        return self._open.wait(timeout)

    def close(self) -> None:
        """Stop the detectors; threads also exit by themselves after a signal.

        The websocket close handshake can hang for seconds, so the join here
        is bounded and the burst must never wait on it.
        """
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=0.5)

    def _fire(self, source: str) -> None:
        if not self._open.is_set():
            self.signal_ts_ms = time.time_ns() // 1_000_000
            self.signal_source = source
            self._open.set()

    def _active(self) -> bool:
        return not self._stop.is_set() and not self._open.is_set()

    def _run_feed(self) -> None:
        while self._active():
            try:
                self._watch_feed()
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                self._stop.wait(RECONNECT_DELAY_SECONDS)

    def _watch_feed(self) -> None:
        with self._connect(self._url, open_timeout=10, close_timeout=2) as socket:
            socket.send(
                json.dumps(
                    {"assets_ids": sorted(self._asset_ids), "type": "market"},
                    separators=(",", ":"),
                )
            )
            next_ping = time.monotonic() + PING_INTERVAL_SECONDS
            while self._active():
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
                    self._fire("ws")
                    return

    def _run_books(self) -> None:
        session = None
        probe = self._books_fn
        if probe is None:
            session = requests.Session()
            body = [{"token_id": token} for token in sorted(self._asset_ids)]

            def probe() -> bool:
                response = session.post(CLOB_BOOKS, json=body, timeout=2)
                if not response.ok:
                    return False
                return books_mention_assets(response.json(), self._asset_ids)

        try:
            while self._active():
                started = time.monotonic()
                try:
                    if probe():
                        self._fire("rest")
                        return
                except Exception as exc:
                    self.error = f"{type(exc).__name__}: {exc}"
                elapsed = time.monotonic() - started
                self._stop.wait(max(0.0, self._rest_poll_s - elapsed))
        finally:
            if session is not None:
                session.close()
