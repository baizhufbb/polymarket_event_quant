from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread

import requests
from websockets.sync.client import connect

from .discovery import MarketDiscovery
from .models import Market


CLOB_BOOKS = "https://clob.polymarket.com/books"
MARKET_STREAM = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MARKET_STREAM_RECONNECT_SECONDS = 1.0
MARKET_STREAM_PING_SECONDS = 10.0
ACTIVATION_POLL_SECONDS = 0.25


@dataclass(frozen=True)
class MarketActivationState:
    healthy: bool
    changed: bool
    recovered: bool
    error: str | None = None


@dataclass(frozen=True)
class MarketActivationUpdate:
    market: Market
    market_announced_ts_ms: int | None
    books_detected_ts_ms: int
    queue_ahead_up: Decimal
    queue_ahead_down: Decimal


@dataclass(frozen=True)
class _Candidate:
    market: Market
    market_announced_ts_ms: int | None


def _bid_size_at(book: dict, price: Decimal) -> Decimal:
    for level in book.get("bids") or []:
        try:
            if Decimal(str(level["price"])) == price:
                return Decimal(str(level["size"]))
        except (InvalidOperation, KeyError, TypeError):
            continue
    return Decimal("0")


class MarketActivationWorker:
    """Discover future Gamma markets, then emit them when both books appear."""

    def __init__(
        self,
        *,
        queue_price: Decimal,
        window_minutes: int,
        farthest_first: bool,
        wake_event: Event | None = None,
    ):
        self.queue_price = queue_price
        self.window_minutes = window_minutes
        self.farthest_first = farthest_first
        self.wake_event = wake_event
        self.discovery = MarketDiscovery()
        self.books_session = requests.Session()
        self.books_session.headers["User-Agent"] = "polymarket-btc-bot/0.1"

        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[
            MarketActivationUpdate | MarketActivationState
        ] = SimpleQueue()
        self._health_lock = Lock()
        self._candidate_lock = Lock()
        self._component_health: dict[str, bool | None] = {
            "market_stream": None,
            "books": None,
        }
        self._attempted = False
        self._threads: list[Thread] = []
        self._candidates: dict[str, _Candidate] = {}
        self._handled_slugs: set[str] = set()

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("market activation worker already started")
        self._threads = [
            Thread(
                target=self._run_market_stream,
                name="polymarket-market-metadata",
                daemon=True,
            ),
            Thread(
                target=self._run_books,
                name="polymarket-market-activation",
                daemon=True,
            ),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5.0)
        self.discovery.close()
        self.books_session.close()

    def drain(self) -> list[MarketActivationUpdate | MarketActivationState]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def _run_market_stream(self) -> None:
        while not self._stop.is_set():
            try:
                markets = self.discovery.discover(
                    self.window_minutes,
                    farthest_first=self.farthest_first,
                    timeout=5,
                )
                if not markets:
                    raise RuntimeError("Gamma returned no BTC five-minute market")
                if self._stop.is_set():
                    return
                self._register_candidates(markets)
                self._listen_market_stream(
                    min(markets, key=lambda market: market.start_ts).up_token_id
                )
            except Exception as exc:
                self._set_component_health(
                    "market_stream", False, f"{type(exc).__name__}: {exc}"
                )
            if not self._stop.is_set():
                self._stop.wait(MARKET_STREAM_RECONNECT_SECONDS)

    def _listen_market_stream(self, token_id: str) -> None:
        with connect(
            MARKET_STREAM,
            open_timeout=5,
            close_timeout=2,
            ping_interval=None,
        ) as websocket:
            websocket.send(
                json.dumps(
                    {
                        "assets_ids": [token_id],
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                )
            )
            self._set_component_health("market_stream", True)
            next_ping = time.monotonic() + MARKET_STREAM_PING_SECONDS
            while not self._stop.is_set():
                timeout = max(0.1, min(0.5, next_ping - time.monotonic()))
                try:
                    raw = websocket.recv(timeout=timeout)
                except TimeoutError:
                    raw = None
                if time.monotonic() >= next_ping:
                    websocket.send("PING")
                    next_ping = time.monotonic() + MARKET_STREAM_PING_SECONDS
                if raw in (None, "PONG"):
                    continue
                messages = json.loads(raw)
                if not isinstance(messages, list):
                    messages = [messages]
                for message in messages:
                    if (
                        not isinstance(message, dict)
                        or message.get("event_type") != "new_market"
                    ):
                        continue
                    market = MarketDiscovery.parse_stream_market(message)
                    if market is None:
                        continue
                    try:
                        announced_ts_ms = int(message["timestamp"])
                    except (KeyError, TypeError, ValueError):
                        announced_ts_ms = None
                    self._register_candidates(
                        [market],
                        market_announced_ts_ms=announced_ts_ms,
                    )

    def _run_books(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                requested = self._poll_books()
            except Exception as exc:
                self._set_component_health(
                    "books", False, f"{type(exc).__name__}: {exc}"
                )
            else:
                if requested:
                    self._set_component_health("books", True)
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, ACTIVATION_POLL_SECONDS - elapsed))

    def _register_candidates(
        self,
        markets: list[Market],
        *,
        market_announced_ts_ms: int | None = None,
    ) -> None:
        if not markets:
            raise ValueError("at least one market is required")

        now_ts = int(time.time())
        with self._candidate_lock:
            for market in markets:
                if market.slug in self._handled_slugs:
                    continue
                if market.end_ts <= now_ts:
                    self._handled_slugs.add(market.slug)
                    self._candidates.pop(market.slug, None)
                    continue
                self._candidates.setdefault(
                    market.slug,
                    _Candidate(market, market_announced_ts_ms),
                )

    def _poll_books(self) -> bool:
        with self._candidate_lock:
            now_ts = int(time.time())
            expired_slugs = [
                slug
                for slug, candidate in self._candidates.items()
                if candidate.market.end_ts <= now_ts
            ]
            for slug in expired_slugs:
                self._candidates.pop(slug)
                self._handled_slugs.add(slug)
            candidates = tuple(self._candidates.values())
        if not candidates:
            return False

        request = [
            {"token_id": token_id}
            for candidate in candidates
            for token_id in (
                candidate.market.up_token_id,
                candidate.market.down_token_id,
            )
        ]
        response = self.books_session.post(CLOB_BOOKS, json=request, timeout=2)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError("CLOB batch books returned an invalid response")

        books = {
            str(book.get("asset_id") or book.get("assetId")): book
            for book in payload
            if isinstance(book, dict)
        }
        books_detected_ts_ms = int(time.time() * 1000)
        for candidate in candidates:
            market = candidate.market
            up_book = books.get(market.up_token_id)
            down_book = books.get(market.down_token_id)
            if up_book is None or down_book is None:
                continue
            with self._candidate_lock:
                if self._candidates.pop(market.slug, None) is None:
                    continue
                self._handled_slugs.add(market.slug)
            self._emit(
                MarketActivationUpdate(
                    market=market,
                    market_announced_ts_ms=candidate.market_announced_ts_ms,
                    books_detected_ts_ms=books_detected_ts_ms,
                    queue_ahead_up=_bid_size_at(up_book, self.queue_price),
                    queue_ahead_down=_bid_size_at(down_book, self.queue_price),
                )
            )
        return True

    def _set_component_health(
        self, component: str, healthy: bool, error: str | None = None
    ) -> None:
        with self._health_lock:
            was_healthy = self._healthy.is_set()
            attempted_before = self._attempted
            self._attempted = True
            self._component_health[component] = healthy
            values = tuple(self._component_health.values())
            overall_healthy = any(value is True for value in values) and all(
                value is not False for value in values
            )
            if overall_healthy:
                self._healthy.set()
            else:
                self._healthy.clear()
            changed = not attempted_before or was_healthy != overall_healthy

        if changed:
            self._emit(
                MarketActivationState(
                    healthy=overall_healthy,
                    changed=True,
                    recovered=(
                        overall_healthy and attempted_before and not was_healthy
                    ),
                    error=f"{component}: {error}" if error else None,
                )
            )

    def _emit(self, update: MarketActivationUpdate | MarketActivationState) -> None:
        self._updates.put(update)
        if self.wake_event is not None:
            self.wake_event.set()
