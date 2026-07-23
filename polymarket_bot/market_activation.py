from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from queue import Empty, SimpleQueue
from threading import Event, Thread

import requests

from .discovery import MarketDiscovery
from .models import Market


CLOB_MARKETS = "https://clob.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"
BTC_FIVE_MINUTE_PREFIX = "btc-updown-5m-"
MARKET_SECONDS = 300
ACTIVATION_POLL_SECONDS = 0.25
CANDIDATE_DISCOVERY_SECONDS = 5.0
CANDIDATE_LOOKAHEAD = 12


@dataclass(frozen=True)
class MarketActivationState:
    healthy: bool
    changed: bool
    recovered: bool
    error: str | None = None


@dataclass(frozen=True)
class MarketActivationUpdate:
    market: Market
    accepting_ts_ms: int | None
    detected_ts_ms: int
    ready_ts_ms: int
    queue_ahead_up: Decimal
    queue_ahead_down: Decimal


def _timestamp_ms(value: object) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _bid_size_at(book: dict, price: Decimal) -> Decimal:
    for level in book.get("bids") or []:
        try:
            if Decimal(str(level["price"])) == price:
                return Decimal(str(level["size"]))
        except (InvalidOperation, KeyError, TypeError):
            continue
    return Decimal("0")


class MarketActivationWorker:
    """Detect when the next deterministic CLOB market starts accepting orders."""

    def __init__(self, *, queue_price: Decimal):
        self.queue_price = queue_price
        self.discovery = MarketDiscovery()
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "polymarket-btc-bot/0.1"
        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[
            MarketActivationUpdate | MarketActivationState
        ] = SimpleQueue()
        self._attempted = False
        self._thread: Thread | None = None
        self._candidates: dict[str, Market] = {}
        self._next_start_ts: int | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("market activation worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-market-activation",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self.session.close()
        self.discovery.session.close()

    def drain(self) -> list[MarketActivationUpdate | MarketActivationState]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def _run(self) -> None:
        next_discovery = 0.0
        while not self._stop.is_set():
            cycle_error = None
            cycle_succeeded = False
            now = time.monotonic()
            if now >= next_discovery:
                try:
                    self._discover_candidates()
                    cycle_succeeded = True
                except Exception as exc:
                    cycle_error = f"{type(exc).__name__}: {exc}"
                next_discovery = now + CANDIDATE_DISCOVERY_SECONDS

            for market in sorted(
                tuple(self._candidates.values()), key=lambda item: item.start_ts
            ):
                if self._stop.is_set():
                    break
                try:
                    update = self._poll_candidate(market)
                    cycle_succeeded = True
                except Exception as exc:
                    cycle_error = f"{type(exc).__name__}: {exc}"
                    continue
                if update is None:
                    continue
                self._updates.put(update)
                self._candidates.pop(market.slug, None)
                for slug, candidate in tuple(self._candidates.items()):
                    if candidate.start_ts < market.start_ts:
                        self._candidates.pop(slug, None)

            if cycle_succeeded:
                self._set_health(True)
            elif cycle_error is not None:
                self._set_health(False, cycle_error)
            self._stop.wait(ACTIVATION_POLL_SECONDS)

    def _discover_candidates(self) -> None:
        if self._next_start_ts is None:
            active = self.discovery.discover(5, farthest_first=True, timeout=2)
            if not active:
                return
            self._next_start_ts = active[0].start_ts + MARKET_SECONDS

        for _ in range(CANDIDATE_LOOKAHEAD):
            if self._stop.is_set():
                return
            slug = f"{BTC_FIVE_MINUTE_PREFIX}{self._next_start_ts}"
            market = self.discovery.candidate(slug)
            if market is None:
                return
            self._candidates.setdefault(slug, market)
            self._next_start_ts += MARKET_SECONDS

    def _poll_candidate(self, market: Market) -> MarketActivationUpdate | None:
        response = self.session.get(
            f"{CLOB_MARKETS}/{market.condition_id}",
            timeout=2,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        status = response.json()
        if not status.get("accepting_orders"):
            return None

        detected_ts_ms = int(time.time() * 1000)
        books = []
        for token_id in (market.up_token_id, market.down_token_id):
            book_response = self.session.get(
                CLOB_BOOK,
                params={"token_id": token_id},
                timeout=2,
            )
            if book_response.status_code == 404:
                return None
            book_response.raise_for_status()
            books.append(book_response.json())

        return MarketActivationUpdate(
            market=market,
            accepting_ts_ms=_timestamp_ms(status.get("accepting_order_timestamp")),
            detected_ts_ms=detected_ts_ms,
            ready_ts_ms=int(time.time() * 1000),
            queue_ahead_up=_bid_size_at(books[0], self.queue_price),
            queue_ahead_down=_bid_size_at(books[1], self.queue_price),
        )

    def _set_health(self, healthy: bool, error: str | None = None) -> None:
        was_healthy = self._healthy.is_set()
        attempted_before = self._attempted
        self._attempted = True
        if healthy:
            self._healthy.set()
        else:
            self._healthy.clear()
        changed = not attempted_before or was_healthy != healthy
        if changed:
            self._updates.put(
                MarketActivationState(
                    healthy=healthy,
                    changed=True,
                    recovered=healthy and attempted_before and not was_healthy,
                    error=error,
                )
            )
