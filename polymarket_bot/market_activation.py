from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread

import requests

from .conditional_tokens import binary_token_ids
from .models import Market


CLOB_MARKETS = "https://clob.polymarket.com/markets"
CLOB_BOOKS = "https://clob.polymarket.com/books"
BTC_FIVE_MINUTE_PREFIX = "btc-updown-5m-"
MARKET_SECONDS = 300
CATALOG_PAGE_SIZE = 1000
CATALOG_LOOKBACK_PAGES = 3
CATALOG_REFRESH_SECONDS = 30.0
ACTIVATION_POLL_SECONDS = 0.25
END_CURSOR = "LTE="


@dataclass(frozen=True)
class MarketActivationState:
    healthy: bool
    changed: bool
    recovered: bool
    error: str | None = None


@dataclass(frozen=True)
class MarketActivationUpdate:
    market: Market
    books_detected_ts_ms: int
    queue_ahead_up: Decimal
    queue_ahead_down: Decimal


def _catalog_cursor(offset: int) -> str:
    return base64.b64encode(str(offset).encode()).decode()


def _catalog_market(row: dict) -> Market | None:
    slug = str(row.get("market_slug") or "")
    if not slug.startswith(BTC_FIVE_MINUTE_PREFIX):
        return None
    if row.get("closed") or row.get("archived"):
        return None

    condition_id = str(row.get("condition_id") or "")
    try:
        start_ts = int(slug.removeprefix(BTC_FIVE_MINUTE_PREFIX))
        up_token_id, down_token_id = binary_token_ids(condition_id)
        min_size = Decimal(str(row.get("minimum_order_size") or 5))
        tick_size = Decimal(str(row.get("minimum_tick_size") or "0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return None

    return Market(
        slug=slug,
        condition_id=condition_id,
        start_ts=start_ts,
        end_ts=start_ts + MARKET_SECONDS,
        up_token_id=up_token_id,
        down_token_id=down_token_id,
        min_size=min_size,
        tick_size=tick_size,
    )


def _bid_size_at(book: dict, price: Decimal) -> Decimal:
    for level in book.get("bids") or []:
        try:
            if Decimal(str(level["price"])) == price:
                return Decimal(str(level["size"]))
        except (InvalidOperation, KeyError, TypeError):
            continue
    return Decimal("0")


class MarketActivationWorker:
    """Register future CLOB markets, then emit them when both books appear."""

    def __init__(self, *, queue_price: Decimal, wake_event: Event | None = None):
        self.queue_price = queue_price
        self.wake_event = wake_event
        self.catalog_session = requests.Session()
        self.books_session = requests.Session()
        for session in (self.catalog_session, self.books_session):
            session.headers["User-Agent"] = "polymarket-btc-bot/0.1"

        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[
            MarketActivationUpdate | MarketActivationState
        ] = SimpleQueue()
        self._health_lock = Lock()
        self._candidate_lock = Lock()
        self._component_health: dict[str, bool | None] = {
            "catalog": None,
            "books": None,
        }
        self._attempted = False
        self._threads: list[Thread] = []
        self._candidates: dict[str, Market] = {}
        self._handled_slugs: set[str] = set()
        self._catalog_initialized = False
        self._books_baselined = False
        self._tail_offset: int | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("market activation worker already started")
        self._threads = [
            Thread(
                target=self._run_catalog,
                name="polymarket-market-catalog",
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
        self.catalog_session.close()
        self.books_session.close()

    def drain(self) -> list[MarketActivationUpdate | MarketActivationState]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def _run_catalog(self) -> None:
        while not self._stop.is_set():
            try:
                rows = self._read_catalog_tail()
                if self._stop.is_set():
                    return
                self._register_candidates(rows)
            except Exception as exc:
                self._set_component_health(
                    "catalog", False, f"{type(exc).__name__}: {exc}"
                )
            else:
                self._set_component_health("catalog", True)
            self._stop.wait(CATALOG_REFRESH_SECONDS)

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

    def _read_catalog_tail(self) -> list[dict]:
        pages: list[dict] = []
        if self._tail_offset is None:
            located = self._locate_tail_page()
            if located is None:
                return []
            self._tail_offset, tail_page = located
            first_offset = max(
                0,
                self._tail_offset
                - (CATALOG_LOOKBACK_PAGES - 1) * CATALOG_PAGE_SIZE,
            )
            for offset in range(
                first_offset, self._tail_offset, CATALOG_PAGE_SIZE
            ):
                if self._stop.is_set():
                    return []
                pages.append(self._catalog_page(offset))
            pages.append(tail_page)
        else:
            pages.append(self._catalog_page(self._tail_offset))

        while (
            not self._stop.is_set()
            and len(pages[-1]["data"]) == CATALOG_PAGE_SIZE
            and pages[-1].get("next_cursor") != END_CURSOR
        ):
            self._tail_offset += CATALOG_PAGE_SIZE
            pages.append(self._catalog_page(self._tail_offset))

        return [row for page in pages for row in page["data"]]

    def _locate_tail_page(self) -> tuple[int, dict] | None:
        pages: dict[int, dict] = {}

        def fetch(offset: int) -> dict:
            page = pages.get(offset)
            if page is None:
                page = self._catalog_page(offset)
                pages[offset] = page
            return page

        low = 0
        high = CATALOG_PAGE_SIZE
        while not self._stop.is_set() and fetch(high)["data"]:
            low = high
            high *= 2
        if self._stop.is_set():
            return None

        while high - low > CATALOG_PAGE_SIZE:
            middle = ((low + high) // (2 * CATALOG_PAGE_SIZE)) * CATALOG_PAGE_SIZE
            if fetch(middle)["data"]:
                low = middle
            else:
                high = middle
        return low, fetch(low)

    def _catalog_page(self, offset: int) -> dict:
        response = self.catalog_session.get(
            CLOB_MARKETS,
            params={"next_cursor": _catalog_cursor(offset)},
            timeout=5,
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, dict) or not isinstance(page.get("data"), list):
            raise RuntimeError("CLOB market catalog returned an invalid page")
        return page

    def _register_candidates(self, rows: list[dict]) -> None:
        parsed = [
            market
            for row in rows
            if (market := _catalog_market(row)) is not None
        ]
        if not parsed:
            raise RuntimeError("CLOB catalog tail contains no BTC five-minute market")

        now_ts = int(time.time())
        with self._candidate_lock:
            if not self._catalog_initialized:
                self._catalog_initialized = True

            for market in parsed:
                if market.slug in self._handled_slugs:
                    continue
                if market.end_ts <= now_ts:
                    self._handled_slugs.add(market.slug)
                    self._candidates.pop(market.slug, None)
                    continue
                self._candidates.setdefault(market.slug, market)

    def _poll_books(self) -> bool:
        with self._candidate_lock:
            catalog_initialized = self._catalog_initialized
            now_ts = int(time.time())
            expired_slugs = [
                slug
                for slug, market in self._candidates.items()
                if market.end_ts <= now_ts
            ]
            for slug in expired_slugs:
                self._candidates.pop(slug)
                self._handled_slugs.add(slug)
            candidates = tuple(self._candidates.values())
        if not candidates:
            if catalog_initialized:
                self._books_baselined = True
            return False

        request = [
            {"token_id": token_id}
            for market in candidates
            for token_id in (market.up_token_id, market.down_token_id)
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
        if not self._books_baselined:
            with self._candidate_lock:
                for market in candidates:
                    if (
                        market.up_token_id in books
                        and market.down_token_id in books
                    ):
                        self._candidates.pop(market.slug, None)
                        self._handled_slugs.add(market.slug)
                self._books_baselined = True
            return True

        for market in candidates:
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
