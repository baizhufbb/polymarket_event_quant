from __future__ import annotations

import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread

import requests

from .discovery import MarketDiscovery
from .models import Market


CLOB_MARKETS = "https://clob.polymarket.com/clob-markets"
MARKET_SECONDS = 300
DISCOVERY_POLL_SECONDS = 1.0
ACTIVATION_POLL_SECONDS = 0.25
# The venue creates a market, fills in its tokens, and only then - 8 .. 16 s
# later on 2026-09-04 - writes `aot`, the accepting-order timestamp the
# placement loop keys its first send on (the book opens 48.9 s after it).
# Emitting on the tokens alone handed the loop a market without `aot`, so it
# knocked from discovery as before. A market whose tokens are known therefore
# waits this long for `aot` before it is emitted without one; the book never
# opens earlier than aot + 47.6 s, so the wait costs nothing on a normal day
# and only delays the blind knocking by this much on a degraded one.
AOT_WAIT_SECONDS = 30.0


def parse_accepting_orders_ts(value: object) -> int | None:
    """The CLOB listing's `aot`, e.g. "2026-09-03T03:17:46Z", as unix seconds."""
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp())


@dataclass(frozen=True)
class MarketActivationState:
    healthy: bool
    changed: bool
    recovered: bool
    error: str | None = None


@dataclass(frozen=True)
class MarketActivationUpdate:
    market: Market
    market_discovered_ts_ms: int
    market_parameters_detected_ts_ms: int


@dataclass(frozen=True)
class _Candidate:
    market: Market
    market_discovered_ts_ms: int
    # When the CLOB listing first showed both tokens; None until it did.
    parameters_seen_ts_ms: int | None = None


class MarketActivationWorker:
    """Discover predictable Gamma markets and emit them when CLOB knows them."""

    def __init__(
        self,
        *,
        window_minutes: int,
        farthest_first: bool,
        wake_event: Event | None = None,
    ):
        self.window_minutes = window_minutes
        self.farthest_first = farthest_first
        self.wake_event = wake_event
        self.discovery = MarketDiscovery()
        self.market_session = requests.Session()
        self.market_session.headers["User-Agent"] = "polymarket-btc-bot/0.1"

        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[
            MarketActivationUpdate | MarketActivationState
        ] = SimpleQueue()
        self._health_lock = Lock()
        self._candidate_lock = Lock()
        self._component_health: dict[str, bool | None] = {
            "gamma": None,
            "clob": None,
        }
        self._attempted = False
        self._threads: list[Thread] = []
        self._candidates: dict[str, _Candidate] = {}
        self._handled_slugs: set[str] = set()
        self._next_start_ts: int | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("market activation worker already started")
        self._threads = [
            Thread(
                target=self._run_discovery,
                name="polymarket-market-metadata",
                daemon=True,
            ),
            Thread(
                target=self._run_market_parameters,
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
        self.market_session.close()

    def drain(self) -> list[MarketActivationUpdate | MarketActivationState]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def _run_discovery(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self._poll_discovery()
            except Exception as exc:
                self._set_component_health(
                    "gamma", False, f"{type(exc).__name__}: {exc}"
                )
            else:
                self._set_component_health("gamma", True)
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, DISCOVERY_POLL_SECONDS - elapsed))

    def _poll_discovery(self) -> None:
        if self._next_start_ts is None:
            seed_only = self.window_minutes == 0
            markets = self.discovery.discover(
                max(self.window_minutes, MARKET_SECONDS // 60),
                farthest_first=self.farthest_first,
                timeout=5,
                fresh=True,
            )
            if not markets:
                raise RuntimeError("Gamma returned no BTC five-minute market")
            if not seed_only:
                self._register_candidates(markets)
            self._next_start_ts = (
                max(market.start_ts for market in markets) + MARKET_SECONDS
            )

        while not self._stop.is_set():
            slug = f"btc-updown-5m-{self._next_start_ts}"
            market = self.discovery.find_by_slug(slug, timeout=5, fresh=True)
            if market is None:
                return
            self._register_candidates([market])
            self._next_start_ts += MARKET_SECONDS

    def _run_market_parameters(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                requested = self._poll_market_parameters()
            except Exception as exc:
                self._set_component_health(
                    "clob", False, f"{type(exc).__name__}: {exc}"
                )
            else:
                if requested:
                    self._set_component_health("clob", True)
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, ACTIVATION_POLL_SECONDS - elapsed))

    def _register_candidates(
        self,
        markets: list[Market],
        *,
        market_discovered_ts_ms: int | None = None,
    ) -> None:
        if not markets:
            raise ValueError("at least one market is required")

        discovered_ts_ms = market_discovered_ts_ms or int(time.time() * 1000)
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
                    _Candidate(market, discovered_ts_ms),
                )

    def _poll_market_parameters(self) -> bool:
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

        for candidate in candidates:
            market = candidate.market
            response = self.market_session.get(
                f"{CLOB_MARKETS}/{market.condition_id}",
                timeout=2,
            )
            if response.status_code == 404:
                continue
            response.raise_for_status()
            payload = response.json()
            tokens = {
                str(token.get("t") or "")
                for token in payload.get("t") or []
                if isinstance(token, dict)
            }
            if not {
                market.up_token_id,
                market.down_token_id,
            }.issubset(tokens):
                continue

            now_ms = int(time.time() * 1000)
            seen_ts_ms = candidate.parameters_seen_ts_ms or now_ms
            accepting_orders_ts = parse_accepting_orders_ts(payload.get("aot"))
            if accepting_orders_ts is not None:
                market = replace(market, accepting_orders_ts=accepting_orders_ts)
            elif now_ms - seen_ts_ms < AOT_WAIT_SECONDS * 1000:
                # Tokens are up but `aot` is not written yet: keep the
                # candidate and come back next poll.
                with self._candidate_lock:
                    if market.slug in self._candidates:
                        self._candidates[market.slug] = _Candidate(
                            market, candidate.market_discovered_ts_ms, seen_ts_ms
                        )
                continue
            detected_ts_ms = seen_ts_ms
            with self._candidate_lock:
                if self._candidates.pop(market.slug, None) is None:
                    continue
                self._handled_slugs.add(market.slug)
            self._emit(
                MarketActivationUpdate(
                    market=market,
                    market_discovered_ts_ms=candidate.market_discovered_ts_ms,
                    market_parameters_detected_ts_ms=detected_ts_ms,
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
