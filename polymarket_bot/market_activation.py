from __future__ import annotations

import time
from dataclasses import dataclass
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread

import requests

from .discovery import MarketDiscovery
from .models import Market


CLOB_MARKETS = "https://clob.polymarket.com/clob-markets"
MARKET_SECONDS = 300
DISCOVERY_POLL_SECONDS = 1.0
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
    market_discovered_ts_ms: int
    market_parameters_detected_ts_ms: int


@dataclass(frozen=True)
class _Candidate:
    market: Market
    market_discovered_ts_ms: int


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

    def requeue(
        self,
        market: Market,
        *,
        market_discovered_ts_ms: int,
    ) -> bool:
        now_ts = int(time.time())
        with self._candidate_lock:
            if self._stop.is_set() or market.end_ts <= now_ts:
                self._candidates.pop(market.slug, None)
                self._handled_slugs.add(market.slug)
                return False
            self._handled_slugs.discard(market.slug)
            self._candidates[market.slug] = _Candidate(
                market, market_discovered_ts_ms
            )
        return True

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
            markets = self.discovery.discover(
                self.window_minutes,
                farthest_first=self.farthest_first,
                timeout=5,
                fresh=True,
            )
            if not markets:
                raise RuntimeError("Gamma returned no BTC five-minute market")
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

            detected_ts_ms = int(time.time() * 1000)
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
