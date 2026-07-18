from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from queue import Empty, SimpleQueue
from threading import Event, Thread

import websockets

from .models import Market


MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BTC_FIVE_MINUTE_PREFIX = "btc-updown-5m-"


@dataclass(frozen=True)
class MarketStreamState:
    healthy: bool
    changed: bool
    recovered: bool
    error: str | None = None


@dataclass(frozen=True)
class MarketReadyUpdate:
    market: Market
    event_ts_ms: int
    received_ts_ms: int
    ready_ts_ms: int
    queue_ahead_up: Decimal
    queue_ahead_down: Decimal


@dataclass
class _PendingMarket:
    market: Market
    event_ts_ms: int
    received_ts_ms: int
    queue_by_token: dict[str, Decimal] = field(default_factory=dict)


def _parse_new_market(message: dict) -> tuple[Market, int] | None:
    if message.get("event_type") != "new_market":
        return None
    slug = str(message.get("slug") or "")
    if not slug.startswith(BTC_FIVE_MINUTE_PREFIX):
        return None

    assets = message.get("assets_ids")
    outcomes = message.get("outcomes")
    if not isinstance(assets, list) or not isinstance(outcomes, list):
        return None
    if len(assets) != 2 or len(outcomes) != 2:
        return None

    try:
        start_ts = int(slug.removeprefix(BTC_FIVE_MINUTE_PREFIX))
        token_by_outcome = dict(zip(outcomes, assets, strict=True))
        condition_id = str(message.get("condition_id") or message["market"])
        event_ts_ms = int(message["timestamp"])
        market = Market(
            slug=slug,
            condition_id=condition_id,
            start_ts=start_ts,
            end_ts=start_ts + 300,
            up_token_id=str(token_by_outcome["Up"]),
            down_token_id=str(token_by_outcome["Down"]),
            min_size=Decimal("5"),
            tick_size=Decimal(
                str(message.get("order_price_min_tick_size") or "0.01")
            ),
        )
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return None
    return market, event_ts_ms


def _bid_size_at(message: dict, price: Decimal) -> Decimal:
    bids = message.get("bids")
    if not isinstance(bids, list):
        return Decimal("0")
    for level in bids:
        if not isinstance(level, dict):
            continue
        try:
            if Decimal(str(level["price"])) == price:
                return Decimal(str(level["size"]))
        except (InvalidOperation, KeyError, TypeError):
            continue
    return Decimal("0")


class MarketStreamWorker:
    """Emit BTC five-minute markets as soon as both CLOB books exist."""

    def __init__(self, *, queue_price: Decimal, logger: logging.Logger):
        self.queue_price = queue_price
        self.logger = logger
        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[MarketReadyUpdate | MarketStreamState] = (
            SimpleQueue()
        )
        self._attempted = False
        self._thread: Thread | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("market stream worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-market-stream",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def drain(self) -> list[MarketReadyUpdate | MarketStreamState]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def _run(self) -> None:
        try:
            asyncio.run(self._run_async())
        except Exception as exc:
            self._set_health(False, f"{type(exc).__name__}: {exc}")
            self.logger.exception("market stream worker stopped unexpectedly")

    async def _run_async(self) -> None:
        while not self._stop.is_set():
            try:
                await self._run_connection()
            except Exception as exc:
                self._set_health(False, f"{type(exc).__name__}: {exc}")
            if not self._stop.is_set():
                await self._wait_for_stop(1.0)

    async def _run_connection(self) -> None:
        pending_by_slug: dict[str, _PendingMarket] = {}
        slug_by_token: dict[str, str] = {}
        async with websockets.connect(
            MARKET_WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "type": "market",
                        "assets_ids": [],
                        "custom_feature_enabled": True,
                    }
                )
            )
            self._set_health(True)
            while not self._stop.is_set():
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=1.0)
                except TimeoutError:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode()
                if raw == "PING":
                    await socket.send("PONG")
                    continue
                if raw == "PONG":
                    continue
                payload = json.loads(raw)
                messages = payload if isinstance(payload, list) else [payload]
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    parsed = _parse_new_market(message)
                    if parsed is not None:
                        market, event_ts_ms = parsed
                        if market.slug in pending_by_slug:
                            continue
                        received_ts_ms = int(time.time() * 1000)
                        pending_by_slug[market.slug] = _PendingMarket(
                            market=market,
                            event_ts_ms=event_ts_ms,
                            received_ts_ms=received_ts_ms,
                        )
                        token_ids = [market.up_token_id, market.down_token_id]
                        slug_by_token.update(
                            {token_id: market.slug for token_id in token_ids}
                        )
                        await socket.send(
                            json.dumps(
                                {
                                    "operation": "subscribe",
                                    "assets_ids": token_ids,
                                    "custom_feature_enabled": True,
                                }
                            )
                        )
                        continue

                    if message.get("event_type") != "book":
                        continue
                    token_id = str(message.get("asset_id") or "")
                    slug = slug_by_token.get(token_id)
                    if slug is None:
                        continue
                    pending = pending_by_slug[slug]
                    pending.queue_by_token[token_id] = _bid_size_at(
                        message, self.queue_price
                    )
                    token_ids = (
                        pending.market.up_token_id,
                        pending.market.down_token_id,
                    )
                    if not all(token in pending.queue_by_token for token in token_ids):
                        continue

                    ready_ts_ms = int(time.time() * 1000)
                    self._updates.put(
                        MarketReadyUpdate(
                            market=pending.market,
                            event_ts_ms=pending.event_ts_ms,
                            received_ts_ms=pending.received_ts_ms,
                            ready_ts_ms=ready_ts_ms,
                            queue_ahead_up=pending.queue_by_token[
                                pending.market.up_token_id
                            ],
                            queue_ahead_down=pending.queue_by_token[
                                pending.market.down_token_id
                            ],
                        )
                    )
                    await socket.send(
                        json.dumps(
                            {
                                "operation": "unsubscribe",
                                "assets_ids": list(token_ids),
                            }
                        )
                    )
                    for token in token_ids:
                        slug_by_token.pop(token, None)
                    pending_by_slug.pop(slug, None)
        self._healthy.clear()

    async def _wait_for_stop(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self._stop.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.1)

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
                MarketStreamState(
                    healthy=healthy,
                    changed=True,
                    recovered=healthy and attempted_before and not was_healthy,
                    error=error,
                )
            )
