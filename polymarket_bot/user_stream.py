from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from queue import Empty, SimpleQueue
from threading import Event, Thread

from polymarket import PRODUCTION
from polymarket._internal.streams.clob.user import ClobUserStreamManager
from polymarket.models import ApiKeyCreds
from polymarket.models.clob.user_events import UserOrderEvent, UserTradeEvent

from .config import BotConfig


@dataclass(frozen=True)
class UserOrderUpdate:
    order_id: str
    status: str
    matched_size: Decimal
    raw: dict
    event_type: str | None = None
    exchange_event_ts_ms: int | None = None
    exchange_created_ts_ms: int | None = None
    received_ts_ms: int | None = None


@dataclass(frozen=True)
class UserStreamState:
    healthy: bool
    changed: bool
    recovered: bool
    error: str | None = None


@dataclass(frozen=True)
class UserTradeUpdate:
    trade_id: str
    status: str
    order_ids: tuple[str, ...]
    raw: dict


class UserStreamWorker:
    """Own the authenticated user socket and expose thread-safe updates."""

    def __init__(
        self,
        config: BotConfig,
        *,
        logger: logging.Logger,
        stabilization_seconds: float = 2.0,
    ):
        if not config.api_key or not config.api_secret or not config.api_passphrase:
            raise ValueError("CLOB API credentials are required for the user stream")
        self.credentials = ApiKeyCreds(
            key=config.api_key,
            secret=config.api_secret,
            passphrase=config.api_passphrase,
        )
        self.logger = logger
        self.stabilization_seconds = stabilization_seconds
        self._stop = Event()
        self._healthy = Event()
        self._updates: SimpleQueue[
            UserOrderUpdate | UserTradeUpdate | UserStreamState
        ] = SimpleQueue()
        self._attempted = False
        self._thread: Thread | None = None

    @property
    def healthy(self) -> bool:
        return self._healthy.is_set()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("user stream worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-user-stream",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def drain(self) -> list[UserOrderUpdate | UserTradeUpdate | UserStreamState]:
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
            self.logger.exception("user stream worker stopped unexpectedly")

    async def _run_async(self) -> None:
        async def resolve_credentials() -> ApiKeyCreds:
            return self.credentials

        manager = ClobUserStreamManager(
            url=PRODUCTION.clob_user_ws_url,
            resolve_credentials=resolve_credentials,
            logger=self.logger,
        )
        handle = None
        next_event: asyncio.Task | None = None
        connected_since: float | None = None
        try:
            handle = await manager.subscribe()
            while not self._stop.is_set():
                if manager.is_open:
                    if connected_since is None:
                        connected_since = time.monotonic()
                    if time.monotonic() - connected_since >= self.stabilization_seconds:
                        self._set_health(True)
                else:
                    connected_since = None
                    self._set_health(False, "user WebSocket disconnected")

                if next_event is None:
                    next_event = asyncio.create_task(handle.__anext__())
                done, _ = await asyncio.wait({next_event}, timeout=0.2)
                if not done:
                    continue
                try:
                    event = next_event.result()
                except StopAsyncIteration:
                    raise RuntimeError("user WebSocket subscription ended")
                finally:
                    next_event = None
                if isinstance(event, UserOrderEvent):
                    self._updates.put(self._normalize_order(event))
                elif isinstance(event, UserTradeEvent):
                    self._updates.put(self._normalize_trade(event))
        finally:
            if self._stop.is_set():
                self._healthy.clear()
            else:
                self._set_health(False, "user WebSocket stopped unexpectedly")
            if next_event is not None:
                next_event.cancel()
                await asyncio.gather(next_event, return_exceptions=True)
            if handle is not None:
                await handle.close()
            await manager.close()

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
                UserStreamState(
                    healthy=healthy,
                    changed=True,
                    recovered=healthy and attempted_before and not was_healthy,
                    error=error,
                )
            )

    @staticmethod
    def _normalize_order(event: UserOrderEvent) -> UserOrderUpdate:
        payload = event.payload
        matched_size = Decimal(payload.size_matched)
        original_size = Decimal(payload.original_size)
        if payload.order_event_type == "CANCELLATION" or payload.status == "CANCELED":
            status = "cancelled"
        elif matched_size >= original_size:
            status = "filled"
        elif payload.status in {"DELAYED", "UNMATCHED"}:
            status = payload.status.lower()
        else:
            status = "live"
        return UserOrderUpdate(
            order_id=payload.id,
            status=status,
            matched_size=matched_size,
            raw=event.model_dump(mode="json", by_alias=True),
            event_type=payload.order_event_type,
            exchange_event_ts_ms=(
                int(payload.timestamp.timestamp() * 1000)
                if payload.timestamp is not None
                else None
            ),
            exchange_created_ts_ms=(
                int(payload.created_at.timestamp() * 1000)
                if payload.created_at is not None
                else None
            ),
            received_ts_ms=time.time_ns() // 1_000_000,
        )

    @staticmethod
    def _normalize_trade(event: UserTradeEvent) -> UserTradeUpdate:
        payload = event.payload
        order_ids = [payload.taker_order_id]
        if payload.maker_orders:
            order_ids.extend(order.order_id for order in payload.maker_orders)
        return UserTradeUpdate(
            trade_id=payload.id,
            status=payload.status,
            order_ids=tuple(dict.fromkeys(order_ids)),
            raw=event.model_dump(mode="json", by_alias=True),
        )
