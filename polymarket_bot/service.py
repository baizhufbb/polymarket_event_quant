from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from threading import Event

from .book_signal import BookOpenSignal
from .config import BotConfig
from .database import BotDatabase
from .discovery import MarketDiscovery, is_eligible
from .exchange import (
    DEFAULT_PLACEMENT_INTERVAL_MS,
    AmbiguousPlacementError,
    Exchange,
    normalize_order,
)
from .geoblock import GeoblockWorker
from .heartbeat import HeartbeatWorker
from .market_activation import (
    MarketActivationState,
    MarketActivationUpdate,
    MarketActivationWorker,
)
from .models import Market, PlacedOrder, PlacementResult, TradePlan
from .reconciliation import ReconciliationWorker
from .user_stream import UserStreamState, UserStreamWorker, UserTradeUpdate


TERMINAL_ORDER_STATES = {
    "cancelled",
    "canceled",
    "closed",
    "filled",
    "matched",
    "expired",
    "rejected",
    "failed",
    "terminal_unknown",
}

BOOK_SIGNAL_TIMEOUT_SECONDS = 240.0


@dataclass
class _PlacementRetryState:
    attempts: int
    first_started_ts_ms: int
    last_finished_ts_ms: int


def _classify_cancel_result(result: object) -> tuple[list[str], list[str]]:
    if not isinstance(result, dict):
        return [], []

    canceled_ids = [str(order_id) for order_id in result.get("canceled", [])]
    not_canceled = result.get("not_canceled")
    if not isinstance(not_canceled, dict):
        return canceled_ids, []

    terminal_unknown_ids = [
        str(order_id)
        for order_id, reason in not_canceled.items()
        if "already canceled or matched" in str(reason).lower()
    ]
    return canceled_ids, terminal_unknown_ids


class BotService:
    placement_interval_ms = DEFAULT_PLACEMENT_INTERVAL_MS
    book_signal_factory = None

    def __init__(
        self,
        config: BotConfig,
        database: BotDatabase,
        plan: TradePlan,
        *,
        hours: Decimal | None,
        max_reserved_usd: Decimal | None,
        max_daily_filled_cost: Decimal | None,
        lookahead_minutes: int,
        placement_order: str,
        placement_interval_ms: Decimal,
        cancel_before_end_seconds: int,
        heartbeat_seconds: Decimal | None,
        live: bool,
        logger: logging.Logger,
    ):
        self.config = config
        self.database = database
        self.plan = plan
        self.hours = hours
        self.max_reserved_usd = max_reserved_usd
        self.max_daily_filled_cost = max_daily_filled_cost
        self.lookahead_minutes = lookahead_minutes
        self.placement_order = placement_order
        self.placement_interval_ms = placement_interval_ms
        self.cancel_before_end_seconds = cancel_before_end_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.live = live
        self.logger = logger
        self.wake_event = Event()
        self.discovery = MarketDiscovery()
        self.exchange = Exchange(config) if live else None
        self.book_signal_factory = BookOpenSignal if live else None
        self.heartbeat_worker = (
            HeartbeatWorker(self.exchange, float(heartbeat_seconds))
            if self.exchange and heartbeat_seconds is not None
            else None
        )
        self.geoblock_worker = (
            GeoblockWorker(
                self.exchange,
                interval_seconds=config.geoblock_seconds,
                retry_seconds=config.geoblock_retry_seconds,
            )
            if self.exchange
            else None
        )
        self.user_stream_worker = (
            UserStreamWorker(config, logger=logger) if live else None
        )
        self.market_activation_worker = (
            MarketActivationWorker(
                window_minutes=lookahead_minutes,
                farthest_first=True,
                wake_event=self.wake_event,
            )
            if live and placement_order == "farthest-first"
            else None
        )
        self.reconciliation_worker = (
            ReconciliationWorker(self.exchange) if self.exchange else None
        )
        self.activation_market_updates: list[MarketActivationUpdate] = []
        self._placement_retries: dict[str, _PlacementRetryState] = {}
        self.run_id = 0
        self.run_started_ts = int(time.time())
        self.last_discovery = 0.0
        self.last_reconcile = 0.0

    def run(self) -> None:
        mode = "live" if self.live else "dry-run"
        run_config = self.plan.as_dict() | {
            "hours": str(self.hours) if self.hours is not None else None,
            "max_reserved_usd": (
                str(self.max_reserved_usd)
                if self.max_reserved_usd is not None
                else None
            ),
            "max_daily_filled_cost": (
                str(self.max_daily_filled_cost)
                if self.max_daily_filled_cost is not None
                else None
            ),
            "lookahead_minutes": self.lookahead_minutes,
            "placement_order": self.placement_order,
            "placement_interval_ms": str(self.placement_interval_ms),
            "cancel_before_end_seconds": self.cancel_before_end_seconds,
            "heartbeat_seconds": (
                str(self.heartbeat_seconds)
                if self.heartbeat_seconds is not None
                else None
            ),
            "early_activation_probe": self.market_activation_worker is not None,
        }
        self.run_id = self.database.start_run(mode, run_config)
        deadline = (
            time.monotonic() + float(self.hours * Decimal("3600"))
            if self.hours is not None
            else None
        )
        self.database.event(self.run_id, "INFO", "run_started", details={"mode": mode})
        if self.heartbeat_worker:
            self.heartbeat_worker.start()
        if self.geoblock_worker:
            self.geoblock_worker.start()
        if self.user_stream_worker:
            self.user_stream_worker.start()
        if self.market_activation_worker:
            self.market_activation_worker.start()
        if self.reconciliation_worker:
            self.reconciliation_worker.start()
        cancel_on_shutdown = False
        try:
            while deadline is None or time.monotonic() < deadline:
                self.wake_event.clear()
                self._tick()
                self.wake_event.wait(0.2)
        except KeyboardInterrupt:
            cancel_on_shutdown = True
            self.logger.info("Ctrl+C received; stopping")
        except Exception as exc:
            self.database.stop_run(
                self.run_id, "failed", f"{type(exc).__name__}: {exc}"
            )
            self.database.event(
                self.run_id,
                "ERROR",
                "run_failed",
                details={"error": f"{type(exc).__name__}: {exc}"},
            )
            raise
        else:
            self.database.stop_run(self.run_id, "completed")
        finally:
            if self.market_activation_worker:
                self.market_activation_worker.stop()
                self._drain_market_activation_updates()
            if self.reconciliation_worker:
                self.reconciliation_worker.stop()
                self._drain_reconciliation_updates()
            if self.user_stream_worker:
                self.user_stream_worker.stop()
                self._drain_user_stream_updates()
            if self.heartbeat_worker:
                self.heartbeat_worker.stop()
            if self.geoblock_worker:
                self.geoblock_worker.stop()
            if cancel_on_shutdown:
                self._cancel_tracked_orders()
            row = self.database.connection.execute(
                "SELECT status FROM runs WHERE id=?", (self.run_id,)
            ).fetchone()
            if row and row["status"] == "running":
                self.database.stop_run(self.run_id, "stopped")

    def _tick(self) -> None:
        now = time.monotonic()
        self._drain_market_activation_updates()
        self._drain_reconciliation_updates()
        stream_recovered = self._drain_user_stream_updates()
        recovered = self._drain_heartbeat_updates()
        self._drain_geoblock_updates()
        self._cancel_due_orders(time.time())
        if (
            recovered
            or stream_recovered
            or now - self.last_reconcile >= self.config.order_poll_seconds
        ):
            if self.reconciliation_worker:
                if self.reconciliation_worker.submit(
                    self.database.tracked_open_orders()
                ):
                    self.last_reconcile = now
            else:
                self._reconcile()
                self.last_reconcile = now
        heartbeat_healthy = (
            self.heartbeat_worker is None or self.heartbeat_worker.healthy
        )
        geoblock_healthy = (
            self.geoblock_worker is None or self.geoblock_worker.healthy
        )
        healthy = not self.live or bool(
            geoblock_healthy
            and heartbeat_healthy
            and self.user_stream_worker
            and self.user_stream_worker.healthy
        )
        if healthy:
            self._place_activated_markets()
        gamma_needed = self.market_activation_worker is None
        if (
            healthy
            and gamma_needed
            and now - self.last_discovery >= self.config.discovery_seconds
        ):
            self._discover_and_place()
            self.last_discovery = now

    def _drain_market_activation_updates(self) -> None:
        if not self.market_activation_worker:
            return
        for update in self.market_activation_worker.drain():
            if isinstance(update, MarketActivationState):
                if update.healthy:
                    self.database.event(
                        self.run_id,
                        "INFO",
                        "market_activation_connected",
                        details={"recovered": update.recovered},
                    )
                    self.logger.info("market activation probe connected")
                else:
                    self.database.event(
                        self.run_id,
                        "ERROR",
                        "market_activation_disconnected",
                        details={"error": update.error},
                    )
                    self.logger.error(
                        "market activation probe unavailable: %s", update.error
                    )
                continue
            self.activation_market_updates.append(update)

    def _place_activated_markets(self) -> None:
        updates, self.activation_market_updates = (
            self.activation_market_updates,
            [],
        )
        now_ts = int(time.time())
        for update in updates:
            if not self._consider_market(
                update.market,
                now_ts=now_ts,
                trigger="market_parameters_activation",
                orderbook_ready=True,
                trigger_details=self._market_activation_details(update),
            ):
                return

    def _market_activation_details(self, update: MarketActivationUpdate) -> dict:
        return {
            "market_discovered_ts_ms": update.market_discovered_ts_ms,
            "market_parameters_detected_ts_ms": (
                update.market_parameters_detected_ts_ms
            ),
        }

    def _drain_reconciliation_updates(self) -> None:
        if not self.reconciliation_worker:
            return
        rearm_discovery = False
        order_changed = False
        for update in self.reconciliation_worker.drain():
            if update.batch_error:
                self.database.event(
                    self.run_id,
                    "WARNING",
                    "order_batch_reconcile_failed",
                    details={"error": update.batch_error},
                )
                continue
            for result in update.orders:
                row = self.database.order(result.snapshot.order_id)
                if row is None:
                    continue
                if result.error or result.raw is None:
                    self.database.event(
                        self.run_id,
                        "WARNING",
                        "order_reconcile_failed",
                        slug=result.snapshot.slug,
                        details={
                            "order_id": result.snapshot.order_id,
                            "error": result.error,
                        },
                    )
                    continue
                previous_status = str(row["status"])
                previous_matched = Decimal(row["matched_size"])
                status, matched = normalize_order(result.raw)
                matched = max(matched, previous_matched)
                if status == "matched" and matched >= Decimal(result.snapshot.size):
                    status = "filled"
                if (
                    previous_status in TERMINAL_ORDER_STATES
                    and status not in TERMINAL_ORDER_STATES
                ):
                    status = previous_status
                self.database.update_order(
                    result.snapshot.order_id,
                    status=status,
                    matched_size=matched,
                    raw=result.raw,
                )
                if row["role"] == "entry" and matched > previous_matched:
                    order_changed = True
                if (
                    previous_status not in TERMINAL_ORDER_STATES
                    and status in TERMINAL_ORDER_STATES
                    and matched == 0
                ):
                    rearm_discovery = True

        if rearm_discovery and self.market_activation_worker is None:
            self.last_discovery = 0.0
        if order_changed and self.exchange and not self.plan.buy_only:
            self._place_missing_exits(int(time.time()))

    def _drain_user_stream_updates(self) -> bool:
        if not self.user_stream_worker:
            return False
        recovered = False
        order_changed = False
        settlement_changed = False
        for update in self.user_stream_worker.drain():
            if isinstance(update, UserStreamState):
                if update.healthy:
                    if update.recovered:
                        recovered = True
                    self.database.event(self.run_id, "INFO", "user_stream_connected")
                    self.logger.info("user WebSocket connected")
                else:
                    self.database.event(
                        self.run_id,
                        "ERROR",
                        "user_stream_disconnected",
                        details={"error": update.error},
                    )
                    self.logger.error(
                        "user WebSocket unavailable; new placements paused: %s",
                        update.error,
                    )
                continue

            if isinstance(update, UserTradeUpdate):
                if update.status not in {"MINED", "CONFIRMED"}:
                    continue
                for order_id in update.order_ids:
                    row = self.database.order(order_id)
                    if row is not None and row["role"] == "entry":
                        settlement_changed = True
                        break
                continue

            row = self.database.order(update.order_id)
            if row is None:
                continue
            previous_matched = Decimal(row["matched_size"])
            self.database.update_order(
                update.order_id,
                status=update.status,
                matched_size=update.matched_size,
                raw=update.raw,
            )
            if update.event_type == "PLACEMENT":
                self.database.event(
                    self.run_id,
                    "INFO",
                    "order_placement_observed",
                    slug=row["slug"],
                    details={
                        "order_id": update.order_id,
                        "exchange_event_ts_ms": update.exchange_event_ts_ms,
                        "exchange_created_ts_ms": update.exchange_created_ts_ms,
                        "user_stream_received_ts_ms": update.received_ts_ms,
                    },
                )
            if row["role"] == "entry" and update.matched_size > previous_matched:
                order_changed = True

        if (
            (order_changed or settlement_changed)
            and self.exchange
            and not self.plan.buy_only
        ):
            self._place_missing_exits(int(time.time()))
        return recovered

    def _cancel_due_orders(self, now_ts: float) -> None:
        if self.cancel_before_end_seconds == 0:
            return
        due = self.database.due_open_orders(
            now_ts + self.cancel_before_end_seconds
        )
        order_ids = [row["order_id"] for row in due]
        if not order_ids:
            return
        if not self.exchange:
            self.database.mark_orders(order_ids, "simulated_closed")
            return
        self.database.mark_orders(order_ids, "cancel_requested")
        try:
            result = self.exchange.cancel_orders(order_ids)
        except Exception as exc:
            self.database.event(
                self.run_id,
                "ERROR",
                "deadline_cancel_failed",
                details={"order_ids": order_ids, "error": str(exc)},
            )
            return
        canceled_ids, terminal_unknown_ids = _classify_cancel_result(result)
        self.database.mark_orders(canceled_ids, "cancelled")
        self.database.mark_orders(terminal_unknown_ids, "terminal_unknown")
        self.database.event(
            self.run_id,
            "INFO",
            "deadline_cancel",
            details={
                "requested_order_ids": order_ids,
                "canceled_order_ids": canceled_ids,
                "terminal_unknown_order_ids": terminal_unknown_ids,
                "result": result,
            },
        )

    def _drain_heartbeat_updates(self) -> bool:
        if not self.heartbeat_worker:
            return False
        recovered = False
        for update in self.heartbeat_worker.drain():
            if update.success:
                self.database.heartbeat(self.run_id)
                if update.recovered:
                    recovered = True
                    self.database.event(self.run_id, "INFO", "heartbeat_recovered")
                    self.logger.info("heartbeat recovered; reconciling open orders")
            else:
                self.database.run_error(self.run_id, update.error or "heartbeat failed")
                if update.changed:
                    self.database.event(
                        self.run_id,
                        "ERROR",
                        "heartbeat_failed",
                        details={"error": update.error},
                    )
                    self.logger.error(
                        "heartbeat failed; new placements paused: %s", update.error
                    )
        return recovered

    def _drain_geoblock_updates(self) -> None:
        if not self.geoblock_worker:
            return
        for update in self.geoblock_worker.drain():
            if update.blocked:
                result = update.result or {}
                self.database.event(
                    self.run_id, "ERROR", "geoblocked", details=result
                )
                raise RuntimeError(
                    f"trading blocked in {result.get('country')} "
                    f"{result.get('region')}"
                )
            if update.available:
                if update.recovered:
                    self.database.event(
                        self.run_id, "INFO", "geoblock_check_recovered"
                    )
                    self.logger.info(
                        "geoblock check recovered; new placements resumed"
                    )
                continue
            self.database.event(
                self.run_id,
                "ERROR",
                "geoblock_check_failed",
                details={"error": update.error},
            )
            self.logger.error(
                "geoblock check unavailable; new placements paused: %s",
                update.error,
            )

    def _discover_and_place(self) -> None:
        try:
            markets = self.discovery.discover(
                self.lookahead_minutes,
                farthest_first=self.placement_order == "farthest-first",
            )
        except Exception as exc:
            self.database.event(
                self.run_id,
                "ERROR",
                "discovery_failed",
                details={"error": f"{type(exc).__name__}: {exc}"},
            )
            return
        now_ts = int(time.time())
        for market in markets:
            if not self._consider_market(
                market,
                now_ts=now_ts,
                trigger="gamma",
                orderbook_ready=False,
            ):
                return

    def _consider_market(
        self,
        market: Market,
        *,
        now_ts: int,
        trigger: str,
        orderbook_ready: bool,
        trigger_details: dict | None = None,
    ) -> bool:
        if not is_eligible(
            market, run_started_ts=self.run_started_ts, now_ts=now_ts
        ):
            retry = self._placement_retries.pop(market.slug, None)
            if retry is not None:
                self.database.set_market_state(
                    market.slug,
                    "expired",
                    "market ended before a complete order pair was accepted",
                )
                self.database.event(
                    self.run_id,
                    "WARNING",
                    "placement_retry_expired",
                    slug=market.slug,
                    details={"placement_retry_attempts": retry.attempts},
                )
            return True
        if not self.database.can_start_entry_plan(market.slug):
            return True
        if self.plan.buy_price % market.tick_size:
            self._skip_market(market, "buy price does not match market tick size")
            return True
        if any(target.price % market.tick_size for target in self.plan.exit_targets):
            self._skip_market(
                market, "take-profit price does not match market tick size"
            )
            return True
        if market.min_size > self.plan.order_size:
            self._skip_market(market, "configured order is below market minimum")
            return True
        if (
            self.max_daily_filled_cost is not None
            and self.database.daily_filled_cost() >= self.max_daily_filled_cost
        ):
            return False
        if (
            self.max_reserved_usd is not None
            and self.database.active_reserved_usd() + self.plan.market_reserve
            > self.max_reserved_usd
        ):
            return False
        if self.exchange and not orderbook_ready:
            try:
                if not self.exchange.order_books_ready(market):
                    return True
            except Exception as exc:
                self.database.event(
                    self.run_id,
                    "ERROR",
                    "orderbook_check_failed",
                    slug=market.slug,
                    details={"error": f"{type(exc).__name__}: {exc}"},
                )
                return True
        self._place(market, trigger=trigger, trigger_details=trigger_details)
        return True

    def _skip_market(self, market: Market, reason: str) -> None:
        self.database.prepare_market(self.run_id, market, state="skipped")
        self.database.set_market_state(market.slug, "skipped", reason)
        self.database.event(
            self.run_id,
            "WARNING",
            "market_skipped",
            slug=market.slug,
            details={"reason": reason},
        )

    def _place(
        self,
        market: Market,
        *,
        trigger: str,
        trigger_details: dict | None = None,
    ) -> None:
        placement_retry = self._placement_retries.get(market.slug)
        if placement_retry is None:
            self.database.prepare_market(self.run_id, market)
        if not self.live:
            for outcome, token_id in (
                ("up", market.up_token_id),
                ("down", market.down_token_id),
            ):
                self.database.add_order(
                    self.run_id,
                    market.slug,
                    PlacedOrder(
                        order_id=f"dry:{market.slug}:{outcome}",
                        outcome=outcome,
                        token_id=token_id,
                        price=self.plan.buy_price,
                        size=self.plan.order_size,
                        status="simulated",
                        raw={"dry_run": True},
                        side="buy",
                        role="entry",
                    ),
                )
            self.database.set_market_state(market.slug, "active")
            self.logger.info("DRY-RUN %s: Up/Down bids prepared", market.slug)
            return

        book_signal = None
        if self.book_signal_factory is not None:
            book_signal = self._await_book_open(market)
            if book_signal is None:
                return

        placement_started_ts_ms = int(time.time() * 1000)
        submission_error = None
        reconciliation_error = None
        try:
            result = self.exchange.place_dual(
                market,
                price=self.plan.buy_price,
                size=self.plan.order_size,
                submission_interval_ms=self.placement_interval_ms,
            )
        except AmbiguousPlacementError as exc:
            submission_error = f"{type(exc).__name__}: {exc}"
            try:
                result = self.exchange.reconcile_ambiguous_dual(
                    market,
                    price=self.plan.buy_price,
                    size=self.plan.order_size,
                    retryable_if_missing=exc.retryable,
                )
            except Exception as reconcile_exc:
                reconciliation_error = (
                    f"submission={submission_error}; reconciliation="
                    f"{type(reconcile_exc).__name__}: {reconcile_exc}"
                )
                result = PlacementResult(
                    (),
                    reconciliation_error,
                    retryable=exc.retryable,
                    attempts=exc.attempts,
                )
            else:
                result = PlacementResult(
                    result.orders,
                    result.error,
                    retryable=result.retryable,
                    attempts=exc.attempts,
                )
        except Exception as exc:
            submission_error = f"{type(exc).__name__}: {exc}"
            result = PlacementResult((), submission_error)

        placement_finished_ts_ms = int(time.time() * 1000)
        placement_details = {
            "trigger": trigger,
            "placement_started_ts_ms": placement_started_ts_ms,
            "placement_finished_ts_ms": placement_finished_ts_ms,
            "submission_attempts": result.attempts,
            **(book_signal or {}),
            **(trigger_details or {}),
        }
        if submission_error:
            placement_details["submission_error"] = submission_error
        if reconciliation_error:
            placement_details["reconciliation_error"] = reconciliation_error
        if placement_retry is not None:
            placement_details.update(
                {
                    "placement_retry_attempts": placement_retry.attempts,
                    "placement_retry_started_ts_ms": (
                        placement_retry.first_started_ts_ms
                    ),
                    "placement_retry_last_failed_ts_ms": (
                        placement_retry.last_finished_ts_ms
                    ),
                    "local_gap_before_acceptance_ms": (
                        placement_started_ts_ms
                        - placement_retry.last_finished_ts_ms
                    ),
                }
            )
        for order in result.orders:
            self.database.add_order(self.run_id, market.slug, order)
        if result.complete:
            self._placement_retries.pop(market.slug, None)
            self.database.set_market_state(market.slug, "active")
            self.database.event(
                self.run_id,
                "INFO",
                "dual_orders_placed",
                slug=market.slug,
                details={
                    "order_ids": [order.order_id for order in result.orders],
                    "placement_ms": (
                        placement_finished_ts_ms - placement_started_ts_ms
                    ),
                    **placement_details,
                },
            )
            self.logger.info("LIVE %s: both orders accepted", market.slug)
        else:
            if self._requeue_placement(
                market,
                result=result,
                retry=placement_retry,
                placement_started_ts_ms=placement_started_ts_ms,
                placement_finished_ts_ms=placement_finished_ts_ms,
                placement_details=placement_details,
                trigger_details=trigger_details,
            ):
                return
            self._placement_retries.pop(market.slug, None)
            self.database.set_market_state(market.slug, "error", result.error)
            self.database.event(
                self.run_id,
                "ERROR",
                "placement_failed",
                slug=market.slug,
                details={
                    "error": result.error,
                    **placement_details,
                },
            )

    def _await_book_open(self, market: Market) -> dict | None:
        """Hold submissions until the market's first public book event.

        Probing the order endpoint while the book is closed burns the
        rate-limit budget exactly when it is needed; the burst must start
        with a clean budget the moment the book opens.
        """
        wait_started_ts_ms = int(time.time() * 1000)
        watcher = self.book_signal_factory(
            market.up_token_id, market.down_token_id
        )
        opened = False
        try:
            timeout = min(
                BOOK_SIGNAL_TIMEOUT_SECONDS,
                max(0.0, market.end_ts - time.time()),
            )
            opened = watcher.wait(timeout)
        finally:
            # Closing joins the watcher threads and the websocket close
            # handshake can hang for seconds (measured: three markets entered
            # five seconds late because of it). After a signal the threads
            # exit on their own, so the burst must start without closing.
            if not opened:
                watcher.close()
        if not opened:
            self._placement_retries.pop(market.slug, None)
            reason = watcher.error or "book-open signal timed out"
            self.database.set_market_state(market.slug, "error", reason)
            self.database.event(
                self.run_id,
                "WARNING",
                "book_signal_timeout",
                slug=market.slug,
                details={
                    "wait_started_ts_ms": wait_started_ts_ms,
                    "waited_ms": int(time.time() * 1000) - wait_started_ts_ms,
                    "error": watcher.error,
                },
            )
            self.logger.warning(
                "LIVE %s: book-open signal not received; market skipped",
                market.slug,
            )
            return None
        signal_ts_ms = watcher.signal_ts_ms or int(time.time() * 1000)
        return {
            "book_signal_ts_ms": signal_ts_ms,
            "book_signal_wait_ms": signal_ts_ms - wait_started_ts_ms,
            "book_signal_source": getattr(watcher, "signal_source", None),
        }

    def _requeue_placement(
        self,
        market: Market,
        *,
        result: PlacementResult,
        retry: _PlacementRetryState | None,
        placement_started_ts_ms: int,
        placement_finished_ts_ms: int,
        placement_details: dict,
        trigger_details: dict | None,
    ) -> bool:
        if not result.retryable or result.orders:
            return False

        if retry is None:
            retry = _PlacementRetryState(
                attempts=result.attempts,
                first_started_ts_ms=placement_started_ts_ms,
                last_finished_ts_ms=placement_finished_ts_ms,
            )
            self._placement_retries[market.slug] = retry
            self.database.set_market_state(
                market.slug, "placement_pending", result.error
            )
            self.database.event(
                self.run_id,
                "INFO",
                "placement_retry_started",
                slug=market.slug,
                details={"error": result.error, **placement_details},
            )
            self.logger.info("LIVE %s: placement pending; retrying", market.slug)
        else:
            retry.attempts += result.attempts
            retry.last_finished_ts_ms = placement_finished_ts_ms

        source_details = trigger_details or {}
        self.activation_market_updates.append(
            MarketActivationUpdate(
                market=market,
                market_discovered_ts_ms=int(
                    source_details.get(
                        "market_discovered_ts_ms", placement_started_ts_ms
                    )
                ),
                market_parameters_detected_ts_ms=int(
                    source_details.get(
                        "market_parameters_detected_ts_ms",
                        placement_started_ts_ms,
                    )
                ),
            )
        )
        self.wake_event.set()
        return True

    def _reconcile(self) -> None:
        now_ts = int(time.time())
        tracked = self.database.tracked_open_orders()
        live_rows = []
        for row in tracked:
            if row["status"] == "simulated":
                if now_ts >= row["end_ts"]:
                    self.database.update_order(
                        row["order_id"],
                        status="expired",
                        matched_size=Decimal("0"),
                        raw={"dry_run": True},
                    )
                continue
            live_rows.append(row)

        if live_rows and self.exchange:
            try:
                open_by_id = {}
                for raw in self.exchange.open_orders():
                    order_id = raw.get("id") or raw.get("orderID") or raw.get("orderId")
                    if order_id:
                        open_by_id[str(order_id)] = raw
            except Exception as exc:
                self.database.event(
                    self.run_id,
                    "WARNING",
                    "order_batch_reconcile_failed",
                    details={"error": f"{type(exc).__name__}: {exc}"},
                )
                return

            for row in live_rows:
                try:
                    raw = open_by_id.get(row["order_id"])
                    if raw is None:
                        raw = self.exchange.get_order(row["order_id"])
                    if raw is None:
                        raw = {
                            "id": row["order_id"],
                            "status": "terminal_unknown",
                            "size_matched": "0",
                        }
                    if not isinstance(raw, dict):
                        raise ValueError("exchange returned no order payload")
                    status, matched = normalize_order(raw)
                    if status == "matched" and matched >= Decimal(row["size"]):
                        status = "filled"
                    self.database.update_order(
                        row["order_id"], status=status, matched_size=matched, raw=raw
                    )
                except Exception as exc:
                    self.database.event(
                        self.run_id,
                        "WARNING",
                        "order_reconcile_failed",
                        slug=row["slug"],
                        details={"order_id": row["order_id"], "error": str(exc)},
                    )
                    continue

        if self.exchange and not self.plan.buy_only:
            self._place_missing_exits(now_ts)

    def _place_missing_exits(self, now_ts: int) -> None:
        if self.plan.buy_only:
            return
        for row in self.database.entry_orders_with_fills():
            if now_ts >= row["end_ts"] - self.cancel_before_end_seconds:
                continue
            matched = Decimal(row["matched_size"])
            min_size = Decimal(row["min_size"])
            pending_targets = []
            for target in self.plan.exit_targets:
                desired_size = matched * target.fraction
                handled = self.database.exit_handled_size(
                    row["slug"], row["outcome"], target.price
                )
                pending_size = desired_size - handled
                if pending_size >= min_size:
                    pending_targets.append((target, pending_size))
            if not pending_targets:
                continue

            balance = self.exchange.conditional_balance(row["token_id"])
            reserved = self.database.active_exit_reserved_size(
                row["slug"], row["outcome"]
            )
            available_size = max(Decimal("0"), balance - reserved)
            if available_size < min_size:
                continue

            market = Market(
                slug=row["slug"],
                condition_id=row["condition_id"],
                start_ts=0,
                end_ts=row["end_ts"],
                up_token_id=row["token_id"] if row["outcome"] == "up" else "",
                down_token_id=row["token_id"] if row["outcome"] == "down" else "",
                min_size=Decimal(row["min_size"]),
                tick_size=Decimal(row["tick_size"]),
            )
            for target, pending_size in pending_targets:
                size = min(pending_size, available_size)
                if size < min_size:
                    continue
                placed = self._place_exit_rung(
                    row=row,
                    market=market,
                    price=target.price,
                    fraction=target.fraction,
                    size=size,
                )
                if placed:
                    available_size -= size
                if available_size < min_size:
                    break

    def _place_exit_rung(
        self,
        *,
        row,
        market: Market,
        price: Decimal,
        fraction: Decimal,
        size: Decimal,
    ) -> bool:
        try:
            order = self.exchange.place_exit(
                market,
                outcome=row["outcome"],
                token_id=row["token_id"],
                price=price,
                size=size,
            )
        except Exception as exc:
            initial_error = f"{type(exc).__name__}: {exc}"
            try:
                order = self.exchange.reconcile_ambiguous_exit(
                    market,
                    outcome=row["outcome"],
                    token_id=row["token_id"],
                    price=price,
                    size=size,
                )
            except Exception as reconcile_exc:
                order = None
                initial_error = (
                    f"submission={initial_error}; reconciliation="
                    f"{type(reconcile_exc).__name__}: {reconcile_exc}"
                )
            if order is None:
                failed = PlacedOrder(
                    order_id=f"failed:{row['slug']}:{row['outcome']}:{time.time_ns()}",
                    outcome=row["outcome"],
                    token_id=row["token_id"],
                    price=price,
                    size=size,
                    status="failed",
                    raw={"error": initial_error},
                    side="sell",
                    role="exit",
                )
                self.database.add_order(self.run_id, row["slug"], failed)
                self.database.set_market_state(
                    row["slug"], "exit_error", initial_error
                )
                self.database.event(
                    self.run_id,
                    "ERROR",
                    "exit_order_failed",
                    slug=row["slug"],
                    details={
                        "outcome": row["outcome"],
                        "price": str(price),
                        "fraction": str(fraction),
                        "size": str(size),
                        "error": initial_error,
                    },
                )
                self.logger.error(
                    "LIVE %s: %s exit at %s failed for %s shares: %s",
                    row["slug"],
                    row["outcome"],
                    price,
                    size,
                    initial_error,
                )
                return False

        self.database.add_order(self.run_id, row["slug"], order)
        self.database.event(
            self.run_id,
            "INFO",
            "exit_order_placed",
            slug=row["slug"],
            details={
                "order_id": order.order_id,
                "outcome": row["outcome"],
                "price": str(price),
                "fraction": str(fraction),
                "size": str(size),
            },
        )
        self.logger.info(
            "LIVE %s: %s exit placed at %s for %s shares",
            row["slug"],
            row["outcome"],
            price,
            size,
        )
        return True

    def _cancel_tracked_orders(self) -> None:
        tracked = self.database.tracked_open_orders()
        order_ids = [row["order_id"] for row in tracked]
        if not order_ids:
            return
        if not self.exchange:
            self.database.mark_orders(order_ids, "simulated_closed")
            return
        self.database.mark_orders(order_ids, "cancel_requested")
        try:
            result = self.exchange.cancel_orders(order_ids)
            canceled_ids, terminal_unknown_ids = _classify_cancel_result(result)
            self.database.mark_orders(canceled_ids, "cancelled")
            self.database.mark_orders(terminal_unknown_ids, "terminal_unknown")
            self.database.event(
                self.run_id or None,
                "INFO",
                "shutdown_cancel",
                details={
                    "requested_order_ids": order_ids,
                    "canceled_order_ids": canceled_ids,
                    "terminal_unknown_order_ids": terminal_unknown_ids,
                    "result": result,
                },
            )
        except Exception as exc:
            self.logger.error("shutdown cancellation failed: %s", exc)
