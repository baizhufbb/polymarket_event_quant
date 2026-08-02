import json
import logging
from decimal import Decimal
from threading import Event
from types import SimpleNamespace

import pytest

from polymarket_bot.database import BotDatabase
from polymarket_bot.exchange import AmbiguousPlacementError
from polymarket_bot.geoblock import GeoblockUpdate
from polymarket_bot.market_activation import MarketActivationUpdate
from polymarket_bot.models import (
    ExitTarget,
    Market,
    PlacedOrder,
    PlacementResult,
    TradePlan,
)
from polymarket_bot.reconciliation import (
    ReconciledOrder,
    ReconciliationUpdate,
    TrackedOrderSnapshot,
)
from polymarket_bot.service import BotService


MARKET = Market(
    slug="btc-updown-5m-2000000000",
    condition_id="0xcondition",
    start_ts=2_000_000_000,
    end_ts=2_000_000_300,
    up_token_id="up-token",
    down_token_id="down-token",
    min_size=Decimal("5"),
    tick_size=Decimal("0.01"),
)


class FakeExchange:
    def __init__(self, cancel_result=None):
        self.open_calls = 0
        self.order_calls = []
        self.cancel_calls = []
        self.cancel_result = cancel_result

    def open_orders(self):
        self.open_calls += 1
        return [
            {
                "id": "up-order",
                "status": "ORDER_STATUS_LIVE",
                "size_matched": "4",
            }
        ]

    def get_order(self, order_id):
        self.order_calls.append(order_id)
        return {
            "id": order_id,
            "status": "ORDER_STATUS_CANCELLED",
            "size_matched": "0",
        }

    def cancel_orders(self, order_ids):
        self.cancel_calls.append(order_ids)
        return self.cancel_result or {"canceled": order_ids, "not_canceled": {}}


class FailingCancelExchange(FakeExchange):
    def cancel_orders(self, order_ids):
        self.cancel_calls.append(order_ids)
        raise RuntimeError("temporary exchange error")


class PendingBookExchange(FakeExchange):
    def __init__(self):
        super().__init__()
        self.readiness_calls = []

    def order_books_ready(self, market):
        self.readiness_calls.append(market.slug)
        return False


class RetryPlacementExchange(FakeExchange):
    def __init__(self, failures=1):
        super().__init__()
        self.place_calls = 0
        self.failures = failures

    def place_dual(self, market, *, price, size, submission_interval_ms):
        self.place_calls += 1
        if self.place_calls <= self.failures:
            return PlacementResult(
                (),
                "the market is not yet ready to process new orders",
                retryable=True,
            )
        return PlacementResult(
            (
                PlacedOrder(
                    order_id="up-order",
                    outcome="up",
                    token_id=market.up_token_id,
                    price=price,
                    size=size,
                    status="live",
                    raw={},
                ),
                PlacedOrder(
                    order_id="down-order",
                    outcome="down",
                    token_id=market.down_token_id,
                    price=price,
                    size=size,
                    status="live",
                    raw={},
                ),
            )
        )


class AmbiguousRetryPlacementExchange(RetryPlacementExchange):
    def place_dual(self, market, *, price, size, submission_interval_ms):
        if self.place_calls == 0:
            self.place_calls = 1
            raise AmbiguousPlacementError("network interrupted")
        return super().place_dual(
            market,
            price=price,
            size=size,
            submission_interval_ms=submission_interval_ms,
        )

    def reconcile_ambiguous_dual(
        self,
        market,
        *,
        price,
        size,
        retryable_if_missing=True,
    ):
        return PlacementResult(
            (),
            "ambiguous submission did not produce exactly two orders",
            retryable=retryable_if_missing,
        )


class ReconciliationOutagePlacementExchange(AmbiguousRetryPlacementExchange):
    def reconcile_ambiguous_dual(
        self,
        market,
        *,
        price,
        size,
        retryable_if_missing=True,
    ):
        raise RuntimeError("reconciliation temporarily unavailable")


class FakeReconciliationWorker:
    def __init__(self, updates):
        self.updates = updates

    def drain(self):
        updates, self.updates = self.updates, []
        return updates


class ExitExchange(FakeExchange):
    def __init__(self):
        super().__init__()
        self.exit_calls = []

    def conditional_balance(self, token_id):
        return Decimal("100")

    def open_orders(self):
        self.open_calls += 1
        return [
            {
                "id": "up-order",
                "status": "ORDER_STATUS_LIVE",
                "size_matched": "6",
            }
        ]

    def place_exit(self, market, *, outcome, token_id, price, size):
        self.exit_calls.append((market.slug, outcome, token_id, price, size))
        return PlacedOrder(
            order_id="up-exit",
            outcome=outcome,
            token_id=token_id,
            price=price,
            size=size,
            status="live",
            raw={},
            side="sell",
            role="exit",
        )


class LadderExitExchange(ExitExchange):
    def __init__(self, balance: Decimal):
        super().__init__()
        self.balance = balance

    def conditional_balance(self, token_id):
        return self.balance

    def place_exit(self, market, *, outcome, token_id, price, size):
        self.exit_calls.append((market.slug, outcome, token_id, price, size))
        return PlacedOrder(
            order_id=f"up-exit-{len(self.exit_calls)}",
            outcome=outcome,
            token_id=token_id,
            price=price,
            size=size,
            status="live",
            raw={},
            side="sell",
            role="exit",
        )


def _service_for_run(database, tick, *, hours=None):
    service = BotService.__new__(BotService)
    service.config = SimpleNamespace()
    service.database = database
    service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
    service.hours = hours
    service.max_reserved_usd = None
    service.max_daily_filled_cost = None
    service.lookahead_minutes = 40
    service.placement_order = "farthest-first"
    service.cancel_before_end_seconds = 2
    service.heartbeat_seconds = None
    service.live = True
    service.logger = logging.getLogger("test")
    service.wake_event = Event()
    service.run_id = 0
    service.heartbeat_worker = None
    service.geoblock_worker = None
    service.user_stream_worker = None
    service.market_activation_worker = None
    service.reconciliation_worker = None
    service._tick = tick
    return service


def test_market_parameter_activation_is_the_primary_placement_trigger() -> None:
    service = BotService.__new__(BotService)
    service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
    service.activation_market_updates = [
        MarketActivationUpdate(
            market=MARKET,
            market_discovered_ts_ms=1_999_999_000_000,
            market_parameters_detected_ts_ms=1_999_999_000_125,
        )
    ]
    calls = []

    def consider(market, **kwargs):
        calls.append((market, kwargs))
        return True

    service._consider_market = consider
    service._place_activated_markets()

    assert service.activation_market_updates == []
    assert calls[0][0] == MARKET
    assert calls[0][1]["trigger"] == "market_parameters_activation"
    assert calls[0][1]["orderbook_ready"] is True
    assert calls[0][1]["trigger_details"] == {
        "market_discovered_ts_ms": 1_999_999_000_000,
        "market_parameters_detected_ts_ms": 1_999_999_000_125,
    }


def test_order_engine_not_ready_requeues_until_pair_is_accepted(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = RetryPlacementExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
        service.max_reserved_usd = None
        service.max_daily_filled_cost = None
        service.exchange = exchange
        service.market_activation_worker = SimpleNamespace()
        service.activation_market_updates = []
        service._placement_retries = {}
        service.wake_event = Event()
        service.run_id = run_id
        service.run_started_ts = 0
        service.live = True
        service.logger = logging.getLogger("test")
        details = {
            "market_discovered_ts_ms": 1_999_999_000_000,
            "market_parameters_detected_ts_ms": 1_999_999_000_125,
        }

        service._consider_market(
            MARKET,
            now_ts=1_999_999_000,
            trigger="market_parameters_activation",
            orderbook_ready=True,
            trigger_details=details,
        )

        row = database.connection.execute(
            "SELECT state FROM markets WHERE slug=?", (MARKET.slug,)
        ).fetchone()
        assert row["state"] == "placement_pending"
        assert database.can_start_entry_plan(MARKET.slug)
        assert service.wake_event.is_set()
        assert service.activation_market_updates == [
            MarketActivationUpdate(
                market=MARKET,
                market_discovered_ts_ms=1_999_999_000_000,
                market_parameters_detected_ts_ms=1_999_999_000_125,
            )
        ]

        service._place_activated_markets()

        row = database.connection.execute(
            "SELECT state FROM markets WHERE slug=?", (MARKET.slug,)
        ).fetchone()
        assert row["state"] == "active"
        assert exchange.place_calls == 2
        assert not database.can_start_entry_plan(MARKET.slug)
        assert {
            row["order_id"]
            for row in database.connection.execute(
                "SELECT order_id FROM orders WHERE slug=?", (MARKET.slug,)
            )
        } == {"up-order", "down-order"}


def test_ambiguous_submission_requeues_and_records_original_error(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = AmbiguousRetryPlacementExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
        service.max_reserved_usd = None
        service.max_daily_filled_cost = None
        service.exchange = exchange
        service.market_activation_worker = SimpleNamespace()
        service.activation_market_updates = []
        service._placement_retries = {}
        service.wake_event = Event()
        service.run_id = run_id
        service.run_started_ts = 0
        service.live = True
        service.logger = logging.getLogger("test")

        service._consider_market(
            MARKET,
            now_ts=1_999_999_000,
            trigger="market_parameters_activation",
            orderbook_ready=True,
            trigger_details={
                "market_discovered_ts_ms": 1_999_999_000_000,
                "market_parameters_detected_ts_ms": 1_999_999_000_125,
            },
        )

        event = database.connection.execute(
            """
            SELECT details_json
            FROM events
            WHERE slug=? AND event_type='placement_retry_started'
            """,
            (MARKET.slug,),
        ).fetchone()
        assert service.wake_event.is_set()
        assert len(service.activation_market_updates) == 1
        assert "AmbiguousPlacementError: network interrupted" in event["details_json"]

        service._place_activated_markets()

        state = database.connection.execute(
            "SELECT state FROM markets WHERE slug=?", (MARKET.slug,)
        ).fetchone()["state"]
        assert state == "active"
        assert exchange.place_calls == 2


def test_reconciliation_outage_requeues_missing_market(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = ReconciliationOutagePlacementExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
        service.max_reserved_usd = None
        service.max_daily_filled_cost = None
        service.exchange = exchange
        service.market_activation_worker = None
        service.activation_market_updates = []
        service._placement_retries = {}
        service.wake_event = Event()
        service.run_id = run_id
        service.run_started_ts = 0
        service.live = True
        service.logger = logging.getLogger("test")

        service._consider_market(
            MARKET,
            now_ts=1_999_999_000,
            trigger="gamma",
            orderbook_ready=True,
        )

        market_row = database.connection.execute(
            "SELECT state FROM markets WHERE slug=?", (MARKET.slug,)
        ).fetchone()
        retry_event = database.connection.execute(
            """
            SELECT details_json
            FROM events
            WHERE slug=? AND event_type='placement_retry_started'
            """,
            (MARKET.slug,),
        ).fetchone()
        assert market_row["state"] == "placement_pending"
        assert database.can_start_entry_plan(MARKET.slug)
        assert len(service.activation_market_updates) == 1
        assert "reconciliation temporarily unavailable" in retry_event["details_json"]

        service._place_activated_markets()

        state = database.connection.execute(
            "SELECT state FROM markets WHERE slug=?", (MARKET.slug,)
        ).fetchone()["state"]
        assert state == "active"
        assert exchange.place_calls == 2
        assert service._placement_retries == {}


def test_pending_placement_expires_when_market_ends(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = RetryPlacementExchange(failures=2)
        service = BotService.__new__(BotService)
        service.database = database
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
        service.max_reserved_usd = None
        service.max_daily_filled_cost = None
        service.exchange = exchange
        service.market_activation_worker = None
        service.activation_market_updates = []
        service._placement_retries = {}
        service.wake_event = Event()
        service.run_id = run_id
        service.run_started_ts = 0
        service.live = True
        service.logger = logging.getLogger("test")

        service._consider_market(
            MARKET,
            now_ts=MARKET.start_ts,
            trigger="gamma",
            orderbook_ready=True,
        )
        service._consider_market(
            MARKET,
            now_ts=MARKET.end_ts,
            trigger="placement_retry",
            orderbook_ready=True,
        )

        market_row = database.connection.execute(
            "SELECT state, error FROM markets WHERE slug=?", (MARKET.slug,)
        ).fetchone()
        event = database.connection.execute(
            """
            SELECT details_json
            FROM events
            WHERE slug=? AND event_type='placement_retry_expired'
            """,
            (MARKET.slug,),
        ).fetchone()
        assert market_row["state"] == "expired"
        assert "market ended" in market_row["error"]
        assert json.loads(event["details_json"])["placement_retry_attempts"] == 1
        assert service._placement_retries == {}


def test_placement_retry_records_only_first_failure_and_final_summary(
    tmp_path,
) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = RetryPlacementExchange(failures=3)
        service = BotService.__new__(BotService)
        service.database = database
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
        service.max_reserved_usd = None
        service.max_daily_filled_cost = None
        service.exchange = exchange
        service.market_activation_worker = SimpleNamespace()
        service.activation_market_updates = []
        service._placement_retries = {}
        service.wake_event = Event()
        service.run_id = run_id
        service.run_started_ts = 0
        service.live = True
        service.logger = logging.getLogger("test")
        details = {
            "market_discovered_ts_ms": 1_999_999_000_000,
            "market_parameters_detected_ts_ms": 1_999_999_000_125,
        }

        service._consider_market(
            MARKET,
            now_ts=1_999_999_000,
            trigger="market_parameters_activation",
            orderbook_ready=True,
            trigger_details=details,
        )
        service._place_activated_markets()
        service._place_activated_markets()
        service._place_activated_markets()

        waiting_events = database.connection.execute(
            """
            SELECT COUNT(*)
            FROM events
            WHERE slug=? AND event_type='placement_retry_started'
            """,
            (MARKET.slug,),
        ).fetchone()[0]
        placed_details = json.loads(
            database.connection.execute(
                """
                SELECT details_json
                FROM events
                WHERE slug=? AND event_type='dual_orders_placed'
                """,
                (MARKET.slug,),
            ).fetchone()[0]
        )

        assert exchange.place_calls == 4
        assert waiting_events == 1
        assert placed_details["placement_retry_attempts"] == 3
        assert placed_details["local_gap_before_acceptance_ms"] >= 0


def test_activation_worker_owns_continuous_gamma_discovery() -> None:
    service = BotService.__new__(BotService)
    service.config = SimpleNamespace(
        order_poll_seconds=60,
        discovery_seconds=0,
    )
    service.live = False
    service.market_activation_worker = SimpleNamespace(
        healthy=False,
        drain=lambda: [],
    )
    service.activation_market_updates = []
    service.reconciliation_worker = None
    service.user_stream_worker = None
    service.heartbeat_worker = None
    service.geoblock_worker = None
    service.last_reconcile = float("inf")
    service.last_discovery = 0.0
    service._cancel_due_orders = lambda _now: None
    discoveries = []
    service._discover_and_place = lambda: discoveries.append(True)

    service._tick()
    service._tick()

    assert discoveries == []


def test_geoblock_network_failure_pauses_without_raising(tmp_path) -> None:
    update = GeoblockUpdate(
        available=False,
        blocked=False,
        changed=True,
        recovered=False,
        error="SSLError: handshake failed",
    )
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        service = BotService.__new__(BotService)
        service.run_id = run_id
        service.database = database
        service.logger = logging.getLogger("test")
        service.geoblock_worker = SimpleNamespace(drain=lambda: [update])

        service._drain_geoblock_updates()

        event = database.connection.execute(
            "SELECT event_type FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert event["event_type"] == "geoblock_check_failed"


def test_explicit_geoblock_still_stops_trading(tmp_path) -> None:
    update = GeoblockUpdate(
        available=True,
        blocked=True,
        changed=True,
        recovered=False,
        result={"blocked": True, "country": "US", "region": "NY"},
    )
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        service = BotService.__new__(BotService)
        service.run_id = run_id
        service.database = database
        service.logger = logging.getLogger("test")
        service.geoblock_worker = SimpleNamespace(drain=lambda: [update])

        with pytest.raises(RuntimeError, match="trading blocked in US NY"):
            service._drain_geoblock_updates()


def test_run_cancels_tracked_orders_on_ctrl_c(tmp_path) -> None:
    def interrupt_tick() -> None:
        raise KeyboardInterrupt

    with BotDatabase(tmp_path / "bot.sqlite") as database:
        service = _service_for_run(database, interrupt_tick)
        cancel_calls = []
        service._cancel_tracked_orders = lambda: cancel_calls.append(True)

        service.run()

        assert cancel_calls == [True]
        status = database.connection.execute(
            "SELECT status FROM runs WHERE id=?", (service.run_id,)
        ).fetchone()["status"]
        assert status == "stopped"


def test_run_leaves_orders_open_on_failure(tmp_path) -> None:
    def failed_tick() -> None:
        raise RuntimeError("network failed")

    with BotDatabase(tmp_path / "bot.sqlite") as database:
        service = _service_for_run(database, failed_tick)
        cancel_calls = []
        service._cancel_tracked_orders = lambda: cancel_calls.append(True)

        with pytest.raises(RuntimeError, match="network failed"):
            service.run()

        assert cancel_calls == []
        status = database.connection.execute(
            "SELECT status FROM runs WHERE id=?", (service.run_id,)
        ).fetchone()["status"]
        assert status == "failed"


def test_run_leaves_orders_open_when_duration_elapses(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        service = _service_for_run(
            database,
            lambda: pytest.fail("expired run must not tick"),
            hours=Decimal("0"),
        )
        cancel_calls = []
        service._cancel_tracked_orders = lambda: cancel_calls.append(True)

        service.run()

        assert cancel_calls == []
        status = database.connection.execute(
            "SELECT status FROM runs WHERE id=?", (service.run_id,)
        ).fetchone()["status"]
        assert status == "completed"


def test_reconcile_batches_open_orders_and_queries_only_missing(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        for outcome in ("up", "down"):
            database.add_order(
                run_id,
                MARKET.slug,
                PlacedOrder(
                    order_id=f"{outcome}-order",
                    outcome=outcome,
                    token_id=f"{outcome}-token",
                    price=Decimal("0.04"),
                    size=Decimal("25"),
                    status="live",
                    raw={},
                ),
            )

        exchange = FakeExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.04"),
            (ExitTarget(Decimal("0.20"), Decimal("1")),),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")

        service._reconcile()

        rows = {
            row["order_id"]: row
            for row in database.connection.execute("SELECT * FROM orders")
        }
        assert exchange.open_calls == 1
        assert exchange.order_calls == ["down-order"]
        assert rows["up-order"]["status"] == "live"
        assert rows["up-order"]["matched_size"] == "4"
        assert rows["down-order"]["status"] == "cancelled"


def test_gamma_discovery_retries_market_until_order_books_exist(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = PendingBookExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.run_started_ts = MARKET.start_ts
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
        service.max_daily_filled_cost = None
        service.max_reserved_usd = None
        service.logger = logging.getLogger("test")

        should_continue = service._consider_market(
            MARKET,
            now_ts=MARKET.start_ts,
            trigger="gamma",
            orderbook_ready=False,
        )

        assert should_continue
        assert exchange.readiness_calls == [MARKET.slug]
        assert database.has_market(MARKET.slug) is False


def test_background_reconciliation_cannot_regress_streamed_fill(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="filled-order",
                outcome="up",
                token_id=MARKET.up_token_id,
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="filled",
                raw={},
            ),
        )
        update = ReconciliationUpdate(
            (
                ReconciledOrder(
                    snapshot=TrackedOrderSnapshot(
                        "filled-order", MARKET.slug, "100"
                    ),
                    raw={
                        "id": "filled-order",
                        "status": "live",
                        "size_matched": "0",
                    },
                ),
            )
        )
        service = BotService.__new__(BotService)
        service.database = database
        service.reconciliation_worker = FakeReconciliationWorker([update])
        service.run_id = run_id
        service.last_discovery = 1.0
        service.exchange = object()
        service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))

        service._drain_reconciliation_updates()

        row = database.order("filled-order")
        assert row["status"] == "filled"
        assert row["matched_size"] == "100"


def test_reconcile_places_fixed_exit_for_newly_matched_shares(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.04"),
                size=Decimal("25"),
                status="live",
                raw={},
            ),
        )

        exchange = ExitExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.04"),
            (ExitTarget(Decimal("0.20"), Decimal("1")),),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")

        service._reconcile()

        assert exchange.exit_calls == [
            (
                MARKET.slug,
                "up",
                "up-token",
                Decimal("0.20"),
                Decimal("6"),
            )
        ]
        exit_row = database.connection.execute(
            "SELECT * FROM orders WHERE order_id='up-exit'"
        ).fetchone()
        assert exit_row["side"] == "sell"
        assert exit_row["role"] == "exit"

        service._place_missing_exits(MARKET.start_ts)
        assert len(exchange.exit_calls) == 1


def test_reconcile_does_not_place_exit_in_buy_only_mode(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.02"),
                size=Decimal("50"),
                status="live",
                raw={},
            ),
        )

        exchange = ExitExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(Decimal("0.02"), (), Decimal("1"))
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")

        service._reconcile()

        assert exchange.exit_calls == []
        assert (
            database.connection.execute(
                "SELECT COUNT(*) FROM orders WHERE role='exit'"
            ).fetchone()[0]
            == 0
        )


def test_take_profit_ladder_is_balance_safe_and_idempotent(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="matched",
                raw={},
            ),
        )

        exchange = LadderExitExchange(Decimal("60"))
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.01"),
            (
                ExitTarget(Decimal("0.02"), Decimal("0.50")),
                ExitTarget(Decimal("0.10"), Decimal("0.10")),
                ExitTarget(Decimal("0.30"), Decimal("0.10")),
            ),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")

        service._place_missing_exits(MARKET.start_ts)
        assert [call[3:] for call in exchange.exit_calls] == [
            (Decimal("0.02"), Decimal("50.00")),
            (Decimal("0.10"), Decimal("10.00")),
        ]

        exchange.balance = Decimal("100")
        service._place_missing_exits(MARKET.start_ts)
        assert [call[3:] for call in exchange.exit_calls] == [
            (Decimal("0.02"), Decimal("50.00")),
            (Decimal("0.10"), Decimal("10.00")),
            (Decimal("0.30"), Decimal("10.00")),
        ]

        service._place_missing_exits(MARKET.start_ts)
        assert len(exchange.exit_calls) == 3
        assert database.active_exit_reserved_size(MARKET.slug, "up") == Decimal(
            "70.00"
        )


def test_take_profit_ladder_expands_after_additional_partial_fills(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )

        exchange = LadderExitExchange(Decimal("25"))
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.01"),
            (
                ExitTarget(Decimal("0.02"), Decimal("0.50")),
                ExitTarget(Decimal("0.10"), Decimal("0.10")),
                ExitTarget(Decimal("0.30"), Decimal("0.10")),
            ),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")

        database.update_order(
            "up-order", status="live", matched_size=Decimal("25"), raw={}
        )
        service._place_missing_exits(MARKET.start_ts)
        assert [call[3:] for call in exchange.exit_calls] == [
            (Decimal("0.02"), Decimal("12.50")),
        ]

        database.update_order(
            "up-order", status="filled", matched_size=Decimal("100"), raw={}
        )
        exchange.balance = Decimal("100")
        service._place_missing_exits(MARKET.start_ts)
        assert [call[3:] for call in exchange.exit_calls] == [
            (Decimal("0.02"), Decimal("12.50")),
            (Decimal("0.02"), Decimal("37.50")),
            (Decimal("0.10"), Decimal("10.00")),
            (Decimal("0.30"), Decimal("10.00")),
        ]

        service._place_missing_exits(MARKET.start_ts)
        assert len(exchange.exit_calls) == 4


def test_take_profit_is_not_reposted_inside_cancel_window(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="matched",
                raw={},
            ),
        )

        exchange = LadderExitExchange(Decimal("100"))
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.01"),
            (ExitTarget(Decimal("0.02"), Decimal("0.50")),),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")

        service._place_missing_exits(MARKET.end_ts - 3)
        assert len(exchange.exit_calls) == 1
        database.mark_orders(["up-exit-1"], "cancelled")

        service._place_missing_exits(MARKET.end_ts - 2)
        assert len(exchange.exit_calls) == 1


def test_cancels_open_orders_before_market_end(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )
        exchange = FakeExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.cancel_before_end_seconds = 2

        service._cancel_due_orders(MARKET.end_ts - 3)
        assert exchange.cancel_calls == []

        service._cancel_due_orders(MARKET.end_ts - 2)
        assert exchange.cancel_calls == [["up-order"]]
        status = database.connection.execute(
            "SELECT status FROM orders WHERE order_id='up-order'"
        ).fetchone()["status"]
        assert status == "cancelled"

        service._cancel_due_orders(MARKET.end_ts - 1)
        assert exchange.cancel_calls == [["up-order"]]


def test_cancel_due_stops_retrying_terminal_unknown_order(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="stale-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )
        exchange = FakeExchange(
            {
                "canceled": [],
                "not_canceled": {
                    "stale-order": "order can't be found - already canceled or matched"
                },
            }
        )
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.cancel_before_end_seconds = 2

        service._cancel_due_orders(MARKET.end_ts - 2)
        service._cancel_due_orders(MARKET.end_ts - 1)

        assert exchange.cancel_calls == [["stale-order"]]
        status = database.connection.execute(
            "SELECT status FROM orders WHERE order_id='stale-order'"
        ).fetchone()["status"]
        assert status == "terminal_unknown"


def test_cancel_due_does_not_repeat_ambiguous_cancel_request(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="ambiguous-order",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )
        exchange = FailingCancelExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.cancel_before_end_seconds = 2

        service._cancel_due_orders(MARKET.end_ts - 2)
        service._cancel_due_orders(MARKET.end_ts - 1)

        assert exchange.cancel_calls == [["ambiguous-order"]]
        status = database.connection.execute(
            "SELECT status FROM orders WHERE order_id='ambiguous-order'"
        ).fetchone()["status"]
        assert status == "cancel_requested"


def test_shutdown_cancel_records_only_confirmed_results(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        for order_id in ("canceled-order", "stale-order", "retryable-order"):
            database.add_order(
                run_id,
                MARKET.slug,
                PlacedOrder(
                    order_id=order_id,
                    outcome="up",
                    token_id="up-token",
                    price=Decimal("0.01"),
                    size=Decimal("100"),
                    status="live",
                    raw={},
                ),
            )
        exchange = FakeExchange(
            {
                "canceled": ["canceled-order"],
                "not_canceled": {
                    "stale-order": "order can't be found - already canceled or matched",
                    "retryable-order": "temporary exchange error",
                },
            }
        )
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.logger = logging.getLogger("test")

        service._cancel_tracked_orders()

        statuses = {
            row["order_id"]: row["status"]
            for row in database.connection.execute("SELECT order_id, status FROM orders")
        }
        assert statuses == {
            "canceled-order": "cancelled",
            "stale-order": "terminal_unknown",
            "retryable-order": "cancel_requested",
        }
