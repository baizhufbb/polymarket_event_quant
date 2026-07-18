import logging
from decimal import Decimal

from polymarket_bot.database import BotDatabase
from polymarket_bot.models import ExitTarget, Market, PlacedOrder, TradePlan
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


class PendingBookExchange(FakeExchange):
    def __init__(self):
        super().__init__()
        self.readiness_calls = []

    def order_books_ready(self, market):
        self.readiness_calls.append(market.slug)
        return False


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
        assert status == "cancel_requested"


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
            "canceled-order": "cancel_requested",
            "stale-order": "terminal_unknown",
            "retryable-order": "live",
        }
