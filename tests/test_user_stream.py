import logging
from decimal import Decimal

from polymarket.models.clob.user_events import UserOrderEvent, UserTradeEvent

from polymarket_bot.database import BotDatabase
from polymarket_bot.models import ExitTarget, Market, PlacedOrder, TradePlan
from polymarket_bot.service import BotService
from polymarket_bot.user_stream import (
    UserOrderUpdate,
    UserStreamWorker,
    UserTradeUpdate,
)


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


class FakeUserStream:
    def __init__(self, updates):
        self.updates = updates

    def drain(self):
        updates, self.updates = self.updates, []
        return updates


class ExitExchange:
    def __init__(self, balance=Decimal("100")):
        self.exit_calls = []
        self.balance = balance

    def conditional_balance(self, token_id):
        return self.balance

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


def _order_event(*, event_type: str, status: str, matched: str) -> UserOrderEvent:
    return UserOrderEvent.model_validate(
        {
            "topic": "user",
            "type": "order",
            "payload": {
                "id": "up-order",
                "owner": "owner",
                "market": MARKET.condition_id,
                "asset_id": MARKET.up_token_id,
                "side": "BUY",
                "original_size": "100",
                "size_matched": matched,
                "price": "0.01",
                "type": event_type,
                "status": status,
            },
        }
    )


def _trade_event(*, status: str) -> UserTradeEvent:
    return UserTradeEvent.model_validate(
        {
            "topic": "user",
            "type": "trade",
            "payload": {
                "id": "trade-id",
                "taker_order_id": "counterparty-order",
                "market": MARKET.condition_id,
                "asset_id": MARKET.up_token_id,
                "side": "BUY",
                "size": "25",
                "price": "0.01",
                "status": status,
                "owner": "owner",
                "maker_orders": [
                    {
                        "order_id": "up-order",
                        "owner": "owner",
                        "matched_amount": "25",
                        "price": "0.01",
                        "asset_id": MARKET.up_token_id,
                        "side": "BUY",
                    }
                ],
            },
        }
    )


def test_normalize_order_keeps_partial_match_live() -> None:
    update = UserStreamWorker._normalize_order(
        _order_event(event_type="UPDATE", status="MATCHED", matched="25")
    )

    assert update.status == "live"
    assert update.matched_size == Decimal("25")


def test_normalize_order_marks_full_match_and_cancellation_terminal() -> None:
    filled = UserStreamWorker._normalize_order(
        _order_event(event_type="UPDATE", status="MATCHED", matched="100")
    )
    cancelled = UserStreamWorker._normalize_order(
        _order_event(event_type="CANCELLATION", status="CANCELED", matched="25")
    )

    assert filled.status == "filled"
    assert cancelled.status == "cancelled"
    assert cancelled.matched_size == Decimal("25")


def test_normalize_trade_includes_maker_order_and_status() -> None:
    update = UserStreamWorker._normalize_trade(_trade_event(status="MINED"))

    assert update.status == "MINED"
    assert update.order_ids == ("counterparty-order", "up-order")


def test_stream_fill_places_exit_without_waiting_for_rest_poll(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id=MARKET.up_token_id,
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )
        update = UserOrderUpdate(
            order_id="up-order",
            status="live",
            matched_size=Decimal("25"),
            raw={"source": "user_ws"},
        )
        exchange = ExitExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.01"),
            (ExitTarget(Decimal("0.02"), Decimal("1")),),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")
        service.user_stream_worker = FakeUserStream([update])

        service._drain_user_stream_updates()

        assert exchange.exit_calls == [
            (
                MARKET.slug,
                "up",
                MARKET.up_token_id,
                Decimal("0.02"),
                Decimal("25"),
            )
        ]
        row = database.order("up-order")
        assert row["matched_size"] == "25"
        assert database.order("up-exit") is not None


def test_matched_order_waits_when_conditional_tokens_are_not_settled(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id=MARKET.up_token_id,
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )
        exchange = ExitExchange(balance=Decimal("0"))
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.01"),
            (ExitTarget(Decimal("0.02"), Decimal("1")),),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")
        service.user_stream_worker = FakeUserStream(
            [
                UserOrderUpdate(
                    order_id="up-order",
                    status="filled",
                    matched_size=Decimal("100"),
                    raw={},
                )
            ]
        )

        service._drain_user_stream_updates()

        assert exchange.exit_calls == []
        assert (
            database.connection.execute(
                "SELECT COUNT(*) FROM orders WHERE role='exit'"
            ).fetchone()[0]
            == 0
        )


def test_mined_trade_places_exit_after_balance_is_available(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="up-order",
                outcome="up",
                token_id=MARKET.up_token_id,
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="filled",
                raw={},
            ),
        )
        exchange = ExitExchange()
        service = BotService.__new__(BotService)
        service.database = database
        service.exchange = exchange
        service.run_id = run_id
        service.plan = TradePlan(
            Decimal("0.01"),
            (ExitTarget(Decimal("0.02"), Decimal("1")),),
            Decimal("1"),
        )
        service.cancel_before_end_seconds = 2
        service.logger = logging.getLogger("test")
        service.user_stream_worker = FakeUserStream(
            [
                UserTradeUpdate(
                    trade_id="trade-id",
                    status="MINED",
                    order_ids=("up-order",),
                    raw={},
                )
            ]
        )

        service._drain_user_stream_updates()

        assert len(exchange.exit_calls) == 1
