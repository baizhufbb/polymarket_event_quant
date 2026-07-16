from decimal import Decimal

import httpx

from polymarket_bot.exchange import Exchange
from polymarket_bot.models import Market
from py_clob_client_v2 import Side
from py_clob_client_v2.exceptions import PolyApiException


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


class FakeClient:
    def __init__(self, responses):
        self.responses = responses
        self.canceled = []
        self.created_order_args = []
        self.posted_order_types = []
        self.posted_batch_types = []

    def create_order(self, order_args, options):
        self.created_order_args.append(order_args)
        return {
            "token_id": order_args.token_id,
            "side": order_args.side,
            "price": order_args.price,
            "size": order_args.size,
        }

    def post_orders(self, signed, post_only=False):
        assert post_only is True
        assert len(signed) == 2
        self.posted_batch_types.extend(item.orderType for item in signed)
        return self.responses

    def cancel_orders(self, order_ids):
        self.canceled.extend(order_ids)

    def post_order(self, signed, order_type, post_only=False):
        assert signed["side"] == Side.SELL
        assert post_only is False
        self.posted_order_types.append(order_type)
        return {"success": True, "orderID": "exit-order", "status": "live"}


def test_partial_batch_is_immediately_canceled() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient(
        [
            {"success": True, "orderID": "up-order", "status": "live"},
            {"success": False, "errorMsg": "rejected"},
        ]
    )
    result = exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))
    assert not result.complete
    assert exchange.client.canceled == ["up-order"]


def test_entries_are_post_only_gtc_without_expiration() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient(
        [
            {"success": True, "orderID": "up-order", "status": "live"},
            {"success": True, "orderID": "down-order", "status": "live"},
        ]
    )

    result = exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))

    assert result.complete
    assert [args.expiration for args in exchange.client.created_order_args] == [0, 0]
    assert exchange.client.posted_batch_types == ["GTC", "GTC"]


def test_ambiguous_submission_adopts_exact_pair() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient([])
    exchange.open_orders = lambda condition_id: [
        {
            "id": "up-order",
            "asset_id": "up-token",
            "side": "BUY",
            "price": "0.01",
            "original_size": "100",
            "status": "live",
        },
        {
            "id": "down-order",
            "asset_id": "down-token",
            "side": "BUY",
            "price": "0.01",
            "original_size": "100",
            "status": "live",
        },
    ]
    result = exchange.reconcile_ambiguous_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )
    assert result.complete
    assert {order.order_id for order in result.orders} == {"up-order", "down-order"}


def test_exit_is_a_non_post_only_sell_limit() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient([])

    order = exchange.place_exit(
        MARKET,
        outcome="up",
        token_id="up-token",
        price=Decimal("0.20"),
        size=Decimal("25"),
    )

    assert order.order_id == "exit-order"
    assert order.side == "sell"
    assert order.role == "exit"
    assert order.price == Decimal("0.20")
    assert order.size == Decimal("25")
    assert exchange.client.created_order_args[-1].expiration == 0
    assert exchange.client.posted_order_types == ["GTC"]


def test_ambiguous_exit_adopts_one_exact_open_sell() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.open_orders = lambda condition_id: [
        {
            "id": "existing-exit",
            "asset_id": "up-token",
            "side": "SELL",
            "price": "0.20",
            "original_size": "25",
            "status": "live",
        }
    ]

    order = exchange.reconcile_ambiguous_exit(
        MARKET,
        outcome="up",
        token_id="up-token",
        price=Decimal("0.20"),
        size=Decimal("25"),
    )

    assert order is not None
    assert order.order_id == "existing-exit"
    assert order.side == "sell"


def test_heartbeat_retries_once_with_server_id() -> None:
    class HeartbeatClient:
        def __init__(self):
            self.calls = []

        def post_heartbeat(self, heartbeat_id):
            self.calls.append(heartbeat_id)
            if len(self.calls) == 1:
                request = httpx.Request(
                    "POST", "https://clob.polymarket.com/v1/heartbeats"
                )
                response = httpx.Response(
                    400,
                    json={"heartbeat_id": "replacement"},
                    request=request,
                )
                raise PolyApiException(response)
            return {"status": "ok", "heartbeat_id": "current"}

    exchange = Exchange.__new__(Exchange)
    exchange.client = HeartbeatClient()
    exchange.heartbeat_id = "expired"

    exchange.heartbeat()

    assert exchange.client.calls == ["expired", "replacement"]
    assert exchange.heartbeat_id == "current"
