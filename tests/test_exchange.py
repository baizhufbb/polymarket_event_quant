from decimal import Decimal

import httpx
import pytest

from polymarket_bot.exchange import Exchange
from polymarket_bot.models import Market, PlacementResult
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
        self.created_order_options = []
        self.posted_order_types = []
        self.posted_batch_types = []

    def create_order(self, order_args, options):
        self.created_order_args.append(order_args)
        self.created_order_options.append(options)
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


class TransientRetryClient(FakeClient):
    def __init__(self, responses):
        super().__init__(responses)
        self.posted_batches = []

    def post_orders(self, signed, post_only=False):
        self.posted_batches.append(tuple(signed))
        if len(self.posted_batches) == 1:
            raise PolyApiException(error_msg="Request exception!")
        return super().post_orders(signed, post_only=post_only)


class SequencedClient(FakeClient):
    def __init__(self, response_batches):
        super().__init__([])
        self.response_batches = list(response_batches)
        self.posted_batches = []

    def post_orders(self, signed, post_only=False):
        assert post_only is True
        self.posted_batches.append(tuple(signed))
        response = self.response_batches.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def api_error(status_code: int, message: str) -> PolyApiException:
    response = httpx.Response(status_code, json={"error": message})
    return PolyApiException(response)


def test_transient_batch_failure_reuses_the_same_signed_orders() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = TransientRetryClient(
        [
            {"success": True, "orderID": "up-order", "status": "live"},
            {"success": True, "orderID": "down-order", "status": "live"},
        ]
    )

    result = exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))

    assert result.complete
    assert len(exchange.client.posted_batches) == 2
    assert exchange.client.posted_batches[0][0] is exchange.client.posted_batches[1][0]
    assert exchange.client.posted_batches[0][1] is exchange.client.posted_batches[1][1]


def test_market_retry_reuses_the_same_signed_orders() -> None:
    not_ready = {
        "success": True,
        "orderID": "",
        "errorMsg": "the market is not yet ready to process new orders",
    }
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            [dict(not_ready), dict(not_ready)],
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )

    first = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )
    second = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert first.retryable
    assert second.complete
    assert len(exchange.client.created_order_args) == 2
    assert exchange.client.posted_batches[0][0] is exchange.client.posted_batches[1][0]
    assert exchange.client.posted_batches[0][1] is exchange.client.posted_batches[1][1]


@pytest.mark.parametrize(
    ("status_code", "message"),
    ((400, "invalid token id"), (404, "market not found")),
)
def test_explicit_engine_rejection_reuses_the_same_signed_orders(
    status_code: int,
    message: str,
) -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            api_error(status_code, message),
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )

    first = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )
    second = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert first == PlacementResult((), message, retryable=True)
    assert second.complete
    assert len(exchange.client.created_order_args) == 2
    assert exchange.client.posted_batches[0][0] is exchange.client.posted_batches[1][0]
    assert exchange.client.posted_batches[0][1] is exchange.client.posted_batches[1][1]


def test_engine_rejection_after_transient_failure_stays_retryable() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            PolyApiException(error_msg="Request exception!"),
            api_error(400, "invalid token id"),
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )

    first = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )
    second = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert first == PlacementResult((), "invalid token id", retryable=True)
    assert second.complete
    assert len(exchange.client.created_order_args) == 2
    assert all(
        batch[0] is exchange.client.posted_batches[0][0]
        and batch[1] is exchange.client.posted_batches[0][1]
        for batch in exchange.client.posted_batches
    )


def test_ambiguous_retry_reuses_the_same_signed_orders() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            PolyApiException(error_msg="Request exception!"),
            PolyApiException(error_msg="Request exception!"),
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )
    exchange.open_orders = lambda condition_id: []

    with pytest.raises(RuntimeError):
        exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))
    reconciliation = exchange.reconcile_ambiguous_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )
    result = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert reconciliation.retryable
    assert result.complete
    assert len(exchange.client.created_order_args) == 2
    assert all(
        batch[0] is exchange.client.posted_batches[0][0]
        and batch[1] is exchange.client.posted_batches[0][1]
        for batch in exchange.client.posted_batches
    )


def test_duplicate_responses_recover_the_original_order_ids() -> None:
    up_id = "0x" + "1" * 64
    down_id = "0x" + "2" * 64
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient(
        [
            {
                "success": False,
                "orderID": "",
                "errorMsg": f"order {up_id} is invalid. Duplicated.",
            },
            {
                "success": False,
                "orderID": "",
                "errorMsg": f"order {down_id} is invalid. Duplicated.",
            },
        ]
    )

    result = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert result.complete
    assert {order.order_id for order in result.orders} == {up_id, down_id}


def test_business_rejection_exception_is_not_retried() -> None:
    class RejectedClient(FakeClient):
        def __init__(self):
            super().__init__([])
            self.calls = 0

        def post_orders(self, signed, post_only=False):
            self.calls += 1
            request = httpx.Request("POST", "https://clob.polymarket.com/orders")
            response = httpx.Response(400, json={"error": "rejected"}, request=request)
            raise PolyApiException(response)

    exchange = Exchange.__new__(Exchange)
    exchange.client = RejectedClient()

    with pytest.raises(PolyApiException):
        exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))

    assert exchange.client.calls == 1


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
    assert not result.retryable
    assert exchange.client.canceled == ["up-order"]


def test_market_not_ready_pair_is_retryable() -> None:
    exchange = Exchange.__new__(Exchange)
    response = {
        "success": True,
        "orderID": "",
        "errorMsg": "the market is not yet ready to process new orders",
    }
    exchange.client = FakeClient([dict(response), dict(response)])

    result = exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))

    assert not result.complete
    assert result.orders == ()
    assert result.retryable
    assert exchange.client.canceled == []


def test_missing_orderbook_pair_is_retryable() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient(
        [
            {
                "success": True,
                "orderID": "",
                "errorMsg": "the orderbook up-token does not exist",
            },
            {
                "success": True,
                "orderID": "",
                "errorMsg": "the orderbook down-token does not exist",
            },
        ]
    )

    result = exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))

    assert not result.complete
    assert result.orders == ()
    assert result.retryable
    assert exchange.client.canceled == []


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
    assert [options.neg_risk for options in exchange.client.created_order_options] == [
        False,
        False,
    ]
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


def test_ambiguous_submission_without_orders_is_retryable() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = FakeClient([])
    exchange.open_orders = lambda condition_id: []

    result = exchange.reconcile_ambiguous_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert not result.complete
    assert result.orders == ()
    assert result.retryable


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
