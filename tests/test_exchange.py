import time
from decimal import Decimal
from threading import Event, Lock

import httpx
import pytest

from polymarket_bot.exchange import (
    AmbiguousPlacementError,
    Exchange,
    submission_slot,
)
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


class StaggeredClient(FakeClient):
    def __init__(self):
        super().__init__([])
        self.lock = Lock()
        self.posted_batches = []
        self.active = 0
        self.max_active = 0

    def post_orders(self, signed, post_only=False):
        assert post_only is True
        with self.lock:
            call_number = len(self.posted_batches) + 1
            self.posted_batches.append(tuple(signed))
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.06)
            up_id = "0x" + "1" * 64
            down_id = "0x" + "2" * 64
            if call_number == 1:
                return [
                    {"success": True, "orderID": up_id, "status": "live"},
                    {
                        "success": True,
                        "orderID": "",
                        "errorMsg": (
                            "the market is not yet ready to process new orders"
                        ),
                    },
                ]
            return [
                {
                    "success": False,
                    "orderID": "",
                    "errorMsg": f"order {up_id} is invalid. Duplicated.",
                },
                {"success": True, "orderID": down_id, "status": "live"},
            ]
        finally:
            with self.lock:
                self.active -= 1


class UncappedInFlightClient(FakeClient):
    def __init__(self):
        super().__init__([])
        self.lock = Lock()
        self.release = Event()
        self.calls = 0
        self.active = 0
        self.max_active = 0

    def post_orders(self, signed, post_only=False):
        assert post_only is True
        with self.lock:
            self.calls += 1
            call_number = self.calls
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active >= 17:
                self.release.set()
        try:
            self.release.wait(timeout=1)
            if call_number == 17:
                return [
                    {"success": True, "orderID": "up-order", "status": "live"},
                    {
                        "success": True,
                        "orderID": "down-order",
                        "status": "live",
                    },
                ]
            return [
                {
                    "success": False,
                    "orderID": "",
                    "errorMsg": "the market is not yet ready to process new orders",
                },
                {
                    "success": False,
                    "orderID": "",
                    "errorMsg": "the market is not yet ready to process new orders",
                },
            ]
        finally:
            with self.lock:
                self.active -= 1


def api_error(status_code: int, message: str) -> PolyApiException:
    response = httpx.Response(status_code, json={"error": message})
    return PolyApiException(response)


def test_staggered_submission_overlaps_identical_signed_batches() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = StaggeredClient()

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("20"),
    )

    assert result.complete
    assert result.attempts >= 2
    assert exchange.client.max_active >= 2
    assert exchange.client.canceled == []
    first = exchange.client.posted_batches[0]
    assert all(batch[0] is first[0] for batch in exchange.client.posted_batches)
    assert all(batch[1] is first[1] for batch in exchange.client.posted_batches)


def test_staggered_submission_has_no_fixed_in_flight_limit() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = UncappedInFlightClient()

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("1"),
    )

    assert result.complete
    assert exchange.client.max_active >= 17


def test_submission_slot_lands_on_the_timetable() -> None:
    kw = {"origin": 1000.0, "phase": 0.0125, "interval": 0.025}

    # A moment already on a slot takes that slot rather than skipping it.
    assert submission_slot(1000.0125, **kw) == pytest.approx(1000.0125)
    # Between slots, the next one.
    assert submission_slot(1000.020, **kw) == pytest.approx(1000.0375)
    # A stall of several slots resumes on the timetable, not at moment+interval.
    assert submission_slot(1000.100, **kw) == pytest.approx(1000.1125)
    # Two members' slots always sit half a cadence apart (which of the two
    # comes first just depends on where the moment falls between them).
    for moment in (1000.0, 1000.007, 1000.031, 1000.4):
        a = submission_slot(moment, origin=1000.0, phase=0.0, interval=0.025)
        b = submission_slot(moment, origin=1000.0, phase=0.0125, interval=0.025)
        assert ((b - a) % 0.025) == pytest.approx(0.0125)


class _SlowFirstReplyClient(FakeClient):
    """Answers not-ready until its budget is reached, then accepts."""

    def __init__(self, stall: float, budget: int):
        super().__init__([])
        self.stall = stall
        self.budget = budget
        self.lock = Lock()
        self.calls = 0
        self.sent_at: list[float] = []

    def post_orders(self, signed, post_only=False):
        with self.lock:
            self.calls += 1
            call_number = self.calls
            self.sent_at.append(time.monotonic())
        if call_number == 1:
            time.sleep(self.stall)
        if call_number >= self.budget:
            up_id = "0x" + "1" * 64
            down_id = "0x" + "2" * 64
            return [
                {"success": True, "orderID": up_id, "status": "live"},
                {"success": True, "orderID": down_id, "status": "live"},
            ]
        return [
            {
                "success": False,
                "orderID": "",
                "errorMsg": "the market is not yet ready to process new orders",
            },
            {
                "success": False,
                "orderID": "",
                "errorMsg": "the market is not yet ready to process new orders",
            },
        ]


def test_sends_stay_on_the_timetable_across_a_stall() -> None:
    """A stalled loop resumes on its own slots instead of a fresh timetable.

    Re-basing on "now" is what collapsed the fleet's offsets: on one core both
    members stall together, so both re-based to the same instant and ended up
    sending together instead of a half cadence apart. The cadence here is
    coarse on purpose - the point is where the sends land relative to the
    timetable, and a coarse cadence keeps the operating system's wake-up
    rounding (about 15 ms on Windows) well inside the tolerance.
    """
    interval = 0.2
    phase = 0.1
    stall = 0.65  # deliberately not a whole number of slots
    origin = time.monotonic()

    exchange = Exchange.__new__(Exchange)
    exchange.client = _SlowFirstReplyClient(stall=0.0, budget=7)

    # Hold the loop thread itself, the way contention for a single core held
    # it in the field. A slow reply would not do it: replies are carried by
    # their own threads and never block the loop.
    sent: list[float] = []
    dispatch = Exchange._submit_placement_request

    def stall_the_loop(self, payload):
        sent.append(time.monotonic())
        if len(sent) == 3:
            time.sleep(stall)
        return dispatch(self, payload)

    exchange._submit_placement_request = stall_the_loop.__get__(exchange, Exchange)

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("200"),
        grid_origin=origin,
        phase_offset_ms=Decimal("100"),
    )

    assert result.complete
    assert len(sent) >= 6
    assert max(b - a for a, b in zip(sent, sent[1:])) > stall  # the stall happened

    off_timetable = []
    for moment in sent:
        distance = (moment - origin - phase) % interval
        error = min(distance, interval - distance)
        if error > 0.04:
            off_timetable.append(round(error * 1000))
    assert not off_timetable, f"sends off the timetable by ms: {off_timetable}"


def test_transient_batch_failure_immediately_reuses_the_same_signed_orders() -> None:
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

    assert first == PlacementResult(
        (), "invalid token id", retryable=True, attempts=2
    )
    assert second.complete
    assert len(exchange.client.created_order_args) == 2
    assert all(
        batch[0] is exchange.client.posted_batches[0][0]
        and batch[1] is exchange.client.posted_batches[0][1]
        for batch in exchange.client.posted_batches
    )


def test_rate_limit_response_retries_the_same_signed_orders() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            api_error(429, "rate limit exceeded"),
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )

    result = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert result.complete
    assert result.attempts == 2
    first, second = exchange.client.posted_batches
    assert first[0] is second[0]
    assert first[1] is second[1]


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

    with pytest.raises(AmbiguousPlacementError):
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


def test_http_protocol_failure_is_retryable_with_the_same_signed_orders() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            httpx.RemoteProtocolError("server disconnected"),
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )
    exchange.open_orders = lambda condition_id: []

    with pytest.raises(AmbiguousPlacementError) as raised:
        exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))
    reconciliation = exchange.reconcile_ambiguous_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )
    result = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert raised.value.retryable
    assert reconciliation.retryable
    assert result.complete
    assert len(exchange.client.created_order_args) == 2
    assert all(
        batch[0] is exchange.client.posted_batches[0][0]
        and batch[1] is exchange.client.posted_batches[0][1]
        for batch in exchange.client.posted_batches
    )


def test_incomplete_batch_response_is_reconciled_before_retry() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            [{"success": True, "orderID": "up-order", "status": "live"}],
            [
                {"success": True, "orderID": "up-order", "status": "live"},
                {"success": True, "orderID": "down-order", "status": "live"},
            ],
        ]
    )
    exchange.open_orders = lambda condition_id: []

    with pytest.raises(AmbiguousPlacementError):
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


def test_terminal_rejection_after_ambiguous_attempt_is_not_requeued() -> None:
    exchange = Exchange.__new__(Exchange)
    exchange.client = SequencedClient(
        [
            PolyApiException(error_msg="Request exception!"),
            api_error(400, "rejected"),
        ]
    )
    exchange.open_orders = lambda condition_id: []

    with pytest.raises(AmbiguousPlacementError) as raised:
        exchange.place_dual(MARKET, price=Decimal("0.01"), size=Decimal("100"))
    result = exchange.reconcile_ambiguous_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        retryable_if_missing=raised.value.retryable,
    )

    assert not raised.value.retryable
    assert not result.retryable


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


def test_business_rejection_is_terminal_and_not_retried() -> None:
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

    result = exchange.place_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert exchange.client.calls == 1
    assert not result.complete
    assert not result.retryable
    assert "rejected" in result.error


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


class SingleModeClient(FakeClient):
    def __init__(self, leg_responses=None):
        super().__init__([])
        self.single_posts = []
        self.leg_responses = leg_responses or {}
        self.raised = {"up-token": 0, "down-token": 0}

    def post_orders(self, signed, post_only=False):
        raise AssertionError("single mode must not use the batch endpoint")

    def post_order(self, signed, order_type, post_only=False):
        assert post_only is True
        token = signed["token_id"]
        self.single_posts.append(token)
        plan = self.leg_responses.get(token)
        if plan:
            action = plan.pop(0)
            if isinstance(action, Exception):
                raise action
            return action
        return {"success": True, "orderID": f"{token}-id", "status": "live"}


def _not_ready_error():
    error = PolyApiException(
        error_msg={"error": "the market is not yet ready to process new orders"}
    )
    error.status_code = 400
    return error


def test_single_mode_places_each_leg_individually():
    client = SingleModeClient()
    exchange = Exchange.__new__(Exchange)
    exchange.client = client
    exchange.entry_submission = "single"

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("1"),
    )

    assert result.complete
    assert {order.order_id for order in result.orders} == {
        "up-token-id",
        "down-token-id",
    }
    assert set(client.single_posts) == {"up-token", "down-token"}


def test_single_mode_treats_not_ready_as_recoverable():
    client = SingleModeClient(
        leg_responses={
            "up-token": [_not_ready_error()],
            "down-token": [_not_ready_error()],
        }
    )
    exchange = Exchange.__new__(Exchange)
    exchange.client = client
    exchange.entry_submission = "single"

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("1"),
    )

    assert result.complete
    assert result.attempts >= 2


def test_single_mode_recovers_after_transient_error():
    client = SingleModeClient(
        leg_responses={
            "up-token": [PolyApiException(error_msg="Request exception!")],
        }
    )
    exchange = Exchange.__new__(Exchange)
    exchange.client = client
    exchange.entry_submission = "single"

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("1"),
    )

    assert result.complete
    assert result.attempts >= 2


def test_solo_mode_places_only_the_chosen_leg_and_never_cancels_it():
    client = SingleModeClient()
    exchange = Exchange.__new__(Exchange)
    exchange.client = client
    exchange.entry_submission = "solo-up"

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("1"),
    )

    assert result.complete
    assert len(result.orders) == 1
    assert result.orders[0].outcome == "up"
    assert set(client.single_posts) == {"up-token"}
    assert client.canceled == []

class _NotReadyThenSigningClient:
    """create_order answers 404 market-not-found a few times, then signs."""

    def __init__(self, failures):
        self.failures = failures
        self.create_calls = 0
        self.canceled = []

    def create_order(self, order_args, options):
        self.create_calls += 1
        if self.create_calls <= self.failures:
            response = httpx.Response(404, json={"error": "market not found"})
            raise PolyApiException(response)
        return {"token_id": order_args.token_id, "side": order_args.side}

    def post_orders(self, signed, post_only=False):
        return [
            {"success": True, "orderID": f"{item.order['token_id']}-id", "status": "live"}
            for item in signed
        ]

    def cancel_orders(self, order_ids):
        self.canceled.extend(order_ids)


def test_signing_waits_out_the_post_announcement_404(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.0)
    exchange = Exchange.__new__(Exchange)
    exchange.client = _NotReadyThenSigningClient(failures=3)

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("20"),
    )

    assert result.complete
    # three rejected attempts, then one attempt that signs both legs
    assert exchange.client.create_calls == 5


def test_signing_hands_the_market_back_after_the_in_place_budget(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_RETRY_SECONDS", 0.3)
    exchange = Exchange.__new__(Exchange)
    exchange.client = _NotReadyThenSigningClient(failures=100)

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("20"),
    )

    assert result.orders == ()
    assert result.retryable
    assert result.attempts == 1
    assert "signing not ready" in result.error
    # 50 ms pacing inside a 300 ms budget: a handful of polls, not one, not hundreds
    assert 4 <= exchange.client.create_calls <= 8


def test_signing_budget_is_wall_clock_even_when_replies_are_slow(monkeypatch) -> None:
    import time

    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.01)
    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_RETRY_SECONDS", 0.25)

    class SlowNotReadyClient(_NotReadyThenSigningClient):
        def create_order(self, order_args, options):
            time.sleep(0.1)  # the venue's open-moment latency, far above the poll pace
            return super().create_order(order_args, options)

    exchange = Exchange.__new__(Exchange)
    exchange.client = SlowNotReadyClient(failures=100)

    started = time.monotonic()
    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("20"),
    )
    elapsed = time.monotonic() - started

    assert result.orders == ()
    assert result.retryable
    # bounded by the budget plus one reply, not by a poll count (25 slots here)
    assert elapsed < 0.8
    assert exchange.client.create_calls <= 4


def test_signing_retries_transient_transport_failures(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.0)

    class TransientThenSigningClient(_NotReadyThenSigningClient):
        def __init__(self, failure):
            super().__init__(failures=1)
            self.failure = failure

        def create_order(self, order_args, options):
            self.create_calls += 1
            if self.create_calls <= self.failures:
                raise self.failure
            return {"token_id": order_args.token_id, "side": order_args.side}

    failures = [
        PolyApiException(httpx.Response(429, json={"error": "too many requests"})),
        PolyApiException(httpx.Response(503, json={"error": "trading is disabled"})),
        PolyApiException(error_msg="ReadTimeout: timed out"),
    ]
    for failure in failures:
        exchange = Exchange.__new__(Exchange)
        exchange.client = TransientThenSigningClient(failure)
        result = exchange.place_dual(
            MARKET,
            price=Decimal("0.01"),
            size=Decimal("100"),
            submission_interval_ms=Decimal("20"),
        )
        assert result.complete, failure
        assert exchange.client.create_calls == 3


class _RejectingClient(_NotReadyThenSigningClient):
    def __init__(self, status, message):
        super().__init__(failures=0)
        self.status = status
        self.message = message

    def create_order(self, order_args, options):
        self.create_calls += 1
        raise PolyApiException(httpx.Response(self.status, json={"error": self.message}))


def test_signing_does_not_retry_other_rejections(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.0)
    for status, message in ((400, "invalid price"), (400, "invalid token id")):
        exchange = Exchange.__new__(Exchange)
        exchange.client = _RejectingClient(status, message)
        with pytest.raises(PolyApiException):
            exchange.place_dual(
                MARKET,
                price=Decimal("0.01"),
                size=Decimal("100"),
                submission_interval_ms=Decimal("20"),
            )
        assert exchange.client.create_calls == 1
