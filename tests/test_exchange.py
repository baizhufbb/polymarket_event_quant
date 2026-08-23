import time
from decimal import Decimal
from threading import Event, Lock, Thread

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
    # What two members actually buy: whichever of them is next, it is never
    # more than half a cadence away, where one member can be a whole one.
    for moment in (1000.0, 1000.007, 1000.031, 1000.4):
        a = submission_slot(moment, origin=1000.0, phase=0.0, interval=0.025)
        b = submission_slot(moment, origin=1000.0, phase=0.0125, interval=0.025)
        assert min(a, b) >= moment
        assert min(a, b) - moment <= 0.0125 + 1e-9


def test_a_placement_without_a_fleet_sends_at_once() -> None:
    """No fleet means no shared origin, and nothing to wait for.

    The timetable starts at this placement's own first moment, so its first
    slot is that moment. Reading the clock a second time to snap it pushed the
    first send a whole interval out whenever the two readings differed.
    """
    delays = []
    for _ in range(20):
        exchange = Exchange.__new__(Exchange)
        exchange.client = _SlowFirstReplyClient(stall=0.0, budget=2)
        sent = []
        dispatch = Exchange._submit_placement_request

        def record(self, payload, sent=sent):
            sent.append(time.monotonic())
            return dispatch(self, payload)

        exchange._submit_placement_request = record.__get__(exchange, Exchange)
        started = time.monotonic()
        exchange.place_dual(
            MARKET,
            price=Decimal("0.01"),
            size=Decimal("100"),
            submission_interval_ms=Decimal("20"),
        )
        delays.append(sent[0] - started)

    assert max(delays) < 0.010, f"first send delayed by {max(delays) * 1000:.1f} ms"


def test_advancing_the_timetable_never_skips_a_slot() -> None:
    """Walking the timetable forward moves exactly one slot at a time.

    The clock reads in the millions of seconds, where a double carries about
    a nanosecond of noise. Without slack in the arithmetic, `slot + interval`
    lands a hair past the next slot about half the time and the loop skips
    it - which halves the send rate, and the send rate is the whole point of
    the cadence.
    """
    interval = 0.025
    # The old arithmetic kept its slack in slot counts, which multiplies the
    # clock's noise by 1/interval, so it began skipping from about three days
    # of uptime (2**18 seconds). These origins cover that point and far past
    # it; the machine's own clock is included so the box we run on is covered.
    for origin in (1234.5, 262_500.0, 987654.321, 1e7, 3e7, time.monotonic()):
        for phase in (0.0, 0.0125, 0.005):
            kw = {"origin": origin, "phase": phase, "interval": interval}
            slot = submission_slot(origin, **kw)
            for _ in range(400):
                nxt = submission_slot(slot + interval, **kw)
                step = (nxt - slot) / interval
                assert step == pytest.approx(1.0, abs=1e-6), (
                    f"origin={origin} phase={phase} jumped {step} slots"
                )
                slot = nxt


class _StuckClient(FakeClient):
    """A venue that answers nothing until it is released."""

    def __init__(self, release):
        super().__init__([])
        self.release = release
        self.lock = Lock()
        self.calls = 0

    def post_orders(self, signed, post_only=False):
        with self.lock:
            self.calls += 1
        self.release.wait(timeout=10)
        up_id = "0x" + "1" * 64
        down_id = "0x" + "2" * 64
        return [
            {"success": True, "orderID": up_id, "status": "live"},
            {"success": True, "orderID": down_id, "status": "live"},
        ]


def test_the_loop_stops_outrunning_a_venue_that_has_gone_quiet(monkeypatch) -> None:
    """Sending past the pool's share of connections only deepens its queue.

    In the live run replies went from 55 ms to a 14 s median; the loop kept
    sending every 25 ms regardless, every send carrying its own thread, and
    the process ran out of threads and lost that market.
    """
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "in_flight_budget", lambda accounts: 6)
    release = Event()
    exchange = Exchange.__new__(Exchange)
    exchange.client = _StuckClient(release)

    def run():
        return exchange.place_dual(
            MARKET,
            price=Decimal("0.01"),
            size=Decimal("100"),
            submission_interval_ms=Decimal("5"),
        )

    outcome = {}
    worker = Thread(target=lambda: outcome.update(result=run()), daemon=True)
    worker.start()
    try:
        # Far more slots pass than the cap allows, and the loop must not send
        # on them: 0.4 s at a 5 ms cadence is 80 slots against a cap of 6.
        time.sleep(0.4)
        assert exchange.client.calls <= 6, (
            f"loop sent {exchange.client.calls} requests with none answered"
        )
    finally:
        release.set()
    worker.join(timeout=10)

    result = outcome["result"]
    assert result.complete
    # The slots it gave up are reported, not silently dropped. The count itself
    # depends on how promptly a busy machine runs the loop; without the cap it
    # is zero, which is what this separates it from.
    assert result.held_back >= 5, result.held_back


def test_held_back_slots_are_reported_when_nothing_registers(monkeypatch) -> None:
    """Every way out of the loop has to carry the count.

    A placement that registered nothing is the one worth reading afterwards,
    and it was the one path that dropped it - so the record would have read
    "we sent on every slot" for exactly the placements that did not.
    """
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "in_flight_budget", lambda accounts: 4)
    release = Event()

    class _NeverAccepts(_StuckClient):
        def post_orders(self, signed, post_only=False):
            with self.lock:
                self.calls += 1
            self.release.wait(timeout=5)
            return [
                {
                    "success": False,
                    "orderID": "",
                    "errorMsg": "the market is not yet ready to process new orders",
                }
                for _ in signed
            ]

    exchange = Exchange.__new__(Exchange)
    exchange.client = _NeverAccepts(release)
    # +2 rather than +1: int() truncates, so +1 can leave only milliseconds.
    ending = MARKET.__class__(**{**MARKET.__dict__, "end_ts": int(time.time()) + 2})

    try:
        result = exchange.place_dual(
            ending,
            price=Decimal("0.01"),
            size=Decimal("100"),
            submission_interval_ms=Decimal("5"),
        )
    finally:
        release.set()

    assert not result.orders
    assert result.held_back > 0, "a placement that registered nothing reported no held slots"


def test_the_loop_resumes_once_the_venue_answers() -> None:
    """Holding back is backpressure, not a stop: replies free the slots again."""
    release = Event()
    release.set()
    exchange = Exchange.__new__(Exchange)
    exchange.client = _SlowFirstReplyClient(stall=0.0, budget=30)

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("5"),
    )

    assert result.complete
    assert result.attempts >= 30
    assert result.held_back == 0, "a venue answering promptly should not be held back"


class _TickAskingClient:
    """A venue client shaped like the real one where signing is concerned.

    py_clob_client_v2 keeps resolved tick sizes in a dict on the client and
    only asks the venue when a token is missing from it; a market announced
    moments ago answers 404 to that lookup. Everything below mirrors that.
    """

    def __init__(self):
        self._ClobClient__tick_sizes = {}
        self.venue_lookups = []
        self.canceled = []

    def create_order(self, order_args, options):
        token_id = order_args.token_id
        if token_id not in self._ClobClient__tick_sizes:
            self.venue_lookups.append(token_id)
            response = httpx.Response(404, json={"error": "market not found"})
            raise PolyApiException(response)
        return {"token_id": token_id, "side": order_args.side}

    def post_orders(self, signed, post_only=False):
        return [
            {"success": True, "orderID": f"{item.order['token_id']}-id", "status": "live"}
            for item in signed
        ]

    def cancel_orders(self, order_ids):
        self.canceled.extend(order_ids)


def test_signing_does_not_ask_the_venue_for_a_tick_size_we_already_have() -> None:
    """Discovery read the tick size off the listing; asking again costs a market.

    The client only fetches the market's minimum tick to check ours is not
    finer - the order it signs carries ours either way. That lookup is the one
    network call left in signing, and in run 18 it answered 404 for a member,
    which sat that account out of the whole market while the other one sent
    three thousand times.
    """
    exchange = Exchange.__new__(Exchange)
    exchange.client = _TickAskingClient()

    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("20"),
    )

    assert result.complete
    assert exchange.client.venue_lookups == [], (
        f"signing still asked the venue about {exchange.client.venue_lookups}"
    )
    assert exchange.client._ClobClient__tick_sizes == {
        MARKET.up_token_id: str(MARKET.tick_size),
        MARKET.down_token_id: str(MARKET.tick_size),
    }


def test_priming_leaves_a_tick_size_the_client_already_learned() -> None:
    """If the client has been told the real value, that is the one to keep."""
    exchange = Exchange.__new__(Exchange)
    exchange.client = _TickAskingClient()
    exchange.client._ClobClient__tick_sizes[MARKET.up_token_id] = "0.001"

    exchange._prime_tick_size(MARKET)

    assert exchange.client._ClobClient__tick_sizes[MARKET.up_token_id] == "0.001"
    assert exchange.client._ClobClient__tick_sizes[MARKET.down_token_id] == str(MARKET.tick_size)


def test_priming_says_so_if_the_library_moves_its_tick_cache() -> None:
    """Silently reverting to a network call in front of every signature is the
    one outcome worth crashing over, so it is only tolerated for test doubles."""
    from py_clob_client_v2 import ClobClient

    moved = ClobClient.__new__(ClobClient)
    exchange = Exchange.__new__(Exchange)
    exchange.client = moved

    with pytest.raises(RuntimeError, match="tick sizes"):
        exchange._prime_tick_size(MARKET)

    class NotTheRealClient:
        pass

    exchange.client = NotTheRealClient()
    exchange._prime_tick_size(MARKET)  # a double without the cache is left alone


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
    # Stale on purpose, the way a fleet's origin is by the time a member has
    # warmed up and signed. A member that ignored it and started its own
    # timetable would land 63 ms off this one.
    origin = time.monotonic() - 0.137

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

    # Judge the middle send, not the worst one. A broken timetable puts every
    # send off it by the same amount; a busy host puts a few of them off it
    # while the rest stay put, and this suite has to survive a busy host.
    errors = []
    for moment in sent:
        distance = (moment - origin - phase) % interval
        errors.append(min(distance, interval - distance))
    errors.sort()
    middle = errors[len(errors) // 2]
    assert middle <= 0.02, (
        f"half the sends are more than {middle * 1000:.0f} ms off the timetable"
    )

    # Landing on the timetable is not enough: sending on every other slot also
    # lands on it, and that halved rate is the defect the field showed. Every
    # send must take the very next slot, except across the deliberate stall.
    slots = [round((moment - origin - phase) / interval) for moment in sent]
    steps = [b - a for a, b in zip(slots, slots[1:])]
    stalled = [step for step in steps if step > 1]
    assert len(stalled) == 1, f"expected one gap, the stall; got steps {steps}"
    assert stalled[0] >= stall / interval, f"the stall skipped too little: {steps}"
    assert all(step == 1 for step in steps if step <= 1), f"steps {steps}"


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
