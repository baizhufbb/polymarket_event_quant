"""The Python half of a placement: signing the entry, handing it to the knock
library, and reading what the knock came to. How the knock itself behaves
(timetable, turns between legs, not-ready and transient replies, the
in-flight ceiling, the drain, late replies) is tested in knocker/ in Go."""

import json
import time
from decimal import Decimal
from types import SimpleNamespace

import certifi
import httpx
import pytest
from py_clob_client_v2 import ApiCreds, Side
from py_clob_client_v2.exceptions import PolyApiException

from polymarket_bot import knocker
from polymarket_bot.exchange import AmbiguousPlacementError, Exchange
from polymarket_bot.models import Market

REAL_ORDER_BODY = Exchange._order_body

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
CREDS = ApiCreds(api_key="key", api_secret="c2VjcmV0", api_passphrase="pass")


def outcome(
    *,
    accepted=(),
    errors=(),
    ambiguous=(),
    attempts=5,
    held_back=0,
    registered_ms=None,
    gave_up=False,
):
    """One member's result, shaped as the knock library returns it."""
    return {
        "attempts": attempts,
        "held_back": held_back,
        "accepted": [
            {
                "outcome": leg,
                "order_id": order_id,
                "status": 200,
                "body": json.dumps({"success": True, "orderID": order_id, "status": "live"}),
            }
            for leg, order_id in accepted
        ],
        "registered_ms": registered_ms,
        "errors": list(errors),
        "ambiguous": list(ambiguous),
        "gave_up": gave_up,
    }


def reply(status, body):
    return {"status": status, "body": body if isinstance(body, str) else json.dumps(body)}


def text(message):
    return {"text": message}


class Knocks:
    """Stands in for the knock library: records each plan and answers with
    the outcomes queued, or with every leg registered."""

    def __init__(self):
        self.plans = []
        self.hooks = []
        self.outcomes = []

    def __call__(self, plan, hooks):
        self.plans.append(plan)
        self.hooks.append(hooks)
        members = []
        for member in plan["members"]:
            if self.outcomes:
                result = self.outcomes.pop(0)
            else:
                result = outcome(
                    accepted=[(leg["outcome"], f"{leg['outcome']}-id") for leg in member["legs"]],
                    registered_ms=1_000,
                )
            members.append({"account": member["account"], **result})
        return {"members": members}


@pytest.fixture(autouse=True)
def knocks(monkeypatch):
    stub = Knocks()
    monkeypatch.setattr(knocker, "knock", stub)
    # The fake clients below sign plain dicts; their body is that dict.
    monkeypatch.setattr(
        Exchange, "_order_body", lambda self, args: json.dumps(args.order, sort_keys=True)
    )
    return stub


class FakeClient:
    def __init__(self):
        self.canceled = []
        self.created_order_args = []
        self.created_order_options = []
        self.posted_order_types = []
        self.creds = CREDS
        self.signer = SimpleNamespace(address=lambda: "0xSigner")

    def create_order(self, order_args, options):
        self.created_order_args.append(order_args)
        self.created_order_options.append(options)
        return {
            "token_id": order_args.token_id,
            "side": order_args.side,
            "price": order_args.price,
            "size": order_args.size,
        }

    def cancel_orders(self, order_ids):
        self.canceled.extend(order_ids)

    def post_order(self, signed, order_type, post_only=False):
        assert signed["side"] == Side.SELL
        assert post_only is False
        self.posted_order_types.append(order_type)
        return {"success": True, "orderID": "exit-order", "status": "live"}


def _exchange(client=None, entry_submission="single") -> Exchange:
    exchange = Exchange.__new__(Exchange)
    exchange.client = client or FakeClient()
    exchange.entry_submission = entry_submission
    exchange.credentials = CREDS
    return exchange


def _place(exchange, market=MARKET, **kwargs):
    return exchange.place_dual(
        market,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=kwargs.pop("submission_interval_ms", Decimal("20")),
        **kwargs,
    )


def test_place_dual_hands_the_signed_pair_to_one_knock(knocks) -> None:
    exchange = _exchange()
    until = time.time() + 240

    result = _place(exchange, knock_until_ts=until)

    assert result.complete
    [plan] = knocks.plans
    assert plan["market"] == MARKET.slug
    assert plan["interval_ms"] == 20.0
    assert plan["knock_until_ms"] == int(until * 1000)
    assert plan["market_end_ms"] == MARKET.end_ts * 1000
    # The venue is reached with the certificate authorities the Python HTTP
    # stack trusted.
    assert plan["ca_file"] == certifi.where()
    [member] = plan["members"]
    assert member["account"] == "primary"
    assert member["phase_ms"] == 0.0
    assert (member["address"], member["api_key"], member["api_secret"], member["api_passphrase"]) == (
        "0xSigner", "key", "c2VjcmV0", "pass",
    )
    signed = exchange.client.created_order_args
    assert [leg["outcome"] for leg in member["legs"]] == ["up", "down"]
    assert [json.loads(leg["body"])["token_id"] for leg in member["legs"]] == [
        args.token_id for args in signed
    ]


def test_the_knock_reports_to_this_accounts_trace_and_heals_its_client(knocks) -> None:
    exchange = _exchange()
    rows = []
    exchange.attempt_trace = rows.append
    resolved = []
    exchange.client._ClobClient__resolve_version = lambda force_update: resolved.append(force_update)

    _place(exchange)

    hooks = knocks.hooks[0]["primary"]
    assert hooks.trace == rows.append
    hooks.version_mismatch()
    hooks.version_mismatch()  # a second one inside the window is not acted on
    deadline = time.monotonic() + 2
    while not resolved and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.05)
    assert resolved == [True]


def test_entries_are_post_only_gtc_without_expiration(monkeypatch) -> None:
    exchange = _exchange()
    _place(exchange)
    assert [args.expiration for args in exchange.client.created_order_args] == [0, 0]
    assert [o.neg_risk for o in exchange.client.created_order_options] == [False, False]

    # The body that goes on the wire, from a real signed order.
    import py_clob_client_v2.http_helpers.helpers as helpers
    from py_clob_client_v2 import (
        ClobClient,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
        PostOrdersV2Args,
    )

    class NoNetwork:
        def request(self, *args, **kwargs):
            raise AssertionError("signing went to the network")

    monkeypatch.setattr(helpers, "_http_client", NoNetwork())
    monkeypatch.setattr(Exchange, "_order_body", REAL_ORDER_BODY)
    client = ClobClient(
        host="https://clob.polymarket.com", chain_id=137, key="0x" + "11" * 32,
        creds=CREDS, signature_type=0, retry_on_error=False,
    )
    client._ClobClient__tick_sizes["123"] = "0.01"
    client._ClobClient__cached_version = 2
    order = client.create_order(
        OrderArgs(token_id="123", price=0.01, size=100.0, side=Side.BUY),
        PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
    )
    real = _exchange(client)
    body = json.loads(real._order_body(PostOrdersV2Args(order=order, orderType=OrderType.GTC)))
    assert (body["orderType"], body["postOnly"], body["owner"]) == ("GTC", True, "key")
    assert body["order"]["expiration"] == "0" or body["order"]["expiration"] == 0


def test_solo_mode_knocks_only_the_chosen_leg_and_never_cancels_it(knocks) -> None:
    exchange = _exchange(entry_submission="solo-up")

    result = _place(exchange)

    assert [leg["outcome"] for leg in knocks.plans[0]["members"][0]["legs"]] == ["up"]
    assert result.complete
    assert [order.outcome for order in result.orders] == ["up"]
    assert exchange.client.canceled == []


def test_a_registered_pair_comes_back_with_its_replies_and_times(knocks) -> None:
    exchange = _exchange()
    knocks.outcomes.append(
        outcome(
            accepted=[("up", "0xup"), ("down", "0xdown")],
            attempts=41,
            held_back=3,
            registered_ms=1_790_400_000_123,
        )
    )

    result = _place(exchange)

    assert result.complete
    assert (result.attempts, result.held_back, result.registered_ts_ms) == (41, 3, 1_790_400_000_123)
    up, down = result.orders
    assert (up.order_id, up.outcome, up.token_id, up.status, up.side, up.role) == (
        "0xup", "up", "up-token", "live", "buy", "entry",
    )
    assert up.raw == {"success": True, "orderID": "0xup", "status": "live"}
    assert (down.order_id, down.token_id) == ("0xdown", "down-token")
    assert exchange._dual_submission_cache() == {}


def test_a_duplicate_reply_counts_as_the_order_it_names(knocks) -> None:
    order_id = "0x" + "ab" * 32
    duplicate = {"error": f"order {order_id} is invalid. duplicated."}
    exchange = _exchange(entry_submission="solo-up")
    knocks.outcomes.append(
        {
            **outcome(),
            "accepted": [{"outcome": "up", "order_id": order_id, **reply(400, duplicate)}],
        }
    )

    [order] = _place(exchange).orders

    assert order.order_id == order_id
    assert order.status == "open"
    assert order.raw == {"errorMsg": duplicate["error"], "success": False}


def test_partial_pair_is_canceled_when_the_market_ends(knocks) -> None:
    """One leg registered, the other never made it: the resting single leg
    is a naked position, so it is cancelled rather than kept."""
    exchange = _exchange()
    knocks.outcomes.append(
        outcome(
            accepted=[("up", "up-id")],
            errors=[
                reply(400, {"error": "invalid token id"}),
                text("market ended before both orders were accepted"),
            ],
        )
    )

    result = _place(exchange)

    assert not result.complete
    assert not result.retryable
    assert [order.order_id for order in result.orders] == ["up-id"]
    assert result.error == (
        "{'errorMsg': 'invalid token id', 'success': False}; "
        "market ended before both orders were accepted"
    )
    assert exchange.client.canceled == ["up-id"]


def test_business_rejection_is_terminal_and_worded_as_before(knocks) -> None:
    exchange = _exchange()
    knocks.outcomes.append(
        outcome(errors=[reply(400, {"error": "not enough balance / allowance"})], attempts=1)
    )

    result = _place(exchange)

    assert result.orders == ()
    assert not result.retryable
    assert result.error == "{'errorMsg': 'not enough balance / allowance', 'success': False}"
    assert exchange.client.canceled == []
    # Terminal: the signed pair is let go, the next placement signs anew.
    _place(exchange)
    assert len(exchange.client.created_order_args) == 4


def test_knocking_gives_up_after_its_budget_and_is_not_retried(knocks) -> None:
    exchange = _exchange()
    knocks.outcomes.append(
        outcome(
            errors=[
                reply(400, {"error": "invalid token id"}),
                text("no acceptance within the knocking budget"),
            ],
            attempts=9600,
            held_back=7,
            gave_up=True,
        )
    )

    result = _place(exchange, knock_until_ts=time.time() + 0.3)

    assert result.gave_up
    assert not result.retryable
    assert result.orders == ()
    assert (result.attempts, result.held_back) == (9600, 7)
    assert "no acceptance within the knocking budget" in result.error
    assert exchange.client.canceled == []


def test_only_sends_without_a_verdict_leave_the_placement_ambiguous(knocks) -> None:
    exchange = _exchange()
    knocks.outcomes.append(
        outcome(
            ambiguous=[reply(0, ""), reply(503, ""), reply(429, {"error": "Too Many Requests"})],
            errors=[text("market ended before both orders were accepted")],
            attempts=12,
        )
    )

    with pytest.raises(AmbiguousPlacementError) as caught:
        _place(exchange)

    assert caught.value.retryable
    assert caught.value.attempts == 12
    assert str(caught.value) == (
        "staggered submission remained ambiguous: "
        "PolyApiException: PolyApiException[status_code=None, error_message=Request exception!]; "
        "PolyApiException: PolyApiException[status_code=503, error_message=]; "
        "PolyApiException: PolyApiException[status_code=429, error_message={'error': 'Too Many Requests'}]"
    )


def test_a_retryable_failure_reuses_the_same_signed_orders(knocks) -> None:
    """Re-signing on retry would change the order fingerprint and orphan any
    copy the venue already holds; the cache must hand back the same tickets."""
    exchange = _exchange()
    knocks.outcomes.append(outcome(ambiguous=[reply(0, "")]))

    with pytest.raises(AmbiguousPlacementError):
        _place(exchange)
    second = _place(exchange)

    assert second.complete
    first_legs, second_legs = (plan["members"][0]["legs"] for plan in knocks.plans)
    assert first_legs == second_legs
    # signed exactly once per leg across both entries
    assert len(exchange.client.created_order_args) == 2


class _TickAskingClient(FakeClient):
    """A venue client shaped like the real one where signing is concerned.

    py_clob_client_v2 keeps resolved tick sizes in a dict on the client and
    only asks the venue when a token is missing from it; a market announced
    moments ago answers 404 to that lookup. Everything below mirrors that.
    """

    def __init__(self):
        super().__init__()
        self._ClobClient__tick_sizes = {}
        self.venue_lookups = []

    def create_order(self, order_args, options):
        token_id = order_args.token_id
        if token_id not in self._ClobClient__tick_sizes:
            self.venue_lookups.append(token_id)
            response = httpx.Response(404, json={"error": "market not found"})
            raise PolyApiException(response)
        return {"token_id": token_id, "side": order_args.side}


def test_signing_does_not_ask_the_venue_for_a_tick_size_we_already_have() -> None:
    """Discovery read the tick size off the listing; asking again costs a market.

    The client only fetches the market's minimum tick to check ours is not
    finer - the order it signs carries ours either way. That lookup is the one
    network call left in signing, and in run 18 it answered 404 for a member,
    which sat that account out of the whole market while the other one sent
    three thousand times.
    """
    exchange = _exchange(_TickAskingClient())

    result = _place(exchange)

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
    exchange = _exchange(_TickAskingClient())
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


def test_signing_does_not_go_to_the_venue_for_the_protocol_version() -> None:
    """The one network call left in signing, and it was still there.

    The client caches the protocol version but only after asking for it once,
    and nothing warmed it - so the first signature of every account still made
    a round trip, in front of the first market of a session.
    """
    exchange = _exchange(_TickAskingClient())
    exchange.client.version_lookups = 0

    def get_version():
        exchange.client.version_lookups += 1
        return 2

    exchange.client.get_version = get_version
    exchange.client._ClobClient__cached_version = None

    exchange._prime_tick_size(MARKET)

    assert exchange.client.version_lookups == 1, "the version was never warmed"
    assert exchange.client._ClobClient__cached_version == 2

    # and warming is idempotent: a second market does not ask again
    exchange._prime_tick_size(MARKET)
    assert exchange.client.version_lookups == 1


class _NotReadyThenSigningClient(FakeClient):
    """create_order answers 404 market-not-found a few times, then signs."""

    def __init__(self, failures):
        super().__init__()
        self.failures = failures
        self.create_calls = 0

    def create_order(self, order_args, options):
        self.create_calls += 1
        if self.create_calls <= self.failures:
            response = httpx.Response(404, json={"error": "market not found"})
            raise PolyApiException(response)
        return {"token_id": order_args.token_id, "side": order_args.side}


def test_signing_waits_out_the_post_announcement_404(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.0)
    exchange = _exchange(_NotReadyThenSigningClient(failures=3))

    result = _place(exchange)

    assert result.complete
    # three rejected attempts, then one attempt that signs both legs
    assert exchange.client.create_calls == 5


def test_signing_hands_the_market_back_after_the_in_place_budget(monkeypatch, knocks) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.05)
    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_RETRY_SECONDS", 0.3)
    exchange = _exchange(_NotReadyThenSigningClient(failures=100))

    result = _place(exchange)

    assert result.orders == ()
    assert result.retryable
    assert result.attempts == 1
    assert "signing not ready" in result.error
    # 50 ms pacing inside a 300 ms budget: a handful of polls, not one, not hundreds
    assert 4 <= exchange.client.create_calls <= 8
    assert knocks.plans == [], "nothing signed, nothing to knock with"


def test_signing_budget_is_wall_clock_even_when_replies_are_slow(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.01)
    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_RETRY_SECONDS", 0.25)

    class SlowNotReadyClient(_NotReadyThenSigningClient):
        def create_order(self, order_args, options):
            time.sleep(0.1)  # the venue's open-moment latency, far above the poll pace
            return super().create_order(order_args, options)

    exchange = _exchange(SlowNotReadyClient(failures=100))

    started = time.monotonic()
    result = _place(exchange)
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
        exchange = _exchange(TransientThenSigningClient(failure))
        result = _place(exchange)
        assert result.complete, failure
        assert exchange.client.create_calls == 3


def test_signing_does_not_retry_other_rejections(monkeypatch) -> None:
    import polymarket_bot.exchange as exchange_module

    monkeypatch.setattr(exchange_module, "SIGNING_NOT_READY_POLL_SECONDS", 0.0)

    class RejectingClient(_NotReadyThenSigningClient):
        def __init__(self, status, message):
            super().__init__(failures=0)
            self.status = status
            self.message = message

        def create_order(self, order_args, options):
            self.create_calls += 1
            raise PolyApiException(httpx.Response(self.status, json={"error": self.message}))

    for status, message in ((400, "invalid price"), (400, "invalid token id")):
        exchange = _exchange(RejectingClient(status, message))
        with pytest.raises(PolyApiException):
            _place(exchange)
        assert exchange.client.create_calls == 1


def test_ambiguous_submission_adopts_exact_pair() -> None:
    exchange = _exchange()
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
    exchange = _exchange()
    exchange.open_orders = lambda condition_id: []

    result = exchange.reconcile_ambiguous_dual(
        MARKET, price=Decimal("0.01"), size=Decimal("100")
    )

    assert not result.complete
    assert result.orders == ()
    assert result.retryable


def test_exit_is_a_non_post_only_sell_limit() -> None:
    exchange = _exchange()

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
