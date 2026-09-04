import time
from dataclasses import replace
from decimal import Decimal

from polymarket_bot.market_activation import (
    AOT_WAIT_SECONDS,
    CLOB_MARKETS,
    MARKET_SECONDS,
    MarketActivationWorker,
    _Candidate,
)
from polymarket_bot.models import Market


BASE_TS = int(time.time()) + 3600


def market(offset: int, name: str) -> Market:
    start_ts = BASE_TS + offset
    return Market(
        slug=f"btc-updown-5m-{start_ts}",
        condition_id=f"condition-{name}",
        start_ts=start_ts,
        end_ts=start_ts + 300,
        up_token_id=f"up-{name}",
        down_token_id=f"down-{name}",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )


AOT_TS = 1_788_405_466  # market_payload()'s default aot, 2026-09-03T03:17:46Z
ACTIVE = market(0, "active")
FUTURE = market(300, "future")
NEWER = market(600, "newer")


class Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class MarketSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def get(self, url, *, timeout):
        self.requests.append((url, timeout))
        return self.responses.pop(0)


class Discovery:
    def __init__(self, initial, by_slug):
        self.initial = initial
        self.by_slug = dict(by_slug)
        self.discover_calls = []
        self.slug_calls = []

    def discover(self, window_minutes, **kwargs):
        self.discover_calls.append((window_minutes, kwargs))
        return self.initial

    def find_by_slug(self, slug, **kwargs):
        self.slug_calls.append((slug, kwargs))
        return self.by_slug.get(slug)


def worker() -> MarketActivationWorker:
    return MarketActivationWorker(
        window_minutes=30,
        farthest_first=True,
    )


def market_payload(item: Market, aot: str | None = "2026-09-03T03:17:46Z") -> dict:
    payload = {
        "t": [
            {"t": item.up_token_id},
            {"t": item.down_token_id},
        ]
    }
    if aot is not None:
        payload["aot"] = aot
    return payload


def test_gamma_registration_tracks_each_unexpired_market() -> None:
    activation = worker()

    activation._register_candidates([ACTIVE, FUTURE])

    assert tuple(activation._candidates) == (ACTIVE.slug, FUTURE.slug)
    assert activation._handled_slugs == set()


def test_parameter_poll_without_candidates_is_idle() -> None:
    activation = worker()

    assert activation._poll_market_parameters() is False
    assert activation.drain() == []


def test_missing_clob_market_remains_pending() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE])
    activation.market_session.close()
    activation.market_session = MarketSession([Response({}, 404)])

    assert activation._poll_market_parameters() is True
    assert tuple(activation._candidates) == (ACTIVE.slug,)
    assert activation.drain() == []


def test_complete_clob_parameters_emit_market() -> None:
    activation = worker()
    activation._register_candidates(
        [ACTIVE], market_discovered_ts_ms=1_999_999_000_000
    )
    activation.market_session.close()
    activation.market_session = MarketSession([Response(market_payload(ACTIVE))])

    before_poll_ts_ms = int(time.time() * 1000)
    assert activation._poll_market_parameters() is True
    after_poll_ts_ms = int(time.time() * 1000)
    update = activation.drain()[0]
    assert update.market == replace(ACTIVE, accepting_orders_ts=AOT_TS)
    assert update.market_discovered_ts_ms == 1_999_999_000_000
    assert (
        before_poll_ts_ms
        <= update.market_parameters_detected_ts_ms
        <= after_poll_ts_ms
    )
    assert activation._candidates == {}
    assert activation._handled_slugs == {ACTIVE.slug}
    assert activation.market_session.requests == [
        (f"{CLOB_MARKETS}/{ACTIVE.condition_id}", 2)
    ]


def test_incomplete_clob_parameters_remain_pending() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE])
    activation.market_session.close()
    activation.market_session = MarketSession(
        [Response({"t": [{"t": ACTIVE.up_token_id}]})]
    )

    activation._poll_market_parameters()

    assert tuple(activation._candidates) == (ACTIVE.slug,)
    assert activation.drain() == []


def test_out_of_order_activation_keeps_each_market_independent() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE, FUTURE, NEWER])
    activation.market_session.close()
    activation.market_session = MarketSession(
        [
            Response({}, 404),
            Response({}, 404),
            Response(market_payload(NEWER)),
            Response({}, 404),
            Response(market_payload(FUTURE)),
        ]
    )

    activation._poll_market_parameters()
    assert activation.drain()[0].market == replace(NEWER, accepting_orders_ts=AOT_TS)
    assert tuple(activation._candidates) == (ACTIVE.slug, FUTURE.slug)

    activation._poll_market_parameters()
    assert activation.drain()[0].market == replace(FUTURE, accepting_orders_ts=AOT_TS)
    assert tuple(activation._candidates) == (ACTIVE.slug,)
    assert activation._handled_slugs == {FUTURE.slug, NEWER.slug}


def test_discovery_advances_only_when_the_next_slug_exists() -> None:
    activation = worker()
    next_market = market(900, "next")
    next_slug = f"btc-updown-5m-{NEWER.start_ts + MARKET_SECONDS}"
    next_market = Market(
        slug=next_slug,
        condition_id=next_market.condition_id,
        start_ts=NEWER.start_ts + MARKET_SECONDS,
        end_ts=NEWER.start_ts + (2 * MARKET_SECONDS),
        up_token_id=next_market.up_token_id,
        down_token_id=next_market.down_token_id,
        min_size=next_market.min_size,
        tick_size=next_market.tick_size,
    )
    following_slug = (
        f"btc-updown-5m-{next_market.start_ts + MARKET_SECONDS}"
    )
    activation.discovery = Discovery(
        [ACTIVE, FUTURE, NEWER],
        {next_slug: next_market},
    )

    activation._poll_discovery()

    assert activation.discovery.discover_calls == [
        (
            30,
            {
                "farthest_first": True,
                "timeout": 5,
                "fresh": True,
            },
        )
    ]
    assert activation.discovery.slug_calls == [
        (next_slug, {"timeout": 5, "fresh": True}),
        (following_slug, {"timeout": 5, "fresh": True}),
    ]
    assert activation._next_start_ts == next_market.start_ts + MARKET_SECONDS
    assert tuple(activation._candidates) == (
        ACTIVE.slug,
        FUTURE.slug,
        NEWER.slug,
        next_market.slug,
    )


def test_zero_window_seeds_cursor_without_registering_existing_market() -> None:
    activation = MarketActivationWorker(
        window_minutes=0,
        farthest_first=True,
    )
    next_slug = f"btc-updown-5m-{NEWER.start_ts + MARKET_SECONDS}"
    next_market = Market(
        slug=next_slug,
        condition_id="next",
        start_ts=NEWER.start_ts + MARKET_SECONDS,
        end_ts=NEWER.end_ts + MARKET_SECONDS,
        up_token_id="next-up",
        down_token_id="next-down",
        min_size=NEWER.min_size,
        tick_size=NEWER.tick_size,
    )
    activation.discovery = Discovery([NEWER], {next_slug: next_market})

    activation._poll_discovery()

    assert activation.discovery.discover_calls == [
        (
            5,
            {
                "farthest_first": True,
                "timeout": 5,
                "fresh": True,
            },
        )
    ]
    assert tuple(activation._candidates) == (next_market.slug,)
    assert NEWER.slug not in activation._handled_slugs


def test_clob_accepting_order_timestamp_rides_along_with_the_market() -> None:
    """The listing's `aot` reaches the placement loop on the market itself."""
    activation = worker()
    activation._register_candidates([ACTIVE], market_discovered_ts_ms=1)
    activation.market_session.close()
    activation.market_session = MarketSession([Response(market_payload(ACTIVE))])

    assert activation._poll_market_parameters() is True
    update = activation.drain()[0]
    assert update.market.accepting_orders_ts == 1_788_405_466
    assert update.market.slug == ACTIVE.slug
    assert update.market.up_token_id == ACTIVE.up_token_id


def test_tokens_without_aot_wait_for_it_before_the_market_is_emitted() -> None:
    """The venue writes `aot` seconds after the tokens; the loop needs both."""
    activation = worker()
    activation._register_candidates([ACTIVE], market_discovered_ts_ms=1)
    activation.market_session.close()
    activation.market_session = MarketSession(
        [
            Response(market_payload(ACTIVE, aot=None)),
            Response(market_payload(ACTIVE, aot="not a timestamp")),
            Response(market_payload(ACTIVE)),
        ]
    )

    assert activation._poll_market_parameters() is True
    assert activation.drain() == []
    first_seen = activation._candidates[ACTIVE.slug].parameters_seen_ts_ms
    assert first_seen is not None

    assert activation._poll_market_parameters() is True
    assert activation.drain() == []
    assert activation._candidates[ACTIVE.slug].parameters_seen_ts_ms == first_seen

    assert activation._poll_market_parameters() is True
    update = activation.drain()[0]
    assert update.market.accepting_orders_ts == 1_788_405_466
    assert update.market_parameters_detected_ts_ms == first_seen
    assert activation._candidates == {}


def test_a_listing_that_never_shows_aot_is_emitted_ungated_after_the_wait() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE], market_discovered_ts_ms=1)
    long_ago = int(time.time() * 1000) - int(AOT_WAIT_SECONDS * 1000) - 1
    activation._candidates[ACTIVE.slug] = _Candidate(ACTIVE, 1, long_ago)
    activation.market_session.close()
    activation.market_session = MarketSession(
        [Response(market_payload(ACTIVE, aot=None))]
    )

    assert activation._poll_market_parameters() is True
    update = activation.drain()[0]
    assert update.market.accepting_orders_ts is None
    assert update.market_parameters_detected_ts_ms == long_ago
    assert activation._candidates == {}
