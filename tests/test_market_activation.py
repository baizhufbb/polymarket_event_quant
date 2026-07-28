import time
from decimal import Decimal

from polymarket_bot.market_activation import (
    CLOB_MARKETS,
    MARKET_SECONDS,
    MarketActivationWorker,
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


def market_payload(item: Market) -> dict:
    return {
        "t": [
            {"t": item.up_token_id},
            {"t": item.down_token_id},
        ]
    }


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
    assert update.market == ACTIVE
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


def test_retryable_market_returns_to_parameter_polling() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE])
    activation.market_session.close()
    activation.market_session = MarketSession([Response(market_payload(ACTIVE))])
    activation._poll_market_parameters()
    activation.drain()

    assert activation.requeue(
        ACTIVE, market_discovered_ts_ms=1_999_999_000_000
    )
    assert tuple(activation._candidates) == (ACTIVE.slug,)
    assert ACTIVE.slug not in activation._handled_slugs
    assert (
        activation._candidates[ACTIVE.slug].market_discovered_ts_ms
        == 1_999_999_000_000
    )


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
    assert activation.drain()[0].market == NEWER
    assert tuple(activation._candidates) == (ACTIVE.slug, FUTURE.slug)

    activation._poll_market_parameters()
    assert activation.drain()[0].market == FUTURE
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
