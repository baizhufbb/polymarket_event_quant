from decimal import Decimal
from threading import Event

from polymarket_bot.market_activation import MarketActivationWorker
from polymarket_bot.models import Market


MARKET = Market(
    slug="btc-updown-5m-2000000300",
    condition_id="0xcondition",
    start_ts=2_000_000_300,
    end_ts=2_000_000_600,
    up_token_id="up-token",
    down_token_id="down-token",
    min_size=Decimal("5"),
    tick_size=Decimal("0.01"),
)


class Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def worker_with(responses) -> MarketActivationWorker:
    worker = MarketActivationWorker.__new__(MarketActivationWorker)
    worker.queue_price = Decimal("0.01")
    worker.session = Session(responses)
    return worker


def test_candidate_waits_until_clob_accepts_orders() -> None:
    worker = worker_with([Response({"accepting_orders": False})])

    assert worker._poll_candidate(MARKET) is None
    assert len(worker.session.calls) == 1


def test_candidate_emits_after_both_books_exist() -> None:
    worker = worker_with(
        [
            Response(
                {
                    "accepting_orders": True,
                    "accepting_order_timestamp": "2033-05-18T03:31:40Z",
                }
            ),
            Response({"bids": [{"price": "0.01", "size": "12.5"}]}),
            Response({"bids": [{"price": "0.01", "size": "8.25"}]}),
        ]
    )

    update = worker._poll_candidate(MARKET)

    assert update is not None
    assert update.market == MARKET
    assert update.queue_ahead_up == Decimal("12.5")
    assert update.queue_ahead_down == Decimal("8.25")
    assert update.accepting_ts_ms == 1_999_999_900_000
    assert len(worker.session.calls) == 3


def test_candidate_discovery_starts_after_farthest_active_market() -> None:
    farthest = Market(
        slug="btc-updown-5m-2000000000",
        condition_id="farthest-condition",
        start_ts=2_000_000_000,
        end_ts=2_000_000_300,
        up_token_id="farthest-up",
        down_token_id="farthest-down",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )

    class Discovery:
        def __init__(self):
            self.slugs = []

        def discover(self, window_minutes, *, farthest_first, timeout):
            assert window_minutes == 5
            assert farthest_first is True
            assert timeout == 2
            return [farthest]

        def candidate(self, slug):
            self.slugs.append(slug)
            return MARKET if slug == MARKET.slug else None

    worker = MarketActivationWorker.__new__(MarketActivationWorker)
    worker.discovery = Discovery()
    worker._stop = Event()
    worker._next_start_ts = None
    worker._candidates = {}

    worker._discover_candidates()

    assert worker.discovery.slugs == [MARKET.slug, "btc-updown-5m-2000000600"]
    assert worker._candidates == {MARKET.slug: MARKET}
