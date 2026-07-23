import time
from decimal import Decimal

from polymarket_bot.market_activation import MarketActivationWorker
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
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


class BooksSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def post(self, url, *, json, timeout):
        self.requests.append((url, json, timeout))
        return Response(self.responses.pop(0))


def worker() -> MarketActivationWorker:
    return MarketActivationWorker(
        queue_price=Decimal("0.01"),
        window_minutes=30,
        farthest_first=True,
    )


def test_gamma_registration_tracks_each_unexpired_market() -> None:
    activation = worker()

    activation._register_candidates([ACTIVE, FUTURE])

    assert tuple(activation._candidates) == (ACTIVE.slug, FUTURE.slug)
    assert activation._handled_slugs == set()


def test_book_poll_without_candidates_is_idle() -> None:
    activation = worker()

    assert activation._poll_books() is False
    assert activation.drain() == []


def test_existing_books_emit_immediately_without_startup_baseline() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE])
    activation.books_session.close()
    activation.books_session = BooksSession(
        [
            [
                {"asset_id": ACTIVE.up_token_id, "bids": []},
                {"asset_id": ACTIVE.down_token_id, "bids": []},
            ]
        ]
    )

    assert activation._poll_books() is True
    update = activation.drain()[0]
    assert update.market == ACTIVE
    assert activation._candidates == {}
    assert activation._handled_slugs == {ACTIVE.slug}


def test_later_gamma_refresh_tracks_only_new_market() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE])
    activation.books_session.close()
    activation.books_session = BooksSession(
        [
            [
                {"asset_id": ACTIVE.up_token_id, "bids": []},
                {"asset_id": ACTIVE.down_token_id, "bids": []},
            ],
            [
                {"asset_id": FUTURE.up_token_id, "bids": []},
                {"asset_id": FUTURE.down_token_id, "bids": []},
            ],
        ]
    )
    activation._poll_books()
    activation.drain()

    activation._register_candidates([ACTIVE, FUTURE])

    assert tuple(activation._candidates) == (FUTURE.slug,)
    activation._poll_books()
    assert activation.drain()[0].market == FUTURE


def test_batch_books_emits_only_after_both_books_exist() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE, FUTURE])
    activation.books_session.close()
    activation.books_session = BooksSession(
        [
            [
                {"asset_id": ACTIVE.up_token_id, "bids": []},
                {"asset_id": ACTIVE.down_token_id, "bids": []},
                {"asset_id": FUTURE.up_token_id, "bids": []},
            ],
            [
                {
                    "asset_id": FUTURE.up_token_id,
                    "bids": [{"price": "0.01", "size": "12.5"}],
                },
                {
                    "asset_id": FUTURE.down_token_id,
                    "bids": [{"price": "0.01", "size": "8.25"}],
                },
            ],
        ]
    )

    assert activation._poll_books() is True
    first_update = activation.drain()[0]
    assert first_update.market == ACTIVE
    assert tuple(activation._candidates) == (FUTURE.slug,)

    assert activation._poll_books() is True
    update = activation.drain()[0]
    assert update.market == FUTURE
    assert update.queue_ahead_up == Decimal("12.5")
    assert update.queue_ahead_down == Decimal("8.25")
    assert activation._candidates == {}

    first_request_tokens = {
        item["token_id"] for item in activation.books_session.requests[0][1]
    }
    assert first_request_tokens == {
        ACTIVE.up_token_id,
        ACTIVE.down_token_id,
        FUTURE.up_token_id,
        FUTURE.down_token_id,
    }
    second_request_tokens = {
        item["token_id"] for item in activation.books_session.requests[1][1]
    }
    assert second_request_tokens == {
        FUTURE.up_token_id,
        FUTURE.down_token_id,
    }


def test_out_of_order_activation_keeps_each_market_independent() -> None:
    activation = worker()
    activation._register_candidates([ACTIVE, FUTURE, NEWER])
    activation.books_session.close()
    activation.books_session = BooksSession(
        [
            [
                {"asset_id": NEWER.up_token_id, "bids": []},
                {"asset_id": NEWER.down_token_id, "bids": []},
            ],
            [
                {"asset_id": FUTURE.up_token_id, "bids": []},
                {"asset_id": FUTURE.down_token_id, "bids": []},
            ],
        ]
    )

    activation._poll_books()
    assert activation.drain()[0].market == NEWER
    assert tuple(activation._candidates) == (ACTIVE.slug, FUTURE.slug)

    activation._poll_books()
    assert activation.drain()[0].market == FUTURE
    assert tuple(activation._candidates) == (ACTIVE.slug,)
    assert activation._handled_slugs == {FUTURE.slug, NEWER.slug}
