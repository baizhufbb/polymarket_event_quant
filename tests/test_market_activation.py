from decimal import Decimal

from polymarket_bot.market_activation import (
    MarketActivationWorker,
    _catalog_market,
)


ACTIVE_CONDITION = (
    "0xbc2143c70ad2af9481e8dd46eb538f267d7c23ad781ddf5380e9a00d46e9e9cd"
)
FUTURE_CONDITION = (
    "0xc46ba93f4d9553e8b4c4c1c9d6cbde68426896a40713f98605c753316e66a706"
)
FUTURE_UP = (
    "40734400368422567759951081235288351135874433660498746514087358797347039269519"
)
FUTURE_DOWN = (
    "12816584899393367254956664538948649976215330480366631889555978899000587885894"
)
ACTIVE_UP = (
    "79690064268849976430077014249758289461450439393597627695915949752735045024504"
)
ACTIVE_DOWN = (
    "62179804371407207770226633627836131020602362404226320415203031505393883439742"
)


def catalog_row(start_ts: int, condition_id: str, *, accepting: bool) -> dict:
    return {
        "market_slug": f"btc-updown-5m-{start_ts}",
        "condition_id": condition_id,
        "minimum_order_size": 5,
        "minimum_tick_size": 0.01,
        "accepting_orders": accepting,
        "closed": False,
        "archived": False,
    }


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


def test_catalog_market_derives_tokens_before_activation() -> None:
    market = _catalog_market(
        catalog_row(1_784_857_200, FUTURE_CONDITION, accepting=False)
    )

    assert market is not None
    assert market.up_token_id == FUTURE_UP
    assert market.down_token_id == FUTURE_DOWN
    assert market.tick_size == Decimal("0.01")


def test_catalog_registers_only_markets_beyond_active_frontier() -> None:
    worker = MarketActivationWorker(queue_price=Decimal("0.01"))
    worker._register_candidates(
        [
            catalog_row(1_784_856_600, ACTIVE_CONDITION, accepting=False),
            catalog_row(1_784_856_900, ACTIVE_CONDITION, accepting=True),
            catalog_row(1_784_857_200, FUTURE_CONDITION, accepting=False),
        ]
    )

    assert worker._frontier_start_ts == 1_784_856_900
    assert tuple(worker._candidates) == ("btc-updown-5m-1784857200",)


def test_batch_books_emits_only_after_both_books_exist() -> None:
    worker = MarketActivationWorker(queue_price=Decimal("0.01"))
    worker._register_candidates(
        [
            catalog_row(1_784_856_900, ACTIVE_CONDITION, accepting=True),
            catalog_row(1_784_857_200, FUTURE_CONDITION, accepting=False),
        ]
    )
    worker.books_session.close()
    worker.books_session = BooksSession(
        [
            [{"asset_id": FUTURE_UP, "bids": []}],
            [
                {
                    "asset_id": FUTURE_UP,
                    "bids": [{"price": "0.01", "size": "12.5"}],
                },
                {
                    "asset_id": FUTURE_DOWN,
                    "bids": [{"price": "0.01", "size": "8.25"}],
                },
            ],
        ]
    )

    assert worker._poll_books() is True
    assert worker.drain() == []

    assert worker._poll_books() is True
    update = worker.drain()[0]
    assert update.market.slug == "btc-updown-5m-1784857200"
    assert update.queue_ahead_up == Decimal("12.5")
    assert update.queue_ahead_down == Decimal("8.25")
    assert worker._candidates == {}

    request_tokens = {
        item["token_id"] for item in worker.books_session.requests[0][1]
    }
    assert request_tokens == {FUTURE_UP, FUTURE_DOWN}


def test_newer_activation_discards_an_unavailable_older_candidate() -> None:
    worker = MarketActivationWorker(queue_price=Decimal("0.01"))
    worker._register_candidates(
        [
            catalog_row(1_784_856_900, ACTIVE_CONDITION, accepting=True),
            catalog_row(1_784_857_200, FUTURE_CONDITION, accepting=False),
            catalog_row(1_784_857_500, ACTIVE_CONDITION, accepting=False),
        ]
    )
    worker.books_session.close()
    worker.books_session = BooksSession(
        [
            [
                {"asset_id": ACTIVE_UP, "bids": []},
                {"asset_id": ACTIVE_DOWN, "bids": []},
            ]
        ]
    )

    worker._poll_books()

    update = worker.drain()[0]
    assert update.market.slug == "btc-updown-5m-1784857500"
    assert worker._candidates == {}
    assert worker._handled_slugs == {
        "btc-updown-5m-1784857200",
        "btc-updown-5m-1784857500",
    }


def test_tail_locator_finds_last_nonempty_page() -> None:
    worker = MarketActivationWorker(queue_price=Decimal("0.01"))

    def page(offset: int) -> dict:
        data = [{}] * 1000 if offset <= 3000 else []
        return {"data": data, "next_cursor": "next" if data else "LTE="}

    worker._catalog_page = page

    located = worker._locate_tail_page()

    assert located is not None
    assert located[0] == 3000
