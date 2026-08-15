import json
import logging
import time
from decimal import Decimal

from polymarket_bot.book_signal import (
    BookOpenSignal,
    books_mention_assets,
    payload_mentions_assets,
)
from polymarket_bot.database import BotDatabase
from polymarket_bot.models import Market, PlacedOrder, PlacementResult, TradePlan
from polymarket_bot.service import BotService


ASSETS = frozenset({"11", "22"})


def _market() -> Market:
    now = int(time.time())
    return Market(
        slug="btc-updown-5m-signal-test",
        condition_id="0xcond",
        start_ts=now + 300,
        end_ts=now + 600,
        up_token_id="11",
        down_token_id="22",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )


def test_payload_matching_covers_books_and_price_changes() -> None:
    assert payload_mentions_assets({"event_type": "book", "asset_id": "11"}, ASSETS)
    assert payload_mentions_assets(
        [{"event_type": "price_change", "price_changes": [{"asset_id": "22"}]}],
        ASSETS,
    )
    assert not payload_mentions_assets({"event_type": "book", "asset_id": "99"}, ASSETS)
    assert not payload_mentions_assets("PONG", ASSETS)
    assert not payload_mentions_assets([{"price_changes": [{"asset_id": "99"}]}], ASSETS)


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def send(self, data):
        self.sent.append(data)

    def recv(self, timeout=None):
        if self.messages:
            return self.messages.pop(0)
        raise TimeoutError


def test_books_matching_requires_listed_book() -> None:
    assert books_mention_assets([{"asset_id": "11", "bids": []}], ASSETS)
    assert not books_mention_assets([{"asset_id": "99"}], ASSETS)
    assert not books_mention_assets({"asset_id": "11"}, ASSETS)
    assert not books_mention_assets([], ASSETS)


def test_signal_fires_on_first_matching_event() -> None:
    sock = FakeSocket([json.dumps({"event_type": "book", "asset_id": "11"})])
    signal = BookOpenSignal(
        "11",
        "22",
        url="ws://test",
        connect_fn=lambda url, **kw: sock,
        rest_poll_ms=None,
    )
    try:
        assert signal.wait(2.0)
        assert signal.signal_ts_ms is not None
        subscription = json.loads(sock.sent[0])
        assert set(subscription["assets_ids"]) == {"11", "22"}
    finally:
        signal.close()


def test_signal_ignores_noise_and_reconnects_after_failure() -> None:
    attempts = {"n": 0}

    def connect_fn(url, **kw):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise OSError("stream unreachable")
        return FakeSocket(
            [
                "PONG",
                json.dumps({"event_type": "book", "asset_id": "99"}),
                json.dumps(
                    [
                        {
                            "event_type": "price_change",
                            "price_changes": [{"asset_id": "22", "price": "0.01"}],
                        }
                    ]
                ),
            ]
        )

    signal = BookOpenSignal(
        "11", "22", url="ws://test", connect_fn=connect_fn, rest_poll_ms=None
    )
    try:
        assert signal.wait(3.0)
    finally:
        signal.close()
    assert attempts["n"] >= 2


def test_close_stops_watcher_without_signal() -> None:
    signal = BookOpenSignal(
        "11",
        "22",
        url="ws://test",
        connect_fn=lambda url, **kw: FakeSocket([]),
        rest_poll_ms=None,
    )
    assert not signal.wait(0.2)
    signal.close()
    assert signal.signal_ts_ms is None


def test_rest_probe_wins_the_signal_race() -> None:
    signal = BookOpenSignal(
        "11",
        "22",
        url="ws://test",
        connect_fn=lambda url, **kw: FakeSocket([]),
        rest_poll_ms=5,
        books_fn=lambda: True,
    )
    try:
        assert signal.wait(2.0)
        assert signal.signal_source == "rest"
        assert signal.signal_ts_ms is not None
    finally:
        signal.close()


def test_rest_probe_survives_errors_until_book_appears() -> None:
    calls = {"n": 0}

    def probe():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("books endpoint hiccup")
        return calls["n"] >= 3

    signal = BookOpenSignal(
        "11",
        "22",
        url="ws://test",
        connect_fn=lambda url, **kw: FakeSocket([]),
        rest_poll_ms=5,
        books_fn=probe,
    )
    try:
        assert signal.wait(2.0)
        assert signal.signal_source == "rest"
    finally:
        signal.close()
    assert calls["n"] >= 3


class RecordingExchange:
    def __init__(self):
        self.calls = []

    def place_dual(self, market, *, price, size, submission_interval_ms):
        self.calls.append(submission_interval_ms)
        return PlacementResult(
            tuple(
                PlacedOrder(
                    order_id=f"{outcome}-1",
                    outcome=outcome,
                    token_id=getattr(market, f"{outcome}_token_id"),
                    price=price,
                    size=size,
                    status="live",
                    raw={},
                )
                for outcome in ("up", "down")
            )
        )


class InstantSignal:
    def __init__(self, up_token_id, down_token_id):
        self.signal_ts_ms = time.time_ns() // 1_000_000
        self.error = None

    def wait(self, timeout):
        return True

    def close(self):
        pass


class NeverSignal:
    def __init__(self, up_token_id, down_token_id):
        self.signal_ts_ms = None
        self.error = "market stream unreachable"

    def wait(self, timeout):
        return False

    def close(self):
        pass


def _service(database, run_id, exchange, factory):
    service = BotService.__new__(BotService)
    service.database = database
    service.exchange = exchange
    service.run_id = run_id
    service.plan = TradePlan(Decimal("0.01"), (), Decimal("1"))
    service.live = True
    service.logger = logging.getLogger("test")
    service._placement_retries = {}
    service.book_signal_factory = factory
    return service


def test_place_waits_for_book_signal_and_records_telemetry(tmp_path) -> None:
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = RecordingExchange()
        service = _service(database, run_id, exchange, InstantSignal)

        service._place(_market(), trigger="test")

        assert exchange.calls
        row = database.connection.execute(
            "SELECT details_json FROM events WHERE event_type='dual_orders_placed'"
        ).fetchone()
        details = json.loads(row["details_json"])
        assert "book_signal_ts_ms" in details
        assert details["book_signal_wait_ms"] >= 0


def test_place_skips_market_when_signal_never_fires(tmp_path) -> None:
    market = _market()
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        exchange = RecordingExchange()
        service = _service(database, run_id, exchange, NeverSignal)

        service._place(market, trigger="test")

        assert exchange.calls == []
        row = database.connection.execute(
            "SELECT state, error FROM markets WHERE slug=?", (market.slug,)
        ).fetchone()
        assert row["state"] == "error"
        assert "unreachable" in row["error"]
        count = database.connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='book_signal_timeout'"
        ).fetchone()[0]
        assert count == 1
