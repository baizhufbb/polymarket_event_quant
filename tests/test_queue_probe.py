from decimal import Decimal

from polymarket_bot.queue_probe import _timestamp_ms, _update_queues


TOKENS = {"up-token": "up", "down-token": "down"}


def test_book_snapshot_reads_only_the_target_bid_level() -> None:
    event = {
        "event_type": "book",
        "market": "condition",
        "asset_id": "up-token",
        "bids": [
            {"price": "0.01", "size": "125.5"},
            {"price": "0.02", "size": "80"},
        ],
        "asks": [],
        "timestamp": "2000000000000",
    }
    queues = {}

    changed = _update_queues(event, token_outcomes=TOKENS, queues=queues)

    assert changed
    assert queues == {"up": Decimal("125.5")}


def test_price_change_replaces_the_target_bid_level() -> None:
    event = {
        "event_type": "price_change",
        "market": "condition",
        "timestamp": "2000000000000",
        "price_changes": [
            {
                "asset_id": "up-token",
                "price": "0.01",
                "size": "225.5",
                "side": "BUY",
            },
            {
                "asset_id": "down-token",
                "price": "0.01",
                "size": "70",
                "side": "SELL",
            },
        ],
    }
    queues = {"up": Decimal("125.5"), "down": Decimal("50")}

    changed = _update_queues(event, token_outcomes=TOKENS, queues=queues)

    assert changed
    assert queues == {"up": Decimal("225.5"), "down": Decimal("50")}


def test_timestamp_is_preserved_in_epoch_milliseconds() -> None:
    assert _timestamp_ms("2000000000000") == 2_000_000_000_000


def test_each_probe_writes_its_own_file() -> None:
    """Two probes sharing one file wrote over each other's lines.

    It only happened while both were writing fast, and the only time they
    write fast is the burst after a market opens - which is the burst the
    whole recording exists for. Two processes cannot share a process id.
    """
    import os

    from polymarket_bot import queue_probe

    path = queue_probe.output_path()
    assert str(os.getpid()) in path.name
    assert path.name.startswith(queue_probe.OUTPUT_PREFIX)
    assert path.suffix == ".jsonl"
    # and the reading side can still find them all
    assert path.match(f"{queue_probe.OUTPUT_PREFIX}*.jsonl")


def test_a_dropped_socket_is_reconnected_and_the_opening_still_recorded(monkeypatch, tmp_path) -> None:
    """The venue drops idle sockets; a book that opens hours later must still be caught."""
    import asyncio
    import io
    import json
    import websockets
    from polymarket_bot import queue_probe
    from polymarket_bot.market_activation import MarketActivationUpdate
    from decimal import Decimal
    from polymarket_bot.models import Market

    market = Market(
        slug="btc-updown-5m-1",
        condition_id="0xc",
        start_ts=1_800_000_000,
        end_ts=1_800_000_300,
        up_token_id="up-token",
        down_token_id="down-token",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )
    update = MarketActivationUpdate(
        market=market, market_discovered_ts_ms=1, market_parameters_detected_ts_ms=2
    )
    book = lambda asset, size: json.dumps(  # noqa: E731
        {"event_type": "book", "asset_id": asset, "timestamp": "1800000000000",
         "bids": [{"price": "0.01", "size": size}], "asks": []}
    )
    scripts = [
        ["closed"],                                   # first connection dies at once
        [book("up-token", "300"), book("down-token", "91")],  # second one sees the opening
    ]
    connections = []

    class FakeSocket:
        def __init__(self, script):
            self.script = list(script)
            self.sent = []
        async def send(self, data):
            self.sent.append(data)
        async def recv(self):
            if not self.script:
                await asyncio.sleep(3600)
            item = self.script.pop(0)
            if item == "closed":
                raise websockets.ConnectionClosedError(None, None)
            return item

    class FakeConnect:
        def __init__(self, *args, **kwargs):
            self.socket = FakeSocket(scripts[len(connections)])
            connections.append(self.socket)
        async def __aenter__(self):
            return self.socket
        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(queue_probe.websockets, "connect", FakeConnect)
    monkeypatch.setattr(queue_probe, "RECONNECT_DELAY_SECONDS", 0)
    output = io.StringIO()

    asyncio.run(queue_probe._monitor_market(update, output, opening_window_seconds=0.05))

    assert len(connections) == 2
    assert all(json.loads(c.sent[0])["type"] == "market" for c in connections)
    rows = [json.loads(line) for line in output.getvalue().splitlines()]
    # nothing is recorded until both sides of the book have been seen
    assert [(r["up_queue_shares"], r["down_queue_shares"]) for r in rows] == [("300", "91")]
