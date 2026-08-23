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
