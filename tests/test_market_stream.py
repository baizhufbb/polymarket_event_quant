from decimal import Decimal

from polymarket_bot.market_stream import _bid_size_at, _parse_new_market


def test_parses_btc_market_and_maps_outcomes_by_name() -> None:
    parsed = _parse_new_market(
        {
            "event_type": "new_market",
            "slug": "btc-updown-5m-2000000000",
            "market": "0xcondition",
            "assets_ids": ["down-token", "up-token"],
            "outcomes": ["Down", "Up"],
            "timestamp": "1999999000123",
            "order_price_min_tick_size": "0.01",
        }
    )

    assert parsed is not None
    market, event_ts_ms = parsed
    assert market.condition_id == "0xcondition"
    assert market.up_token_id == "up-token"
    assert market.down_token_id == "down-token"
    assert market.start_ts == 2_000_000_000
    assert event_ts_ms == 1_999_999_000_123


def test_ignores_other_markets() -> None:
    assert (
        _parse_new_market(
            {
                "event_type": "new_market",
                "slug": "eth-updown-5m-2000000000",
            }
        )
        is None
    )


def test_reads_queue_size_at_configured_price() -> None:
    message = {
        "bids": [
            {"price": "0.02", "size": "12.5"},
            {"price": "0.01", "size": "38322.6"},
        ]
    }

    assert _bid_size_at(message, Decimal("0.01")) == Decimal("38322.6")
    assert _bid_size_at(message, Decimal("0.03")) == Decimal("0")
