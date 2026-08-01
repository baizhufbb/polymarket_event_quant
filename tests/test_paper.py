from decimal import Decimal
import time

from polymarket_bot.cli import _parser
from polymarket_bot.market_activation import MarketActivationUpdate
from polymarket_bot.models import Market, TradePlan
from polymarket_bot.paper import (
    ExecutionEvidence,
    PaperDatabase,
    calculate_result,
    execution_evidence,
    fifo_fill,
    queue_at_price,
    strategy_key,
)


def _plan() -> TradePlan:
    return TradePlan(
        buy_price=Decimal("0.01"),
        exit_targets=(),
        usd_per_side=Decimal("1"),
    )


def _market() -> Market:
    return Market(
        slug="btc-updown-5m-2000000000",
        condition_id="condition",
        start_ts=2_000_000_000,
        end_ts=2_000_000_300,
        up_token_id="up-token",
        down_token_id="down-token",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )


def test_paper_cli_defaults_to_next_new_market() -> None:
    args = _parser().parse_args(
        [
            "paper",
            "--buy-price",
            "0.01",
            "--usd-per-side",
            "1",
        ]
    )

    assert args.lookahead_minutes == 0
    assert args.hours is None


def test_queue_at_price_sums_only_the_target_bid() -> None:
    book = {
        "bids": [
            {"price": "0.01", "size": "40.5"},
            {"price": "0.02", "size": "100"},
            {"price": "0.01", "size": "9.5"},
        ]
    }

    assert queue_at_price(book, Decimal("0.01")) == Decimal("50.0")


def test_execution_evidence_includes_direct_and_complementary_takers() -> None:
    trades = [
        {
            "asset": "up-token",
            "side": "SELL",
            "price": "0.01",
            "size": "40",
            "timestamp": 1,
        },
        {
            "asset": "down-token",
            "side": "BUY",
            "price": "0.99",
            "size": "30",
            "timestamp": 2,
        },
        {
            "asset": "down-token",
            "side": "BUY",
            "price": "0.98",
            "size": "500",
            "timestamp": 3,
        },
    ]

    evidence = execution_evidence(
        trades,
        token_id="up-token",
        opposite_token_id="down-token",
        buy_price=Decimal("0.01"),
    )

    assert evidence.boundary_volume == Decimal("70")
    assert not evidence.crossed
    assert [event["route"] for event in evidence.events] == [
        "direct",
        "complementary",
    ]


def test_fifo_fill_does_not_assume_cancellations_ahead() -> None:
    evidence = ExecutionEvidence(
        boundary_volume=Decimal("120"),
        crossed=False,
        events=(),
    )

    assert fifo_fill(
        queue_ahead=Decimal("50"),
        order_size=Decimal("100"),
        evidence=evidence,
    ) == Decimal("70")


def test_trade_through_price_confirms_a_full_fill() -> None:
    evidence = execution_evidence(
        [
            {
                "asset": "up-token",
                "side": "SELL",
                "price": "0.009",
                "size": "1",
            }
        ],
        token_id="up-token",
        opposite_token_id="down-token",
        buy_price=Decimal("0.01"),
    )

    assert evidence.crossed
    assert fifo_fill(
        queue_ahead=Decimal("10000"),
        order_size=Decimal("100"),
        evidence=evidence,
    ) == Decimal("100")


def test_result_uses_only_fifo_filled_inventory() -> None:
    result = calculate_result(
        winner_outcome="Up",
        trades=[
            {
                "asset": "up-token",
                "side": "SELL",
                "price": "0.01",
                "size": "150",
            }
        ],
        up_token_id="up-token",
        down_token_id="down-token",
        buy_price=Decimal("0.01"),
        order_size=Decimal("100"),
        up_queue_ahead=Decimal("50"),
        down_queue_ahead=Decimal("0"),
    )

    assert result.up_fill == Decimal("100")
    assert result.down_fill == Decimal("0")
    assert result.gross_cost == Decimal("1.00")
    assert result.gross_payout == Decimal("100")
    assert result.pnl_before_rebates == Decimal("99.00")


def test_paper_database_reports_fill_pattern_and_pnl(tmp_path) -> None:
    plan = _plan()
    market = _market()
    update = MarketActivationUpdate(
        market=market,
        market_discovered_ts_ms=1_999_999_000_000,
        market_parameters_detected_ts_ms=1_999_999_001_000,
    )
    result = calculate_result(
        winner_outcome="down",
        trades=[
            {
                "asset": "down-token",
                "side": "SELL",
                "price": "0.009",
                "size": "1",
            }
        ],
        up_token_id=market.up_token_id,
        down_token_id=market.down_token_id,
        buy_price=plan.buy_price,
        order_size=plan.order_size,
        up_queue_ahead=Decimal("40"),
        down_queue_ahead=Decimal("50"),
    )

    with PaperDatabase(tmp_path / "paper.sqlite") as database:
        key = database.start(plan)
        assert key == strategy_key(plan)
        assert database.register(key, update, plan)
        snapshot_ts_ms = int(time.time() * 1000)
        database.set_snapshot(
            key,
            market.slug,
            snapshot_ts_ms=snapshot_ts_ms,
            book_source_ts_ms=snapshot_ts_ms - 100,
            up_queue_ahead=Decimal("40"),
            down_queue_ahead=Decimal("50"),
        )
        database.set_result(key, market.slug, result=result, trade_count=1)

        status = database.status()

    assert status["settled"] == 1
    assert status["fill_patterns"] == {
        "neither": 0,
        "up_only": 0,
        "down_only": 1,
        "both": 0,
    }
    assert status["profitable_markets"] == 1
    assert status["pnl_before_rebates"] == "99.00"
    assert status["markets_with_fill"] == 1
    assert status["fill_rate"] == "1"
    assert status["active_order_reserve_usd"] == "2.00"
    assert status["peak_order_reserve_usd"] == "2.00"


def test_expired_market_without_a_book_is_marked_missed(tmp_path) -> None:
    plan = _plan()
    market = _market()
    update = MarketActivationUpdate(
        market=market,
        market_discovered_ts_ms=1_999_999_000_000,
        market_parameters_detected_ts_ms=1_999_999_001_000,
    )

    with PaperDatabase(tmp_path / "paper.sqlite") as database:
        key = database.start(plan)
        database.register(key, update, plan)
        database.expire_pending_snapshots(key, market.end_ts)

        assert database.pending_snapshots(key) == []
        assert database.status()["markets"] == {"missed": 1}
