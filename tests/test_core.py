import json
from decimal import Decimal

from polymarket_bot.discovery import MarketDiscovery, is_eligible
from polymarket_bot.exchange import _accepted, _order_id, normalize_order
from polymarket_bot.database import BotDatabase
from polymarket_bot.cli import _parser
from polymarket_bot.models import ExitTarget, Market, PlacedOrder, TradePlan


def test_trade_plan_uses_fixed_cli_values() -> None:
    plan = TradePlan(
        buy_price=Decimal("0.04"),
        exit_targets=(ExitTarget(Decimal("0.20"), Decimal("0.50")),),
        usd_per_side=Decimal("1"),
    )
    plan.validate()
    assert plan.order_size == Decimal("25")
    assert plan.market_reserve == Decimal("2")


def test_trade_plan_without_take_profit_is_buy_only() -> None:
    plan = TradePlan(
        buy_price=Decimal("0.02"),
        exit_targets=(),
        usd_per_side=Decimal("1"),
    )
    plan.validate()
    assert plan.buy_only
    assert plan.order_size == Decimal("50")
    assert plan.as_dict()["take_profit"] == []
    assert plan.as_dict()["hold_fraction"] == "1"


def test_trade_plan_serializes_ladder_and_hold_fraction() -> None:
    plan = TradePlan(
        buy_price=Decimal("0.01"),
        exit_targets=(
            ExitTarget(Decimal("0.02"), Decimal("0.50")),
            ExitTarget(Decimal("0.10"), Decimal("0.10")),
            ExitTarget(Decimal("0.30"), Decimal("0.10")),
        ),
        usd_per_side=Decimal("1"),
    )
    plan.validate()
    assert plan.hold_fraction == Decimal("0.30")
    assert plan.as_dict()["take_profit"] == [
        {"price": "0.02", "fraction": "0.50"},
        {"price": "0.10", "fraction": "0.10"},
        {"price": "0.30", "fraction": "0.10"},
    ]


def test_run_limits_are_optional_cli_parameters() -> None:
    args = _parser().parse_args(
        [
            "run",
            "--buy-price",
            "0.01",
            "--take-profit",
            "0.02:0.50",
            "--take-profit",
            "0.10:0.10",
            "--usd-per-side",
            "1",
        ]
    )
    assert args.hours is None
    assert args.max_reserved_usd is None
    assert args.max_daily_filled_cost is None
    assert args.lookahead_minutes == 40
    assert args.placement_order == "nearest-first"
    assert args.cancel_before_end_seconds == 2
    assert args.heartbeat_seconds is None

    buy_only = _parser().parse_args(
        [
            "run",
            "--buy-price",
            "0.02",
            "--usd-per-side",
            "1",
        ]
    )
    assert buy_only.take_profit == []

    reverse = _parser().parse_args(
        [
            "run",
            "--buy-price",
            "0.01",
            "--take-profit",
            "0.02:0.50",
            "--usd-per-side",
            "1",
            "--lookahead-minutes",
            "120",
            "--placement-order",
            "farthest-first",
            "--heartbeat-seconds",
            "5",
        ]
    )
    assert reverse.lookahead_minutes == 120
    assert reverse.placement_order == "farthest-first"
    assert reverse.heartbeat_seconds == Decimal("5")


def test_parse_market() -> None:
    event = {
        "slug": "btc-updown-5m-2000000000",
        "markets": [
            {
                "conditionId": "0xcondition",
                "clobTokenIds": json.dumps(["up-token", "down-token"]),
                "outcomes": json.dumps(["Up", "Down"]),
                "acceptingOrders": True,
                "enableOrderBook": True,
                "orderMinSize": 5,
                "orderPriceMinTickSize": 0.01,
            }
        ],
    }
    market = MarketDiscovery._parse(event)
    assert market is not None
    assert market.up_token_id == "up-token"
    assert market.down_token_id == "down-token"
    assert market.end_ts == 2_000_000_300


def test_farthest_discovery_requests_only_the_far_edge_window() -> None:
    class Response:
        @staticmethod
        def raise_for_status() -> None:
            return None

        @staticmethod
        def json() -> list:
            return []

    class Session:
        def __init__(self) -> None:
            self.params = None

        def get(self, _url, *, params, timeout):
            self.params = params
            assert timeout == 20
            return Response()

    discovery = MarketDiscovery()
    discovery.session = Session()

    assert discovery.discover(40, farthest_first=True) == []
    assert discovery.session.params["ascending"] == "false"
    assert discovery.session.params["limit"] == 8
    assert "end_date_max" not in discovery.session.params


def test_rejects_current_market_from_new_run() -> None:
    event = {
        "slug": "btc-updown-5m-2000000000",
        "markets": [
            {
                "conditionId": "0xcondition",
                "clobTokenIds": json.dumps(["up", "down"]),
                "outcomes": json.dumps(["Up", "Down"]),
                "acceptingOrders": True,
                "enableOrderBook": True,
            }
        ],
    }
    market = MarketDiscovery._parse(event)
    assert market is not None
    assert not is_eligible(market, run_started_ts=2_000_000_010, now_ts=2_000_000_010)


def test_rejects_ended_market() -> None:
    event = {
        "slug": "btc-updown-5m-2000000000",
        "markets": [
            {
                "conditionId": "0xcondition",
                "clobTokenIds": json.dumps(["up", "down"]),
                "outcomes": json.dumps(["Up", "Down"]),
                "acceptingOrders": True,
                "enableOrderBook": True,
            }
        ],
    }
    market = MarketDiscovery._parse(event)
    assert market is not None
    assert not is_eligible(
        market,
        run_started_ts=market.start_ts,
        now_ts=market.end_ts,
    )


def test_order_response_normalization() -> None:
    response = {"success": True, "orderID": "0xorder", "status": "live"}
    assert _accepted(response)
    assert _order_id(response) == "0xorder"
    assert normalize_order({"status": "LIVE", "size_matched": "2.5"}) == (
        "live",
        Decimal("2.5"),
    )
    assert normalize_order({"status": "ORDER_STATUS_LIVE", "size_matched": "0"}) == (
        "live",
        Decimal("0"),
    )


def test_simulated_orders_reserve_capital(tmp_path) -> None:
    event = {
        "slug": "btc-updown-5m-2000000000",
        "markets": [
            {
                "conditionId": "0xcondition",
                "clobTokenIds": json.dumps(["up", "down"]),
                "outcomes": json.dumps(["Up", "Down"]),
                "acceptingOrders": True,
                "enableOrderBook": True,
            }
        ],
    }
    market = MarketDiscovery._parse(event)
    assert market is not None
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("dry-run")
        database.add_market(run_id, market)
        for outcome, token in (("up", "up"), ("down", "down")):
            database.add_order(
                run_id,
                market.slug,
                PlacedOrder(
                    order_id=f"dry:{outcome}",
                    outcome=outcome,
                    token_id=token,
                    price=Decimal("0.01"),
                    size=Decimal("100"),
                    status="simulated",
                    raw={},
                ),
            )
        assert database.active_reserved_usd() == Decimal("2")


def test_immediately_matched_order_is_terminal_and_records_full_size(tmp_path) -> None:
    event = {
        "slug": "btc-updown-5m-2000000000",
        "markets": [
            {
                "conditionId": "0xcondition",
                "clobTokenIds": json.dumps(["up", "down"]),
                "outcomes": json.dumps(["Up", "Down"]),
                "acceptingOrders": True,
                "enableOrderBook": True,
            }
        ],
    }
    market = MarketDiscovery._parse(event)
    assert market is not None
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        database.add_market(run_id, market)
        database.add_order(
            run_id,
            market.slug,
            PlacedOrder(
                order_id="matched-exit",
                outcome="up",
                token_id="up",
                price=Decimal("0.20"),
                size=Decimal("5"),
                status="matched",
                raw={},
                side="sell",
                role="exit",
            ),
        )

        row = database.connection.execute(
            "SELECT matched_size FROM orders WHERE order_id='matched-exit'"
        ).fetchone()
        assert row["matched_size"] == "5"
        assert database.tracked_open_orders() == []


def test_existing_market_is_never_rearmed(tmp_path) -> None:
    market = Market(
        slug="btc-updown-5m-2000000000",
        condition_id="0xcondition",
        start_ts=2_000_000_000,
        end_ts=2_000_000_300,
        up_token_id="up-token",
        down_token_id="down-token",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        first_run = database.start_run("live")
        database.add_market(first_run, market)
        for outcome in ("up", "down"):
            database.add_order(
                first_run,
                    market.slug,
                PlacedOrder(
                    order_id=f"{outcome}-order",
                    outcome=outcome,
                    token_id=f"{outcome}-token",
                    price=Decimal("0.01"),
                    size=Decimal("100"),
                    status="cancel_requested",
                    raw={},
                ),
            )

        second_run = database.start_run("live")
        assert not database.can_start_entry_plan(market.slug)
        assert len(database.tracked_open_orders()) == 2

        database.mark_orders(["up-order", "down-order"], "cancelled")
        assert not database.can_start_entry_plan(market.slug)

        database.update_order(
            "up-order", status="cancelled", matched_size=Decimal("1"), raw={}
        )
        assert not database.can_start_entry_plan(market.slug)

        database.update_order(
            "up-order", status="cancelled", matched_size=Decimal("0"), raw={}
        )
        database.prepare_market(second_run, market)
        market_row = database.connection.execute(
            "SELECT run_id, state FROM markets WHERE slug=?", (market.slug,)
        ).fetchone()
        assert market_row["run_id"] == second_run
        assert market_row["state"] == "placing"
        assert not database.can_start_entry_plan(market.slug)
