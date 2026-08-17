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
    assert args.placement_interval_ms == Decimal("20")
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
            "--cancel-before-end-seconds",
            "0",
            "--heartbeat-seconds",
            "5",
            "--placement-interval-ms",
            "12.5",
        ]
    )
    assert reverse.lookahead_minutes == 120
    assert reverse.placement_order == "farthest-first"
    assert reverse.cancel_before_end_seconds == 0
    assert reverse.heartbeat_seconds == Decimal("5")
    assert reverse.placement_interval_ms == Decimal("12.5")

    next_market_only = _parser().parse_args(
        [
            "run",
            "--buy-price",
            "0.01",
            "--usd-per-side",
            "1",
            "--lookahead-minutes",
            "0",
            "--placement-order",
            "farthest-first",
        ]
    )
    assert next_market_only.lookahead_minutes == 0
    assert next_market_only.placement_order == "farthest-first"


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


def test_parse_market_before_accepting_orders() -> None:
    event = {
        "slug": "btc-updown-5m-1784857200",
        "markets": [
            {
                "conditionId": "0xcondition",
                "acceptingOrders": False,
                "enableOrderBook": True,
                "clobTokenIds": '["up-token", "down-token"]',
                "outcomes": '["Up", "Down"]',
                "orderMinSize": 5,
                "orderPriceMinTickSize": 0.01,
            }
        ],
    }

    market = MarketDiscovery._parse(event)

    assert market is not None
    assert market.up_token_id == "up-token"
    assert market.down_token_id == "down-token"


def test_fresh_slug_lookup_bypasses_gamma_cache() -> None:
    class Response:
        status_code = 200

        @staticmethod
        def raise_for_status() -> None:
            return None

        @staticmethod
        def json() -> dict:
            return {
                "conditionId": "0xcondition",
                "enableOrderBook": True,
                "clobTokenIds": '["up-token", "down-token"]',
                "outcomes": '["Up", "Down"]',
                "orderMinSize": 5,
                "orderPriceMinTickSize": 0.01,
            }

    class Session:
        def __init__(self) -> None:
            self.call = None

        def get(self, url, **kwargs):
            self.call = (url, kwargs)
            return Response()

    slug = "btc-updown-5m-1784857200"
    discovery = MarketDiscovery()
    discovery.session = Session()

    market = discovery.find_by_slug(slug, fresh=True)

    assert market is not None
    assert market.up_token_id == "up-token"
    assert market.down_token_id == "down-token"
    url, kwargs = discovery.session.call
    assert url.endswith(f"/markets/slug/{slug}")
    assert kwargs["timeout"] == 5
    assert kwargs["params"]["_cb"].isdigit()
    assert kwargs["headers"] == {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def test_missing_slug_returns_none() -> None:
    class Response:
        status_code = 404

    class Session:
        @staticmethod
        def get(_url, **_kwargs):
            return Response()

    discovery = MarketDiscovery()
    discovery.session = Session()

    assert discovery.find_by_slug("btc-updown-5m-1784857200") is None


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
            self.calls = []

        def get(self, _url, *, params, timeout):
            self.calls.append(params)
            assert timeout == 20
            return Response()

    discovery = MarketDiscovery()
    discovery.session = Session()

    assert discovery.discover(40, farthest_first=True) == []
    assert len(discovery.session.calls) == 1
    params = discovery.session.calls[0]
    assert params["ascending"] == "false"
    assert params["limit"] == 8
    assert params["offset"] == 0
    assert "end_date_max" not in params


def test_farthest_discovery_pages_across_a_23_hour_window() -> None:
    class Response:
        def __init__(self, count) -> None:
            self.count = count

        @staticmethod
        def raise_for_status() -> None:
            return None

        def json(self) -> list:
            return [{} for _ in range(self.count)]

    class Session:
        def __init__(self) -> None:
            self.calls = []

        def get(self, _url, *, params, timeout):
            self.calls.append(params)
            assert timeout == 20
            return Response(params["limit"])

    discovery = MarketDiscovery()
    discovery.session = Session()

    assert discovery.discover(1380, farthest_first=True) == []
    assert [call["limit"] for call in discovery.session.calls] == [100, 100, 76]
    assert [call["offset"] for call in discovery.session.calls] == [0, 100, 200]


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


def test_pending_placement_can_retry_only_without_orders(tmp_path) -> None:
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
        run_id = database.start_run("live")
        database.add_market(
            run_id, market, state="placement_pending"
        )

        assert database.can_start_entry_plan(market.slug)

        database.add_order(
            run_id,
            market.slug,
            PlacedOrder(
                order_id="unexpected-order",
                outcome="up",
                token_id=market.up_token_id,
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="live",
                raw={},
            ),
        )

        assert not database.can_start_entry_plan(market.slug)


def test_due_open_orders_filters_by_market_deadline(tmp_path) -> None:
    early = Market(
        slug="btc-updown-5m-2000000000",
        condition_id="early-condition",
        start_ts=2_000_000_000,
        end_ts=2_000_000_300,
        up_token_id="early-up",
        down_token_id="early-down",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )
    later = Market(
        slug="btc-updown-5m-2000000300",
        condition_id="later-condition",
        start_ts=2_000_000_300,
        end_ts=2_000_000_600,
        up_token_id="later-up",
        down_token_id="later-down",
        min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
    )
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live")
        for market in (early, later):
            database.add_market(run_id, market)
            database.add_order(
                run_id,
                market.slug,
                PlacedOrder(
                    order_id=f"{market.slug}-live",
                    outcome="up",
                    token_id=market.up_token_id,
                    price=Decimal("0.01"),
                    size=Decimal("100"),
                    status="live",
                    raw={},
                ),
            )
        database.add_order(
            run_id,
            early.slug,
            PlacedOrder(
                order_id="early-filled",
                outcome="down",
                token_id=early.down_token_id,
                price=Decimal("0.01"),
                size=Decimal("100"),
                status="filled",
                raw={},
            ),
        )

        assert database.due_open_orders(early.end_ts - 1) == []
        assert [row["order_id"] for row in database.due_open_orders(early.end_ts)] == [
            f"{early.slug}-live"
        ]
