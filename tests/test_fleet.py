import time
from decimal import Decimal

import pytest

from polymarket_bot.config import BotConfig
from polymarket_bot.database import BotDatabase
from polymarket_bot.exchange import Exchange
from polymarket_bot.fleet import Fleet, FleetMember, FleetOrderView, evenly_phased
from polymarket_bot.models import Market, PlacedOrder, PlacementResult


MARKET = Market(
    slug="btc-updown-5m-2000000000",
    condition_id="0xcondition",
    start_ts=2_000_000_000,
    end_ts=2_000_000_300,
    up_token_id="up-token",
    down_token_id="down-token",
    min_size=Decimal("5"),
    tick_size=Decimal("0.01"),
)


class FakeExchange:
    def __init__(
        self,
        name,
        *,
        registered_ts_ms=None,
        complete=True,
        raise_exc=None,
        cancel_result=None,
        cancel_exc=None,
        open_rows=(),
        orders_by_id=None,
    ):
        self.name = name
        self.registered_ts_ms = registered_ts_ms
        self.complete = complete
        self.raise_exc = raise_exc
        self.cancel_result = cancel_result
        self.cancel_exc = cancel_exc
        self.open_rows = list(open_rows)
        self.orders_by_id = orders_by_id or {}
        self.started_at = None
        self.sizes = []
        self.canceled = []
        self.grid_origin = None
        self.phase_offset_ms = None

    def place_dual(
        self,
        market,
        *,
        price,
        size,
        submission_interval_ms,
        grid_origin=None,
        phase_offset_ms=Decimal(0),
    ):
        self.started_at = time.monotonic()
        self.grid_origin = grid_origin
        self.phase_offset_ms = phase_offset_ms
        self.sizes.append(size)
        time.sleep(0.02)
        if self.raise_exc:
            raise self.raise_exc
        if not self.complete:
            return PlacementResult((), "not ready", retryable=True, attempts=3, expected=1)
        order = PlacedOrder(
            order_id=f"{self.name}-order",
            outcome="up",
            token_id=market.up_token_id,
            price=price,
            size=size,
            status="live",
            raw={},
        )
        return PlacementResult(
            (order,), attempts=5, expected=1, registered_ts_ms=self.registered_ts_ms
        )

    def cancel_orders(self, order_ids):
        self.canceled.extend(order_ids)
        if self.cancel_exc:
            raise self.cancel_exc
        if self.cancel_result is not None:
            return self.cancel_result
        return {"canceled": list(order_ids)}

    def open_orders(self, condition_id=None):
        return list(self.open_rows)

    def get_order(self, order_id):
        if order_id in self.orders_by_id:
            return self.orders_by_id[order_id]
        raise RuntimeError("not found")


def member(name, offset_ms=0, **kwargs):
    return FleetMember(name, FakeExchange(name, **kwargs), Decimal("103.7"), Decimal(offset_ms))


def place(fleet):
    return fleet.place(MARKET, price=Decimal("0.01"), submission_interval_ms=Decimal("25"))


def test_every_member_shares_one_timetable_with_its_own_offset():
    """The offsets ride on a common origin instead of a per-member sleep.

    Sleeping the offset before a member started put it ahead of that
    member's warm-up and signing, which cost hundreds of milliseconds and
    varied per call, so the intended offsets never reached the wire.
    """
    fleet = Fleet([member("primary", 0), member("m1", 60), member("m2", 120)])
    place(fleet)

    origins = {m.exchange.grid_origin for m in fleet.members}
    assert len(origins) == 1
    assert origins != {None}
    assert [m.exchange.phase_offset_ms for m in fleet.members] == [
        Decimal(0),
        Decimal(60),
        Decimal(120),
    ]
    # No member is held back before it starts preparing.
    starts = [m.exchange.started_at for m in fleet.members]
    assert max(starts) - min(starts) < 0.05


class SlowSigningClient:
    """A venue that keeps answering not-ready, so the loop keeps its cadence.

    Signing costs differ per member on purpose: that difference is what used
    to decide where each member's sends landed.
    """

    def __init__(self, sign_cost):
        self.sign_cost = sign_cost
        self.calls = 0

    def create_order(self, order_args, options):
        time.sleep(self.sign_cost)
        return {"token_id": order_args.token_id, "side": order_args.side}

    def post_orders(self, signed, post_only=False):
        self.calls += 1
        if self.calls >= 40:
            return [
                {"success": True, "orderID": f"{id(self)}-{index}", "status": "live"}
                for index in range(len(signed))
            ]
        return [
            {
                "success": False,
                "orderID": "",
                "errorMsg": "the market is not yet ready to process new orders",
            }
            for _ in signed
        ]

    def cancel_orders(self, order_ids):
        return {"canceled": list(order_ids)}


def _recording_exchange(sign_cost):
    exchange = Exchange.__new__(Exchange)
    exchange.client = SlowSigningClient(sign_cost)
    exchange.sends = []
    dispatch = Exchange._submit_placement_request

    def record(self, payload):
        self.sends.append(time.monotonic())
        return dispatch(self, payload)

    exchange._submit_placement_request = record.__get__(exchange, Exchange)
    return exchange


def test_two_members_reach_the_wire_half_a_cadence_apart():
    """The acceptance criterion for the whole timetable change.

    Run 15 and run 16 failed exactly here: both accounts fired within a few
    milliseconds of each other instead of half a cadence apart, so the second
    account bought nothing. The signing costs below differ by 127 ms, which
    is 2 ms in cadence terms - close to what the field actually showed - so a
    member that started its own timetable would land there instead of at
    12.5 ms.
    """
    interval = 25.0
    members = [
        ("primary", _recording_exchange(0.010), Decimal("103.7")),
        ("m1", _recording_exchange(0.137), Decimal("106.1")),
    ]
    fleet = Fleet(evenly_phased(members, Decimal("25")))

    fleet.place(MARKET, price=Decimal("0.01"), submission_interval_ms=Decimal("25"))

    first, second = (member.exchange.sends for member in fleet.members)
    assert len(first) >= 20 and len(second) >= 20

    base = first[0]
    differences = []
    for moment in second:
        offset = ((moment - base) * 1000.0) % interval
        differences.append(offset)
    differences.sort()
    middle = differences[len(differences) // 2]
    assert 12.5 - 5.0 <= middle <= 12.5 + 5.0, (
        f"members reached the wire {middle:.2f} ms apart, wanted 12.5"
    )


def test_keeps_earliest_registration_and_cancels_laggards():
    fleet = Fleet([
        member("primary", registered_ts_ms=1000),
        member("m1", registered_ts_ms=900),
        member("m2", registered_ts_ms=1100),
    ])
    placement = place(fleet)

    assert placement.kept == "m1"
    assert placement.error is None
    assert fleet.member("primary").exchange.canceled == ["primary-order"]
    assert fleet.member("m2").exchange.canceled == ["m2-order"]
    assert fleet.member("m1").exchange.canceled == []
    statuses = {order.order_id: order.status for order in placement.orders}
    assert statuses == {
        "primary-order": "cancelled",
        "m1-order": "live",
        "m2-order": "cancelled",
    }
    assert {order.account for order in placement.orders} == {"primary", "m1", "m2"}
    assert sorted(placement.cancelled_order_ids) == ["m2-order", "primary-order"]
    assert placement.kept_order_ids() == ["m1-order"]
    assert placement.attempts == 15
    assert placement.details()["members"]["m1"]["registered_ts_ms"] == 900


def test_missing_registration_time_sorts_last():
    fleet = Fleet([member("primary"), member("m1", registered_ts_ms=950)])
    assert place(fleet).kept == "m1"


def test_nothing_registered_is_retryable():
    fleet = Fleet([member("primary", complete=False), member("m1", complete=False)])
    placement = place(fleet)
    assert placement.kept is None
    assert placement.orders == ()
    assert placement.retryable
    assert "not ready" in placement.error


def test_member_exception_is_reported_not_raised():
    fleet = Fleet([
        member("primary", raise_exc=RuntimeError("boom")),
        member("m1", registered_ts_ms=900),
    ])
    placement = place(fleet)
    assert placement.kept == "m1"
    assert placement.placements[0].error.startswith("RuntimeError")
    assert placement.error is None


def test_cancel_failure_marks_cancel_requested():
    fleet = Fleet([
        member("primary", registered_ts_ms=900),
        member("m1", registered_ts_ms=1000, cancel_exc=RuntimeError("offline")),
    ])
    placement = place(fleet)
    statuses = {order.order_id: order.status for order in placement.orders}
    assert statuses["m1-order"] == "cancel_requested"
    assert statuses["primary-order"] == "live"
    assert placement.cancel_errors and "offline" in placement.cancel_errors[0]


def test_unconfirmed_cancel_stays_requested_and_terminal_is_marked():
    fleet = Fleet([
        member("primary", registered_ts_ms=900),
        member(
            "m1",
            registered_ts_ms=1000,
            cancel_result={"canceled": [], "not_canceled": {"m1-order": "already canceled or matched"}},
        ),
        member("m2", registered_ts_ms=1100, cancel_result={"canceled": []}),
    ])
    statuses = {o.order_id: o.status for o in place(fleet).orders}
    assert statuses["m1-order"] == "terminal_unknown"
    assert statuses["m2-order"] == "cancel_requested"


def test_keep_best_disabled_keeps_every_ticket():
    fleet = Fleet(
        [member("primary", registered_ts_ms=1000), member("m1", registered_ts_ms=900)],
        keep_best=False,
    )
    placement = place(fleet)
    assert placement.kept == "m1"
    assert all(order.status == "live" for order in placement.orders)
    assert all(m.exchange.canceled == [] for m in fleet.members)


def test_member_sizes_are_passed_through():
    big = FleetMember("m1", FakeExchange("m1"), Decimal("106.1"), Decimal(0))
    fleet = Fleet([member("primary"), big])
    place(fleet)
    assert big.exchange.sizes == [Decimal("106.1")]


def test_evenly_phased_spreads_offsets():
    members = evenly_phased(
        [(f"m{i}", FakeExchange(f"m{i}"), Decimal("100")) for i in range(5)],
        Decimal("25"),
    )
    assert [m.phase_offset_ms for m in members] == [Decimal(x) for x in (0, 5, 10, 15, 20)]


def test_duplicate_member_names_are_rejected():
    with pytest.raises(ValueError):
        Fleet([member("a"), member("a")])


def test_order_view_aggregates_and_routes():
    primary = FakeExchange("primary", open_rows=[{"id": "p1"}], orders_by_id={"p1": {"id": "p1"}})
    other = FakeExchange("m1", open_rows=[{"id": "o1"}], orders_by_id={"o1": {"id": "o1"}})
    view = FleetOrderView((
        FleetMember("primary", primary, Decimal("1"), Decimal(0)),
        FleetMember("m1", other, Decimal("1"), Decimal(0)),
    ))
    rows = view.open_orders()
    assert [(row["id"], row["account"]) for row in rows] == [("p1", "primary"), ("o1", "m1")]
    assert view.get_order("o1") == {"id": "o1", "account": "m1"}
    with pytest.raises(RuntimeError):
        view.get_order("missing")


def test_database_records_account(tmp_path):
    with BotDatabase(tmp_path / "bot.sqlite") as database:
        run_id = database.start_run("live", {})
        database.prepare_market(run_id, MARKET)
        database.add_order(
            run_id,
            MARKET.slug,
            PlacedOrder(
                order_id="o1",
                outcome="up",
                token_id="up-token",
                price=Decimal("0.01"),
                size=Decimal("103.7"),
                status="live",
                raw={},
                account="m1",
            ),
        )
        rows = database.tracked_open_orders()
        assert rows[0]["account"] == "m1"


def test_config_from_env_file_reads_crlf(tmp_path):
    path = tmp_path / "b.env"
    path.write_bytes(
        b"# comment\r\nPOLYMARKET_PRIVATE_KEY=0xabc\r\n"
        b"POLYMARKET_FUNDER_ADDRESS=0xdef\r\nPOLYMARKET_SIGNATURE_TYPE=0\r\n"
    )
    config = BotConfig.from_env_file(path, project_root=tmp_path)
    assert config.private_key == "0xabc"
    assert config.funder_address == "0xdef"
    assert config.signature_type == 0
    assert config.api_key is None


def test_config_from_env_file_requires_key_and_funder(tmp_path):
    path = tmp_path / "b.env"
    path.write_text("POLYMARKET_PRIVATE_KEY=0xabc\n")
    with pytest.raises(ValueError):
        BotConfig.from_env_file(path, project_root=tmp_path)


class AcceptingClient:
    def create_order(self, order_args, options):
        return {
            "token_id": order_args.token_id,
            "side": order_args.side,
            "price": order_args.price,
            "size": order_args.size,
        }

    def post_orders(self, signed, post_only=False):
        return [
            {"success": True, "orderID": f"{item.order['token_id']}-id", "status": "live"}
            for item in signed
        ]

    def cancel_orders(self, order_ids):
        return {"canceled": list(order_ids)}


def test_place_dual_reports_registration_time():
    exchange = Exchange.__new__(Exchange)
    exchange.client = AcceptingClient()
    before = int(time.time() * 1000)
    result = exchange.place_dual(
        MARKET,
        price=Decimal("0.01"),
        size=Decimal("100"),
        submission_interval_ms=Decimal("20"),
    )
    assert result.complete
    assert result.registered_ts_ms is not None
    assert before <= result.registered_ts_ms <= int(time.time() * 1000)

def test_every_member_is_told_how_many_share_the_pool():
    """The cap has to come from the fleet running, not from a constant.

    One pool serves the whole process. Divided by a fixed number instead, a
    solo account was held to half the pool it owned, and a fleet larger than
    that number was promised more connections than the pool has.
    """
    from polymarket_bot.transport import in_flight_budget

    for size in (1, 2, 3, 5):
        members = [
            (f"m{index}", _recording_exchange(0.0), Decimal("103.7"))
            for index in range(size)
        ]
        fleet = Fleet(evenly_phased(members, Decimal("25")))
        for member in fleet.members:
            assert member.exchange.accounts_sharing_the_pool == size
            assert member.exchange.max_requests_in_flight == in_flight_budget(size)

    # A single Exchange with no fleet around it still owns the whole pool.
    alone = _recording_exchange(0.0)
    assert alone.accounts_sharing_the_pool == 1
    assert alone.max_requests_in_flight == in_flight_budget(1)


def test_a_single_leg_mode_halves_the_cap():
    """Outside batch mode each request is carried by two threads, not one.

    Threads, not requests, are what ran out in the field, so the budget has to
    be counted in the resource that ran out.
    """
    from polymarket_bot.transport import in_flight_budget

    batch = _recording_exchange(0.0)
    batch.entry_submission = "batch"
    solo = _recording_exchange(0.0)
    solo.entry_submission = "solo-up"

    assert batch.max_requests_in_flight == in_flight_budget(1)
    assert solo.max_requests_in_flight == in_flight_budget(1) // 2
