import time
from decimal import Decimal

import pytest

from polymarket_bot import knocker
from polymarket_bot.config import BotConfig
from polymarket_bot.database import BotDatabase
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
        gave_up=False,
        not_ready=False,
        member_exc=None,
    ):
        self.member_exc = member_exc
        self.gave_up = gave_up
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
        self.phase_offset_ms = None
        self.not_ready = not_ready

    def prepare_entry(self, market, *, price, size):
        self.started_at = time.monotonic()
        self.sizes.append(size)
        if self.not_ready:
            return PlacementResult((), "signing not ready", retryable=True, attempts=1)
        return (market, price, size)

    def knock_member(self, entry, *, name, phase_offset_ms):
        if self.member_exc:
            raise self.member_exc
        self.phase_offset_ms = phase_offset_ms
        return {"account": name, "phase_ms": float(phase_offset_ms), "legs": [{"outcome": "up", "body": "{}"}]}

    def knock_hooks(self):
        return knocker.Hooks(trace=self.trace)

    def trace(self, row):
        pass

    def settle_entry(self, entry, outcome):
        market, price, size = entry
        self.outcome = outcome
        if self.raise_exc:
            raise self.raise_exc
        if self.gave_up:
            return PlacementResult(
                (), "no acceptance within the knocking budget",
                retryable=False, attempts=3, expected=1, gave_up=True,
            )
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


class Knocks:
    """Stands in for the knock library: records each plan, answers every
    member with an empty result (the fake exchanges settle on their own)."""

    def __init__(self):
        self.plans = []
        self.hooks = []
        self.error = None

    def __call__(self, plan, hooks):
        if self.error:
            raise self.error
        self.plans.append(plan)
        self.hooks.append(hooks)
        return {"members": [{"account": m["account"]} for m in plan["members"]]}


@pytest.fixture(autouse=True)
def knocks(monkeypatch):
    stub = Knocks()
    monkeypatch.setattr(knocker, "knock", stub)
    return stub


def test_one_knock_carries_every_member_at_its_own_offset(knocks):
    """The offsets ride on one timetable in one knock; nobody sleeps its
    offset before it starts."""
    fleet = Fleet([member("primary", 0), member("m1", 60), member("m2", 120)])
    place(fleet)

    [plan] = knocks.plans
    assert plan["interval_ms"] == 25.0
    assert [m["account"] for m in plan["members"]] == ["primary", "m1", "m2"]
    assert [m["phase_ms"] for m in plan["members"]] == [0.0, 60.0, 120.0]
    assert set(knocks.hooks[0]) == {"primary", "m1", "m2"}
    # Every member signs before the knock, none is held back to start.
    starts = [m.exchange.started_at for m in fleet.members]
    assert max(starts) - min(starts) < 0.05
    # Each member settles its own part of the result.
    assert [m.exchange.outcome["account"] for m in fleet.members] == ["primary", "m1", "m2"]


def test_a_member_whose_signing_is_not_ready_sits_the_knock_out(knocks):
    fleet = Fleet([member("primary", not_ready=True), member("m1", 12.5, registered_ts_ms=900)])
    placement = place(fleet)

    assert [m["account"] for m in knocks.plans[0]["members"]] == ["m1"]
    assert placement.kept == "m1"
    assert placement.placements[0].result.retryable


def test_a_member_that_cannot_join_the_knock_is_reported_alone(knocks):
    fleet = Fleet([member("primary", member_exc=RuntimeError("no creds")), member("m1", 12.5, registered_ts_ms=900)])
    placement = place(fleet)

    assert [m["account"] for m in knocks.plans[0]["members"]] == ["m1"]
    assert placement.placements[0].error == "RuntimeError: no creds"
    assert placement.kept == "m1"


def test_a_knock_that_fails_is_reported_for_every_member(knocks):
    knocks.error = knocker.KnockerError("library missing")
    fleet = Fleet([member("primary"), member("m1", 12.5)])
    placement = place(fleet)

    assert placement.kept is None
    assert [p.error for p in placement.placements] == [
        "KnockerError: library missing",
        "KnockerError: library missing",
    ]


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


def test_every_member_shares_one_knocking_budget(knocks):
    """The members give up together, on the deadline the fleet was handed."""
    fleet = Fleet([member("primary", 0), member("m1", 60), member("m2", 120)])
    until = time.time() + 240

    fleet.place(
        MARKET, price=Decimal("0.01"), submission_interval_ms=Decimal("25"), knock_until_ts=until
    )

    [plan] = knocks.plans
    assert plan["knock_until_ms"] == int(until * 1000)


def test_the_fleet_gives_up_only_when_every_member_did():
    everyone = Fleet([member("primary", 0, gave_up=True), member("m1", 60, gave_up=True)])
    placement = place(everyone)
    assert placement.gave_up
    assert not placement.retryable

    one_registered = Fleet([member("primary", 0, gave_up=True), member("m1", 60)])
    placement = place(one_registered)
    assert not placement.gave_up
    assert placement.kept == "m1"


def test_the_venue_registration_time_decides_which_order_is_kept():
    """The first acceptance reply to reach us is not the first registration:
    in run38 keeping it picked a later registration in 10 of 70 markets,
    412 shares deeper at the median."""
    fleet = Fleet([
        member("primary", registered_ts_ms=900),  # its reply came back first
        member("m1", registered_ts_ms=1000),
        member("m2", registered_ts_ms=1100),
    ])
    fleet.registration_clock = {
        "primary-order": 5010,
        "m1-order": 5003,
        "m2-order": 5020,
    }.get

    placement = place(fleet)

    assert placement.kept == "m1"
    assert fleet.member("primary").exchange.canceled == ["primary-order"]
    assert fleet.member("m1").exchange.canceled == []
    details = placement.details()
    assert details["kept_by"] == "venue"
    assert details["members"]["m1"]["venue_registered_ts_ms"] == 5003


def test_a_missing_venue_time_falls_back_to_the_reply_order_for_everyone(monkeypatch):
    """The two clocks are never compared: without every registered member's
    venue time, the reply order decides for all of them."""
    import polymarket_bot.fleet as fleet_module

    monkeypatch.setattr(fleet_module, "VENUE_STAMP_WAIT_SECONDS", 0.05)
    fleet = Fleet([
        member("primary", registered_ts_ms=900),
        member("m1", registered_ts_ms=1000),
    ])
    fleet.registration_clock = {"m1-order": 5003}.get  # primary's never lands

    placement = place(fleet)

    assert placement.kept == "primary"
    details = placement.details()
    assert details["kept_by"] == "reply"
    assert details["members"]["m1"]["venue_registered_ts_ms"] is None


def test_a_venue_time_that_lands_during_the_wait_is_used():
    fleet = Fleet([
        member("primary", registered_ts_ms=900),
        member("m1", registered_ts_ms=1000),
    ])
    landed = {"m1-order": 5003}
    asked = []

    def clock(order_id):
        asked.append(order_id)
        if len(asked) > 6:  # primary's push lands a few looks later
            landed["primary-order"] = 5010
        return landed.get(order_id)

    fleet.registration_clock = clock

    placement = place(fleet)

    assert placement.kept == "m1"
    assert placement.details()["kept_by"] == "venue"
