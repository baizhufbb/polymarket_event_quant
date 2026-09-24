"""Run several accounts as one phased fleet.

The venue meters orders per signer (about forty a second each) and the
queue slot depends on the millisecond an order reaches the engine, so a
single account on a 25 ms cadence arrives on average 12.5 ms late. N
members on the same cadence with phases offset by 25/N ms put one order
on the wire every 25/N ms while every account stays inside its own
budget. After the open the fleet keeps the ticket that registered first
(the earliest registration is the best queue slot) and cancels the rest,
so exposure stays at one order per market.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import Thread

from .exchange import KNOCK_SECONDS, Exchange
from .models import Market, PlacedOrder, PlacementResult

# How long the choice of which order to keep may wait for the venue's own
# registration times. In run38 each push reached us 10 ms (median, 247 at
# most) after its registration, and the choice came 157 ms or more after the
# last of them in all 70 markets, so this wait almost never runs.
VENUE_STAMP_WAIT_SECONDS = 1.0


@dataclass(frozen=True)
class FleetMember:
    name: str
    exchange: Exchange
    size: Decimal
    phase_offset_ms: Decimal


@dataclass(frozen=True)
class MemberPlacement:
    member: str
    result: PlacementResult | None
    error: str | None = None

    @property
    def registered(self) -> bool:
        return self.result is not None and self.result.complete


@dataclass(frozen=True)
class FleetPlacement:
    placements: tuple[MemberPlacement, ...]
    kept: str | None
    orders: tuple[PlacedOrder, ...]
    cancelled_order_ids: tuple[str, ...] = ()
    cancel_errors: tuple[str, ...] = ()
    # "venue": chosen on the venue's registration times; "reply": on when
    # each acceptance reply reached us, because a venue time was missing.
    kept_by: str | None = None
    venue_registered_ts_ms: dict[str, int] | None = None

    @property
    def attempts(self) -> int:
        return sum(
            placement.result.attempts
            for placement in self.placements
            if placement.result is not None
        )

    @property
    def error(self) -> str | None:
        if self.kept is not None:
            return None
        parts = []
        for placement in self.placements:
            if placement.error:
                parts.append(f"{placement.member}: {placement.error}")
            elif placement.result is not None and placement.result.error:
                parts.append(f"{placement.member}: {placement.result.error}")
        return "; ".join(parts) or "no member registered an order"

    @property
    def retryable(self) -> bool:
        """Retry only when every member failed cleanly with nothing resting."""
        return self.kept is None and all(
            placement.result is not None
            and placement.result.retryable
            and not placement.result.orders
            for placement in self.placements
        )

    @property
    def gave_up(self) -> bool:
        """Every member knocked out its budget with nothing accepted."""
        return self.kept is None and all(
            placement.result is not None
            and placement.result.gave_up
            and not placement.result.orders
            for placement in self.placements
        )

    def kept_order_ids(self) -> list[str]:
        return [order.order_id for order in self.orders if order.account == self.kept]

    def details(self) -> dict:
        members = {}
        for placement in self.placements:
            result = placement.result
            members[placement.member] = {
                "registered": placement.registered,
                "attempts": result.attempts if result else 0,
                "registered_ts_ms": result.registered_ts_ms if result else None,
                "venue_registered_ts_ms": (self.venue_registered_ts_ms or {}).get(
                    placement.member
                ),
                "held_back": result.held_back if result else None,
                "order_ids": [order.order_id for order in result.orders] if result else [],
                "error": placement.error or (result.error if result else None),
            }
        return {
            "kept": self.kept,
            "kept_by": self.kept_by,
            "cancelled_order_ids": list(self.cancelled_order_ids),
            "cancel_errors": list(self.cancel_errors),
            "members": members,
        }


def _cancel_outcome(result: object) -> tuple[list[str], list[str]]:
    if not isinstance(result, dict):
        return [], []
    canceled = [str(order_id) for order_id in result.get("canceled", [])]
    not_canceled = result.get("not_canceled")
    if not isinstance(not_canceled, dict):
        return canceled, []
    terminal = [
        str(order_id)
        for order_id, reason in not_canceled.items()
        if "already canceled or matched" in str(reason).lower()
    ]
    return canceled, terminal


def _raw_order_id(raw: object) -> str:
    if not isinstance(raw, dict):
        return ""
    return str(raw.get("id") or raw.get("orderID") or raw.get("orderId") or "")


class FleetOrderView:
    """Order lookups across every member, for reconciliation."""

    def __init__(self, members: tuple[FleetMember, ...]):
        self.members = tuple(members)

    def open_orders(self, condition_id: str | None = None) -> list[dict]:
        rows: list[dict] = []
        for member in self.members:
            for raw in member.exchange.open_orders(condition_id):
                if isinstance(raw, dict):
                    raw = {**raw, "account": member.name}
                rows.append(raw)
        return rows

    def get_order(self, order_id: str) -> dict | None:
        last_error: Exception | None = None
        for member in self.members:
            try:
                raw = member.exchange.get_order(order_id)
            except Exception as exc:  # noqa: BLE001 - next member may own it
                last_error = exc
                continue
            if _raw_order_id(raw) == order_id:
                return {**raw, "account": member.name}
        if last_error is not None:
            raise last_error
        return None


def evenly_phased(
    members: list[tuple[str, Exchange, Decimal]],
    interval_ms: Decimal,
) -> list[FleetMember]:
    """Spread N members across one cadence: offsets 0, 1/N, 2/N ... of it."""
    count = len(members)
    return [
        FleetMember(name, exchange, size, (interval_ms * index) / count)
        for index, (name, exchange, size) in enumerate(members)
    ]


class Fleet:
    def __init__(self, members: list[FleetMember], *, keep_best: bool = True):
        members = tuple(members)
        if not members:
            raise ValueError("a fleet needs at least one member")
        names = [member.name for member in members]
        if len(set(names)) != len(names):
            raise ValueError("fleet member names must be unique")
        self.members = members
        self.keep_best = keep_best
        self.order_view = FleetOrderView(members)
        # order id -> when the venue registered it, from the members' user
        # streams; the service wires it once those streams exist.
        self.registration_clock: Callable[[str], int | None] | None = None
        # Every member's requests come out of one process-wide pool, so each
        # one may only hold its share of it.
        for member in members:
            member.exchange.accounts_sharing_the_pool = len(members)

    @property
    def primary(self) -> FleetMember:
        return self.members[0]

    def member(self, name: str) -> FleetMember:
        for member in self.members:
            if member.name == name:
                return member
        raise KeyError(name)

    def place(
        self,
        market: Market,
        *,
        price: Decimal,
        submission_interval_ms: Decimal,
        knock_until_ts: float | None = None,
    ) -> FleetPlacement:
        outcomes: dict[str, MemberPlacement] = {}
        # One timetable for the whole fleet: every member sends only at
        # origin + its own offset + k * interval. Sleeping each member's
        # offset before it started instead put the offset ahead of the
        # warm-up and signing, whose cost is far larger and varies per call.
        grid_origin = time.monotonic()
        # And one knocking budget, so the members give up together.
        if knock_until_ts is None:
            knock_until_ts = time.time() + KNOCK_SECONDS

        def run(member: FleetMember) -> None:
            try:
                result = member.exchange.place_dual(
                    market,
                    price=price,
                    size=member.size,
                    submission_interval_ms=submission_interval_ms,
                    grid_origin=grid_origin,
                    phase_offset_ms=member.phase_offset_ms,
                    knock_until_ts=knock_until_ts,
                )
            except Exception as exc:  # noqa: BLE001 - reported per member
                outcomes[member.name] = MemberPlacement(
                    member.name, None, f"{type(exc).__name__}: {exc}"
                )
            else:
                outcomes[member.name] = MemberPlacement(member.name, result)

        threads = [
            Thread(target=run, args=(member,), name=f"fleet-{member.name}", daemon=True)
            for member in self.members
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        placements = tuple(outcomes[member.name] for member in self.members)
        kept, kept_by, stamps = self._choose(placements)
        orders, cancelled, errors = self._settle(placements, kept)
        return FleetPlacement(
            placements,
            kept,
            orders,
            tuple(cancelled),
            tuple(errors),
            kept_by=kept_by,
            venue_registered_ts_ms=stamps,
        )

    def _choose(
        self, placements: tuple[MemberPlacement, ...]
    ) -> tuple[str | None, str | None, dict[str, int] | None]:
        """Keep the member whose order the venue registered first.

        Which acceptance reply reaches us first is not the same thing: it
        adds the venue's reply, the way back, the HTTP stack handing the
        reply over and a member thread waking on a shared core, while the
        members register a few ms apart. In run38 that picked a later
        registration in 10 of 70 markets, 412 shares deeper at the median.
        The venue's own time settles it; the reply time is used only when a
        registered member's venue time never arrived, and then for every
        member, so the two clocks are never compared.
        """
        registered = [
            (index, placement)
            for index, placement in enumerate(placements)
            if placement.registered
        ]
        if not registered:
            return None, None, None
        stamps = self._venue_stamps(registered)
        if stamps is not None:
            chosen = min(
                registered, key=lambda item: (stamps[item[1].member], item[0])
            )
            return chosen[1].member, "venue", stamps

        def reply_key(item):
            registered_ts_ms = item[1].result.registered_ts_ms
            return (registered_ts_ms is None, registered_ts_ms or 0, item[0])

        return min(registered, key=reply_key)[1].member, "reply", None

    def _venue_stamps(
        self, registered: list[tuple[int, MemberPlacement]]
    ) -> dict[str, int] | None:
        """Each registered member's venue registration time, or None."""
        if self.registration_clock is None:
            return None
        deadline = time.monotonic() + VENUE_STAMP_WAIT_SECONDS
        while True:
            stamps: dict[str, int] = {}
            for _, placement in registered:
                found = [
                    self.registration_clock(order.order_id)
                    for order in placement.result.orders
                ]
                if found and all(stamp is not None for stamp in found):
                    stamps[placement.member] = min(found)
            if len(stamps) == len(registered):
                return stamps
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.01)

    def _settle(
        self,
        placements: tuple[MemberPlacement, ...],
        kept: str | None,
    ) -> tuple[tuple[PlacedOrder, ...], list[str], list[str]]:
        orders: list[PlacedOrder] = []
        cancelled: list[str] = []
        errors: list[str] = []
        for placement in placements:
            result = placement.result
            if result is None or not result.orders:
                continue
            stamped = [replace(order, account=placement.member) for order in result.orders]
            laggard = (
                self.keep_best
                and kept is not None
                and placement.member != kept
                and placement.registered
            )
            if not laggard:
                orders.extend(stamped)
                continue
            order_ids = [order.order_id for order in stamped]
            try:
                outcome = self.member(placement.member).exchange.cancel_orders(order_ids)
            except Exception as exc:  # noqa: BLE001 - reconciliation will retry
                errors.append(f"{placement.member}: {type(exc).__name__}: {exc}")
                orders.extend(replace(order, status="cancel_requested") for order in stamped)
                continue
            canceled_ids, terminal_ids = _cancel_outcome(outcome)
            for order in stamped:
                if order.order_id in canceled_ids:
                    status = "cancelled"
                    cancelled.append(order.order_id)
                elif order.order_id in terminal_ids:
                    status = "terminal_unknown"
                else:
                    status = "cancel_requested"
                orders.append(replace(order, status=status))
        return tuple(orders), cancelled, errors
