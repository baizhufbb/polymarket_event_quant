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
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import Thread

from .exchange import Exchange
from .models import Market, PlacedOrder, PlacementResult


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
                "order_ids": [order.order_id for order in result.orders] if result else [],
                "error": placement.error or (result.error if result else None),
            }
        return {
            "kept": self.kept,
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
    ) -> FleetPlacement:
        outcomes: dict[str, MemberPlacement] = {}

        def run(member: FleetMember) -> None:
            delay = float(member.phase_offset_ms) / 1000.0
            if delay > 0:
                time.sleep(delay)
            try:
                result = member.exchange.place_dual(
                    market,
                    price=price,
                    size=member.size,
                    submission_interval_ms=submission_interval_ms,
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
        kept = self._choose(placements)
        orders, cancelled, errors = self._settle(placements, kept)
        return FleetPlacement(placements, kept, orders, tuple(cancelled), tuple(errors))

    @staticmethod
    def _choose(placements: tuple[MemberPlacement, ...]) -> str | None:
        best: tuple[tuple, str] | None = None
        for index, placement in enumerate(placements):
            if not placement.registered:
                continue
            registered_ts_ms = placement.result.registered_ts_ms
            key = (registered_ts_ms is None, registered_ts_ms or 0, index)
            if best is None or key < best[0]:
                best = (key, placement.member)
        return best[1] if best else None

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
