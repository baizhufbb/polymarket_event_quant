from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ExitTarget:
    price: Decimal
    fraction: Decimal


@dataclass(frozen=True)
class TradePlan:
    buy_price: Decimal
    exit_targets: tuple[ExitTarget, ...]
    usd_per_side: Decimal

    @property
    def buy_only(self) -> bool:
        return not self.exit_targets

    @property
    def hold_fraction(self) -> Decimal:
        return Decimal("1") - sum(
            (target.fraction for target in self.exit_targets), Decimal("0")
        )

    @property
    def order_size(self) -> Decimal:
        return self.usd_per_side / self.buy_price

    @property
    def market_reserve(self) -> Decimal:
        return self.usd_per_side * 2

    def validate(self) -> None:
        if not Decimal("0") < self.buy_price < Decimal("1"):
            raise ValueError("--buy-price must be between 0 and 1")
        if self.usd_per_side <= 0:
            raise ValueError("--usd-per-side must be positive")
        if self.order_size < 5:
            raise ValueError("configured entry must be at least 5 shares")
        prices = [target.price for target in self.exit_targets]
        if prices != sorted(prices) or len(prices) != len(set(prices)):
            raise ValueError("--take-profit prices must be unique and ascending")
        for target in self.exit_targets:
            if not self.buy_price < target.price < Decimal("1"):
                raise ValueError(
                    "--take-profit prices must be above --buy-price and below 1"
                )
            if not Decimal("0") < target.fraction <= Decimal("1"):
                raise ValueError(
                    "--take-profit fractions must be above 0 and at most 1"
                )
            if self.order_size * target.fraction < 5:
                raise ValueError(
                    "each --take-profit rung must contain at least 5 shares"
                )
        if self.hold_fraction < 0:
            raise ValueError("--take-profit fractions cannot total more than 1")

    def as_dict(self) -> dict:
        return {
            "buy_price": str(self.buy_price),
            "take_profit": [
                {"price": str(target.price), "fraction": str(target.fraction)}
                for target in self.exit_targets
            ],
            "hold_fraction": str(self.hold_fraction),
            "usd_per_side": str(self.usd_per_side),
        }


@dataclass(frozen=True)
class Market:
    slug: str
    condition_id: str
    start_ts: int
    end_ts: int
    up_token_id: str
    down_token_id: str
    min_size: Decimal
    tick_size: Decimal


@dataclass(frozen=True)
class PlacedOrder:
    order_id: str
    outcome: str
    token_id: str
    price: Decimal
    size: Decimal
    status: str
    raw: object
    side: str = "buy"
    role: str = "entry"
    account: str = ""


@dataclass(frozen=True)
class PlacementResult:
    orders: tuple[PlacedOrder, ...]
    error: str | None = None
    retryable: bool = False
    attempts: int = 1
    expected: int = 2
    registered_ts_ms: int | None = None

    @property
    def complete(self) -> bool:
        return self.error is None and len(self.orders) == self.expected
