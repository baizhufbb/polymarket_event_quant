from __future__ import annotations

import re
import time
from decimal import Decimal

import requests
from py_clob_client_v2 import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    PostOrdersV2Args,
    Side,
)
from py_clob_client_v2.exceptions import PolyApiException

from .config import BotConfig
from .models import Market, PlacedOrder, PlacementResult


CLOB_HOST = "https://clob.polymarket.com"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
TOKEN_SCALE = Decimal("1000000")
MARKET_NOT_READY = "the market is not yet ready to process new orders"
ORDERBOOK_MISSING_PREFIX = "the orderbook "
ORDERBOOK_MISSING_SUFFIX = " does not exist"
ORDER_ENGINE_NOT_READY_ERRORS = {
    (400, "invalid token id"),
    (404, "market not found"),
}
TRANSIENT_RETRY_SECONDS = 0.03
DUPLICATE_ORDER_PATTERN = re.compile(
    r"\border\s+(0x[0-9a-f]{64})\s+is invalid\.\s*duplicated\.",
    re.IGNORECASE,
)


def _duplicate_order_id(response: object) -> str | None:
    if not isinstance(response, dict):
        return None
    match = DUPLICATE_ORDER_PATTERN.search(str(response.get("errorMsg") or ""))
    return match.group(1) if match else None


def _order_id(response: object) -> str | None:
    if not isinstance(response, dict):
        return None
    return (
        response.get("orderID") or response.get("orderId") or response.get("order_id")
        or _duplicate_order_id(response)
    )


def _accepted(response: object) -> bool:
    if not isinstance(response, dict):
        return False
    if _duplicate_order_id(response):
        return True
    if response.get("success") is False:
        return False
    return bool(_order_id(response))


def _order_engine_not_ready(response: object) -> bool:
    if not isinstance(response, dict) or _order_id(response):
        return False
    error = str(response.get("errorMsg") or "").lower()
    return error == MARKET_NOT_READY or (
        error.startswith(ORDERBOOK_MISSING_PREFIX)
        and error.endswith(ORDERBOOK_MISSING_SUFFIX)
    )


def _transient_submission_error(error: PolyApiException) -> bool:
    status = error.status_code
    return status is None or 500 <= status < 600


def _order_engine_not_ready_error(error: PolyApiException) -> str | None:
    payload = error.error_msg
    if not isinstance(payload, dict):
        return None
    message = str(payload.get("error") or "").lower()
    if (error.status_code, message) not in ORDER_ENGINE_NOT_READY_ERRORS:
        return None
    return message


def _heartbeat_id(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("heartbeat_id") or payload.get("heartbeatId")
    return str(value) if value else None


class Exchange:
    def __init__(self, config: BotConfig):
        creds = None
        if config.api_key:
            creds = ApiCreds(
                api_key=config.api_key,
                api_secret=config.api_secret or "",
                api_passphrase=config.api_passphrase or "",
            )
        bootstrap = ClobClient(
            host=CLOB_HOST,
            chain_id=137,
            key=config.private_key,
            creds=creds,
            signature_type=config.signature_type,
            funder=config.funder_address,
            retry_on_error=False,
        )
        if creds is None:
            creds = bootstrap.create_or_derive_api_key()
        self.client = ClobClient(
            host=CLOB_HOST,
            chain_id=137,
            key=config.private_key,
            creds=creds,
            signature_type=config.signature_type,
            funder=config.funder_address,
            retry_on_error=False,
        )
        self.heartbeat_id = ""
        self._dual_submissions: dict[tuple, list[PostOrdersV2Args]] = {}

    @staticmethod
    def geoblock() -> dict:
        response = requests.get(GEOBLOCK_URL, timeout=20)
        response.raise_for_status()
        return response.json()

    def heartbeat(self) -> dict:
        try:
            response = self.client.post_heartbeat(self.heartbeat_id)
        except PolyApiException as exc:
            replacement = (
                _heartbeat_id(exc.error_msg) if exc.status_code == 400 else None
            )
            if replacement is None:
                raise
            self.heartbeat_id = replacement
            response = self.client.post_heartbeat(self.heartbeat_id)
        heartbeat_id = response.get("heartbeat_id") or response.get("heartbeatId")
        if heartbeat_id:
            self.heartbeat_id = str(heartbeat_id)
        return response

    def doctor(self, signature_type: int) -> dict:
        return {
            "geoblock": self.geoblock(),
            "server_time": self.client.get_server_time(),
            "collateral": self.client.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=signature_type,
                )
            ),
            "open_orders": self.client.get_open_orders(),
        }

    def place_dual(
        self,
        market: Market,
        *,
        price: Decimal,
        size: Decimal,
    ) -> PlacementResult:
        options = PartialCreateOrderOptions(
            tick_size=str(market.tick_size),
            neg_risk=False,
        )
        specifications = (
            ("up", market.up_token_id),
            ("down", market.down_token_id),
        )
        submission_key = self._dual_submission_key(market, price=price, size=size)
        submissions = self._dual_submission_cache()
        signed = submissions.get(submission_key)
        if signed is None:
            signed = []
            for _, token_id in specifications:
                order = self.client.create_order(
                    OrderArgs(
                        token_id=token_id,
                        price=float(price),
                        size=float(size),
                        side=Side.BUY,
                    ),
                    options,
                )
                signed.append(PostOrdersV2Args(order=order, orderType=OrderType.GTC))
            submissions[submission_key] = signed

        try:
            responses = self.client.post_orders(signed, post_only=True)
        except PolyApiException as exc:
            not_ready_error = _order_engine_not_ready_error(exc)
            if not_ready_error:
                return PlacementResult((), not_ready_error, retryable=True)
            if not _transient_submission_error(exc):
                submissions.pop(submission_key, None)
                raise
            time.sleep(TRANSIENT_RETRY_SECONDS)
            try:
                responses = self.client.post_orders(signed, post_only=True)
            except Exception as retry_exc:
                if isinstance(retry_exc, PolyApiException):
                    not_ready_error = _order_engine_not_ready_error(retry_exc)
                    if not_ready_error:
                        return PlacementResult((), not_ready_error, retryable=True)
                raise RuntimeError(
                    f"transient submission retry failed: "
                    f"initial={exc}; retry={type(retry_exc).__name__}: {retry_exc}"
                ) from retry_exc
        accepted = []
        errors = []
        for (outcome, token_id), response in zip(
            specifications, responses, strict=True
        ):
            if _accepted(response):
                accepted.append(
                    PlacedOrder(
                        order_id=str(_order_id(response)),
                        outcome=outcome,
                        token_id=token_id,
                        price=price,
                        size=size,
                        status=str(response.get("status") or "open").lower(),
                        raw=response,
                        side="buy",
                        role="entry",
                    )
                )
            else:
                errors.append(str(response))

        if len(accepted) == 2:
            submissions.pop(submission_key, None)
            return PlacementResult(tuple(accepted))
        if accepted:
            self.client.cancel_orders([order.order_id for order in accepted])
            submissions.pop(submission_key, None)
        retryable = (
            not accepted
            and len(responses) == 2
            and all(_order_engine_not_ready(response) for response in responses)
        )
        if not retryable:
            submissions.pop(submission_key, None)
        return PlacementResult(
            tuple(accepted),
            "; ".join(errors) or "partial placement",
            retryable=retryable,
        )

    @staticmethod
    def _dual_submission_key(
        market: Market,
        *,
        price: Decimal,
        size: Decimal,
    ) -> tuple:
        return (
            market.condition_id,
            market.up_token_id,
            market.down_token_id,
            price,
            size,
        )

    def _dual_submission_cache(self) -> dict[tuple, list[PostOrdersV2Args]]:
        cache = getattr(self, "_dual_submissions", None)
        if cache is None:
            cache = {}
            self._dual_submissions = cache
        return cache

    def _forget_dual_submission(
        self,
        market: Market,
        *,
        price: Decimal,
        size: Decimal,
    ) -> None:
        key = self._dual_submission_key(market, price=price, size=size)
        self._dual_submission_cache().pop(key, None)

    def reconcile_ambiguous_dual(
        self,
        market: Market,
        *,
        price: Decimal,
        size: Decimal,
    ) -> PlacementResult:
        expected = {
            market.up_token_id: "up",
            market.down_token_id: "down",
        }
        matches: dict[str, list[dict]] = {token_id: [] for token_id in expected}
        for raw in self.open_orders(market.condition_id):
            token_id = str(raw.get("asset_id") or raw.get("assetId") or "")
            raw_price = Decimal(str(raw.get("price") or "0"))
            raw_size = Decimal(
                str(
                    raw.get("original_size")
                    or raw.get("originalSize")
                    or raw.get("size")
                    or "0"
                )
            )
            side = str(raw.get("side") or "").upper()
            if (
                token_id in expected
                and side == "BUY"
                and raw_price == price
                and raw_size == size
            ):
                matches[token_id].append(raw)

        if all(len(rows) == 1 for rows in matches.values()):
            orders = []
            for token_id, rows in matches.items():
                raw = rows[0]
                order_id = _order_id(raw) or raw.get("id")
                if not order_id:
                    return PlacementResult((), "matched open order has no order id")
                orders.append(
                    PlacedOrder(
                        order_id=str(order_id),
                        outcome=expected[token_id],
                        token_id=token_id,
                        price=price,
                        size=size,
                        status=str(raw.get("status") or "open").lower(),
                        raw=raw,
                        side="buy",
                        role="entry",
                    )
                )
            self._forget_dual_submission(market, price=price, size=size)
            return PlacementResult(tuple(orders))

        found_ids = [
            str(_order_id(raw) or raw.get("id"))
            for rows in matches.values()
            for raw in rows
            if _order_id(raw) or raw.get("id")
        ]
        if found_ids:
            self.cancel_orders(found_ids)
            self._forget_dual_submission(market, price=price, size=size)
            return PlacementResult(
                (),
                "ambiguous submission produced a partial or duplicate order set",
            )
        return PlacementResult(
            (),
            "ambiguous submission did not produce exactly two orders",
            retryable=True,
        )

    def place_exit(
        self,
        market: Market,
        *,
        outcome: str,
        token_id: str,
        price: Decimal,
        size: Decimal,
    ) -> PlacedOrder:
        signed = self.client.create_order(
            OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=Side.SELL,
            ),
            PartialCreateOrderOptions(
                tick_size=str(market.tick_size),
                neg_risk=False,
            ),
        )
        response = self.client.post_order(signed, OrderType.GTC, post_only=False)
        if not _accepted(response):
            raise RuntimeError(f"sell order rejected: {response}")
        return PlacedOrder(
            order_id=str(_order_id(response)),
            outcome=outcome,
            token_id=token_id,
            price=price,
            size=size,
            status=str(response.get("status") or "open").lower(),
            raw=response,
            side="sell",
            role="exit",
        )

    def reconcile_ambiguous_exit(
        self,
        market: Market,
        *,
        outcome: str,
        token_id: str,
        price: Decimal,
        size: Decimal,
    ) -> PlacedOrder | None:
        matches = []
        for raw in self.open_orders(market.condition_id):
            raw_token_id = str(raw.get("asset_id") or raw.get("assetId") or "")
            raw_price = Decimal(str(raw.get("price") or "0"))
            raw_size = Decimal(
                str(
                    raw.get("original_size")
                    or raw.get("originalSize")
                    or raw.get("size")
                    or "0"
                )
            )
            side = str(raw.get("side") or "").upper()
            if (
                raw_token_id == token_id
                and side == "SELL"
                and raw_price == price
                and raw_size == size
            ):
                matches.append(raw)
        if len(matches) != 1:
            return None
        raw = matches[0]
        order_id = _order_id(raw) or raw.get("id")
        if not order_id:
            return None
        return PlacedOrder(
            order_id=str(order_id),
            outcome=outcome,
            token_id=token_id,
            price=price,
            size=size,
            status=str(raw.get("status") or "open").lower(),
            raw=raw,
            side="sell",
            role="exit",
        )

    def open_orders(self, condition_id: str | None = None) -> list[dict]:
        if condition_id is None:
            return self.client.get_open_orders()
        from py_clob_client_v2 import OpenOrderParams

        return self.client.get_open_orders(OpenOrderParams(market=condition_id))

    def get_order(self, order_id: str) -> dict:
        return self.client.get_order(order_id)

    def order_books_ready(self, market: Market) -> bool:
        for token_id in (market.up_token_id, market.down_token_id):
            try:
                self.client.get_order_book(token_id)
            except PolyApiException as exc:
                if "does not exist" in str(exc).lower():
                    return False
                raise
        return True

    def conditional_balance(self, token_id: str) -> Decimal:
        response = self.client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
        )
        return Decimal(str(response.get("balance") or "0")) / TOKEN_SCALE

    def cancel_orders(self, order_ids: list[str]) -> object:
        if not order_ids:
            return None
        return self.client.cancel_orders(order_ids)


def normalize_order(raw: dict) -> tuple[str, Decimal]:
    status = str(raw.get("status") or raw.get("type") or "unknown").lower()
    if status.startswith("order_status_"):
        status = status.removeprefix("order_status_")
    if status in {"cancelled_market_resolved", "canceled_market_resolved"}:
        status = "cancelled"
    matched = Decimal(
        str(
            raw.get("size_matched")
            or raw.get("sizeMatched")
            or raw.get("matched_size")
            or "0"
        )
    )
    return status, matched
