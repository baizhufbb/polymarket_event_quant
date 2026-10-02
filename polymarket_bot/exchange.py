from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from decimal import Decimal

import certifi
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
from py_clob_client_v2.client import _is_v2_order
from py_clob_client_v2.exceptions import PolyApiException
from py_clob_client_v2.order_utils.model.order_data_v1 import order_to_json_v1
from py_clob_client_v2.order_utils.model.order_data_v2 import order_to_json_v2

from . import knocker
from .config import BotConfig
from .models import Market, PlacedOrder, PlacementResult
from .transport import install_parallel_transport, warm_connections

logger = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
TOKEN_SCALE = Decimal("1000000")
DEFAULT_PLACEMENT_INTERVAL_MS = Decimal("20")
# The client asks the venue for the market's tick size before signing, and a
# freshly announced market answers 404 "market not found" for its first tens
# of milliseconds. Run 14 lost 17 of 72 markets to that reply being fatal.
# Signing runs on the service loop, and that loop is also what cancels
# resting orders before a market ends (default margin 2 s), so the in-place
# wait must stay well below that margin; past it the market is handed back
# to the loop as a retryable placement and re-enters on the next 0.2 s tick.
SIGNING_NOT_READY_RETRY_SECONDS = 0.5
SIGNING_NOT_READY_POLL_SECONDS = 0.1
# How long a market is knocked before it is given up as skipped. Doors open
# 53..125 s after the listing on the venue's normal days (hourly p90 about
# 110 s over 2026-09-22/23) and minutes to hours on its bad ones; 240 s takes
# the whole normal distribution and bounds the damage of a bad one to four
# minutes of not-ready replies instead of a session pinned on one market.
KNOCK_SECONDS = 240.0
# The client re-resolves the order version when a reply says it is stale;
# at most this often.
VERSION_HEAL_INTERVAL_SECONDS = 30.0


class _SigningNotReady(RuntimeError):
    """The venue still rejects the market's tick-size lookup; retry later."""


class AmbiguousPlacementError(RuntimeError):
    """The exchange may have received a submission whose response was lost."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        attempts: int = 1,
    ):
        super().__init__(message)
        self.retryable = retryable
        self.attempts = attempts


# Replies are judged once, by the knock library; these read its verdict for
# the replies Python still handles itself (exits, reconciliation).


def _order_id(response: object) -> str | None:
    if not isinstance(response, dict):
        return None
    return knocker.classify(response).order_id


def _accepted(response: object) -> bool:
    return isinstance(response, dict) and knocker.classify(response).accepted


def _transient_submission_error(error: PolyApiException) -> bool:
    status = error.status_code
    return status is None or status == 429 or 500 <= status < 600


def _signing_retryable(error: PolyApiException) -> bool:
    """The tick-size lookup's own not-ready reply, or a transient transport failure."""
    payload = error.error_msg
    message = str(payload.get("error") or "").lower() if isinstance(payload, dict) else ""
    if error.status_code == 404 and message == "market not found":
        return True
    return _transient_submission_error(error)


@dataclass(frozen=True)
class Entry:
    """One account's signed entry for a market, ready to knock."""

    market: Market
    price: Decimal
    size: Decimal
    specifications: tuple[tuple[str, str], ...]
    # (outcome, the exact request body), in the order of specifications
    legs: tuple[tuple[str, str], ...]
    submission_key: tuple


def knock_plan(
    market: Market,
    members: list[dict],
    *,
    interval_ms: Decimal,
    knock_until_ts: float,
) -> dict:
    """What the knock library needs for one market.

    The certificate authorities are the ones the Python HTTP stack trusted
    (certifi), so the orders reach the same venue they always did.
    """
    return {
        "market": market.slug,
        "interval_ms": float(interval_ms),
        "knock_until_ms": int(knock_until_ts * 1000),
        "market_end_ms": int(market.end_ts * 1000),
        "ca_file": certifi.where(),
        "members": members,
    }


class Exchange:
    entry_submission = "single"
    attempt_trace = None
    _last_version_heal = 0.0

    def __init__(self, config: BotConfig):
        install_parallel_transport()
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
        # Whoever wants to open this account's user stream needs these, and an
        # account whose env file carries no API key has them only after the
        # derivation above.
        self.config = config
        self.credentials = creds
        self._dual_submissions: dict[tuple, list[PostOrdersV2Args]] = {}

    @staticmethod
    def geoblock() -> dict:
        response = requests.get(GEOBLOCK_URL, timeout=20)
        response.raise_for_status()
        return response.json()

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
        submission_interval_ms: Decimal | None = None,
        knock_until_ts: float | None = None,
    ) -> PlacementResult:
        """Knock this account's entry on its own (no fleet)."""
        if knock_until_ts is None:
            knock_until_ts = time.time() + KNOCK_SECONDS
        entry = self.prepare_entry(market, price=price, size=size)
        if isinstance(entry, PlacementResult):
            return entry
        if submission_interval_ms is None or submission_interval_ms <= 0:
            raise ValueError("submission_interval_ms must be above 0")
        outcome = knocker.knock(
            knock_plan(
                market,
                [self.knock_member(entry, name="primary", phase_offset_ms=Decimal(0))],
                interval_ms=submission_interval_ms,
                knock_until_ts=knock_until_ts,
            ),
            {"primary": self.knock_hooks()},
        )
        return self.settle_entry(entry, outcome["members"][0])

    def prepare_entry(
        self, market: Market, *, price: Decimal, size: Decimal
    ) -> Entry | PlacementResult:
        """Sign this account's entry orders for a knock.

        A market handed back earlier reuses the orders signed then: every
        send for a market is the same signed order, so a late one earns "not
        ready" or "duplicated", never a second order. Signing that the venue
        is not ready for comes back as a retryable PlacementResult.
        """
        # Cancels right after the door ride the sync pool; open its
        # connections before the burst.
        warm_connections()
        options = PartialCreateOrderOptions(
            tick_size=str(market.tick_size),
            neg_risk=False,
        )
        specifications = self._entry_specifications(market)
        submission_key = self._dual_submission_key(market, price=price, size=size)
        submissions = self._dual_submission_cache()
        signed = submissions.get(submission_key)
        if signed is None:
            self._prime_tick_size(market)
            try:
                signed = self._sign_entries(
                    specifications, options, price=price, size=size,
                    market_end_ts=market.end_ts,
                )
            except _SigningNotReady as exc:
                return PlacementResult((), str(exc), retryable=True, attempts=1)
            submissions[submission_key] = signed
        legs = tuple(
            (outcome, self._order_body(args))
            for (outcome, _), args in zip(specifications, signed, strict=True)
        )
        return Entry(market, price, size, specifications, legs, submission_key)

    def _order_body(self, args: PostOrdersV2Args) -> str:
        """The exact body the official client posts for this order, post-only.

        It is built by the library's own functions and never changes for a
        signed order; only the auth headers, stamped per send, move.
        """
        owner = self.client.creds.api_key or ""
        to_json = order_to_json_v2 if _is_v2_order(args.order) else order_to_json_v1
        body = to_json(
            args.order, owner, args.orderType, True, getattr(args, "deferExec", False)
        )
        return json.dumps(body, separators=(",", ":"), ensure_ascii=False)

    def knock_member(
        self, entry: Entry, *, name: str, phase_offset_ms: Decimal
    ) -> dict:
        """This account's part of a knock plan."""
        creds = self.credentials
        return {
            "account": name,
            "phase_ms": float(phase_offset_ms),
            "address": self.client.signer.address(),
            "api_key": creds.api_key,
            "api_secret": creds.api_secret,
            "api_passphrase": creds.api_passphrase,
            "legs": [{"outcome": outcome, "body": body} for outcome, body in entry.legs],
        }

    def knock_hooks(self) -> knocker.Hooks:
        return knocker.Hooks(
            trace=self.attempt_trace, version_mismatch=self._heal_order_version
        )

    def settle_entry(self, entry: Entry, outcome: dict) -> PlacementResult:
        """What this account's knock came to.

        Raises AmbiguousPlacementError when nothing registered and some sends
        never got a verdict: an order may rest at the venue that no reply
        told us about, and the caller reconciles against the open orders.
        """
        attempts = outcome["attempts"]
        held_back = outcome["held_back"]
        registered_ts_ms = outcome.get("registered_ms")
        tokens = dict(entry.specifications)
        accepted = []
        for order in outcome["accepted"]:
            reply = knocker.reply_value(order["status"], order["body"])
            status = reply.get("status") if isinstance(reply, dict) else None
            accepted.append(
                PlacedOrder(
                    order_id=str(order["order_id"]),
                    outcome=order["outcome"],
                    token_id=tokens[order["outcome"]],
                    price=entry.price,
                    size=entry.size,
                    status=str(status or "open").lower(),
                    raw=reply,
                    side="buy",
                    role="entry",
                )
            )
        ambiguous = [knocker.ambiguous_text(item) for item in outcome["ambiguous"]]
        if ambiguous and not accepted:
            unique = "; ".join(dict.fromkeys(ambiguous))
            raise AmbiguousPlacementError(
                f"staggered submission remained ambiguous: {unique}",
                attempts=attempts,
            )
        expected = len(entry.specifications)
        if len(accepted) == expected:
            # Every leg registered; replies gathered while draining (repeat
            # duplicates, late not-ready) are expected and not failures.
            result = PlacementResult(
                tuple(accepted),
                attempts=attempts,
                held_back=held_back,
                expected=expected,
                registered_ts_ms=registered_ts_ms,
            )
        else:
            errors = [knocker.error_text(item) for item in outcome["errors"]]
            error = "; ".join(e for e in dict.fromkeys(errors) if e) or "partial placement"
            if accepted:
                result = PlacementResult(
                    tuple(accepted),
                    error,
                    attempts=attempts,
                    held_back=held_back,
                    expected=expected,
                    registered_ts_ms=registered_ts_ms,
                )
            else:
                result = PlacementResult(
                    (),
                    error,
                    retryable=False,
                    attempts=attempts,
                    held_back=held_back,
                    expected=expected,
                    gave_up=outcome["gave_up"],
                )
        return self._finalize_dual_result(
            result,
            submission_key=entry.submission_key,
            submissions=self._dual_submission_cache(),
        )

    def _heal_order_version(self) -> None:
        """A reply said the order version is stale: have the client look the
        version up again, off the knock's thread, at most every 30 s."""
        now = time.monotonic()
        if now - self._last_version_heal < VERSION_HEAL_INTERVAL_SECONDS:
            return
        self._last_version_heal = now
        client = self.client

        def heal() -> None:
            try:
                client._ClobClient__resolve_version(force_update=True)
            except Exception as exc:  # noqa: BLE001 - healing is best effort
                logger.warning("order version heal failed: %s", exc)

        threading.Thread(target=heal, name="order-version-heal", daemon=True).start()

    def _finalize_dual_result(
        self,
        result: PlacementResult,
        *,
        submission_key: tuple,
        submissions: dict[tuple, list[PostOrdersV2Args]],
    ) -> PlacementResult:
        if result.complete:
            submissions.pop(submission_key, None)
            return result
        if result.orders:
            self.client.cancel_orders([order.order_id for order in result.orders])
            submissions.pop(submission_key, None)
        elif not result.retryable:
            submissions.pop(submission_key, None)
        return result

    def _entry_specifications(
        self, market: Market
    ) -> tuple[tuple[str, str], ...]:
        """Solo modes trade one deliberate leg; paired modes trade both."""
        if self.entry_submission == "solo-up":
            return (("up", market.up_token_id),)
        if self.entry_submission == "solo-down":
            return (("down", market.down_token_id),)
        return (
            ("up", market.up_token_id),
            ("down", market.down_token_id),
        )

    def _prime_tick_size(self, market: Market) -> None:
        """Hand the client the tick size it would otherwise ask the venue for.

        The client asks the venue for the market's minimum tick only to check
        ours is not finer; the order it signs carries the value we pass either
        way, and discovery already read that value off the market listing. The
        lookup is the one network call left in signing, it happens once per
        token, and a market announced moments ago answers 404 to it - which is
        how a fleet member sat out a whole market while the other one sent
        three thousand times.
        """
        cache = getattr(self.client, "_ClobClient__tick_sizes", None)
        if not isinstance(cache, dict):
            if isinstance(self.client, ClobClient):
                raise RuntimeError(
                    "py_clob_client_v2 no longer keeps tick sizes in "
                    "_ClobClient__tick_sizes; signing would quietly go back "
                    "to asking the venue for them"
                )
            return
        for token_id in (market.up_token_id, market.down_token_id):
            cache.setdefault(token_id, str(market.tick_size))
        self._warm_protocol_version()

    def _warm_protocol_version(self) -> None:
        """Ask for the protocol version now rather than inside a signature.

        The client caches it, but lazily, and nothing warmed it - so the first
        signature of every account still went to the venue and the round trip
        landed in front of the first market of a session. The reply is
        forgiving (any failure answers the current version), so this only ever
        moves the call, never adds one.
        """
        if getattr(self.client, "_ClobClient__cached_version", None) is not None:
            return
        getter = getattr(self.client, "get_version", None)
        if not callable(getter):
            return
        try:
            self.client._ClobClient__cached_version = getter()
        except Exception:  # noqa: BLE001 - the library answers a default anyway
            pass

    def _sign_entries(
        self,
        specifications,
        options: PartialCreateOrderOptions,
        *,
        price: Decimal,
        size: Decimal,
        market_end_ts: int,
    ) -> list[PostOrdersV2Args]:
        """Sign the entry legs, waiting out the venue's post-announcement gap.

        Signing is local, but the client first asks the venue for the tick
        size, and a market announced moments ago still answers 404 "market
        not found"; transient transport failures (429, 5xx, no status) are
        treated the same way, as the submission loop already does. Polls are
        paced so the retry does not spin, and the exit is the wall clock, so
        the loop is held for at most the budget plus one reply however slow
        the venue answers. Past the budget the market is handed back as
        _SigningNotReady; anything else is raised as before.
        """
        start = time.monotonic()
        next_poll = start
        deadline = start + SIGNING_NOT_READY_RETRY_SECONDS
        while True:
            try:
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
                    signed.append(
                        PostOrdersV2Args(order=order, orderType=OrderType.GTC)
                    )
                return signed
            except PolyApiException as exc:
                if not _signing_retryable(exc):
                    raise
                if time.time() >= market_end_ts:
                    raise
                now = time.monotonic()
                # Clamp the next slot to now: a slow reply must not turn the
                # slots it overran into a zero-delay burst (against 429 too).
                next_poll = max(next_poll + SIGNING_NOT_READY_POLL_SECONDS, now)
                if now >= deadline or next_poll >= deadline:
                    raise _SigningNotReady(
                        f"signing not ready after {SIGNING_NOT_READY_RETRY_SECONDS:g}s: {exc}"
                    )
                time.sleep(next_poll - now)

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
        retryable_if_missing: bool = True,
    ) -> PlacementResult:
        specifications = self._entry_specifications(market)
        expected = {token_id: outcome for outcome, token_id in specifications}
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
            return PlacementResult(tuple(orders), expected=len(specifications))

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
        if not retryable_if_missing:
            self._forget_dual_submission(market, price=price, size=size)
        return PlacementResult(
            (),
            "ambiguous submission did not produce exactly two orders",
            retryable=retryable_if_missing,
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
