from __future__ import annotations

import math
import re
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from decimal import Decimal
from threading import Thread

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
from .transport import install_parallel_transport, warm_connections


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
DEFAULT_PLACEMENT_INTERVAL_MS = Decimal("20")
# The client asks the venue for the market's tick size before signing, and a
# freshly announced market answers 404 "market not found" for its first tens
# of milliseconds. Run 14 lost 17 of 72 markets to that reply being fatal.
# Signing runs on the service loop (or a fleet member thread the loop joins),
# and that loop is also what cancels resting orders before a market ends
# (default margin 2 s), so the in-place wait must stay well below that
# margin; past it the market is handed back to the loop as a retryable
# placement, the same path the submission loop uses, and re-enters on the
# next 0.2 s tick.
SIGNING_NOT_READY_RETRY_SECONDS = 0.5
SIGNING_NOT_READY_POLL_SECONDS = 0.1
DRAIN_TIMEOUT_SECONDS = 3.0
# A moment this close after a slot still counts as that slot. The monotonic
# clock reads in the millions of seconds, where a double carries about a
# nanosecond of noise, so without slack the arithmetic below rounds up at
# random and silently skips every other slot. One microsecond swamps that
# noise and is far below anything the scheduler can resolve.
_GRID_TOLERANCE_SECONDS = 1e-6


def submission_slot(
    moment: float, *, origin: float, phase: float, interval: float
) -> float:
    """First slot at or after `moment` on the timetable origin+phase+k*interval.

    Fleet members share `origin` and differ only in `phase`, so their sends
    stay that far apart however long each member's warm-up and signing took,
    and a member that misses slots resumes on its own next one rather than
    starting a fresh timetable at "now".
    """
    offset = moment - origin - phase - _GRID_TOLERANCE_SECONDS
    return origin + phase + math.ceil(offset / interval) * interval


DUPLICATE_ORDER_PATTERN = re.compile(
    r"\border\s+(0x[0-9a-f]{64})\s+is invalid\.\s*duplicated\.",
    re.IGNORECASE,
)


class _SigningNotReady(RuntimeError):
    """The venue still rejects the market's tick-size lookup; retry later."""


class AmbiguousPlacementError(RuntimeError):
    """The exchange may have received a batch whose response was lost."""

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
    return (
        error == MARKET_NOT_READY
        or error in {"invalid token id", "market not found"}
        or (
            error.startswith(ORDERBOOK_MISSING_PREFIX)
            and error.endswith(ORDERBOOK_MISSING_SUFFIX)
        )
    )


def _transient_submission_error(error: PolyApiException) -> bool:
    status = error.status_code
    return status is None or status == 429 or 500 <= status < 600


def _order_engine_not_ready_error(error: PolyApiException) -> str | None:
    payload = error.error_msg
    if not isinstance(payload, dict):
        return None
    message = str(payload.get("error") or "").lower()
    if (error.status_code, message) not in ORDER_ENGINE_NOT_READY_ERRORS:
        return None
    return message


def _signing_retryable(error: PolyApiException) -> bool:
    """The tick-size lookup's own not-ready reply, or a transient transport failure."""
    payload = error.error_msg
    message = str(payload.get("error") or "").lower() if isinstance(payload, dict) else ""
    if error.status_code == 404 and message == "market not found":
        return True
    return _transient_submission_error(error)


def classify_response(response: object) -> str:
    """Bucket one submission response for the attempt trace."""
    if isinstance(response, BaseException):
        status = getattr(response, "status_code", None)
        if status == 429:
            return "rate_limited"
        return f"error_{status}" if status else "transport_error"
    if _duplicate_order_id(response):
        return "duplicate"
    if _accepted(response):
        return "accepted"
    if _order_engine_not_ready(response):
        return "not_ready"
    return "rejected"


class Exchange:
    _next_placement_submission = 0.0
    entry_submission = "batch"
    attempt_trace = None

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
        self._dual_submissions: dict[tuple, list[PostOrdersV2Args]] = {}
        self._next_placement_submission = 0.0

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
        grid_origin: float | None = None,
        phase_offset_ms: Decimal = Decimal(0),
    ) -> PlacementResult:
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
            try:
                signed = self._sign_entries(
                    specifications, options, price=price, size=size,
                    market_end_ts=market.end_ts,
                )
            except _SigningNotReady as exc:
                return PlacementResult((), str(exc), retryable=True, attempts=1)
            submissions[submission_key] = signed

        if submission_interval_ms is not None:
            if submission_interval_ms <= 0:
                raise ValueError("submission_interval_ms must be above 0")
            return self._place_dual_staggered(
                specifications,
                signed,
                price=price,
                size=size,
                submission_interval_ms=submission_interval_ms,
                grid_origin=grid_origin,
                phase_offset_ms=phase_offset_ms,
                market_end_ts=market.end_ts,
                submission_key=submission_key,
                submissions=submissions,
            )

        attempts = 1
        try:
            responses = self.client.post_orders(signed, post_only=True)
        except PolyApiException as exc:
            not_ready_error = _order_engine_not_ready_error(exc)
            if not_ready_error:
                return PlacementResult(
                    (), not_ready_error, retryable=True, attempts=attempts
                )
            if not _transient_submission_error(exc):
                submissions.pop(submission_key, None)
                return PlacementResult(
                    (),
                    f"{type(exc).__name__}: {exc}",
                    attempts=attempts,
                )
            attempts += 1
            try:
                responses = self.client.post_orders(signed, post_only=True)
            except Exception as retry_exc:
                retryable = True
                if isinstance(retry_exc, PolyApiException):
                    not_ready_error = _order_engine_not_ready_error(retry_exc)
                    if not_ready_error:
                        return PlacementResult(
                            (),
                            not_ready_error,
                            retryable=True,
                            attempts=attempts,
                        )
                    retryable = _transient_submission_error(retry_exc)
                raise AmbiguousPlacementError(
                    f"transient submission retry failed: "
                    f"initial={exc}; retry={type(retry_exc).__name__}: {retry_exc}",
                    retryable=retryable,
                    attempts=attempts,
                ) from retry_exc
        except Exception as exc:
            raise AmbiguousPlacementError(
                f"submission response unavailable: {type(exc).__name__}: {exc}",
                attempts=attempts,
            ) from exc

        result = self._parse_dual_responses(
            specifications,
            responses,
            price=price,
            size=size,
            attempts=attempts,
        )
        return self._finalize_dual_result(
            result,
            submission_key=submission_key,
            submissions=submissions,
        )

    def _place_dual_staggered(
        self,
        specifications: tuple[tuple[str, str], tuple[str, str]],
        signed: list[PostOrdersV2Args],
        *,
        price: Decimal,
        size: Decimal,
        submission_interval_ms: Decimal,
        market_end_ts: int,
        submission_key: tuple,
        submissions: dict[tuple, list[PostOrdersV2Args]],
        grid_origin: float | None = None,
        phase_offset_ms: Decimal = Decimal(0),
    ) -> PlacementResult:
        """Submit one immutable signed pair on this member's timetable.

        Every send falls on `grid_origin + phase_offset + k * interval`. The
        fleet hands all members one origin, so their offsets hold whatever
        each member's warm-up and signing cost, and a member that misses a
        slot resumes on its own next slot instead of re-basing on "now" -
        which is what collapsed the offsets when both members stalled on the
        same core.
        """
        interval = float(submission_interval_ms / Decimal("1000"))
        phase = float(phase_offset_ms / Decimal("1000"))
        # Without a fleet the timetable starts here, so this same reading is
        # what the first slot must be snapped from. Reading the clock a second
        # time below would push the first send a whole interval out whenever
        # the two readings differ, which they usually do.
        started = time.monotonic()
        origin = started if grid_origin is None else grid_origin

        def slot_at_or_after(moment: float) -> float:
            return submission_slot(
                moment, origin=origin, phase=phase, interval=interval
            )

        pending: dict[Future, tuple[tuple[tuple[str, str], ...], int, int]] = {}
        accepted: dict[str, PlacedOrder] = {}
        errors: list[str] = []
        ambiguous_errors: list[str] = []
        attempts = 0
        registered_ts_ms: int | None = None
        next_submission = slot_at_or_after(
            max(started, self._next_placement_submission)
        )
        stop_submitting = False
        drain_deadline: float | None = None

        while pending or not stop_submitting:
            now = time.monotonic()
            if not stop_submitting and time.time() >= market_end_ts:
                errors.append("market ended before both orders were accepted")
                stop_submitting = True
            if not stop_submitting and now >= next_submission:
                submit_specs, payload = self._next_submission(
                    specifications, signed, accepted, attempts
                )
                pending[self._submit_placement_request(payload)] = (
                    submit_specs,
                    int(time.time() * 1000),
                    attempts + 1,
                )
                attempts += 1
                # Next slot on this member's own timetable. A stall skips the
                # slots it ate rather than re-basing the timetable, so the
                # offset from the other members survives it.
                next_submission = slot_at_or_after(
                    max(next_submission + interval, time.monotonic())
                )
                self._next_placement_submission = next_submission
                continue

            if not stop_submitting:
                timeout = max(0.0, next_submission - now)
            elif drain_deadline is None:
                timeout = None
            else:
                timeout = max(0.0, drain_deadline - now)
                if timeout <= 0:
                    break
            if not pending:
                if stop_submitting:
                    break
                if timeout is not None:
                    time.sleep(timeout)
                continue

            completed, _ = wait(
                set(pending),
                timeout=timeout,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                continue

            for future in completed:
                submit_specs, sent_ts_ms, attempt_no = pending.pop(future)
                returned_ts_ms = int(time.time() * 1000)
                try:
                    responses = future.result()
                    self._trace(
                        submit_specs, attempt_no, sent_ts_ms, returned_ts_ms, responses
                    )
                except PolyApiException as exc:
                    self._trace(
                        submit_specs, attempt_no, sent_ts_ms, returned_ts_ms, exc
                    )
                    not_ready_error = _order_engine_not_ready_error(exc)
                    if not_ready_error:
                        errors.append(not_ready_error)
                    elif _transient_submission_error(exc):
                        ambiguous_errors.append(f"{type(exc).__name__}: {exc}")
                    else:
                        errors.append(f"{type(exc).__name__}: {exc}")
                        stop_submitting = True
                    drain_deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
                    continue
                except Exception as exc:
                    self._trace(
                        submit_specs, attempt_no, sent_ts_ms, returned_ts_ms, exc
                    )
                    ambiguous_errors.append(f"{type(exc).__name__}: {exc}")
                    stop_submitting = True
                    continue

                try:
                    result = self._parse_dual_responses(
                        submit_specs,
                        responses,
                        price=price,
                        size=size,
                        attempts=attempts,
                    )
                except AmbiguousPlacementError as exc:
                    ambiguous_errors.append(str(exc))
                    continue

                errors.append(result.error or "")
                for order in result.orders:
                    existing = accepted.get(order.outcome)
                    if existing is not None and existing.order_id != order.order_id:
                        ambiguous_errors.append(
                            f"conflicting {order.outcome} order ids: "
                            f"{existing.order_id}, {order.order_id}"
                        )
                        stop_submitting = True
                        continue
                    accepted[order.outcome] = order
                    if registered_ts_ms is None or returned_ts_ms < registered_ts_ms:
                        registered_ts_ms = returned_ts_ms
                    # sent_ts_ms/returned_ts_ms belong to the request whose
                    # response is being handled right now, not to whichever
                    # tick the loop has reached: responses come back out of
                    # order and dozens of ticks later.

                if len(accepted) == len(specifications):
                    # Stop sending, but drain the replies still in flight: the
                    # request that actually registered the order is usually an
                    # earlier one whose reply has not arrived yet, and dropping
                    # it here hides which attempt won the queue slot.
                    stop_submitting = True
                    continue

                recoverable = (
                    isinstance(responses, (list, tuple))
                    and len(responses) == len(submit_specs)
                    and all(
                        _accepted(response) or _order_engine_not_ready(response)
                        for response in responses
                    )
                )
                if not recoverable:
                    stop_submitting = True

        if ambiguous_errors and not accepted:
            unique = "; ".join(dict.fromkeys(ambiguous_errors))
            raise AmbiguousPlacementError(
                f"staggered submission remained ambiguous: {unique}",
                attempts=attempts,
            )

        if len(accepted) == len(specifications):
            # Every leg registered; replies gathered while draining (repeat
            # duplicates, late not-ready) are expected and not failures.
            ordered = tuple(accepted[outcome] for outcome, _ in specifications)
            return self._finalize_dual_result(
                PlacementResult(
                    ordered,
                    attempts=attempts,
                    expected=len(specifications),
                    registered_ts_ms=registered_ts_ms,
                ),
                submission_key=submission_key,
                submissions=submissions,
            )

        meaningful_errors = [error for error in dict.fromkeys(errors) if error]
        error = "; ".join(meaningful_errors) or "partial placement"
        if accepted:
            accepted_orders = tuple(
                accepted[outcome]
                for outcome, _ in specifications
                if outcome in accepted
            )
            result = PlacementResult(
                accepted_orders,
                error,
                attempts=attempts,
                expected=len(specifications),
                registered_ts_ms=registered_ts_ms,
            )
        else:
            result = PlacementResult(
                (),
                error,
                retryable=not stop_submitting,
                attempts=attempts,
                expected=len(specifications),
            )
        return self._finalize_dual_result(
            result,
            submission_key=submission_key,
            submissions=submissions,
        )

    @staticmethod
    def _parse_dual_responses(
        specifications: tuple[tuple[str, str], tuple[str, str]],
        responses: object,
        *,
        price: Decimal,
        size: Decimal,
        attempts: int,
    ) -> PlacementResult:
        if not isinstance(responses, (list, tuple)) or len(responses) != len(
            specifications
        ):
            raise AmbiguousPlacementError(
                "submission returned an incomplete dual-order response",
                attempts=attempts,
            )
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

        if len(accepted) == len(specifications):
            return PlacementResult(
                tuple(accepted), attempts=attempts, expected=len(specifications)
            )
        retryable = not accepted and all(
            _order_engine_not_ready(response) for response in responses
        )
        return PlacementResult(
            tuple(accepted),
            "; ".join(errors) or "partial placement",
            retryable=retryable,
            attempts=attempts,
            expected=len(specifications),
        )

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

    def _submit_placement_request(
        self,
        signed: list[PostOrdersV2Args],
    ) -> Future:
        future: Future = Future()

        def submit() -> None:
            if not future.set_running_or_notify_cancel():
                return
            try:
                if self.entry_submission == "batch":
                    response = self.client.post_orders(signed, post_only=True)
                else:
                    response = self._post_dual_singles(signed)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(response)

        Thread(target=submit, name="placement", daemon=True).start()
        return future

    def _trace(
        self,
        specs: tuple[tuple[str, str], ...],
        attempt_no: int,
        sent_ts_ms: int,
        returned_ts_ms: int,
        responses: object,
    ) -> None:
        """Record one attempt: which legs, when it left, when it came back.

        The engine-not-ready replies are filtered out of the console log, so
        without this trace a session cannot show when the book actually
        started accepting our orders.
        """
        if self.attempt_trace is None:
            return
        if isinstance(responses, BaseException):
            outcomes = [classify_response(responses)] * len(specs)
        elif isinstance(responses, (list, tuple)):
            outcomes = [classify_response(item) for item in responses]
        else:
            outcomes = [classify_response(responses)]
        self.attempt_trace(
            {
                "attempt": attempt_no,
                "legs": [outcome for outcome, _ in specs],
                "sent_ts_ms": sent_ts_ms,
                "returned_ts_ms": returned_ts_ms,
                "results": outcomes,
            }
        )

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

    def _next_submission(
        self,
        specifications: tuple[tuple[str, str], ...],
        signed: list[PostOrdersV2Args],
        accepted: dict[str, PlacedOrder],
        attempts: int,
    ) -> tuple[tuple[tuple[str, str], ...], list[PostOrdersV2Args]]:
        """Pick the payload for one cadence tick.

        Batch mode always submits the full pair. Single mode submits one
        leg per tick, rotating over the legs that are not registered yet:
        one rate-limit token per tick, and a registered leg stops
        consuming budget while the remaining leg inherits every tick.
        """
        if self.entry_submission != "single":
            return specifications, signed
        remaining = [
            (spec, args)
            for spec, args in zip(specifications, signed, strict=True)
            if spec[0] not in accepted
        ]
        if not remaining:
            return specifications, signed
        spec, args = remaining[attempts % len(remaining)]
        return (spec,), [args]

    def _post_dual_singles(self, signed: list[PostOrdersV2Args]) -> list[object]:
        """Submit each leg as its own single-order request, in parallel.

        The batch endpoint keeps answering "not ready" for a while after the
        book is publicly live; this probes whether the single-order path
        opens earlier. A leg whose response is lost is recovered on the next
        tick through the duplicate-order recognition.
        """
        results: list[object] = [None] * len(signed)
        failures: list[BaseException] = []

        def run(index: int) -> None:
            try:
                results[index] = self._post_single(signed[index])
            except BaseException as exc:
                failures.append(exc)

        threads = [
            Thread(target=run, args=(index,), daemon=True)
            for index in range(len(signed))
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if failures:
            raise failures[0]
        return results

    def _post_single(self, args: PostOrdersV2Args) -> object:
        try:
            return self.client.post_order(
                args.order, args.orderType, post_only=True
            )
        except PolyApiException as exc:
            if _transient_submission_error(exc):
                raise
            payload = exc.error_msg if isinstance(exc.error_msg, dict) else {}
            message = str(payload.get("error") or exc.error_msg or exc)
            return {"errorMsg": message, "success": False}

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
