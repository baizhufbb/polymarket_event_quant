"""One event loop carries every account's order submissions.

The thread-per-send path died in the field (run22): three accounts in a
venue slow spell held ~460 requests in flight, each carried by two OS
threads, and ~930 threads on the one-core box spent the core on context
switching - reply reads starved, the user streams missed their
heartbeats 52 times, and placements sat paused for hours.

Here a request in flight is one entry in an event loop's book: no thread,
no GIL contention, one epoll wakeup per batch of socket activity. The
sending loop in exchange.py is untouched - it still holds Futures and
waits on them; this module only changes who fulfils the Future.

The wire bytes are built by the official library's own pure functions
(order_to_json_v2 / create_level_2_headers), so a request from this path
is byte-identical to one from the sync client. The body never changes for
a given signed order and is serialized once; only the auth header's
timestamp moves, and it is whole seconds.

Response semantics mirror the sync stack exactly:
  - HTTP 200        -> parsed JSON (or raw text)
  - other statuses  -> PolyApiException(resp)        [helpers.request]
  - network errors  -> PolyApiException("Request exception!")
  - non-transient PolyApiException per leg -> {"errorMsg": ..., "success":
    False}                                           [Exchange._post_single]
On top, the loop can do what the sync stack could not: a hard total
lifetime per request. A reply that trickles bytes forever held a thread
and a socket for up to 69 s in the field; here it is cancelled cleanly at
TOTAL_LIFETIME_SECONDS and surfaces as the same transient error a timeout
always did.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from concurrent.futures import Future

import httpx
from py_clob_client_v2.client import _is_v2_order
from py_clob_client_v2.clob_types import RequestArgs
from py_clob_client_v2.endpoints import POST_ORDER
from py_clob_client_v2.exceptions import PolyApiException
from py_clob_client_v2.headers.headers import create_level_2_headers
from py_clob_client_v2.order_utils.model.order_data_v1 import order_to_json_v1
from py_clob_client_v2.order_utils.model.order_data_v2 import order_to_json_v2

from .transport import (
    CLOB_TIME_URL,
    MAX_CONNECTIONS,
    MAX_KEEPALIVE_CONNECTIONS,
    WORST_REPLY_SECONDS,
)

logger = logging.getLogger(__name__)

# A reply we are still waiting on after this long has already lost us the
# moment it was sent for. Cancelling is not losing the order: the next send
# returns "Duplicated" with the order's id, and the user streams report
# placements.
#
# This sat at 30 s from 2026-08-28 to 2026-09-02, and not because 30 s was a
# good number: over HTTP/1.1 a cancelled request killed its socket (the
# reply left half-read cannot be reused), so every cancel was a redial, and
# a cancel landing between the pool assigning a connection and the request
# driving it stranded that connection for good (run23, 5.5 h). Cancelling
# often was the disease, so the lifetime was stretched until almost nothing
# was cancelled - at the cost of a request holding its in-flight slot for
# 30 s while the venue thought about it, which is what pinned in-flight at
# the cap and skipped a quarter of the send slots (run31-34).
#
# Over HTTP/2 a cancel is one RST_STREAM frame on a connection that keeps
# serving, and the stranded-connection window closes (an undriven h2
# connection is `is_available()`, so the pool reuses it). With cancels
# cheap again the lifetime can be what the strategy actually wants: a
# request the venue has not answered in a few seconds is not going to be
# the one that wins the queue, and holding it only deepens the backlog
# during a slow spell.
TOTAL_LIFETIME_SECONDS = 3.0
_VERSION_HEAL_INTERVAL_SECONDS = 30.0


class PreparedLeg:
    """Everything constant about one signed order's request, built once.

    Holding `args` is load-bearing, not bookkeeping: the cache below keys
    entries by id(args), and CPython reuses a freed object's id for the
    next same-shaped allocation - reliably, not rarely. An entry that let
    its args die could be looked up by a NEW signed order that landed on
    the recycled id and answer with the PREVIOUS market's body, which
    headers() would then sign freshly - an authentic order for the wrong
    market. Pinning args means its id stays taken for as long as the entry
    can be found.
    """

    __slots__ = ("client", "args", "url", "body_dict", "body_bytes", "serialized")

    def __init__(self, client, args) -> None:
        owner = client.creds.api_key or ""
        to_json = order_to_json_v2 if _is_v2_order(args.order) else order_to_json_v1
        self.client = client
        self.args = args
        self.body_dict = to_json(
            args.order, owner, args.orderType, True, getattr(args, "deferExec", False)
        )
        self.serialized = json.dumps(
            self.body_dict, separators=(",", ":"), ensure_ascii=False
        )
        self.body_bytes = self.serialized.encode("utf-8")
        self.url = f"{client.host}{POST_ORDER}"

    def headers(self) -> dict:
        """L2 auth headers for this instant; the HMAC covers whole seconds."""
        request_args = RequestArgs(
            method="POST",
            request_path=POST_ORDER,
            body=self.body_dict,
            serialized_body=self.serialized,
        )
        built = create_level_2_headers(
            self.client.signer,
            self.client.creds,
            request_args,
            timestamp=self.client._get_timestamp(),
        )
        built["User-Agent"] = "py_clob_client_v2"
        built["Accept"] = "*/*"
        built["Connection"] = "keep-alive"
        built["Content-Type"] = "application/json"
        return built


class AsyncSubmitter:
    """The process-wide loop thread and its HTTP client."""

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: httpx.AsyncClient | None = None
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._prepared: dict[int, PreparedLeg] = {}
        self._last_version_heal = 0.0
        self._last_warm: float | None = None
        self._lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._ready.clear()
            self._thread = threading.Thread(
                target=self._run, name="async-submitter", daemon=True
            )
            self._thread.start()
        if not self._ready.wait(timeout=10.0):
            raise RuntimeError("async submitter loop failed to start")

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # HTTP/2 multiplexes every in-flight order onto a handful of
        # connections (the venue allows 100 concurrent streams per
        # connection), so the pool no longer grows one socket per request
        # and cancelling a request costs a RST_STREAM frame instead of a
        # dead socket. The original HTTP/2 concern - one slow reply
        # blocking every other reply behind a read lock - was diagnosed in
        # the *sync* httpcore transport; the async transport pumps one
        # network read per lock acquisition and dispatches to every
        # stream, so it does not head-of-line block the same way. The
        # cancel-and-redial storm and the O(connections) pool scan that
        # collapsed run30 (pool 512->1280) both disappear when there are
        # only a few connections to scan. Measured 2026-09-01: from the
        # same box at the same moment, an authenticated request bypassing
        # this pool answered in 33ms while orders through it took 3554ms -
        # the bottleneck is this transport, not the venue.
        client = httpx.AsyncClient(
            http2=True,
            transport=self._transport,
            limits=httpx.Limits(
                max_connections=MAX_CONNECTIONS,
                max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
                keepalive_expiry=300.0,
            ),
            timeout=httpx.Timeout(WORST_REPLY_SECONDS),
        )
        self._loop = loop
        self._client = client
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(client.aclose())
            loop.close()

    def stop(self) -> None:
        """For tests. Production lets the daemon die with the process."""
        with self._lock:
            loop, thread = self._loop, self._thread
            self._thread = None
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5.0)
        with self._lock:
            self._loop = None
            self._client = None
            self._prepared.clear()
        self._ready.clear()

    # -- request preparation ----------------------------------------------

    def prepare(self, client, args) -> PreparedLeg:
        key = id(args)
        leg = self._prepared.get(key)
        if leg is None or leg.client is not client or leg.args is not args:
            leg = PreparedLeg(client, args)
            if len(self._prepared) > 4096:
                self._prepared.clear()
            self._prepared[key] = leg
        return leg

    # -- submission --------------------------------------------------------

    def submit(self, legs: list[PreparedLeg]) -> Future:
        """Send every leg concurrently; one Future, sync-stack semantics."""
        self.start()
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(self._send_all(legs), self._loop)

    async def _send_all(self, legs: list[PreparedLeg]) -> list[object]:
        results = await asyncio.gather(
            *(self._send_one(leg) for leg in legs), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return list(results)

    async def _send_one(self, leg: PreparedLeg) -> object:
        try:
            response = await self._request(leg)
        except PolyApiException as exc:
            from .exchange import _transient_submission_error

            if _transient_submission_error(exc):
                raise
            payload = exc.error_msg if isinstance(exc.error_msg, dict) else {}
            message = str(payload.get("error") or exc.error_msg or exc)
            return {"errorMsg": message, "success": False}
        if leg.client._is_order_version_mismatch(response):
            self._heal_order_version(leg.client)
        return response

    async def _request(self, leg: PreparedLeg) -> object:
        assert self._client is not None
        try:
            async with asyncio.timeout(TOTAL_LIFETIME_SECONDS):
                response = await self._client.post(
                    leg.url, content=leg.body_bytes, headers=leg.headers()
                )
        except (httpx.RequestError, TimeoutError) as exc:
            logger.error(
                "[async-submitter] request error: %s",
                str(exc) or type(exc).__name__,
            )
            raise PolyApiException(error_msg="Request exception!")
        if response.status_code != 200:
            logger.error(
                "[async-submitter] request error status=%s url=%s body=%s",
                response.status_code,
                leg.url,
                response.text,
            )
            raise PolyApiException(response)
        try:
            return response.json()
        except ValueError:
            return response.text

    # -- side channels -----------------------------------------------------

    def warm(self, count: int) -> None:
        """Dial pool connections ahead of the burst; fire and forget.

        Rate-limited like the sync warm-up, and for the same reason: a
        market handed back by signing re-enters place_dual every loop
        tick, and every entry asks to warm - unguarded, three members
        would bunch hundreds of dials into the seconds before the open,
        on the loop thread whose next job is the first sends.
        """
        from . import transport

        if not transport._installed:
            # Same marker the sync warm-up keys on: no live process has
            # installed the transport, so this is a test - do not dial out.
            return
        with self._lock:
            now = time.monotonic()
            if (
                self._last_warm is not None
                and now - self._last_warm < transport.WARM_INTERVAL_SECONDS
            ):
                return
            self._last_warm = now
        self.start()
        assert self._loop is not None

        async def dial() -> None:
            try:
                assert self._client is not None
                await self._client.get(CLOB_TIME_URL)
            except Exception:  # noqa: BLE001 - warming is best effort
                pass

        for _ in range(count):
            asyncio.run_coroutine_threadsafe(dial(), self._loop)

    def _heal_order_version(self, client) -> None:
        """The sync client re-resolves the order version on mismatch; keep
        that behaviour without blocking the loop."""
        now = time.monotonic()
        if now - self._last_version_heal < _VERSION_HEAL_INTERVAL_SECONDS:
            return
        self._last_version_heal = now

        def heal() -> None:
            try:
                client._ClobClient__resolve_version(force_update=True)
            except Exception as exc:  # noqa: BLE001 - healing is best effort
                logger.warning("order version heal failed: %s", exc)

        threading.Thread(target=heal, name="order-version-heal", daemon=True).start()


_submitter: AsyncSubmitter | None = None
_submitter_lock = threading.Lock()


def get_submitter() -> AsyncSubmitter:
    global _submitter
    with _submitter_lock:
        if _submitter is None:
            _submitter = AsyncSubmitter()
        return _submitter
