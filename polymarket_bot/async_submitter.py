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

Nothing is cancelled: a request waits for its reply however slow it is.
Cancelling was never real over HTTP/2 here. httpcore does not send a
RST_STREAM when a request is cancelled - it releases its own count of open
streams and forgets the request - so the request still runs at the venue
and still holds its stream in the protocol state underneath. The two counts
drift apart, httpcore opens streams the connection does not have, those
sends fail on the spot with "Max outbound streams is 100, 100 open", and
the connection soon errors outright. That is how run38 lost a market on
2026-09-24: a three-second lifetime cut every request once the venue slowed,
and at the door none of our sends were getting through. Room for slow
spells comes from spreading the sends over transport.ORDER_CONNECTIONS
connections instead. Leaving them uncancelled costs nothing in orders: every
send for a market is the same signed order, so a late one is rejected as not
ready or answered "Duplicated".
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
    ORDER_CLIENT_CONNECTIONS,
    ORDER_CONNECT_SECONDS,
    ORDER_CONNECTIONS,
    ORDER_SILENCE_SECONDS,
    STREAMS_PER_CONNECTION,
)

logger = logging.getLogger(__name__)

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
    """The process-wide loop thread and its HTTP clients."""

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport
        self._loop: asyncio.AbstractEventLoop | None = None
        # One HTTP/2 connection each, and how many requests each is carrying
        # right now. Both are built on the loop thread; the counts are only
        # ever touched there.
        self._clients: list[httpx.AsyncClient] = []
        self._in_flight: list[int] = []
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._prepared: dict[int, PreparedLeg] = {}
        self._last_version_heal = 0.0
        self._last_warm: float | None = None
        self._lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Start the loop if needed; every caller returns only once it is up.

        A caller that found the thread already alive used to return at once,
        while the loop was still building its clients - in a fleet, every
        member but the one that started the loop then failed its first send.
        """
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._ready.clear()
                self._thread = threading.Thread(
                    target=self._run, name="async-submitter", daemon=True
                )
                self._thread.start()
        if not self._ready.wait(timeout=30.0):
            raise RuntimeError("async submitter loop failed to start")

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # HTTP/2 multiplexes every in-flight order onto a handful of
        # connections (the venue allows 100 concurrent streams per
        # connection), so the pool no longer grows one socket per request.
        # The original HTTP/2 concern - one slow reply
        # blocking every other reply behind a read lock - was diagnosed in
        # the *sync* httpcore transport. The async transport has a milder
        # form: a reply that has arrived reaches its request only when the
        # read in progress returns, i.e. with the connection's next bytes -
        # a few ms on a busy connection, tens on a quiet one. The fleet
        # keeps the order whose acceptance came back first, and its members
        # are 5 ms apart, so sends fill one connection before spilling onto
        # the next (_pick_client) rather than spreading thin. The
        # cancel-and-redial storm and the O(connections) pool scan that
        # collapsed run30 (pool 512->1280) both disappear when there are
        # only a few connections to scan. Measured 2026-09-01: from the
        # same box at the same moment, an authenticated request bypassing
        # this pool answered in 33ms while orders through it took 3554ms -
        # the bottleneck is this transport, not the venue.
        #
        # Several clients rather than one bigger pool: a client keeps every
        # request on its one live HTTP/2 connection until that connection's
        # streams run out, then makes the rest wait (see
        # transport.ORDER_CONNECTIONS). No timeout here may fire on a merely
        # slow venue - over HTTP/2 a read or write timeout fails the whole
        # connection. One TLS context for all of them: each client would
        # otherwise load the CA bundle itself, about half a second apiece.
        tls = httpx.create_ssl_context()
        clients = [
            httpx.AsyncClient(
                http2=True,
                verify=tls,
                transport=self._transport,
                limits=httpx.Limits(
                    max_connections=ORDER_CLIENT_CONNECTIONS,
                    max_keepalive_connections=ORDER_CLIENT_CONNECTIONS,
                    keepalive_expiry=300.0,
                ),
                timeout=httpx.Timeout(
                    ORDER_SILENCE_SECONDS, connect=ORDER_CONNECT_SECONDS
                ),
            )
            for _ in range(ORDER_CONNECTIONS)
        ]
        self._loop = loop
        self._clients = clients
        self._in_flight = [0] * len(clients)
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            for client in clients:
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
            self._clients = []
            self._in_flight = []
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

    def _pick_client(self) -> int:
        """The first connection with a stream to spare; loop thread only.

        Filling one connection before the next keeps a normal market's
        traffic on a single busy connection, where an arrived reply is
        read within a few ms; the others take the overflow of a slow spell.
        If every connection is full the least loaded one queues the request
        - the send loop's per-account ceiling is sized so it never gets
        there.
        """
        for index, carrying in enumerate(self._in_flight):
            if carrying < STREAMS_PER_CONNECTION:
                return index
        return min(range(len(self._in_flight)), key=self._in_flight.__getitem__)

    async def _request(self, leg: PreparedLeg) -> object:
        assert self._clients
        index = self._pick_client()
        self._in_flight[index] += 1
        try:
            response = await self._clients[index].post(
                leg.url, content=leg.body_bytes, headers=leg.headers()
            )
        except httpx.RequestError as exc:
            logger.error(
                "[async-submitter] request error: %s",
                str(exc) or type(exc).__name__,
            )
            raise PolyApiException(error_msg="Request exception!")
        finally:
            self._in_flight[index] -= 1
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

    def warm(self) -> None:
        """Dial every order connection ahead of the burst; fire and forget.

        One request per client opens that client's connection, so the first
        sends at the open do not pay a TLS handshake. Rate-limited like the
        sync warm-up, and for the same reason: a market handed back by
        signing re-enters place_dual every loop tick, and every entry asks
        to warm - unguarded, the members would bunch hundreds of dials into
        the seconds before the open, on the loop thread whose next job is
        the first sends.
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

        async def dial(client: httpx.AsyncClient) -> None:
            try:
                await client.get(CLOB_TIME_URL)
            except Exception:  # noqa: BLE001 - warming is best effort
                pass

        for client in self._clients:
            asyncio.run_coroutine_threadsafe(dial(client), self._loop)

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
