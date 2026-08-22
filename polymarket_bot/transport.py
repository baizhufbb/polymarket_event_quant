"""Route the official client's HTTP traffic over parallel connections.

py_clob_client_v2 ships one module-level httpx.Client(http2=True): every
request in the process multiplexes onto a single TCP connection, and
httpcore's synchronous HTTP/2 transport serializes socket reads behind a
lock. At the open the exchange takes 300-1000ms to register the first
order, so that one slow reply blocks every other in-flight reply (they
arrive in one burst when the lock frees) and can stall outgoing writes
once the shared flow-control window drains.

An HTTP/1.1 pool gives each in-flight request its own connection, so one
slow exchange-side response delays nothing else. The official library is
untouched on disk: its request() helper looks the client up by name on
every call, so replacing the module attribute redirects all traffic.
"""

from __future__ import annotations

import time
import logging
from threading import Lock, Thread

import httpx
import py_clob_client_v2.http_helpers.helpers as _helpers

CLOB_TIME_URL = "https://clob.polymarket.com/time"
# At the open, replies take up to ~1s while each account submits every 25ms,
# so an account can have ~40 requests in flight. Every account in the process
# shares this one pool - the client library keeps a single module-level client
# - and so does the warm-up, which now dials while the accounts are sending.
# Beyond the pool's size requests queue inside it, which is latency added at
# exactly the moment the whole strategy is about queueing.
#
# The budget: the warm-up gets WARM_CONNECTIONS, and the accounts share what
# is left. Sized for the largest fleet planned, five accounts:
#   48 + 5 x (1.0 / 0.025) = 248
# A test holds these numbers to that arithmetic.
FLEET_ACCOUNTS = 5
WORST_REPLY_SECONDS = 1.0
FASTEST_INTERVAL_SECONDS = 0.025
WARM_CONNECTIONS = 48
MAX_CONNECTIONS = 256


def in_flight_budget(accounts: int) -> int:
    """How many requests one account may have outstanding at once.

    What is left of the pool once the warm-up has its slice, divided by the
    accounts actually running - not by a constant, so a solo account gets the
    whole pool and a fleet of any size gets an honest share.
    """
    usable = MAX_CONNECTIONS - WARM_CONNECTIONS
    return max(4, usable // max(1, accounts))
# A market handed back by signing re-enters place_dual every loop tick, and
# each entry asks to warm the pool; the pool is process-wide, so one warm-up
# per window serves every member and every re-entry of that market.
WARM_INTERVAL_SECONDS = 60.0

logger = logging.getLogger(__name__)

_installed = False
_last_warm_monotonic: float | None = None
_warm_lock = Lock()


def install_parallel_transport() -> None:
    """Replace the official client's shared transport. Idempotent."""
    global _installed
    if _installed:
        return
    if not isinstance(getattr(_helpers, "_http_client", None), httpx.Client):
        raise RuntimeError(
            "py_clob_client_v2 no longer exposes _http_client as an "
            "httpx.Client; the parallel-transport swap would silently "
            "not take effect"
        )
    replacement = httpx.Client(
        http2=False,
        limits=httpx.Limits(
            max_connections=MAX_CONNECTIONS,
            max_keepalive_connections=MAX_CONNECTIONS,
            keepalive_expiry=300.0,
        ),
        timeout=httpx.Timeout(10.0, connect=5.0),
    )
    previous = _helpers._http_client
    _helpers._http_client = replacement
    previous.close()
    if _helpers._http_client is not replacement:
        raise RuntimeError("parallel-transport swap did not take effect")
    _installed = True


def warm_connections(count: int = WARM_CONNECTIONS) -> None:
    """Open pool connections ahead of the open-moment burst.

    The pool dials on demand: steady one-reply-per-tick traffic keeps only
    two or three connections alive, so the burst at the open would pay a
    TLS handshake per new connection exactly when it hurts. Firing `count`
    cheap concurrent requests forces the dials now. Failures are ignored;
    a connection that failed to warm is simply dialed on demand later.

    Each dial runs on its own daemon thread and this returns at once. Waiting
    for them held the caller until the slowest answered - leaving an
    executor's `with` block waits for every task, whatever timeout sits above
    it - and the caller is the member thread about to start sending. In the
    live run the member that paid for the warm-up started sending 0.6 s, 3.5 s
    and once 38 s behind the other one. Plain threads rather than an executor:
    an executor's workers are not daemons, so the interpreter joins them at
    exit however this thread is flagged.
    """
    global _last_warm_monotonic
    if not _installed:
        return
    with _warm_lock:
        now = time.monotonic()
        if (
            _last_warm_monotonic is not None
            and now - _last_warm_monotonic < WARM_INTERVAL_SECONDS
        ):
            return
        _last_warm_monotonic = now
    client = _helpers._http_client

    def dial() -> None:
        try:
            client.get(CLOB_TIME_URL)
        except Exception:  # noqa: BLE001 - warming is best effort
            pass

    for _ in range(count):
        try:
            Thread(target=dial, name="warm-connection", daemon=True).start()
        except RuntimeError as exc:
            # Out of threads is the failure this whole area exists to avoid;
            # say so rather than let it vanish into stderr.
            logger.warning("could not warm the connection pool: %s", exc)
            return
