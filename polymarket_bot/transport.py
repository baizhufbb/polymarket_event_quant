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
from concurrent.futures import ThreadPoolExecutor, wait

import httpx
import py_clob_client_v2.http_helpers.helpers as _helpers

CLOB_TIME_URL = "https://clob.polymarket.com/time"
# At the open, replies take up to ~1s while each account submits every 25ms,
# so an account can have ~40 requests in flight. Every account in the fleet
# shares this one pool - the client library keeps a single module-level
# client - so the requirement is accounts x reply_seconds / interval_seconds:
# Beyond the pool's size the requests queue inside it, which is latency added
# at exactly the moment the whole strategy is about queueing. Raise
# FLEET_ACCOUNTS with the fleet: a test holds the pool to this arithmetic.
FLEET_ACCOUNTS = 2
WORST_REPLY_SECONDS = 1.0
FASTEST_INTERVAL_SECONDS = 0.025
MAX_CONNECTIONS = 128
WARM_CONNECTIONS = 96
# A market handed back by signing re-enters place_dual every loop tick, and
# each entry asks to warm the pool; the pool is process-wide, so one warm-up
# per window serves every member and every re-entry of that market.
WARM_INTERVAL_SECONDS = 60.0

_installed = False
_last_warm_monotonic: float | None = None


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
    """
    global _last_warm_monotonic
    if not _installed:
        return
    now = time.monotonic()
    if (
        _last_warm_monotonic is not None
        and now - _last_warm_monotonic < WARM_INTERVAL_SECONDS
    ):
        return
    _last_warm_monotonic = now
    client = _helpers._http_client
    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(client.get, CLOB_TIME_URL) for _ in range(count)]
        wait(futures, timeout=10)
