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

from concurrent.futures import ThreadPoolExecutor, wait

import httpx
import py_clob_client_v2.http_helpers.helpers as _helpers

CLOB_TIME_URL = "https://clob.polymarket.com/time"
# At the open, replies take up to ~1s while submissions leave every 25ms,
# so about 40 requests can be in flight at once; keep headroom above that.
MAX_CONNECTIONS = 64
WARM_CONNECTIONS = 48

_installed = False


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
    if not _installed:
        return
    client = _helpers._http_client
    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(client.get, CLOB_TIME_URL) for _ in range(count)]
        wait(futures, timeout=10)
