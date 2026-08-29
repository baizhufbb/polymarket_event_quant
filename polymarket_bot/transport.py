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
# The venue has slow spells, whole markets at a time: reply medians sit at
# 30ms for clean markets and jump to 1.2-2.2s for spell markets, flat from
# the first knock to the last (run20: 16/72 markets, run21: 56/72). During a
# spell the fleet's demand for connections is
#   accounts x spell_seconds / interval
# and when that passes the pool, new sends wait at our own door before they
# even leave - latency added at exactly the moment the strategy is about
# queueing. Run21 (3 accounts, 2.2s spells) demanded 264 of the old 208
# usable connections: 57% of the session's sends left with 200+ already in
# flight, at 2.2s median lifetimes.
#
# The pool is therefore sized for spells, not for the happy path: the
# warm-up gets WARM_CONNECTIONS and the accounts share the rest, which at
# three accounts covers a sustained 154 x 25ms = 3.85s spell. The deep tail
# (p90 spells reach ~7s) still trips the in-flight cap and skips slots -
# that is the designed protection, the pool does not chase 23-second tails.
#
# WORST_REPLY_SECONDS is the httpx timeout. It is per-phase (connect, write,
# read-gap), NOT a lifetime bound: a reply that trickles never sits silent
# for a second and can live many times longer (run21 p99 lifetime 11.5s).
FLEET_ACCOUNTS = 5
WORST_REPLY_SECONDS = 1.0
FASTEST_INTERVAL_SECONDS = 0.025
WARM_CONNECTIONS = 48
MAX_CONNECTIONS = 512
# The sync pool no longer carries placements - those go through the event
# loop's own pool, sized by MAX_CONNECTIONS above. What is left here is
# cancels, reconciliation reads and the warm-up: small, bursty at market
# end, never hundreds deep.
SYNC_POOL_CONNECTIONS = 64
# No account needs more outstanding requests than a 4-second spell fills;
# past the ceiling the extra would only buy thread count, not coverage.
ACCOUNT_BUDGET_CEILING = 160


def in_flight_budget(accounts: int) -> int:
    """How many requests one account may have outstanding at once.

    What is left of the pool once the warm-up has its slice, divided by the
    accounts actually running - not by a constant, so a fleet of any size
    gets an honest share - and clipped at the ceiling, because outstanding
    requests cost threads and nothing above a 4-second spell's worth of
    them buys any coverage.
    """
    usable = MAX_CONNECTIONS - WARM_CONNECTIONS
    return max(4, min(usable // max(1, accounts), ACCOUNT_BUDGET_CEILING))
# A market handed back by signing re-enters place_dual every loop tick, and
# each entry asks to warm the pool; the pool is process-wide, so one warm-up
# per window serves every member and every re-entry of that market.
WARM_INTERVAL_SECONDS = 60.0

logger = logging.getLogger(__name__)

_installed = False
_last_warm_monotonic: float | None = None
_warm_lock = Lock()


def _raise_file_descriptor_limit(target: int = 8192) -> None:
    """Lift this process's own fd soft limit toward `target`.

    The 512-connection pool holds one descriptor per socket, and the launch
    path runs through runuser, whose PAM session resets the soft limit to
    the 1024 default no matter what the calling shell set. A process may
    raise its own soft limit up to the hard limit without privilege, so the
    bot does it here rather than trusting any launcher to.
    """
    try:
        import resource
    except ImportError:  # not a POSIX system; no 1024 default applies
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if soft >= wanted:
        return
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
    except (ValueError, OSError) as exc:
        logger.warning(
            "could not raise the fd soft limit from %d to %d: %s",
            soft, wanted, exc,
        )


def install_parallel_transport() -> None:
    """Replace the official client's shared transport. Idempotent."""
    global _installed
    if _installed:
        return
    _raise_file_descriptor_limit()
    if not isinstance(getattr(_helpers, "_http_client", None), httpx.Client):
        raise RuntimeError(
            "py_clob_client_v2 no longer exposes _http_client as an "
            "httpx.Client; the parallel-transport swap would silently "
            "not take effect"
        )
    replacement = httpx.Client(
        http2=False,
        limits=httpx.Limits(
            max_connections=SYNC_POOL_CONNECTIONS,
            max_keepalive_connections=SYNC_POOL_CONNECTIONS,
            keepalive_expiry=300.0,
        ),
        # The pool above is sized on WORST_REPLY_SECONDS, but nothing made a
        # reply keep to it: the timeout was ten seconds, so a slow reply held
        # its slot forty times longer than the arithmetic assumed. Requests
        # then piled up until the send loop hit its in-flight cap and gave up
        # slots - 18% of them across the six-hour run, and the markets where
        # that happened landed 2834 shares deeper in the queue than the ones
        # where it did not.
        #
        # A request we are still waiting on after a second has already lost us
        # the moment it was sent for. Abandoning it is not a lost order: the
        # next send returns "Duplicated" carrying that order's id, and every
        # account now has a user stream that reports the placement without
        # asking us to wait for anything.
        timeout=httpx.Timeout(WORST_REPLY_SECONDS),
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
