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
# Orders travel over HTTP/2 (async_submitter.py), where one connection
# carries many requests at once: the venue's SETTINGS_MAX_CONCURRENT_STREAMS
# is 100 (measured 2026-09-01). A request in flight therefore costs a
# stream, not a socket, and the pool below is sized in connections that
# each hold STREAMS_PER_CONNECTION requests - a handful, not hundreds.
#
# That ends the arithmetic that used to live here. Over HTTP/1.1 every
# outstanding request held its own socket, so the pool had to be sized for
# accounts x slow-spell seconds / interval (512 for three accounts riding a
# 3.85 s spell), the pool's own bookkeeping walked every socket on every
# event, and raising the pool to chase deeper spells made that walk slower
# than the spell (run30: 512 -> 1280 collapsed from the first minute). None
# of that scales with streams.
#
# What still matters: the venue retires a connection after 10,000 streams
# with GOAWAY, so a replacement must be dialable while the old one drains -
# hence a pool a few times larger than the streams alone require.
#
# WORST_REPLY_SECONDS is the httpx per-phase timeout (connect, write,
# read-gap), not a lifetime bound; the hard lifetime is
# async_submitter.TOTAL_LIFETIME_SECONDS.
FLEET_ACCOUNTS = 5
WORST_REPLY_SECONDS = 1.0
FASTEST_INTERVAL_SECONDS = 0.025
STREAMS_PER_CONNECTION = 100
# One warm connection per planned account is plenty over HTTP/2; the warm-up
# exists so the first sends at the open do not pay a TLS handshake, and a
# single h2 connection already carries an account's whole burst.
WARM_CONNECTIONS = 3
MAX_CONNECTIONS = 16
# Over HTTP/1.1 this had to sit strictly below MAX_CONNECTIONS: the pool
# prunes an idle connection promptly only when the idle count exceeds it,
# and with the two equal that rule was unreachable, so sockets the venue had
# closed sat in CLOSE-WAIT for the whole keepalive expiry (run31: 365 of 512
# slots). Over HTTP/2 there are a few connections, each kept busy by the
# streams on it, and an idle one costs nothing to keep - the rule is inert
# rather than needed, so this simply matches the pool.
MAX_KEEPALIVE_CONNECTIONS = MAX_CONNECTIONS
# The sync pool no longer carries placements - those go through the event
# loop's own pool, sized by MAX_CONNECTIONS above. What is left here is
# cancels, reconciliation reads and the warm-up: small, bursty at market
# end, never hundreds deep.
SYNC_POOL_CONNECTIONS = 64
# The most requests one account may hold outstanding. A safety net on the
# send loop, not a share of anything: at TOTAL_LIFETIME_SECONDS a request
# lives at most lifetime / interval sends (3 s / 25 ms = 120), so at this
# ceiling the loop never has to skip a slot for want of a request slot.
ACCOUNT_BUDGET_CEILING = 160


def in_flight_budget(accounts: int) -> int:
    """How many requests one account may have outstanding at once.

    Over HTTP/1.1 this was the account's share of a socket pool - the pool
    minus the warm-up, divided by the accounts running, clipped at the
    ceiling - because a request in flight owned a socket. Over HTTP/2 it
    does not, so there is nothing to share: every account gets the ceiling,
    which the hard request lifetime keeps unreachable in normal operation.
    The argument is kept so callers and tests that pass a fleet size still
    read naturally; only nonsense input is guarded.
    """
    if accounts < 1:
        return max(4, ACCOUNT_BUDGET_CEILING)
    return ACCOUNT_BUDGET_CEILING
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

    The launch path runs through runuser, whose PAM session resets the soft
    limit to the 1024 default no matter what the calling shell set. Over
    HTTP/2 the pool needs a few dozen descriptors, so 1024 would do; this
    stays because a process may raise its own soft limit without privilege
    and the cost is nothing, while the failure mode - connection errors at
    the open - is the one thing the strategy cannot absorb.
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
