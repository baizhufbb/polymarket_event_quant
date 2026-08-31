import time
from threading import Event, Lock

import httpx
import py_clob_client_v2.http_helpers.helpers as helpers

from polymarket_bot import transport


def test_install_swaps_the_shared_client_once(monkeypatch):
    monkeypatch.setattr(transport, "_installed", False)
    original = helpers._http_client
    try:
        transport.install_parallel_transport()
        swapped = helpers._http_client
        assert swapped is not original
        assert isinstance(swapped, httpx.Client)
        transport.install_parallel_transport()
        assert helpers._http_client is swapped
    finally:
        helpers._http_client = httpx.Client(http2=True)


def test_install_refuses_unrecognized_library_internals(monkeypatch):
    monkeypatch.setattr(transport, "_installed", False)
    monkeypatch.setattr(helpers, "_http_client", object())
    try:
        transport.install_parallel_transport()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError on unrecognized internals")


def test_warm_connections_noop_before_install(monkeypatch):
    monkeypatch.setattr(transport, "_installed", False)
    calls = []

    class Fake:
        def get(self, url):
            calls.append(url)

    monkeypatch.setattr(helpers, "_http_client", Fake())
    transport.warm_connections(count=3)
    assert calls == []


def _wait_for(predicate, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_warm_connections_dials_count_requests(monkeypatch):
    monkeypatch.setattr(transport, "_installed", True)
    monkeypatch.setattr(transport, "_last_warm_monotonic", None)
    calls = []
    lock = Lock()

    class Fake:
        def get(self, url):
            with lock:
                calls.append(url)

    monkeypatch.setattr(helpers, "_http_client", Fake())
    transport.warm_connections(count=5)
    assert _wait_for(lambda: len(calls) == 5)


def test_warm_connections_does_not_hold_the_caller(monkeypatch):
    """Warming is preparation; the thread about to send must not wait for it.

    Leaving an executor's `with` block waits for every task it was given, so
    warming used to hold its caller until the slowest dial answered - and the
    caller is the fleet member about to start sending.
    """
    monkeypatch.setattr(transport, "_installed", True)
    monkeypatch.setattr(transport, "_last_warm_monotonic", None)
    released = Event()
    started = []
    lock = Lock()

    class SlowFake:
        def get(self, url):
            with lock:
                started.append(url)
            released.wait(timeout=5)

    monkeypatch.setattr(helpers, "_http_client", SlowFake())
    began = time.monotonic()
    transport.warm_connections(count=4)
    elapsed = time.monotonic() - began
    try:
        assert elapsed < 0.5, f"warming held its caller for {elapsed:.2f}s"
        assert _wait_for(lambda: len(started) == 4)
    finally:
        released.set()


def test_warm_connections_runs_once_per_window(monkeypatch):
    monkeypatch.setattr(transport, "_installed", True)
    monkeypatch.setattr(transport, "_last_warm_monotonic", None)
    monkeypatch.setattr(transport, "WARM_INTERVAL_SECONDS", 60.0)
    calls = []

    lock = Lock()

    class Fake:
        def get(self, url):
            with lock:
                calls.append(url)

    monkeypatch.setattr(helpers, "_http_client", Fake())
    transport.warm_connections(count=5)
    transport.warm_connections(count=5)  # a handed-back market re-entering
    assert _wait_for(lambda: len(calls) == 5)

    monkeypatch.setattr(transport, "WARM_INTERVAL_SECONDS", 0.0)
    transport.warm_connections(count=5)  # next market, window elapsed
    assert _wait_for(lambda: len(calls) == 10)


def test_the_process_lifts_its_own_fd_limit_as_far_as_the_pool_needs():
    """runuser's PAM resets the soft fd limit to 1024; the pool holds 512.

    The launcher cannot fix this from outside - the reset happens after it -
    so the process must, and a soft limit below the pool plus daily overhead
    would surface as connection errors at the open.
    """
    transport._raise_file_descriptor_limit()
    try:
        import resource
    except ImportError:
        return  # no rlimits on this platform, nothing to verify
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = 8192 if hard == resource.RLIM_INFINITY else min(8192, hard)
    assert soft >= min(wanted, transport.MAX_CONNECTIONS + 256)


def test_the_pool_covers_the_warm_up_and_every_account_at_the_open():
    """One pool serves the whole process: the warm-up and every account.

    It was sized for a single account. Two things then grew past it at once -
    a true 25 ms cadence on two accounts, and a warm-up that now dials while
    the accounts send - so requests queued inside the pool at the one moment
    the strategy is entirely about.
    """
    per_account = transport.WORST_REPLY_SECONDS / transport.FASTEST_INTERVAL_SECONDS
    needed = transport.WARM_CONNECTIONS + transport.FLEET_ACCOUNTS * per_account
    assert transport.MAX_CONNECTIONS >= needed


def test_idle_connections_can_actually_be_pruned():
    """The surplus-idle rule must be reachable, and the warm-up must survive.

    The pool closes an idle connection promptly only when the idle count
    exceeds max_keepalive_connections. Setting that equal to the pool size
    makes the condition unreachable - the idle count is bounded by the pool -
    so a socket the venue closed while idle keeps its slot for the whole
    keepalive expiry. Run31 ended with 365 of 512 slots held that way. The
    ceiling must therefore sit below the pool, and above the warm-up, whose
    dials are paid for before the open and must not be pruned between
    markets.
    """
    assert transport.MAX_KEEPALIVE_CONNECTIONS < transport.MAX_CONNECTIONS
    assert transport.MAX_KEEPALIVE_CONNECTIONS >= transport.WARM_CONNECTIONS


def test_the_placement_pool_uses_the_keepalive_ceiling():
    """The submitter's own client is the one that carries placements."""
    import inspect

    from polymarket_bot import async_submitter

    source = inspect.getsource(async_submitter.AsyncSubmitter._run)
    assert "max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS" in source


def test_each_account_gets_a_share_of_what_the_warm_up_leaves():
    """A share of the pool, from the fleet actually running - not a constant.

    Dividing by a fixed account count gave a solo account half the pool it
    owned, and a fleet larger than that count more than the pool has. The
    share is clipped at the ceiling: outstanding requests cost threads, and
    nothing past a 4-second spell's worth of them buys any coverage.
    """
    usable = transport.MAX_CONNECTIONS - transport.WARM_CONNECTIONS
    per_account = transport.WORST_REPLY_SECONDS / transport.FASTEST_INTERVAL_SECONDS

    # A solo account gets the ceiling, not the whole pool.
    assert transport.in_flight_budget(1) == transport.ACCOUNT_BUDGET_CEILING
    # The ceiling covers a 4-second spell at the fastest cadence.
    assert (
        transport.ACCOUNT_BUDGET_CEILING * transport.FASTEST_INTERVAL_SECONDS
        >= 4.0
    )
    # No fleet size can promise more than the pool holds.
    for accounts in range(1, transport.FLEET_ACCOUNTS + 1):
        assert transport.in_flight_budget(accounts) * accounts <= usable
    # And up to the fleet this is sized for, the share still covers an open.
    assert transport.in_flight_budget(transport.FLEET_ACCOUNTS) >= per_account
    # Three accounts must ride out the spells run21 actually measured: a
    # sustained 2.2s of slow replies demands 264 connections and the old
    # 256-connection pool queued our own sends at the door (57% of sends
    # left with 200+ in flight).
    spell_demand = 3 * 2.2 / transport.FASTEST_INTERVAL_SECONDS
    assert 3 * transport.in_flight_budget(3) >= spell_demand
    # Nonsense input cannot produce a nonsense cap.
    assert transport.in_flight_budget(0) >= 4


def test_a_reply_cannot_outlive_what_the_in_flight_budget_assumed() -> None:
    """The pool arithmetic assumes a reply lands within WORST_REPLY_SECONDS.

    Nothing enforced that: the client's timeout was ten seconds, so a reply
    could hold its slot forty times longer than the sizing above allows. The
    send loop then hit its in-flight cap and skipped slots - 18% of them over
    a six-hour run, and the markets where it happened sat 2834 shares deeper
    in the queue than the ones where it did not.

    With the timeout tied to the same constant, an account sending every
    FASTEST_INTERVAL_SECONDS can never accumulate more than the ratio between
    them, so the cap becomes unreachable rather than merely generous - and it
    must stay unreachable for every fleet size this pool was sized for, which
    is what the old halved cap broke at three accounts and above.
    """
    from polymarket_bot.transport import in_flight_budget

    alive = transport.WORST_REPLY_SECONDS / transport.FASTEST_INTERVAL_SECONDS

    for accounts in range(1, transport.FLEET_ACCOUNTS + 1):
        # Exchange.max_requests_in_flight: the account's share of the pool.
        cap = in_flight_budget(accounts)
        assert alive < cap, (
            f"a request lives up to {alive:.0f} sends, but {accounts} "
            f"account(s) get a cap of {cap} - the send loop will skip slots "
            f"at the open, which cost 2834 shares a market when it last fired"
        )


def test_the_full_fleet_cannot_exhaust_the_thread_supply() -> None:
    """Threads are what actually ran out in the field (run17).

    The in-flight cap used to be halved outside batch mode to protect them,
    which re-armed slot-skipping at three accounts. The guard belongs here
    instead: at worst every account holds its whole cap, and each of those
    requests is carried by a dispatch thread plus one thread per leg (two
    legs at most), so the fleet's ceiling is accounts x cap x 3 plus the
    warm-up dials. The field failure came at several thousand threads; keep
    the whole planned fleet an order of magnitude under it.
    """
    # Non-batch sends ride the event loop and cost no thread at all; batch
    # mode - the one entry mode still carried by threads - spends a single
    # dispatch thread per in-flight send.
    per_request_threads = 1
    worst = max(
        accounts * transport.in_flight_budget(accounts) * per_request_threads
        for accounts in range(1, transport.FLEET_ACCOUNTS + 1)
    ) + transport.WARM_CONNECTIONS
    # The field crash ran out at several thousand threads (the server
    # allows 7277); keep the worst case an order of magnitude under it.
    assert worst <= 700
