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


def test_each_account_gets_a_share_of_what_the_warm_up_leaves():
    """A share of the pool, from the fleet actually running - not a constant.

    Dividing by a fixed account count gave a solo account half the pool it
    owned, and a fleet larger than that count more than the pool has.
    """
    usable = transport.MAX_CONNECTIONS - transport.WARM_CONNECTIONS
    per_account = transport.WORST_REPLY_SECONDS / transport.FASTEST_INTERVAL_SECONDS

    # A solo account owns everything the warm-up does not.
    assert transport.in_flight_budget(1) == usable
    # No fleet size can promise more than the pool holds.
    for accounts in range(1, transport.FLEET_ACCOUNTS + 1):
        assert transport.in_flight_budget(accounts) * accounts <= usable
    # And up to the fleet this is sized for, the share still covers an open.
    assert transport.in_flight_budget(transport.FLEET_ACCOUNTS) >= per_account
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
    them, so the cap becomes unreachable rather than merely generous.
    """
    from polymarket_bot.transport import in_flight_budget

    alive = transport.WORST_REPLY_SECONDS / transport.FASTEST_INTERVAL_SECONDS

    def cap(accounts: int) -> int:
        # Exchange.max_requests_in_flight: a share of the pool, halved outside
        # batch mode, where each request is carried by two threads.
        return max(2, in_flight_budget(accounts) // 2)

    largest_safe = max(
        (n for n in range(1, transport.FLEET_ACCOUNTS + 1) if alive < cap(n)),
        default=0,
    )
    assert largest_safe >= 2, (
        f"a request lives up to {alive:.0f} sends, but the cap allows only "
        f"{cap(2)} on two accounts - backpressure will fire at the open"
    )
    # Beyond this the halving leaves less room than a request's lifetime, and
    # the skipping this change removes comes back. Raising the fleet means
    # revisiting the budget, not just the account count.
    assert largest_safe < transport.FLEET_ACCOUNTS or alive < cap(
        transport.FLEET_ACCOUNTS
    )
