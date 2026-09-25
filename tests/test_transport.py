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
    """runuser's PAM resets the soft fd limit to 1024 after launch.

    The launcher cannot fix this from outside - the reset happens after it -
    so the process must. Over HTTP/2 the pool is far under 1024 anyway; the
    lift stays because it costs nothing and connection errors at the open
    are the one failure the strategy cannot absorb.
    """
    transport._raise_file_descriptor_limit()
    try:
        import resource
    except ImportError:
        return  # no rlimits on this platform, nothing to verify
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = 8192 if hard == resource.RLIM_INFINITY else min(8192, hard)
    # The knock library's order connections (15, and a replacement each
    # after a GOAWAY) share the process's descriptors with this pool.
    sockets = 30 + transport.SYNC_POOL_CONNECTIONS
    assert soft >= min(wanted, sockets + 256)
