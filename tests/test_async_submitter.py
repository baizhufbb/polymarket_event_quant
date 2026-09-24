"""The event-loop submitter must answer exactly like the thread path did.

The sending loop in exchange.py classifies replies by their shape: parsed
JSON for accepted/duplicate/not-ready, PolyApiException for transport
trouble, the _post_single wrapping for non-transient rejections. Every
test here pins one of those shapes against the async path, because a
drifted shape would not crash - it would silently misclassify replies at
the open.
"""

import asyncio
import json
import threading
import time
from concurrent.futures import FIRST_COMPLETED, wait

import httpx
import pytest
from py_clob_client_v2.exceptions import PolyApiException

from polymarket_bot.async_submitter import AsyncSubmitter, PreparedLeg


class FakeClient:
    host = "https://clob.polymarket.com"

    @staticmethod
    def _is_order_version_mismatch(response) -> bool:
        return False

    @staticmethod
    def _get_timestamp():
        return None


def _leg(client=None) -> PreparedLeg:
    leg = PreparedLeg.__new__(PreparedLeg)
    leg.client = client or FakeClient()
    leg.url = "https://clob.polymarket.com/order"
    leg.body_dict = {}
    leg.serialized = "{}"
    leg.body_bytes = b"{}"
    return leg


@pytest.fixture()
def flat_headers(monkeypatch):
    monkeypatch.setattr(PreparedLeg, "headers", lambda self: {})


def _submitter(handler) -> AsyncSubmitter:
    return AsyncSubmitter(transport=httpx.MockTransport(handler))


def test_a_200_reply_comes_back_as_parsed_json(flat_headers):
    def handler(request):
        return httpx.Response(200, json={"success": True, "orderId": "0xabc"})

    submitter = _submitter(handler)
    try:
        result = submitter.submit([_leg()]).result(timeout=10)
        assert result == [{"success": True, "orderId": "0xabc"}]
    finally:
        submitter.stop()


def test_a_rejection_is_wrapped_the_way_post_single_wraps_it(flat_headers):
    """400 with an error body is not transport trouble: the loop expects the
    {"errorMsg": ...} dict the sync path's _post_single hands it."""

    def handler(request):
        return httpx.Response(
            400, json={"error": "order 0xdead is invalid. Duplicated."}
        )

    submitter = _submitter(handler)
    try:
        result = submitter.submit([_leg()]).result(timeout=10)
        assert result == [
            {"errorMsg": "order 0xdead is invalid. Duplicated.", "success": False}
        ]
    finally:
        submitter.stop()


def test_a_500_reply_raises_the_transient_exception(flat_headers):
    def handler(request):
        return httpx.Response(500, text="upstream burped")

    submitter = _submitter(handler)
    try:
        with pytest.raises(PolyApiException) as caught:
            submitter.submit([_leg()]).result(timeout=10)
        assert caught.value.status_code == 500
    finally:
        submitter.stop()


def test_network_trouble_raises_the_same_request_exception(flat_headers):
    def handler(request):
        raise httpx.ConnectError("no route")

    submitter = _submitter(handler)
    try:
        with pytest.raises(PolyApiException) as caught:
            submitter.submit([_leg()]).result(timeout=10)
        assert caught.value.status_code is None
        assert "Request exception" in str(caught.value.error_msg)
    finally:
        submitter.stop()


def test_a_slow_reply_is_waited_for_not_cancelled(flat_headers):
    """A cancel was never real over HTTP/2: httpcore sends no RST_STREAM, so
    the request kept running at the venue and kept its stream while httpcore
    counted the stream free - the next sends failed with "Max outbound
    streams is 100, 100 open" and the connection broke (run38). A reply
    slower than the old three-second lifetime must simply arrive."""

    async def handler(request):
        await asyncio.sleep(3.3)
        return httpx.Response(200, json={"success": True, "orderId": "0xslow"})

    submitter = _submitter(handler)
    try:
        result = submitter.submit([_leg()]).result(timeout=10)
        assert result == [{"success": True, "orderId": "0xslow"}]
    finally:
        submitter.stop()


def test_sends_fill_one_connection_before_spilling_onto_the_next(flat_headers):
    """A client never dials a second connection for room, so the room comes
    from several clients. Spread thin, each connection goes quiet and an
    arrived reply waits for that connection's next bytes before its request
    sees it - noise larger than the 5 ms between fleet members, whose order
    of acceptance decides which order the fleet keeps. So a normal market
    stays on one busy connection and only a slow spell spills over."""
    from polymarket_bot import transport

    submitter = _submitter(lambda request: httpx.Response(200, json={}))
    used = []
    release = threading.Event()

    class Holding:
        def __init__(self, index):
            self.index = index

        async def post(self, url, content, headers):
            used.append(self.index)
            while not release.is_set():
                await asyncio.sleep(0.005)
            return httpx.Response(200, json={})

    try:
        submitter.start()
        submitter._clients = [Holding(index) for index in range(3)]
        submitter._in_flight = [0, 0, 0]
        waiting = transport.STREAMS_PER_CONNECTION + 3
        futures = [submitter.submit([_leg()]) for _ in range(waiting)]
        deadline = time.monotonic() + 10
        while len(used) < waiting and time.monotonic() < deadline:
            time.sleep(0.01)
        assert used.count(0) == transport.STREAMS_PER_CONNECTION
        assert used.count(1) == 3
        release.set()
        for future in futures:
            future.result(timeout=10)
        # Replies back, streams free: the next send is on the first again.
        submitter.submit([_leg()]).result(timeout=10)
        assert used[-1] == 0
    finally:
        release.set()
        submitter.stop()


def test_a_failed_send_gives_its_stream_back(flat_headers):
    """A count left high would mark a healthy connection full for good."""
    submitter = _submitter(lambda request: httpx.Response(200, json={}))

    class Failing:
        async def post(self, url, content, headers):
            raise httpx.ConnectError("no route")

    try:
        submitter.start()
        submitter._clients = [Failing(), Failing()]
        submitter._in_flight = [0, 0]
        with pytest.raises(PolyApiException):
            submitter.submit([_leg()]).result(timeout=10)
        assert submitter._in_flight == [0, 0]
    finally:
        submitter.stop()


def test_a_second_caller_waits_for_the_loop_to_be_ready(flat_headers, monkeypatch):
    """Fleet members start the loop concurrently. One that found the thread
    alive used to return while the clients were still being built, and its
    first send then failed - every member but one sat out the first market."""
    real_context = httpx.create_ssl_context

    def slow_context(*args, **kwargs):
        time.sleep(0.5)
        return real_context(*args, **kwargs)

    monkeypatch.setattr(httpx, "create_ssl_context", slow_context)
    submitter = _submitter(lambda request: httpx.Response(200, json={"ok": True}))
    try:
        first = threading.Thread(target=submitter.start)
        first.start()
        time.sleep(0.1)  # the loop thread is alive and still building
        submitter.start()
        assert submitter._loop is not None and submitter._clients
        assert submitter.submit([_leg()]).result(timeout=10) == [{"ok": True}]
        first.join(timeout=10)
    finally:
        submitter.stop()


def test_order_connections_are_http2_and_outlast_a_slow_venue():
    """Over HTTP/2 a read or write timeout fails every stream on the
    connection at once. At one second, a venue pause of one second killed
    every request on the one connection orders used (run38, 08:18); its
    slow spell answered in 3-5 s, so no timeout may sit anywhere near that."""
    from polymarket_bot import transport

    submitter = AsyncSubmitter()
    try:
        submitter.start()
        clients = submitter._clients
        assert len({id(client) for client in clients}) == transport.ORDER_CONNECTIONS
        # One TLS context for all: loading the CA bundle per client cost
        # about half a second apiece, and fleet members wait on the build.
        contexts = {id(client._transport._pool._ssl_context) for client in clients}
        assert len(contexts) == 1
        for client in clients:
            pool = client._transport._pool
            assert pool._http2 is True
            # Room for a replacement to dial while a connection the venue
            # retired (GOAWAY after 10,000 streams) drains its streams.
            assert pool._max_connections >= 2
            assert pool._max_connections == transport.ORDER_CLIENT_CONNECTIONS
            assert pool._max_keepalive_connections == transport.ORDER_CLIENT_CONNECTIONS
            assert client.timeout.read >= 10
            assert client.timeout.write >= 10
            assert client.timeout.connect >= 5
    finally:
        submitter.stop()


def test_two_legs_come_back_together_in_order(flat_headers):
    def handler(request):
        return httpx.Response(200, json={"echo": request.url.path})

    submitter = _submitter(handler)
    try:
        first, second = _leg(), _leg()
        second.url = "https://clob.polymarket.com/order2"
        result = submitter.submit([first, second]).result(timeout=10)
        assert [row["echo"] for row in result] == ["/order", "/order2"]
    finally:
        submitter.stop()


def test_the_future_wakes_concurrent_futures_wait(flat_headers):
    """The sending loop blocks in concurrent.futures.wait; the loop-made
    future must wake it like the thread-made one did."""

    def handler(request):
        return httpx.Response(200, json={"success": True})

    submitter = _submitter(handler)
    try:
        future = submitter.submit([_leg()])
        done, _ = wait({future}, timeout=10, return_when=FIRST_COMPLETED)
        assert future in done
    finally:
        submitter.stop()


def test_prepare_serializes_once_and_reuses(flat_headers, monkeypatch):
    built = []

    def fake_init(self, client, args):
        built.append(args)
        self.client = client
        self.args = args
        self.url = "u"
        self.body_dict = {}
        self.serialized = "{}"
        self.body_bytes = b"{}"

    monkeypatch.setattr(PreparedLeg, "__init__", fake_init)
    submitter = AsyncSubmitter()
    client = FakeClient()

    class Args:
        pass

    args = Args()
    first = submitter.prepare(client, args)
    second = submitter.prepare(client, args)
    assert first is second
    assert len(built) == 1


def test_the_cache_cannot_answer_for_a_recycled_object_id(monkeypatch):
    """CPython hands a freed object's id to the next same-shaped allocation,
    and the cache is keyed by id: an entry that let its args die could be
    found by a NEW signed order on the recycled id and answer with the
    PREVIOUS market's body - an authentic order for the wrong market.

    Two guarantees close it: the entry pins args (its id stays taken while
    the entry is findable), and the lookup checks object identity, not just
    the key."""

    def fake_init(self, client, args):
        self.client = client
        self.args = args
        self.url = "u"
        self.body_dict = {"market": getattr(args, "label", "?")}
        self.serialized = "{}"
        self.body_bytes = b"{}"

    monkeypatch.setattr(PreparedLeg, "__init__", fake_init)
    submitter = AsyncSubmitter()
    client = FakeClient()

    class Args:
        def __init__(self, label):
            self.label = label

    first_args = Args("market-A")
    leg = submitter.prepare(client, first_args)
    # The entry must hold the object itself; while it does, no new object
    # can be allocated at this id.
    assert leg.args is first_args

    # Even if a lookup ever arrives with a different object on the same
    # key, identity - not the key - decides.
    impostor = Args("market-B")
    submitter._prepared[id(impostor)] = leg
    fresh = submitter.prepare(client, impostor)
    assert fresh.body_dict == {"market": "market-B"}


def test_warm_is_rate_limited_like_the_sync_warm_up(monkeypatch):
    """place_dual asks to warm on every re-entry; unguarded, a signing-not-
    ready window bunches hundreds of dials into the moments before the
    open."""
    from polymarket_bot import transport

    monkeypatch.setattr(transport, "_installed", True)
    submitter = AsyncSubmitter()
    started = []
    monkeypatch.setattr(submitter, "start", lambda: started.append(True))

    class FakeLoop:
        pass

    submitter._loop = None  # start() is stubbed; warm must bail before use

    def fake_run_coroutine_threadsafe(coro, loop):
        coro.close()
        raise AssertionError("dial scheduled despite stubbed start")

    # First call passes the rate gate (then fails on the stubbed start's
    # missing loop only if it tries to schedule - we stop before that by
    # asserting on the gate itself).
    submitter._last_warm = None
    submitter._last_warm = 0.0  # long ago -> allowed
    import time as time_module

    monkeypatch.setattr(time_module, "monotonic", lambda: 1000.0)
    try:
        submitter.warm()
    except AssertionError:
        pass
    first_stamp = submitter._last_warm
    assert first_stamp == 1000.0

    # A second call inside the window must not even reach start().
    started.clear()
    monkeypatch.setattr(time_module, "monotonic", lambda: 1000.0 + 5.0)
    submitter.warm()
    assert submitter._last_warm == first_stamp
    assert started == []


def test_warm_opens_every_order_connection(monkeypatch):
    """Each client opens its connection on its first request; one dial each
    keeps the first sends at the open from paying a TLS handshake."""
    from polymarket_bot import transport

    monkeypatch.setattr(transport, "_installed", True)
    seen = []

    def handler(request):
        seen.append(request.url.path)
        return httpx.Response(200, json=0)

    submitter = _submitter(handler)
    try:
        submitter.warm()
        deadline = time.monotonic() + 5
        while len(seen) < transport.ORDER_CONNECTIONS and time.monotonic() < deadline:
            time.sleep(0.01)
        assert seen == ["/time"] * transport.ORDER_CONNECTIONS
    finally:
        submitter.stop()


def test_exchange_routes_every_send_through_the_loop(monkeypatch):
    """The loop is the only road: the thread-per-send path was removed
    after run22, and batch mode - the last dispatch-thread user - after
    run23's post-mortem."""
    from polymarket_bot import exchange as exchange_module
    from polymarket_bot.exchange import Exchange

    class StubSubmitter:
        def __init__(self):
            self.calls = []

        def prepare(self, client, args):
            return ("prepared", args)

        def submit(self, legs):
            self.calls.append(legs)
            from concurrent.futures import Future

            future = Future()
            future.set_result([{"success": True}])
            return future

    stub = StubSubmitter()
    monkeypatch.setattr(exchange_module, "get_submitter", lambda: stub)

    ex = Exchange.__new__(Exchange)
    ex.entry_submission = "solo-up"
    ex.client = FakeClient()

    future = ex._submit_placement_request(["signed-args"])
    assert future.result(timeout=1) == [{"success": True}]
    assert stub.calls == [[("prepared", "signed-args")]]
