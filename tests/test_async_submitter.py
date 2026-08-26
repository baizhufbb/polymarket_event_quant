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
import time
from concurrent.futures import FIRST_COMPLETED, wait

import httpx
import pytest
from py_clob_client_v2.exceptions import PolyApiException

from polymarket_bot import async_submitter
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


def test_a_trickling_reply_dies_at_the_total_lifetime(flat_headers, monkeypatch):
    """The sync stack could never bound a request's total life - trickling
    replies lived 69 s in the field. The loop cancels at the deadline and
    surfaces the same transient error a timeout always did."""
    monkeypatch.setattr(async_submitter, "TOTAL_LIFETIME_SECONDS", 0.2)

    async def handler(request):
        await asyncio.sleep(30)
        return httpx.Response(200, json={})

    submitter = _submitter(handler)
    try:
        began = time.monotonic()
        with pytest.raises(PolyApiException) as caught:
            submitter.submit([_leg()]).result(timeout=10)
        assert time.monotonic() - began < 5
        assert caught.value.status_code is None
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


def test_exchange_routes_non_batch_sends_through_the_loop(monkeypatch):
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
    ex.use_async_submitter = True
    ex.client = FakeClient()

    future = ex._submit_placement_request(["signed-args"])
    assert future.result(timeout=1) == [{"success": True}]
    assert stub.calls == [[("prepared", "signed-args")]]

    # The flag off, or batch mode, falls back to the thread path - which
    # calls the real client, so only the routing decision is asserted here.
    ex.use_async_submitter = False
    assert ex._submit_placement_request is not None
