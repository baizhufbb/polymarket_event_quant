"""The knock library as Python sees it: that the library shipped is built
from the sources here, that it judges and words replies the way the Python
sender did, and - where the stand-in venue is built - a whole knock through
the real library."""

import _thread
import json
import logging
import subprocess
import sys
import threading
import time
import uuid
from decimal import Decimal
from types import SimpleNamespace

import pytest
from py_clob_client_v2 import ApiCreds
from py_clob_client_v2.clob_types import RequestArgs
from py_clob_client_v2.headers.headers import create_level_2_headers

import polymarket_bot.fleet as fleet_module
from polymarket_bot import knocker
from polymarket_bot.exchange import Exchange
from polymarket_bot.fleet import Fleet, evenly_phased
from polymarket_bot.models import Market

TESTDATA = knocker.KNOCKER_DIR / "testdata"
FAKEVENUE = knocker.KNOCKER_DIR / "build" / (
    "fakevenue.exe" if sys.platform == "win32" else "fakevenue"
)
built = pytest.mark.skipif(
    not knocker.library_path().exists(), reason="the knock library is not built here"
)
with_venue = pytest.mark.skipif(
    not (knocker.library_path().exists() and FAKEVENUE.exists()),
    reason="the stand-in venue is only built for local runs (knocker/build.ps1)",
)


def _replies():
    return json.loads((TESTDATA / "replies.json").read_text(encoding="utf-8"))


def test_the_committed_library_is_built_from_these_sources():
    """The server runs the committed .so; a Go change without a rebuild
    would leave it running the old code."""
    stamp = knocker.source_hash().encode()
    assert stamp in knocker.LINUX_LIBRARY.read_bytes(), (
        "polymarket_bot/libknocker.so is behind knocker/; run knocker/build.ps1"
    )


@built
def test_the_library_loaded_here_is_built_from_these_sources():
    assert knocker.version()["source_hash"] == knocker.source_hash()


@built
def test_replies_are_judged_as_the_python_sender_judged_them():
    """replies.json froze the Python sender's verdict on each reply
    (commit ec4d08b); the library must reach the same one."""
    for case in _replies():
        if case["transient"]:
            continue
        if case["reply_json"] is not None:
            value = json.loads(case["reply_json"])
        else:
            value = case["reply_text"]
        verdict = knocker.classify(value)
        assert (
            verdict.trace,
            verdict.accepted,
            verdict.order_id,
            verdict.duplicate_id,
            verdict.not_ready,
        ) == (
            case["trace"],
            case["accepted"],
            case["order_id"],
            case["duplicate_id"],
            case["not_ready"],
        ), case["body"]


def test_errors_are_worded_as_the_python_sender_worded_them():
    for case in _replies():
        item = {"status": case["status"], "body": case["body"]}
        if case["transient"]:
            assert knocker.ambiguous_text(item) == case["ambiguous"], case["body"]
        else:
            assert knocker.error_text(item) == (case["error_str"] or "partial placement"), case["body"]
    assert knocker.error_text({"text": "no acceptance within the knocking budget"}) == (
        "no acceptance within the knocking budget"
    )


def test_the_header_fixtures_still_match_the_official_client():
    """The Go headers are checked against l2_headers.json; this keeps that
    file honest against the official client installed here."""
    cases = json.loads((TESTDATA / "l2_headers.json").read_text(encoding="utf-8"))
    for case in cases:
        signer = SimpleNamespace(address=lambda address=case["address"]: address)
        creds = ApiCreds(
            api_key=case["api_key"],
            api_secret=case["api_secret"],
            api_passphrase=case["api_passphrase"],
        )
        args = RequestArgs(
            method=case["method"], request_path=case["path"], body=None,
            serialized_body=case["body"],
        )
        built_headers = create_level_2_headers(signer, creds, args, timestamp=case["timestamp"])
        assert built_headers == case["headers"]


def test_a_reply_reaches_its_accounts_trace_and_the_log(monkeypatch, caplog):
    rows = []
    healed = []
    monkeypatch.setattr(
        knocker, "_hooks", {"m1": knocker.Hooks(trace=rows.append, version_mismatch=lambda: healed.append(1))}
    )
    attempt = {
        "account": "m1", "attempt": 3, "legs": ["up"], "sent_ts_ms": 10,
        "returned_ts_ms": 42, "results": ["not_ready"], "status": 400,
        "body": '{"error":"invalid token id"}',
    }
    with caplog.at_level(logging.ERROR, logger="polymarket_bot.knocker"):
        knocker._record(attempt)
        knocker._record({**attempt, "attempt": 4, "status": 200, "body": "", "version_mismatch": True, "results": ["rejected"]})
        knocker._record({**attempt, "attempt": 5, "status": 0, "error": "EOF", "results": ["transport_error"]})
        knocker._record({**attempt, "account": "nobody", "status": 429, "results": ["rate_limited"]})

    # The trace keeps its old shape: the tracer adds the account.
    assert rows[0] == {
        "attempt": 3, "legs": ["up"], "sent_ts_ms": 10, "returned_ts_ms": 42,
        "results": ["not_ready"],
    }
    assert [row["attempt"] for row in rows] == [3, 4, 5]
    assert healed == [1]
    messages = [record.getMessage() for record in caplog.records]
    assert any("status=400" in m and "invalid token id" in m for m in messages)
    assert any("request error: EOF" in m for m in messages)
    # The session script counts these lines.
    assert any("status=429" in m for m in messages)


def test_a_stop_during_a_knock_stops_it_and_keeps_what_it_registered(monkeypatch):
    """Ctrl+C (the launcher's stop) must not throw away the orders a knock
    registered: the knock is told to stop, its result still comes back, and
    the stop is handed on once that result is in hand."""
    calls = []
    stop = threading.Event()

    def call(name, *args):
        calls.append(name)
        if name == "KnockerKnock":
            stop.wait(5)
            return {"members": [{"account": "a", "stopped": stop.is_set()}]}
        if name == "KnockerStop":
            stop.set()
            return True
        raise AssertionError(name)

    monkeypatch.setattr(knocker, "_call", call)
    monkeypatch.setattr(knocker, "_start_trace_thread", lambda: None)
    threading.Timer(0.2, _thread.interrupt_main).start()

    result = knocker.knock({"members": []}, {})

    assert result == {"members": [{"account": "a", "stopped": True}]}
    assert calls == ["KnockerKnock", "KnockerStop"]
    assert knocker.take_interrupt() is True
    assert knocker.take_interrupt() is False


def _member(account, phase_ms, legs):
    return {
        "account": account,
        "phase_ms": phase_ms,
        "address": "0xSigner",
        "api_key": "key",
        "api_secret": "c2VjcmV0",
        "api_passphrase": "pass",
        "legs": [
            {"outcome": leg, "body": json.dumps({"account": account, "outcome": leg})}
            for leg in legs
        ],
    }


@pytest.fixture
def venue(tmp_path):
    process = subprocess.Popen(
        [str(FAKEVENUE), "-dir", str(tmp_path), "-open-after", "400ms"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    try:
        yield json.loads(process.stdout.readline())
    finally:
        process.stdin.close()
        process.wait(timeout=10)


def _wait_for(predicate, seconds=6.0):
    deadline = time.monotonic() + seconds
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.02)
    return predicate()


@with_venue
def test_a_whole_knock_runs_through_the_real_library(venue):
    run = uuid.uuid4().hex[:8]
    names = [f"a-{run}", f"b-{run}"]
    rows = {name: [] for name in names}
    now_ms = int(time.time() * 1000)
    plan = {
        "market": "integration",
        "interval_ms": 25.0,
        "knock_until_ms": now_ms + 10_000,
        "market_end_ms": now_ms + 60_000,
        "base_url": venue["url"],
        "ca_file": venue["ca_file"],
        "members": [_member(names[0], 0.0, ["up", "down"]), _member(names[1], 12.5, ["up", "down"])],
    }

    result = knocker.knock(plan, {name: knocker.Hooks(trace=rows[name].append) for name in names})

    for name, outcome in zip(names, result["members"], strict=True):
        assert outcome["account"] == name
        assert [order["outcome"] for order in outcome["accepted"]] == ["up", "down"]
        assert outcome["registered_ms"] is not None
        assert not outcome["gave_up"]
        assert outcome["ambiguous"] == []
        # Whatever came back before the door was the venue's not-ready reply.
        assert {knocker.error_text(item) for item in outcome["errors"]} <= {
            "{'errorMsg': 'invalid token id', 'success': False}"
        }
        assert _wait_for(lambda name=name, outcome=outcome: len(rows[name]) >= outcome["attempts"])
        assert len(rows[name]) == outcome["attempts"]
        assert set(rows[name][0]) == {"attempt", "legs", "sent_ts_ms", "returned_ts_ms", "results"}


@with_venue
def test_a_fleet_places_through_the_real_library(venue, monkeypatch):
    """Sign, knock through the library, keep the first registration and
    cancel the rest - with only the venue standing in."""

    class Client:
        def __init__(self):
            self.canceled = []
            self.creds = ApiCreds(api_key="key", api_secret="c2VjcmV0", api_passphrase="pass")
            self.signer = SimpleNamespace(address=lambda: "0xSigner")

        def create_order(self, order_args, options):
            return {"token_id": order_args.token_id, "size": order_args.size}

        def cancel_orders(self, order_ids):
            self.canceled.extend(order_ids)
            return {"canceled": list(order_ids)}

    def exchange():
        made = Exchange.__new__(Exchange)
        made.client = Client()
        made.credentials = made.client.creds
        made.entry_submission = "single"
        return made

    monkeypatch.setattr(Exchange, "_order_body", lambda self, args: json.dumps(args.order, sort_keys=True))
    plain = fleet_module.knock_plan
    monkeypatch.setattr(
        fleet_module,
        "knock_plan",
        lambda *a, **k: {**plain(*a, **k), "base_url": venue["url"], "ca_file": venue["ca_file"]},
    )
    run = uuid.uuid4().hex[:8]
    market = Market(
        slug=f"btc-updown-5m-{run}", condition_id="0xc", start_ts=0,
        end_ts=int(time.time()) + 300, up_token_id=f"up-{run}", down_token_id=f"down-{run}",
        min_size=Decimal("5"), tick_size=Decimal("0.01"),
    )
    fleet = Fleet(
        evenly_phased(
            [(f"p-{run}", exchange(), Decimal("103.7")), (f"m-{run}", exchange(), Decimal("106.1"))],
            Decimal("25"),
        )
    )

    placement = fleet.place(market, price=Decimal("0.01"), submission_interval_ms=Decimal("25"))

    assert placement.kept is not None
    assert placement.kept_by == "reply"
    laggard = next(m for m in fleet.members if m.name != placement.kept)
    kept = fleet.member(placement.kept)
    assert sorted(laggard.exchange.client.canceled) == sorted(placement.cancelled_order_ids)
    assert len(placement.cancelled_order_ids) == 2
    assert kept.exchange.client.canceled == []
    assert len(placement.kept_order_ids()) == 2
