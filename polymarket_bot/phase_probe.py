"""TEMPORARY scaffolding: does the venue rate-limit per account or per address?

Two credential sets hammer the same upcoming market from one machine, phases
interleaved. Account B needs no funding: its orders are rejected, but any
reply other than 429 proves a rate token was granted to B's bucket. If B
(and A) stay free of 429 at a combined 80 requests/second, limiting is
per-account and one server can host the whole phased fleet; if 429s appear
at the combined rate, an address-level component exists and the fleet needs
multiple egress addresses.

Delete together with the other measurement scaffolding once the fleet
architecture is decided.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from pathlib import Path
from threading import Lock, Thread

import requests

from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
from py_clob_client_v2.exceptions import PolyApiException

from .config import BotConfig
from .discovery import MarketDiscovery
from .exchange import Exchange, classify_response
from .market_activation import CLOB_MARKETS
from .models import Market


def _load_env_file(path: Path) -> dict:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def _config_b(path: Path, project_root: Path) -> BotConfig:
    values = _load_env_file(path)
    return BotConfig(
        project_root=project_root,
        private_key=values["POLYMARKET_PRIVATE_KEY"],
        funder_address=values["POLYMARKET_FUNDER_ADDRESS"],
        signature_type=int(values.get("POLYMARKET_SIGNATURE_TYPE", "0")),
        api_key=values.get("POLYMARKET_CLOB_API_KEY") or None,
        api_secret=values.get("POLYMARKET_CLOB_API_SECRET") or None,
        api_passphrase=values.get("POLYMARKET_CLOB_API_PASSPHRASE") or None,
    )


def _tokens_live(session: requests.Session, market: Market) -> bool:
    response = session.get(f"{CLOB_MARKETS}/{market.condition_id}", timeout=5)
    if response.status_code != 200:
        return False
    tokens = {
        str(token.get("t") or "")
        for token in response.json().get("t") or []
        if isinstance(token, dict)
    }
    return {market.up_token_id, market.down_token_id} <= tokens


def _wait_for_next_market(discovery: MarketDiscovery) -> Market:
    """Return the next newly announced market.

    Tokens go live essentially at announcement and the book opens roughly
    ninety seconds later, so the catchable signal is a fresh slug, not a
    not-yet-live token set: remember the newest market at entry and return
    the first different one.
    """
    known = None
    while True:
        markets = discovery.discover(
            window_minutes=5, farthest_first=True, timeout=10
        )
        if markets:
            candidate = markets[0]
            if known is None:
                known = candidate.slug
            elif candidate.slug != known:
                return candidate
        time.sleep(1.0)


def _wait_until_active(market: Market) -> None:
    session = requests.Session()
    while not _tokens_live(session, market):
        time.sleep(0.25)


class Stream:
    def __init__(self, name: str, exchange: Exchange, market: Market, size: str):
        self.name = name
        self.exchange = exchange
        options = PartialCreateOrderOptions(
            tick_size=str(market.tick_size), neg_risk=False
        )
        self.signed = exchange.client.create_order(
            OrderArgs(
                token_id=market.up_token_id,
                price=0.01,
                size=float(size),
                side=Side.BUY,
            ),
            options,
        )
        self.lock = Lock()
        self.rows = []

    def fire(self) -> None:
        sent = int(time.time() * 1000)

        def post() -> None:
            try:
                response = self.exchange.client.post_order(
                    self.signed, OrderType.GTC, post_only=True
                )
            except BaseException as exc:  # noqa: BLE001 - classified below
                response = exc
            with self.lock:
                self.rows.append(
                    (sent, int(time.time() * 1000), classify_response(response))
                )

        Thread(target=post, daemon=True).start()

    def has_registered(self) -> bool:
        with self.lock:
            return any(kind in ("accepted", "duplicate") for _, _, kind in self.rows)


def _summary(stream: Stream) -> dict:
    with stream.lock:
        rows = list(stream.rows)
    counts = Counter(kind for _, _, kind in rows)
    rtts = sorted(returned - sent for sent, returned, _ in rows)
    return {
        "stream": stream.name,
        "requests": len(rows),
        "replies": dict(counts),
        "rate_limited": counts.get("rate_limited", 0),
        "rtt_ms_median": rtts[len(rtts) // 2] if rtts else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-b", required=True, type=Path)
    parser.add_argument("--markets", type=int, default=2)
    parser.add_argument("--interval-ms", type=float, default=25.0)
    parser.add_argument("--offset-ms", type=float, default=12.5)
    parser.add_argument("--tail-seconds", type=float, default=3.0)
    args = parser.parse_args()

    if os.getenv("POLYMARKET_LIVE_ACK", "") != "I_UNDERSTAND_REAL_ORDERS":
        raise SystemExit("live ack not loaded - aborting before any order")

    config_a = BotConfig.load(live=True)
    config_b = _config_b(args.env_b, config_a.project_root)
    exchange_a = Exchange(config_a)
    try:
        exchange_b = Exchange(config_b)
    except PolyApiException as exc:
        print("=" * 47)
        print("VERDICT: the venue REFUSED credentials for the fresh key.")
        print("Unregistered keys are gated; fleet accounts must be created")
        print(f"through the site. Venue reply: {exc}")
        print("=" * 47)
        raise SystemExit(2)
    print("fresh key B obtained API credentials - no registration gate")
    discovery = MarketDiscovery()

    for round_no in range(1, args.markets + 1):
        print(f"[{round_no}/{args.markets}] waiting for the next announced market...")
        market = _wait_for_next_market(discovery)
        print(f"  market {market.slug}; waiting for CLOB parameters...")
        _wait_until_active(market)
        print("  parameters live - dual-stream hammering starts")

        streams = (
            Stream("A", exchange_a, market, "103.7"),
            Stream("B", exchange_b, market, "106.1"),
        )
        interval = args.interval_ms / 1000.0
        offset = args.offset_ms / 1000.0
        started = time.monotonic()
        hard_deadline = started + 120.0
        next_fire = [started, started + offset]
        tail_deadline = None
        while True:
            now = time.monotonic()
            if now >= hard_deadline:
                break
            if tail_deadline is not None and now >= tail_deadline:
                break
            for index, stream in enumerate(streams):
                if now >= next_fire[index]:
                    stream.fire()
                    next_fire[index] += interval
            if tail_deadline is None and streams[0].has_registered():
                tail_deadline = now + args.tail_seconds
            time.sleep(0.001)

        time.sleep(2.0)
        report = [_summary(stream) for stream in streams]
        print(json.dumps({"market": market.slug, "streams": report}, indent=2))

        for name, exchange in (("A", exchange_a), ("B", exchange_b)):
            try:
                exchange.client.cancel_all()
                print(f"  {name}: cancel-all sent")
            except PolyApiException as exc:
                print(f"  {name}: cancel-all failed: {exc}")

    print("phase-0 finished")


if __name__ == "__main__":
    main()
