from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import ssl
import time
from decimal import Decimal
from pathlib import Path

import websockets
from polymarket import PRODUCTION

from .market_activation import MarketActivationUpdate, MarketActivationWorker

logger = logging.getLogger(__name__)


TARGET_PRICE = Decimal("0.01")
# How long a market's book may take to appear after the listing shows its
# tokens. Until 2026-09-05 ~17:00 UTC that was about a minute (52.7 .. 103 s
# over 42 markets on 09-04) and 120 s covered it. Since then the venue opens
# books 100 s .. 6+ h after creation with no pattern (169 markets over 14 h:
# p50 3094 s, p90 9354 s, max 22682 s), so a market is watched for up to this
# long, reconnecting whenever the venue drops the idle socket.
BOOK_WAIT_SECONDS = 12 * 3600
RECONNECT_DELAY_SECONDS = 5
# While the book has not appeared, a healthy socket answers every 10-second
# PING with a PONG, so this long without any message means the connection is
# dead even though it never closed: on 2026-09-19 such sockets sat "open" for
# 1-3 hours past the door and 19 of 23 openings were missed. Take a fresh
# subscription now and then as well, in case a live socket stops delivering
# for a subscription while still answering PONGs.
SILENCE_SECONDS = 30
RESUBSCRIBE_SECONDS = 1800
# One TLS context for every connection. websockets builds a fresh default
# context per connect(), which loads the whole CA bundle each time; with
# dozens of markets waiting and each reconnecting on the schedule above,
# that churn took the probe from 70 MB to 263 MB in three hours on the
# 463 MB Hong Kong box (2026-09-20) and the box had already hung once.
_SSL_CONTEXT: ssl.SSLContext | None = None


def _ssl_context() -> ssl.SSLContext:
    global _SSL_CONTEXT
    if _SSL_CONTEXT is None:
        _SSL_CONTEXT = ssl.create_default_context()
    return _SSL_CONTEXT
OPENING_WINDOW_SECONDS = 2
OUTPUT_DIRECTORY = Path(__file__).resolve().parents[1] / "logs"
OUTPUT_PREFIX = "queue_probe_opening"


def output_path() -> Path:
    """This process's own recording file.

    Several probes run at once, because about half of this network's websocket
    handshakes to the venue fail and one instance alone misses markets. Sharing
    one file cost us observations: two writers ask for the end of the same file,
    get the same answer, and one lands on top of the other. It only shows up
    when they write fast, and the only time they write fast is the burst right
    after a market opens - 92.5% of the lines destroyed that way fell inside
    that first second, the one that decides queue position.

    Reading side: take every file matching queue_probe_opening*.jsonl.
    """
    return OUTPUT_DIRECTORY / f"{OUTPUT_PREFIX}.{os.getpid()}.jsonl"


def _timestamp_ms(value: object) -> int:
    text = str(value or "")
    return int(text) if text.isdecimal() else int(time.time() * 1000)


def _update_queues(
    event: dict,
    *,
    token_outcomes: dict[str, str],
    queues: dict[str, Decimal],
) -> bool:
    event_type = str(event.get("event_type") or "")
    if event_type == "book":
        outcome = token_outcomes.get(str(event.get("asset_id") or ""))
        if outcome is None:
            return False
        size = sum(
            (
                Decimal(str(level["size"]))
                for level in event.get("bids") or []
                if Decimal(str(level["price"])) == TARGET_PRICE
            ),
            Decimal("0"),
        )
        changed = queues.get(outcome) != size
        queues[outcome] = size
        return changed

    if event_type == "price_change":
        changed = False
        for change in event.get("price_changes") or []:
            outcome = token_outcomes.get(str(change.get("asset_id") or ""))
            if (
                outcome is None
                or str(change.get("side") or "").upper() != "BUY"
                or Decimal(str(change.get("price") or "0")) != TARGET_PRICE
            ):
                continue
            size = Decimal(str(change.get("size") or "0"))
            if queues.get(outcome) != size:
                queues[outcome] = size
                changed = True
        return changed

    return False


def _record(
    output,
    *,
    update: MarketActivationUpdate,
    event: dict,
    queues: dict[str, Decimal],
) -> None:
    row = {
        "observed_ts_ms": int(time.time() * 1000),
        "source_ts_ms": _timestamp_ms(event.get("timestamp")),
        "slug": update.market.slug,
        "market_discovered_ts_ms": update.market_discovered_ts_ms,
        "market_parameters_detected_ts_ms": (
            update.market_parameters_detected_ts_ms
        ),
        "up_queue_shares": str(queues["up"]),
        "down_queue_shares": str(queues["down"]),
    }
    output.write(json.dumps(row, separators=(",", ":")) + "\n")
    output.flush()


async def _heartbeat(socket) -> None:
    while True:
        await asyncio.sleep(10)
        await socket.send("PING")


async def _monitor_market(
    update: MarketActivationUpdate,
    output,
    opening_window_seconds: float = OPENING_WINDOW_SECONDS,
) -> None:
    market = update.market
    token_outcomes = {
        market.up_token_id: "up",
        market.down_token_id: "down",
    }
    queues: dict[str, Decimal] = {}
    loop = asyncio.get_running_loop()
    book_deadline = loop.time() + BOOK_WAIT_SECONDS
    opening_deadline: float | None = None
    while True:
        try:
            opening_deadline = await _watch_book(
                market.slug,
                token_outcomes=token_outcomes,
                queues=queues,
                output=output,
                update=update,
                book_deadline=book_deadline,
                opening_deadline=opening_deadline,
                opening_window_seconds=opening_window_seconds,
            )
            return
        except (websockets.ConnectionClosed, OSError, TimeoutError) as exc:
            # The venue drops sockets that sit idle for long enough, and a
            # book that opens hours after the listing means sitting idle for
            # hours. Reconnect until the market's deadline, keeping whatever
            # queue state the earlier connection already saw.
            remaining = book_deadline - loop.time()
            if remaining <= 0:
                logger.info("%s: gave up, no book within %.0f s", market.slug, BOOK_WAIT_SECONDS)
                return
            logger.info(
                "%s: socket lost (%s: %s), reconnecting in %d s, %.0f s left",
                market.slug, type(exc).__name__, str(exc)[:80], RECONNECT_DELAY_SECONDS, remaining,
            )
            await asyncio.sleep(min(RECONNECT_DELAY_SECONDS, max(0.0, remaining)))


async def _watch_book(
    slug: str,
    *,
    token_outcomes: dict[str, str],
    queues: dict[str, Decimal],
    output,
    update: MarketActivationUpdate,
    book_deadline: float,
    opening_deadline: float | None,
    opening_window_seconds: float,
) -> float | None:
    """One connection's worth of watching; returns when the market is done.

    Raises the socket's own exception when the venue drops the connection so
    the caller can reconnect; returns normally when the opening window has
    been recorded or the book deadline passed.
    """
    loop = asyncio.get_running_loop()
    async with websockets.connect(
        PRODUCTION.clob_market_ws_url,
        ssl=_ssl_context(),
        ping_interval=None,
        close_timeout=5,
        open_timeout=10,
    ) as socket:
        await socket.send(
            json.dumps(
                {
                    "assets_ids": list(token_outcomes),
                    "type": "market",
                },
                separators=(",", ":"),
            )
        )
        heartbeat = asyncio.create_task(_heartbeat(socket))
        connected_at = loop.time()
        last_message_at = connected_at
        try:
            while True:
                deadline = opening_deadline or book_deadline
                remaining = deadline - loop.time()
                if remaining <= 0:
                    if opening_deadline is None:
                        logger.info("%s: gave up, no book within %.0f s", slug, BOOK_WAIT_SECONDS)
                    return opening_deadline
                timeout = remaining
                if opening_deadline is None:
                    # Still waiting for the book. Silence means the socket is
                    # dead even though it never closed, and a subscription
                    # can go stale on a live socket: raise so the caller
                    # reconnects, the same way it does for a dropped socket.
                    silence_left = last_message_at + SILENCE_SECONDS - loop.time()
                    if silence_left <= 0:
                        raise TimeoutError(f"no message for {SILENCE_SECONDS} s")
                    resubscribe_left = connected_at + RESUBSCRIBE_SECONDS - loop.time()
                    if resubscribe_left <= 0:
                        raise TimeoutError(f"resubscribing after {RESUBSCRIBE_SECONDS} s")
                    timeout = min(remaining, silence_left, resubscribe_left)
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=timeout)
                except TimeoutError:
                    # Whichever limit ran out, the top of the loop decides:
                    # the deadline gives up, the other two reconnect.
                    continue
                last_message_at = loop.time()
                if raw == "PONG":
                    continue
                payload = json.loads(raw)
                events = payload if isinstance(payload, list) else [payload]
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    changed = _update_queues(
                        event,
                        token_outcomes=token_outcomes,
                        queues=queues,
                    )
                    if changed and len(queues) == 2:
                        if opening_deadline is None:
                            opening_deadline = (
                                loop.time() + opening_window_seconds
                            )
                        _record(
                            output,
                            update=update,
                            event=event,
                            queues=queues,
                        )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)


async def _run(
    opening_window_seconds: float = OPENING_WINDOW_SECONDS,
) -> None:
    destination = output_path()
    destination.parent.mkdir(parents=True, exist_ok=True)
    worker = MarketActivationWorker(window_minutes=0, farthest_first=True)
    tasks: set[asyncio.Task] = set()
    worker.start()
    try:
        with destination.open("a", encoding="utf-8", buffering=1) as output:
            while True:
                for update in worker.drain():
                    if not isinstance(update, MarketActivationUpdate):
                        continue
                    task = asyncio.create_task(
                        _monitor_market(update, output, opening_window_seconds),
                        name=f"queue-probe-{update.market.slug}",
                    )
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                await asyncio.sleep(0.05)
    finally:
        worker.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Record the public 0.01 queue around each market's opening"
    )
    parser.add_argument(
        "--opening-window-seconds",
        type=float,
        default=OPENING_WINDOW_SECONDS,
        help=(
            "keep recording this long after the first observed queue change; "
            f"default {OPENING_WINDOW_SECONDS}"
        ),
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(_run(args.opening_window_seconds))


if __name__ == "__main__":
    main()
