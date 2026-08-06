from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from pathlib import Path

import websockets
from polymarket import PRODUCTION

from .market_activation import MarketActivationUpdate, MarketActivationWorker


TARGET_PRICE = Decimal("0.01")
BOOK_WAIT_SECONDS = 120
OPENING_WINDOW_SECONDS = 2
OUTPUT_PATH = (
    Path(__file__).resolve().parents[1] / "logs" / "queue_probe_opening.jsonl"
)


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
) -> None:
    market = update.market
    token_outcomes = {
        market.up_token_id: "up",
        market.down_token_id: "down",
    }
    queues: dict[str, Decimal] = {}
    async with websockets.connect(
        PRODUCTION.clob_market_ws_url,
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
        try:
            loop = asyncio.get_running_loop()
            book_deadline = loop.time() + BOOK_WAIT_SECONDS
            opening_deadline: float | None = None
            while True:
                deadline = opening_deadline or book_deadline
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
                except TimeoutError:
                    break
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
                                loop.time() + OPENING_WINDOW_SECONDS
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


async def _run() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    worker = MarketActivationWorker(window_minutes=0, farthest_first=True)
    tasks: set[asyncio.Task] = set()
    worker.start()
    try:
        with OUTPUT_PATH.open("a", encoding="utf-8", buffering=1) as output:
            while True:
                for update in worker.drain():
                    if not isinstance(update, MarketActivationUpdate):
                        continue
                    task = asyncio.create_task(
                        _monitor_market(update, output),
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(_run())


if __name__ == "__main__":
    main()
