from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from threading import Event

import requests

from .market_activation import (
    MarketActivationState,
    MarketActivationUpdate,
    MarketActivationWorker,
)
from .models import Market, TradePlan


CLOB_BOOKS = "https://clob.polymarket.com/books"
DATA_TRADES = "https://data-api.polymarket.com/trades"
GAMMA_MARKET_BY_SLUG = "https://gamma-api.polymarket.com/markets/slug"
FINALIZATION_DELAY_SECONDS = 120
SNAPSHOT_RETRY_SECONDS = 0.05
SETTLEMENT_POLL_SECONDS = 30
TRADE_PAGE_LIMIT = 10_000


PAPER_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS paper_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    strategy_key TEXT NOT NULL,
    buy_price TEXT NOT NULL,
    usd_per_side TEXT NOT NULL,
    order_size TEXT NOT NULL,
    started_ts INTEGER NOT NULL,
    heartbeat_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_markets (
    strategy_key TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    start_ts INTEGER NOT NULL,
    end_ts INTEGER NOT NULL,
    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,
    market_discovered_ts_ms INTEGER NOT NULL,
    market_parameters_detected_ts_ms INTEGER NOT NULL,
    snapshot_ts_ms INTEGER,
    book_source_ts_ms INTEGER,
    buy_price TEXT NOT NULL,
    order_size TEXT NOT NULL,
    up_queue_ahead TEXT,
    down_queue_ahead TEXT,
    status TEXT NOT NULL,
    winner_outcome TEXT,
    up_boundary_volume TEXT,
    down_boundary_volume TEXT,
    up_crossed INTEGER,
    down_crossed INTEGER,
    up_fifo_fill TEXT,
    down_fifo_fill TEXT,
    trade_count INTEGER,
    gross_cost TEXT,
    gross_payout TEXT,
    pnl_before_rebates TEXT,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    error TEXT,
    updated_ts INTEGER NOT NULL,
    PRIMARY KEY(strategy_key, slug)
);

CREATE INDEX IF NOT EXISTS idx_paper_markets_status
ON paper_markets(strategy_key, status, end_ts);
"""


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def strategy_key(plan: TradePlan) -> str:
    return (
        f"dual-hold:price={_decimal_text(plan.buy_price)}:"
        f"size={_decimal_text(plan.order_size)}:fifo-v1"
    )


def queue_at_price(book: dict, price: Decimal) -> Decimal:
    return sum(
        (
            Decimal(str(level.get("size") or "0"))
            for level in book.get("bids") or []
            if Decimal(str(level.get("price") or "0")) == price
        ),
        Decimal("0"),
    )


@dataclass(frozen=True)
class ExecutionEvidence:
    boundary_volume: Decimal
    crossed: bool
    events: tuple[dict, ...]


def execution_evidence(
    trades: list[dict],
    *,
    token_id: str,
    opposite_token_id: str,
    buy_price: Decimal,
) -> ExecutionEvidence:
    complement_price = Decimal("1") - buy_price
    boundary_volume = Decimal("0")
    crossed = False
    events = []

    for trade in trades:
        asset_id = str(trade.get("asset") or trade.get("asset_id") or "")
        side = str(trade.get("side") or "").upper()
        price = Decimal(str(trade.get("price") or "0"))
        size = Decimal(str(trade.get("size") or "0"))

        direct = asset_id == token_id and side == "SELL"
        complementary = asset_id == opposite_token_id and side == "BUY"
        if not direct and not complementary:
            continue

        threshold = buy_price if direct else complement_price
        through = price < threshold if direct else price > threshold
        at_boundary = price == threshold
        if not through and not at_boundary:
            continue

        if through:
            crossed = True
        else:
            boundary_volume += size
        events.append(
            {
                "timestamp": trade.get("timestamp"),
                "transaction_hash": trade.get("transactionHash"),
                "asset_id": asset_id,
                "side": side,
                "price": str(price),
                "size": str(size),
                "route": "direct" if direct else "complementary",
                "through": through,
            }
        )

    return ExecutionEvidence(boundary_volume, crossed, tuple(events))


def fifo_fill(
    *,
    queue_ahead: Decimal,
    order_size: Decimal,
    evidence: ExecutionEvidence,
) -> Decimal:
    if evidence.crossed:
        return order_size
    return min(
        order_size,
        max(Decimal("0"), evidence.boundary_volume - queue_ahead),
    )


@dataclass(frozen=True)
class PaperResult:
    winner_outcome: str
    up_evidence: ExecutionEvidence
    down_evidence: ExecutionEvidence
    up_fill: Decimal
    down_fill: Decimal
    gross_cost: Decimal
    gross_payout: Decimal
    pnl_before_rebates: Decimal


def calculate_result(
    *,
    winner_outcome: str,
    trades: list[dict],
    up_token_id: str,
    down_token_id: str,
    buy_price: Decimal,
    order_size: Decimal,
    up_queue_ahead: Decimal,
    down_queue_ahead: Decimal,
) -> PaperResult:
    winner = winner_outcome.lower()
    if winner not in {"up", "down"}:
        raise ValueError(f"unsupported winning outcome: {winner_outcome}")

    up_evidence = execution_evidence(
        trades,
        token_id=up_token_id,
        opposite_token_id=down_token_id,
        buy_price=buy_price,
    )
    down_evidence = execution_evidence(
        trades,
        token_id=down_token_id,
        opposite_token_id=up_token_id,
        buy_price=buy_price,
    )
    up_fill = fifo_fill(
        queue_ahead=up_queue_ahead,
        order_size=order_size,
        evidence=up_evidence,
    )
    down_fill = fifo_fill(
        queue_ahead=down_queue_ahead,
        order_size=order_size,
        evidence=down_evidence,
    )
    gross_cost = buy_price * (up_fill + down_fill)
    gross_payout = up_fill if winner == "up" else down_fill
    return PaperResult(
        winner_outcome=winner,
        up_evidence=up_evidence,
        down_evidence=down_evidence,
        up_fill=up_fill,
        down_fill=down_fill,
        gross_cost=gross_cost,
        gross_payout=gross_payout,
        pnl_before_rebates=gross_payout - gross_cost,
    )


class PaperDatabase:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(PAPER_SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "PaperDatabase":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def start(self, plan: TradePlan) -> str:
        key = strategy_key(plan)
        now = int(time.time())
        self.connection.execute(
            """
            INSERT INTO paper_state(
                id, strategy_key, buy_price, usd_per_side, order_size,
                started_ts, heartbeat_ts
            ) VALUES (1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                strategy_key=excluded.strategy_key,
                buy_price=excluded.buy_price,
                usd_per_side=excluded.usd_per_side,
                order_size=excluded.order_size,
                started_ts=excluded.started_ts,
                heartbeat_ts=excluded.heartbeat_ts
            """,
            (
                key,
                _decimal_text(plan.buy_price),
                _decimal_text(plan.usd_per_side),
                _decimal_text(plan.order_size),
                now,
                now,
            ),
        )
        self.connection.commit()
        return key

    def heartbeat(self) -> None:
        self.connection.execute(
            "UPDATE paper_state SET heartbeat_ts=? WHERE id=1",
            (int(time.time()),),
        )
        self.connection.commit()

    def register(
        self,
        key: str,
        update: MarketActivationUpdate,
        plan: TradePlan,
    ) -> bool:
        market = update.market
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO paper_markets(
                strategy_key, slug, condition_id, start_ts, end_ts,
                up_token_id, down_token_id, market_discovered_ts_ms,
                market_parameters_detected_ts_ms, buy_price, order_size,
                status, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'snapshot_pending', ?)
            """,
            (
                key,
                market.slug,
                market.condition_id,
                market.start_ts,
                market.end_ts,
                market.up_token_id,
                market.down_token_id,
                update.market_discovered_ts_ms,
                update.market_parameters_detected_ts_ms,
                _decimal_text(plan.buy_price),
                _decimal_text(plan.order_size),
                int(time.time()),
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def pending_snapshots(self, key: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT * FROM paper_markets
                WHERE strategy_key=? AND status='snapshot_pending'
                ORDER BY market_parameters_detected_ts_ms
                """,
                (key,),
            )
        )

    def expire_pending_snapshots(self, key: str, now_ts: int) -> None:
        self.connection.execute(
            """
            UPDATE paper_markets
            SET status='missed',
                error='public order book was not observed before market end',
                updated_ts=?
            WHERE strategy_key=? AND status='snapshot_pending' AND end_ts<=?
            """,
            (now_ts, key, now_ts),
        )
        self.connection.commit()

    def ready_to_settle(self, key: str, now_ts: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT * FROM paper_markets
                WHERE strategy_key=? AND status='monitoring' AND end_ts<=?
                ORDER BY end_ts
                """,
                (key, now_ts - FINALIZATION_DELAY_SECONDS),
            )
        )

    def set_snapshot(
        self,
        key: str,
        slug: str,
        *,
        snapshot_ts_ms: int,
        book_source_ts_ms: int,
        up_queue_ahead: Decimal,
        down_queue_ahead: Decimal,
    ) -> None:
        self.connection.execute(
            """
            UPDATE paper_markets
            SET snapshot_ts_ms=?, book_source_ts_ms=?, up_queue_ahead=?,
                down_queue_ahead=?, status='monitoring', error=NULL, updated_ts=?
            WHERE strategy_key=? AND slug=?
            """,
            (
                snapshot_ts_ms,
                book_source_ts_ms,
                str(up_queue_ahead),
                str(down_queue_ahead),
                int(time.time()),
                key,
                slug,
            ),
        )
        self.connection.commit()

    def set_error(self, key: str, slug: str, error: str) -> bool:
        row = self.connection.execute(
            """
            SELECT error FROM paper_markets
            WHERE strategy_key=? AND slug=?
            """,
            (key, slug),
        ).fetchone()
        changed = row is None or row["error"] != error
        self.connection.execute(
            """
            UPDATE paper_markets SET error=?, updated_ts=?
            WHERE strategy_key=? AND slug=?
            """,
            (error, int(time.time()), key, slug),
        )
        self.connection.commit()
        return changed

    def set_result(
        self,
        key: str,
        slug: str,
        *,
        result: PaperResult,
        trade_count: int,
    ) -> None:
        evidence = list(result.up_evidence.events) + list(result.down_evidence.events)
        self.connection.execute(
            """
            UPDATE paper_markets
            SET status='settled', winner_outcome=?, up_boundary_volume=?,
                down_boundary_volume=?, up_crossed=?, down_crossed=?,
                up_fifo_fill=?, down_fifo_fill=?, trade_count=?, gross_cost=?,
                gross_payout=?, pnl_before_rebates=?, evidence_json=?, error=NULL,
                updated_ts=?
            WHERE strategy_key=? AND slug=?
            """,
            (
                result.winner_outcome,
                str(result.up_evidence.boundary_volume),
                str(result.down_evidence.boundary_volume),
                int(result.up_evidence.crossed),
                int(result.down_evidence.crossed),
                str(result.up_fill),
                str(result.down_fill),
                trade_count,
                str(result.gross_cost),
                str(result.gross_payout),
                str(result.pnl_before_rebates),
                json.dumps(evidence, separators=(",", ":")),
                int(time.time()),
                key,
                slug,
            ),
        )
        self.connection.commit()

    def status(self) -> dict:
        state = self.connection.execute(
            "SELECT * FROM paper_state WHERE id=1"
        ).fetchone()
        if state is None:
            return {"running_state": None, "markets": {}}

        key = state["strategy_key"]
        rows = list(
            self.connection.execute(
                """
                SELECT * FROM paper_markets
                WHERE strategy_key=? ORDER BY start_ts DESC
                """,
                (key,),
            )
        )
        counts: dict[str, int] = {}
        settled = []
        for row in rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
            if row["status"] == "settled":
                settled.append(row)

        cost = sum(
            (Decimal(row["gross_cost"]) for row in settled), Decimal("0")
        )
        payout = sum(
            (Decimal(row["gross_payout"]) for row in settled), Decimal("0")
        )
        pnl = sum(
            (Decimal(row["pnl_before_rebates"]) for row in settled), Decimal("0")
        )
        fill_patterns = {
            "neither": 0,
            "up_only": 0,
            "down_only": 0,
            "both": 0,
        }
        profitable = 0
        losing = 0
        breakeven = 0
        for row in settled:
            up_filled = Decimal(row["up_fifo_fill"]) > 0
            down_filled = Decimal(row["down_fifo_fill"]) > 0
            if up_filled and down_filled:
                fill_patterns["both"] += 1
            elif up_filled:
                fill_patterns["up_only"] += 1
            elif down_filled:
                fill_patterns["down_only"] += 1
            else:
                fill_patterns["neither"] += 1
            market_pnl = Decimal(row["pnl_before_rebates"])
            if market_pnl > 0:
                profitable += 1
            elif market_pnl < 0:
                losing += 1
            else:
                breakeven += 1

        now = int(time.time())
        reserve_events: list[tuple[int, Decimal]] = []
        active_order_reserve = Decimal("0")
        for row in rows:
            if row["snapshot_ts_ms"] is None:
                continue
            reserve = (
                Decimal(row["buy_price"])
                * Decimal(row["order_size"])
                * Decimal("2")
            )
            placed_ts = int(row["snapshot_ts_ms"]) // 1000
            reserve_events.append((placed_ts, reserve))
            reserve_events.append((row["end_ts"], -reserve))
            if placed_ts <= now < row["end_ts"]:
                active_order_reserve += reserve

        peak_order_reserve = Decimal("0")
        running_reserve = Decimal("0")
        for _timestamp, delta in sorted(
            reserve_events,
            key=lambda item: (item[0], item[1] > 0),
        ):
            running_reserve += delta
            peak_order_reserve = max(peak_order_reserve, running_reserve)

        filled_markets = len(settled) - fill_patterns["neither"]
        return {
            "running_state": {
                **dict(state),
                "heartbeat_age_seconds": max(0, now - state["heartbeat_ts"]),
            },
            "model": (
                "public-book paper placement with FIFO lower-bound fills from "
                "public taker trades; cancellations ahead are not assumed"
            ),
            "markets": counts,
            "settled": len(settled),
            "fill_patterns": fill_patterns,
            "markets_with_fill": filled_markets,
            "fill_rate": (
                str(Decimal(filled_markets) / Decimal(len(settled)))
                if settled
                else None
            ),
            "profitable_markets": profitable,
            "losing_markets": losing,
            "breakeven_markets": breakeven,
            "profitable_rate": (
                str(Decimal(profitable) / Decimal(len(settled)))
                if settled
                else None
            ),
            "gross_cost": str(cost),
            "gross_payout": str(payout),
            "pnl_before_rebates": str(pnl),
            "average_pnl_per_settled_market": (
                str(pnl / Decimal(len(settled))) if settled else None
            ),
            "roi_on_filled_cost": str(pnl / cost) if cost else None,
            "active_order_reserve_usd": str(active_order_reserve),
            "peak_order_reserve_usd": str(peak_order_reserve),
            "latest": [
                {
                    "slug": row["slug"],
                    "status": row["status"],
                    "start_ts": row["start_ts"],
                    "up_queue_ahead": row["up_queue_ahead"],
                    "down_queue_ahead": row["down_queue_ahead"],
                    "up_fifo_fill": row["up_fifo_fill"],
                    "down_fifo_fill": row["down_fifo_fill"],
                    "winner_outcome": row["winner_outcome"],
                    "pnl_before_rebates": row["pnl_before_rebates"],
                    "error": row["error"],
                }
                for row in rows[:10]
            ],
        }


class ResolutionPending(RuntimeError):
    pass


class PaperSimulator:
    def __init__(
        self,
        database: PaperDatabase,
        plan: TradePlan,
        *,
        lookahead_minutes: int,
        hours: Decimal | None,
        logger: logging.Logger,
    ):
        self.database = database
        self.plan = plan
        self.lookahead_minutes = lookahead_minutes
        self.hours = hours
        self.logger = logger
        self.wake_event = Event()
        self.worker = MarketActivationWorker(
            window_minutes=lookahead_minutes,
            farthest_first=True,
            wake_event=self.wake_event,
        )
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "polymarket-btc-paper/0.1"
        self.key = strategy_key(plan)

    def run(self) -> None:
        self.key = self.database.start(self.plan)
        deadline = (
            time.monotonic() + float(self.hours * Decimal("3600"))
            if self.hours is not None
            else None
        )
        snapshot_pending = bool(self.database.pending_snapshots(self.key))
        next_snapshot_retry = 0.0 if snapshot_pending else float("inf")
        next_settlement_poll = 0.0
        next_heartbeat = 0.0
        self.worker.start()
        self.logger.info(
            "paper simulation started: price=%s size=%s lookahead=%s",
            self.plan.buy_price,
            self.plan.order_size,
            self.lookahead_minutes,
        )
        try:
            while deadline is None or time.monotonic() < deadline:
                self.wake_event.clear()
                registered = self._drain_activation_updates()
                if registered:
                    snapshot_pending = self._capture_pending_snapshots()
                    next_snapshot_retry = (
                        time.monotonic() + SNAPSHOT_RETRY_SECONDS
                        if snapshot_pending
                        else float("inf")
                    )
                now = time.monotonic()
                if snapshot_pending and now >= next_snapshot_retry:
                    snapshot_pending = self._capture_pending_snapshots()
                    next_snapshot_retry = (
                        now + SNAPSHOT_RETRY_SECONDS
                        if snapshot_pending
                        else float("inf")
                    )
                if now >= next_settlement_poll:
                    self._settle_finished_markets()
                    next_settlement_poll = now + SETTLEMENT_POLL_SECONDS
                if now >= next_heartbeat:
                    self.database.heartbeat()
                    next_heartbeat = now + 10
                next_due = min(
                    next_snapshot_retry,
                    next_settlement_poll,
                    next_heartbeat,
                )
                self.wake_event.wait(min(0.2, max(0.0, next_due - time.monotonic())))
        except KeyboardInterrupt:
            self.logger.info("paper simulation stopped by Ctrl+C")
        finally:
            self.worker.stop()
            self._drain_activation_updates()
            self.session.close()

    def _drain_activation_updates(self) -> bool:
        registered = False
        for update in self.worker.drain():
            if isinstance(update, MarketActivationState):
                if update.healthy:
                    self.logger.info("paper market activation connected")
                else:
                    self.logger.warning(
                        "paper market activation unavailable: %s", update.error
                    )
                continue
            if self.database.register(self.key, update, self.plan):
                registered = True
                self.logger.info(
                    "paper market registered: %s parameters_t=%s",
                    update.market.slug,
                    update.market_parameters_detected_ts_ms,
                )
        return registered

    def _capture_pending_snapshots(self) -> bool:
        self.database.expire_pending_snapshots(self.key, int(time.time()))
        for row in self.database.pending_snapshots(self.key):
            try:
                books = self._books(row)
                up_book = books[row["up_token_id"]]
                down_book = books[row["down_token_id"]]
                snapshot_ts_ms = int(time.time() * 1000)
                source_timestamps = [
                    int(str(book.get("timestamp") or "0"))
                    for book in (up_book, down_book)
                    if str(book.get("timestamp") or "").isdigit()
                ]
                self.database.set_snapshot(
                    self.key,
                    row["slug"],
                    snapshot_ts_ms=snapshot_ts_ms,
                    book_source_ts_ms=max(source_timestamps, default=0),
                    up_queue_ahead=queue_at_price(
                        up_book, Decimal(row["buy_price"])
                    ),
                    down_queue_ahead=queue_at_price(
                        down_book, Decimal(row["buy_price"])
                    ),
                )
                self.logger.info(
                    "paper queue captured: %s lag_ms=%s",
                    row["slug"],
                    snapshot_ts_ms - row["market_parameters_detected_ts_ms"],
                )
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if self.database.set_error(self.key, row["slug"], error):
                    self.logger.warning(
                        "paper queue capture failed: %s: %s", row["slug"], error
                    )
        return bool(self.database.pending_snapshots(self.key))

    def _books(self, row: sqlite3.Row) -> dict[str, dict]:
        response = self.session.post(
            CLOB_BOOKS,
            json=[
                {"token_id": row["up_token_id"]},
                {"token_id": row["down_token_id"]},
            ],
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("CLOB books response is not a list")
        books = {
            str(book.get("asset_id") or ""): book
            for book in payload
            if isinstance(book, dict)
        }
        required = {row["up_token_id"], row["down_token_id"]}
        if not required.issubset(books):
            raise ValueError("CLOB books response is missing an outcome")
        return books

    def _settle_finished_markets(self) -> None:
        now_ts = int(time.time())
        for row in self.database.ready_to_settle(self.key, now_ts):
            try:
                winner = self._winner(row["slug"])
                trades = self._trades(row, now_ts)
                result = calculate_result(
                    winner_outcome=winner,
                    trades=trades,
                    up_token_id=row["up_token_id"],
                    down_token_id=row["down_token_id"],
                    buy_price=Decimal(row["buy_price"]),
                    order_size=Decimal(row["order_size"]),
                    up_queue_ahead=Decimal(row["up_queue_ahead"]),
                    down_queue_ahead=Decimal(row["down_queue_ahead"]),
                )
                self.database.set_result(
                    self.key,
                    row["slug"],
                    result=result,
                    trade_count=len(trades),
                )
                self.logger.info(
                    "paper market settled: %s winner=%s up_fill=%s "
                    "down_fill=%s pnl=%s",
                    row["slug"],
                    result.winner_outcome,
                    result.up_fill,
                    result.down_fill,
                    result.pnl_before_rebates,
                )
            except ResolutionPending:
                continue
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if self.database.set_error(self.key, row["slug"], error):
                    self.logger.warning(
                        "paper settlement failed: %s: %s", row["slug"], error
                    )

    def _winner(self, slug: str) -> str:
        response = self.session.get(
            f"{GAMMA_MARKET_BY_SLUG}/{slug}",
            params={"_cb": str(time.time_ns())},
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("closed") is not True:
            raise ResolutionPending(slug)
        outcomes = json.loads(payload["outcomes"])
        prices = [Decimal(str(value)) for value in json.loads(payload["outcomePrices"])]
        winners = [
            str(outcome).lower()
            for outcome, price in zip(outcomes, prices, strict=True)
            if price == Decimal("1")
        ]
        if len(winners) != 1:
            raise ResolutionPending(slug)
        return winners[0]

    def _trades(self, row: sqlite3.Row, now_ts: int) -> list[dict]:
        start_ts = (int(row["snapshot_ts_ms"]) + 999) // 1000
        response = self.session.get(
            DATA_TRADES,
            params={
                "market": row["condition_id"],
                "limit": TRADE_PAGE_LIMIT,
                "offset": 0,
                "takerOnly": "true",
                "start": start_ts,
                "end": now_ts,
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("trade response is not a list")
        if len(payload) >= TRADE_PAGE_LIMIT:
            raise ValueError("trade response reached the 10000-row audit limit")
        return [
            trade
            for trade in payload
            if isinstance(trade, dict)
            and int(trade.get("timestamp") or 0) >= start_ts
        ]


def paper_database_path(project_root: Path) -> Path:
    return project_root / "data" / "paper.sqlite"
