from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from .models import Market, PlacedOrder


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_ts INTEGER NOT NULL,
    stopped_ts INTEGER,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    plan_json TEXT NOT NULL DEFAULT '{}',
    heartbeat_ts INTEGER,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS markets (
    slug TEXT PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    condition_id TEXT NOT NULL,
    start_ts INTEGER NOT NULL,
    end_ts INTEGER NOT NULL,
    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,
    min_size TEXT NOT NULL DEFAULT '5',
    tick_size TEXT NOT NULL DEFAULT '0.01',
    state TEXT NOT NULL,
    error TEXT,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    slug TEXT NOT NULL REFERENCES markets(slug),
    outcome TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL DEFAULT 'buy',
    role TEXT NOT NULL DEFAULT 'entry',
    account TEXT NOT NULL DEFAULT '',
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    matched_size TEXT NOT NULL DEFAULT '0',
    status TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_slug ON orders(slug);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_markets_end_ts ON markets(end_ts);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    ts INTEGER NOT NULL,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    slug TEXT,
    details_json TEXT NOT NULL
);
"""


def _now() -> int:
    return int(time.time())


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class BotDatabase:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        required = {
            "runs": {
                "plan_json": "TEXT NOT NULL DEFAULT '{}'",
            },
            "markets": {
                "min_size": "TEXT NOT NULL DEFAULT '5'",
                "tick_size": "TEXT NOT NULL DEFAULT '0.01'",
            },
            "orders": {
                "side": "TEXT NOT NULL DEFAULT 'buy'",
                "role": "TEXT NOT NULL DEFAULT 'entry'",
                "account": "TEXT NOT NULL DEFAULT ''",
            },
        }
        for table, columns in required.items():
            existing = {
                row["name"]
                for row in self.connection.execute(f"PRAGMA table_info({table})")
            }
            for name, definition in columns.items():
                if name not in existing:
                    self.connection.execute(
                        f"ALTER TABLE {table} ADD COLUMN {name} {definition}"
                    )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "BotDatabase":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def start_run(self, mode: str, plan: object = None) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO runs(started_ts, mode, status, plan_json)
            VALUES (?, ?, 'running', ?)
            """,
            (_now(), mode, _json(plan)),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def stop_run(self, run_id: int, status: str, error: str | None = None) -> None:
        self.connection.execute(
            "UPDATE runs SET stopped_ts=?, status=?, last_error=? WHERE id=?",
            (_now(), status, error, run_id),
        )
        self.connection.commit()

    def event(
        self,
        run_id: int | None,
        level: str,
        event_type: str,
        *,
        slug: str | None = None,
        details: object = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO events(run_id, ts, level, event_type, slug, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, _now(), level, event_type, slug, _json(details)),
        )
        self.connection.commit()

    def has_market(self, slug: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM markets WHERE slug=?", (slug,)
            ).fetchone()
            is not None
        )

    def can_start_entry_plan(self, slug: str) -> bool:
        row = self.connection.execute(
            "SELECT state FROM markets WHERE slug=?", (slug,)
        ).fetchone()
        if row is None:
            return True
        if row["state"] != "placement_pending":
            return False
        return (
            self.connection.execute(
                "SELECT 1 FROM orders WHERE slug=? LIMIT 1", (slug,)
            ).fetchone()
            is None
        )

    def add_market(self, run_id: int, market: Market, state: str = "placing") -> None:
        self.connection.execute(
            """
            INSERT INTO markets(
                slug, run_id, condition_id, start_ts, end_ts,
                up_token_id, down_token_id, min_size, tick_size, state, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market.slug,
                run_id,
                market.condition_id,
                market.start_ts,
                market.end_ts,
                market.up_token_id,
                market.down_token_id,
                str(market.min_size),
                str(market.tick_size),
                state,
                _now(),
            ),
        )
        self.connection.commit()

    def prepare_market(
        self, run_id: int, market: Market, state: str = "placing"
    ) -> None:
        if not self.has_market(market.slug):
            self.add_market(run_id, market, state)
            return
        self.connection.execute(
            """
            UPDATE markets
            SET run_id=?, condition_id=?, start_ts=?, end_ts=?,
                up_token_id=?, down_token_id=?, min_size=?, tick_size=?,
                state=?, error=NULL, updated_ts=?
            WHERE slug=?
            """,
            (
                run_id,
                market.condition_id,
                market.start_ts,
                market.end_ts,
                market.up_token_id,
                market.down_token_id,
                str(market.min_size),
                str(market.tick_size),
                state,
                _now(),
                market.slug,
            ),
        )
        self.connection.commit()

    def set_market_state(self, slug: str, state: str, error: str | None = None) -> None:
        self.connection.execute(
            "UPDATE markets SET state=?, error=?, updated_ts=? WHERE slug=?",
            (state, error, _now(), slug),
        )
        self.connection.commit()

    def add_order(self, run_id: int, slug: str, order: PlacedOrder) -> None:
        now = _now()
        initial_matched = (
            order.size if order.status in {"matched", "filled"} else Decimal("0")
        )
        self.connection.execute(
            """
            INSERT OR REPLACE INTO orders(
                order_id, run_id, slug, outcome, token_id, side, role, account,
                price, size, matched_size, status, raw_json, created_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order.order_id,
                run_id,
                slug,
                order.outcome,
                order.token_id,
                order.side,
                order.role,
                order.account,
                str(order.price),
                str(order.size),
                str(initial_matched),
                order.status,
                _json(order.raw),
                now,
                now,
            ),
        )
        self.connection.commit()

    def tracked_open_orders(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT o.*, m.condition_id, m.end_ts, m.min_size, m.tick_size
                FROM orders o JOIN markets m ON m.slug=o.slug
                WHERE o.status IN (
                    'open', 'live', 'delayed', 'unmatched', 'simulated',
                    'cancel_requested'
                )
                ORDER BY o.created_ts
                """
            )
        )

    def due_open_orders(self, cutoff_ts: float) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT o.*, m.condition_id, m.end_ts, m.min_size, m.tick_size
                FROM markets AS m INDEXED BY idx_markets_end_ts
                CROSS JOIN orders AS o
                WHERE m.end_ts <= ? AND o.slug=m.slug AND o.status IN (
                    'open', 'live', 'delayed', 'unmatched', 'simulated'
                )
                ORDER BY m.end_ts, o.created_ts
                """,
                (cutoff_ts,),
            )
        )

    def update_order(
        self,
        order_id: str,
        *,
        status: str,
        matched_size: Decimal,
        raw: object,
    ) -> None:
        self.connection.execute(
            """
            UPDATE orders
            SET status=?, matched_size=?, raw_json=?, updated_ts=?
            WHERE order_id=?
            """,
            (status, str(matched_size), _json(raw), _now(), order_id),
        )
        self.connection.commit()

    def order(self, order_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM orders WHERE order_id=?", (order_id,)
        ).fetchone()

    def active_reserved_usd(self) -> Decimal:
        rows = self.connection.execute(
            """
            SELECT price, size, matched_size FROM orders
            WHERE side='buy'
              AND status IN (
                  'open', 'live', 'delayed', 'unmatched', 'simulated',
                  'cancel_requested'
              )
            """
        )
        return sum(
            (
                Decimal(row["price"])
                * max(Decimal("0"), Decimal(row["size"]) - Decimal(row["matched_size"]))
                for row in rows
            ),
            Decimal("0"),
        )

    def mark_orders(self, order_ids: list[str], status: str) -> None:
        if not order_ids:
            return
        placeholders = ",".join("?" for _ in order_ids)
        self.connection.execute(
            f"UPDATE orders SET status=?, updated_ts=? WHERE order_id IN ({placeholders})",
            (status, _now(), *order_ids),
        )
        self.connection.commit()

    def daily_filled_cost(self) -> Decimal:
        day_start = int(
            datetime.now(timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
        )
        rows = self.connection.execute(
            """
            SELECT price, matched_size FROM orders
            WHERE created_ts>=? AND role='entry' AND side='buy'
            """,
            (day_start,),
        )
        return sum(
            (Decimal(row["price"]) * Decimal(row["matched_size"]) for row in rows),
            Decimal("0"),
        )

    def entry_orders_with_fills(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT o.*, m.condition_id, m.end_ts, m.min_size, m.tick_size
                FROM orders o JOIN markets m ON m.slug=o.slug
                WHERE o.role='entry' AND CAST(o.matched_size AS REAL)>0
                ORDER BY o.created_ts
                """
            )
        )

    def exit_handled_size(
        self, slug: str, outcome: str, price: Decimal
    ) -> Decimal:
        total = Decimal("0")
        active_states = {
            "open",
            "live",
            "delayed",
            "unmatched",
            "cancel_requested",
        }
        rows = self.connection.execute(
            """
            SELECT size, matched_size, status FROM orders
            WHERE slug=? AND outcome=? AND role='exit'
              AND CAST(price AS NUMERIC)=CAST(? AS NUMERIC)
            """,
            (slug, outcome, str(price)),
        )
        for row in rows:
            size = Decimal(row["size"])
            matched = Decimal(row["matched_size"])
            total += matched
            if row["status"] in active_states:
                total += max(Decimal("0"), size - matched)
            elif row["status"] == "failed":
                total += size
        return total

    def active_exit_reserved_size(self, slug: str, outcome: str) -> Decimal:
        rows = self.connection.execute(
            """
            SELECT size, matched_size FROM orders
            WHERE slug=? AND outcome=? AND role='exit'
              AND status IN (
                  'open', 'live', 'delayed', 'unmatched', 'cancel_requested'
              )
            """,
            (slug, outcome),
        )
        return sum(
            (
                max(
                    Decimal("0"),
                    Decimal(row["size"]) - Decimal(row["matched_size"]),
                )
                for row in rows
            ),
            Decimal("0"),
        )

    def status(self) -> dict:
        latest = self.connection.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        latest_run = dict(latest) if latest else None
        if latest_run is not None:
            latest_run["plan"] = json.loads(latest_run.pop("plan_json"))
        counts = {
            row["status"]: row["count"]
            for row in self.connection.execute(
                "SELECT status, COUNT(*) AS count FROM orders GROUP BY status"
            )
        }
        counts_by_role = {
            role: {
                row["status"]: row["count"]
                for row in self.connection.execute(
                    """
                    SELECT status, COUNT(*) AS count FROM orders
                    WHERE role=? GROUP BY status
                    """,
                    (role,),
                )
            }
            for role in ("entry", "exit")
        }
        return {
            "latest_run": latest_run,
            "markets": self.connection.execute(
                "SELECT COUNT(*) FROM markets"
            ).fetchone()[0],
            "orders": counts,
            "orders_by_role": counts_by_role,
            "active_reserved_usd": str(self.active_reserved_usd()),
            "daily_filled_cost": str(self.daily_filled_cost()),
            "last_events": [
                dict(row)
                for row in self.connection.execute(
                    "SELECT ts, level, event_type, slug, details_json FROM events ORDER BY id DESC LIMIT 10"
                )
            ],
        }
