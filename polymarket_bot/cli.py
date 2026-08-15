from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal, InvalidOperation
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from .config import BotConfig, SetupConfig
from .database import BotDatabase
from .exchange import DEFAULT_PLACEMENT_INTERVAL_MS, Exchange
from .lock import SingleInstance
from .models import ExitTarget, TradePlan
from .paper import PaperDatabase, PaperSimulator, paper_database_path
from .service import BotService
from .setup import setup_wallet


class _ExpectedOrderEngineFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "py_clob_client_v2.http_helpers.helpers":
            return True
        message = record.getMessage().lower()
        return "invalid token id" not in message and "market not found" not in message


def _decimal_arg(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}") from exc
    if not parsed.is_finite():
        raise argparse.ArgumentTypeError(f"invalid decimal value: {value}")
    return parsed


def _take_profit_arg(value: str) -> ExitTarget:
    try:
        price_text, fraction_text = value.split(":", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "take-profit must use PRICE:FRACTION, for example 0.02:0.50"
        ) from exc
    return ExitTarget(
        price=_decimal_arg(price_text),
        fraction=_decimal_arg(fraction_text),
    )


def _placement_interval_ms_arg(value: str) -> Decimal:
    parsed = _decimal_arg(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("placement interval must be above 0 ms")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket BTC 5-minute trading bot")
    subparsers = parser.add_subparsers(dest="command", required=True)
    setup = subparsers.add_parser(
        "setup", help="inspect or initialize the dedicated Polymarket wallet"
    )
    setup.add_argument(
        "--apply",
        action="store_true",
        help="deploy the Deposit Wallet, set approvals, and save derived credentials",
    )
    subparsers.add_parser(
        "doctor", help="check account and network without placing orders"
    )
    subparsers.add_parser("status", help="show local bot state")
    subparsers.add_parser("paper-status", help="show paper simulation results")
    paper = subparsers.add_parser(
        "paper", help="simulate buy-and-hold fills without credentials or orders"
    )
    paper.add_argument("--buy-price", type=_decimal_arg, required=True)
    paper.add_argument("--usd-per-side", type=_decimal_arg, required=True)
    paper.add_argument("--hours", type=_decimal_arg)
    paper.add_argument(
        "--lookahead-minutes",
        type=int,
        default=0,
        help=(
            "startup observation window; 0 skips existing markets and starts "
            "with the next newly announced market"
        ),
    )
    run = subparsers.add_parser("run", help="run continuously; dry-run unless --live")
    run.add_argument("--live", action="store_true")
    run.add_argument("--buy-price", type=_decimal_arg, required=True)
    run.add_argument(
        "--take-profit",
        type=_take_profit_arg,
        action="append",
        default=[],
        metavar="PRICE:FRACTION",
        help="repeatable exit rung; omitted fractions remain held to resolution",
    )
    run.add_argument("--usd-per-side", type=_decimal_arg, required=True)
    run.add_argument("--hours", type=_decimal_arg)
    run.add_argument("--max-reserved-usd", type=_decimal_arg)
    run.add_argument("--max-daily-filled-cost", type=_decimal_arg)
    run.add_argument(
        "--lookahead-minutes",
        type=int,
        default=40,
        help=(
            "startup placement window; with farthest-first, 0 skips all "
            "existing markets and starts with the next newly announced market"
        ),
    )
    run.add_argument("--cancel-before-end-seconds", type=int, default=2)
    run.add_argument(
        "--heartbeat-seconds",
        type=_decimal_arg,
        metavar="SECONDS",
        help=(
            "enable Polymarket's disconnect-cancels-orders heartbeat and send "
            "every SECONDS; omitted disables it"
        ),
    )
    run.add_argument(
        "--placement-interval-ms",
        type=_placement_interval_ms_arg,
        default=DEFAULT_PLACEMENT_INTERVAL_MS,
        metavar="MILLISECONDS",
        help=(
            "delay between identical signed Up/Down batch submissions in the "
            "burst that follows the book-open signal; "
            f"default {DEFAULT_PLACEMENT_INTERVAL_MS} ms"
        ),
    )
    run.add_argument(
        "--placement-order",
        choices=("nearest-first", "farthest-first"),
        default="nearest-first",
    )
    return parser


def _logger(config: BotConfig) -> logging.Logger:
    logger = _rotating_logger("polymarket_bot", config.log_path)
    clob_logger = logging.getLogger("py_clob_client_v2.http_helpers.helpers")
    clob_logger.addFilter(_ExpectedOrderEngineFilter())
    return logger


def _rotating_logger(name: str, path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = TimedRotatingFileHandler(
        path, when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


def main() -> None:
    args = _parser().parse_args()
    if args.command == "setup":
        result = setup_wallet(SetupConfig.load(apply=args.apply), apply=args.apply)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    project_root = Path(__file__).resolve().parents[1]
    if args.command == "paper-status":
        with PaperDatabase(paper_database_path(project_root)) as database:
            print(json.dumps(database.status(), ensure_ascii=False, indent=2))
        return

    if args.command == "paper":
        plan = TradePlan(
            buy_price=args.buy_price,
            exit_targets=(),
            usd_per_side=args.usd_per_side,
        )
        plan.validate()
        if args.hours is not None and args.hours <= 0:
            raise SystemExit("--hours must be positive when provided")
        if args.lookahead_minutes < 0:
            raise SystemExit("--lookahead-minutes cannot be negative")
        with SingleInstance(port=47832):
            with PaperDatabase(paper_database_path(project_root)) as database:
                PaperSimulator(
                    database,
                    plan,
                    lookahead_minutes=args.lookahead_minutes,
                    hours=args.hours,
                    logger=_rotating_logger(
                        "polymarket_paper", project_root / "logs" / "paper.log"
                    ),
                ).run()
        return

    live = bool(getattr(args, "live", False))
    config = BotConfig.load(live=live, authenticated=args.command == "doctor")

    if args.command == "doctor":
        result = Exchange(config).doctor(config.signature_type)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    with BotDatabase(config.database_path) as database:
        if args.command == "status":
            print(json.dumps(database.status(), ensure_ascii=False, indent=2))
            return
        plan = TradePlan(
            buy_price=args.buy_price,
            exit_targets=tuple(args.take_profit),
            usd_per_side=args.usd_per_side,
        )
        plan.validate()
        optional_positive = {
            "--hours": args.hours,
            "--max-reserved-usd": args.max_reserved_usd,
            "--max-daily-filled-cost": args.max_daily_filled_cost,
        }
        for name, value in optional_positive.items():
            if value is not None and value <= 0:
                raise SystemExit(f"{name} must be positive when provided")
        if args.lookahead_minutes < 0:
            raise SystemExit("--lookahead-minutes cannot be negative")
        if (
            args.lookahead_minutes == 0
            and args.placement_order != "farthest-first"
        ):
            raise SystemExit(
                "--lookahead-minutes 0 requires --placement-order farthest-first"
            )
        if args.cancel_before_end_seconds < 0:
            raise SystemExit("--cancel-before-end-seconds cannot be negative")
        if args.heartbeat_seconds is not None and not (
            Decimal("0") < args.heartbeat_seconds < Decimal("10")
        ):
            raise SystemExit("--heartbeat-seconds must be above 0 and below 10")
        if (
            args.max_reserved_usd is not None
            and args.max_reserved_usd < plan.market_reserve
        ):
            raise SystemExit(
                "--max-reserved-usd must cover both sides of at least one market"
            )
        with SingleInstance():
            service = BotService(
                config,
                database,
                plan,
                hours=args.hours,
                max_reserved_usd=args.max_reserved_usd,
                max_daily_filled_cost=args.max_daily_filled_cost,
                lookahead_minutes=args.lookahead_minutes,
                placement_order=args.placement_order,
                placement_interval_ms=args.placement_interval_ms,
                cancel_before_end_seconds=args.cancel_before_end_seconds,
                heartbeat_seconds=args.heartbeat_seconds,
                live=args.live,
                logger=_logger(config),
            )
            service.run()
