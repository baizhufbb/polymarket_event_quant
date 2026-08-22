from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from .config import BotConfig, SetupConfig
from .database import BotDatabase
from .exchange import DEFAULT_PLACEMENT_INTERVAL_MS, Exchange
from .fleet import Fleet, evenly_phased
from .lock import SingleInstance
from .models import ExitTarget, TradePlan
from .paper import PaperDatabase, PaperSimulator, paper_database_path
from .service import BotService
from .setup import setup_wallet


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
        "--placement-interval-ms",
        type=_placement_interval_ms_arg,
        default=DEFAULT_PLACEMENT_INTERVAL_MS,
        metavar="MILLISECONDS",
        help=(
            "delay between submission ticks; single mode sends one pending "
            "leg per tick, batch mode sends the signed pair; "
            f"default {DEFAULT_PLACEMENT_INTERVAL_MS} ms"
        ),
    )
    run.add_argument(
        "--placement-order",
        choices=("nearest-first", "farthest-first"),
        default="nearest-first",
    )
    run.add_argument(
        "--entry-submission",
        choices=("batch", "single", "solo-up", "solo-down"),
        default="batch",
        help=(
            "batch submits the pair in one request, single rotates both legs "
            "through single-order requests, solo-up/solo-down trade only that "
            "leg; default batch"
        ),
    )
    run.add_argument(
        "--fleet-env",
        action="append",
        default=[],
        metavar="ENV_FILE",
        help=(
            "extra account env file (repeatable); every account runs the "
            "same cadence with its phase offset by interval/N and a distinct "
            "order size, and only the earliest registered order per market "
            "is kept"
        ),
    )
    run.add_argument(
        "--fleet-size-step",
        type=_decimal_arg,
        default=Decimal("0.024"),
        metavar="USD",
        help=(
            "per-member increase of --usd-per-side so each member's order "
            "carries a distinct fingerprint size; default 0.024"
        ),
    )
    return parser


class _ExpectedOrderEngineFilter(logging.Filter):
    """Keep the human log readable during the pre-open hammering.

    These replies are expected thousands of times per market. They stay in
    logs/attempts.jsonl, which records every attempt regardless.
    """

    EXPECTED = ("invalid token id", "market not found", "trading is disabled")

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "py_clob_client_v2.http_helpers.helpers":
            return True
        message = record.getMessage().lower()
        return not any(expected in message for expected in self.EXPECTED)


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


# A submission cadence of 25 ms is finer than the interpreter's default
# 5 ms hand-off between threads, so a loop whose slot has come can be left
# waiting behind whatever else is running - on a single core, all the
# members wait together and their offsets vanish. Measured on the server
# against a fake venue, two members 12.5 ms apart under load: 14.3 ms apart
# with the default and 121 of 200 slots missed, 12.49 ms apart and no slot
# missed at 0.2 ms, at the same processor cost.
THREAD_SWITCH_SECONDS = 0.0002


def main() -> None:
    sys.setswitchinterval(THREAD_SWITCH_SECONDS)
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
        if (
            args.max_reserved_usd is not None
            and args.max_reserved_usd < plan.market_reserve
        ):
            raise SystemExit(
                "--max-reserved-usd must cover both sides of at least one market"
            )
        if args.fleet_env and args.take_profit:
            raise SystemExit("--fleet-env supports buy-only plans for now")
        fleet = None
        if live and args.fleet_env:
            members = [("primary", Exchange(config), plan.order_size)]
            for index, env_path in enumerate(args.fleet_env, start=1):
                member_config = BotConfig.from_env_file(
                    Path(env_path), project_root=config.project_root
                )
                member_plan = replace(
                    plan,
                    usd_per_side=plan.usd_per_side + args.fleet_size_step * index,
                )
                member_plan.validate()
                members.append(
                    (f"m{index}", Exchange(member_config), member_plan.order_size)
                )
            fleet = Fleet(evenly_phased(members, args.placement_interval_ms))
        trace_path = config.project_root / "logs" / "attempts.jsonl"
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
                entry_submission=args.entry_submission,
                fleet=fleet,
                cancel_before_end_seconds=args.cancel_before_end_seconds,
                live=args.live,
                logger=_logger(config),
            )
            if service.exchange:
                trace_path.parent.mkdir(parents=True, exist_ok=True)
                trace_file = trace_path.open("a", encoding="utf-8", buffering=1)

                def _tracer(account: str):
                    return lambda row: trace_file.write(
                        json.dumps({"account": account, **row}, separators=(",", ":"))
                        + "\n"
                    )

                if service.fleet:
                    for member in service.fleet.members:
                        member.exchange.attempt_trace = _tracer(member.name)
                else:
                    service.exchange.attempt_trace = _tracer("primary")
            service.run()
