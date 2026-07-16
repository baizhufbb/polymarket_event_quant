from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from queue import Empty, SimpleQueue
from threading import Event, Thread
from typing import Any

from polymarket import ApiKeyCreds, RelayerApiKey, SecureClient

from .config import BotConfig


@dataclass(frozen=True)
class RedemptionResult:
    condition_id: str
    payout: Decimal
    transaction_hash: str
    transaction_id: str | None


@dataclass(frozen=True)
class RedemptionError:
    condition_id: str
    error: str
    transaction_id: str | None = None


@dataclass(frozen=True)
class RedemptionUpdate:
    scanned_positions: int
    eligible_conditions: int
    redeemed: tuple[RedemptionResult, ...] = ()
    errors: tuple[RedemptionError, ...] = ()
    scan_error: str | None = None


class RedemptionWorker:
    def __init__(
        self,
        config: BotConfig,
        interval_seconds: float,
        *,
        client_factory: Callable[[], Any] | None = None,
    ):
        self.config = config
        self.interval_seconds = interval_seconds
        self._client_factory = client_factory or self._create_client
        self._stop = Event()
        self._updates: SimpleQueue[RedemptionUpdate] = SimpleQueue()
        self._completed_conditions: set[str] = set()
        self._blocked_conditions: set[str] = set()
        self._thread: Thread | None = None

    def _create_client(self) -> SecureClient:
        credentials = ApiKeyCreds(
            apiKey=str(self.config.api_key),
            secret=str(self.config.api_secret),
            passphrase=str(self.config.api_passphrase),
        )
        relayer_key = RelayerApiKey(
            key=str(self.config.relayer_api_key),
            address=str(self.config.relayer_api_key_address),
        )
        return SecureClient.create(
            private_key=self.config.private_key,
            credentials=credentials,
            api_key=relayer_key,
        )

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("redemption worker already started")
        self._thread = Thread(
            target=self._run,
            name="polymarket-redemption",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def drain(self) -> list[RedemptionUpdate]:
        updates = []
        while True:
            try:
                updates.append(self._updates.get_nowait())
            except Empty:
                return updates

    def run_once(self) -> RedemptionUpdate:
        try:
            with self._client_factory() as client:
                positions = list(
                    client.list_positions(
                        redeemable=True,
                        size_threshold=0,
                        page_size=500,
                    ).iter_items()
                )
                values: dict[str, Decimal] = {}
                for position in positions:
                    current_value = position.current_value or Decimal("0")
                    condition_id = str(position.condition_id)
                    if current_value <= 0:
                        continue
                    if condition_id in self._completed_conditions:
                        continue
                    if condition_id in self._blocked_conditions:
                        continue
                    values[condition_id] = (
                        values.get(condition_id, Decimal("0")) + current_value
                    )

                redeemed = []
                errors = []
                for condition_id, payout in values.items():
                    transaction_id = None
                    try:
                        handle = client.redeem_positions(
                            condition_id=condition_id,
                            metadata=f"BTC bot redeem {condition_id}",
                        )
                        transaction_id = getattr(handle, "transaction_id", None)
                        outcome = handle.wait()
                    except Exception as exc:
                        self._blocked_conditions.add(condition_id)
                        errors.append(
                            RedemptionError(
                                condition_id=condition_id,
                                transaction_id=transaction_id,
                                error=f"{type(exc).__name__}: {exc}",
                            )
                        )
                        continue

                    self._completed_conditions.add(condition_id)
                    redeemed.append(
                        RedemptionResult(
                            condition_id=condition_id,
                            payout=payout,
                            transaction_hash=str(outcome.transaction_hash),
                            transaction_id=(
                                str(outcome.transaction_id)
                                if outcome.transaction_id is not None
                                else transaction_id
                            ),
                        )
                    )
        except Exception as exc:
            update = RedemptionUpdate(
                scanned_positions=0,
                eligible_conditions=0,
                scan_error=f"{type(exc).__name__}: {exc}",
            )
        else:
            update = RedemptionUpdate(
                scanned_positions=len(positions),
                eligible_conditions=len(values),
                redeemed=tuple(redeemed),
                errors=tuple(errors),
            )
        self._updates.put(update)
        return update

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            self.run_once()
            remaining = max(0.0, self.interval_seconds - (time.monotonic() - started))
            if self._stop.wait(remaining):
                return
