from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


LIVE_ACK = "I_UNDERSTAND_REAL_ORDERS"


def _api_credentials() -> tuple[str | None, str | None, str | None]:
    values = (
        os.getenv("POLYMARKET_CLOB_API_KEY", "").strip(),
        os.getenv("POLYMARKET_CLOB_API_SECRET", "").strip(),
        os.getenv("POLYMARKET_CLOB_API_PASSPHRASE", "").strip(),
    )
    if any(values) and not all(values):
        raise ValueError("CLOB API credentials must be all present or all absent")
    return tuple(value or None for value in values)


@dataclass(frozen=True)
class BotConfig:
    project_root: Path
    private_key: str
    funder_address: str
    signature_type: int
    api_key: str | None
    api_secret: str | None
    api_passphrase: str | None
    discovery_seconds: float = 5.0
    order_poll_seconds: float = 60.0
    geoblock_seconds: float = 300.0
    geoblock_retry_seconds: float = 5.0

    @property
    def database_path(self) -> Path:
        return self.project_root / "data" / "bot.sqlite"

    @property
    def log_path(self) -> Path:
        return self.project_root / "logs" / "bot.log"

    @classmethod
    def load(cls, *, live: bool, authenticated: bool = False) -> "BotConfig":
        project_root = Path(__file__).resolve().parents[1]
        load_dotenv(project_root / ".env.trading", override=False)
        api_key, api_secret, api_passphrase = _api_credentials()

        config = cls(
            project_root=project_root,
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", "").strip(),
            funder_address=os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip(),
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "3")),
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
        config.validate(live=live, authenticated=authenticated)
        return config

    @classmethod
    def from_env_file(cls, path: Path, *, project_root: Path) -> "BotConfig":
        """Load one extra fleet account from a KEY=VALUE file."""
        values: dict[str, str] = {}
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
        credentials = tuple(
            values.get(name) or None
            for name in (
                "POLYMARKET_CLOB_API_KEY",
                "POLYMARKET_CLOB_API_SECRET",
                "POLYMARKET_CLOB_API_PASSPHRASE",
            )
        )
        if any(credentials) and not all(credentials):
            raise ValueError(f"{path}: CLOB API credentials must be all present or all absent")
        config = cls(
            project_root=project_root,
            private_key=values.get("POLYMARKET_PRIVATE_KEY", ""),
            funder_address=values.get("POLYMARKET_FUNDER_ADDRESS", ""),
            signature_type=int(values.get("POLYMARKET_SIGNATURE_TYPE", "0")),
            api_key=credentials[0],
            api_secret=credentials[1],
            api_passphrase=credentials[2],
        )
        if not config.private_key or not config.funder_address:
            raise ValueError(f"{path}: private key and funder address are required")
        return config

    def validate(self, *, live: bool, authenticated: bool = False) -> None:
        if live or authenticated:
            if not self.private_key or not self.funder_address:
                raise ValueError("private key and funder address are required")
        if live:
            if os.getenv("POLYMARKET_LIVE_ACK", "") != LIVE_ACK:
                raise ValueError(
                    f"POLYMARKET_LIVE_ACK must equal {LIVE_ACK} for live mode"
                )
            if not self.api_key or not self.api_secret or not self.api_passphrase:
                raise ValueError("CLOB API credentials are required for live mode")


@dataclass(frozen=True)
class SetupConfig:
    project_root: Path
    private_key: str
    existing_funder_address: str | None
    api_key: str | None
    api_secret: str | None
    api_passphrase: str | None
    relayer_api_key: str | None
    relayer_api_key_address: str | None

    @property
    def env_path(self) -> Path:
        return self.project_root / ".env.trading"

    @classmethod
    def load(cls, *, apply: bool) -> "SetupConfig":
        project_root = Path(__file__).resolve().parents[1]
        load_dotenv(project_root / ".env.trading", override=False)
        api_key, api_secret, api_passphrase = _api_credentials()
        config = cls(
            project_root=project_root,
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", "").strip(),
            existing_funder_address=(
                os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip() or None
            ),
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            relayer_api_key=(
                os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip() or None
            ),
            relayer_api_key_address=(
                os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip() or None
            ),
        )
        config.validate(apply=apply)
        return config

    def validate(self, *, apply: bool) -> None:
        if not self.private_key:
            raise ValueError("POLYMARKET_PRIVATE_KEY is required for setup")
        relayer_values = (self.relayer_api_key, self.relayer_api_key_address)
        if any(relayer_values) and not all(relayer_values):
            raise ValueError(
                "POLYMARKET_RELAYER_API_KEY and "
                "POLYMARKET_RELAYER_API_KEY_ADDRESS must be set together"
            )
        if apply and not all(relayer_values):
            raise ValueError(
                "Relayer API key and address are required for setup --apply"
            )
