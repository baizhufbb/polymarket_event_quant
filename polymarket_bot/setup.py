from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import httpx
from dotenv import set_key
from eth_account import Account
from polymarket import ApiKeyCreds, RelayerApiKey, SecureClient

from .config import SetupConfig


CHAIN_ID = 137
BRIDGE_URL = "https://bridge.polymarket.com"
GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
POLYGON_USDC = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"


@dataclass(frozen=True)
class SetupResult:
    mode: str
    chain_id: int
    signer_address: str
    funder_address: str | None = None
    wallet_type: str | None = None
    bridge_evm_address: str | None = None
    deposit_asset: dict[str, Any] | None = None
    collateral: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


def _normalized_private_key(value: str) -> str:
    key = value if value.startswith("0x") else f"0x{value}"
    if len(key) != 66:
        raise ValueError("POLYMARKET_PRIVATE_KEY must contain exactly 32 bytes")
    try:
        bytes.fromhex(key[2:])
    except ValueError as exc:
        raise ValueError("POLYMARKET_PRIVATE_KEY must be hexadecimal") from exc
    return key


def _signer_address(private_key: str) -> str:
    return Account.from_key(_normalized_private_key(private_key)).address


def _get_json(url: str) -> Any:
    response = httpx.get(url, timeout=20)
    response.raise_for_status()
    return response.json()


def _post_json(url: str, payload: dict[str, Any]) -> Any:
    response = httpx.post(url, json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def _polygon_usdc() -> dict[str, Any]:
    payload = _get_json(f"{BRIDGE_URL}/supported-assets")
    for asset in payload.get("supportedAssets", []):
        token = asset.get("token", {})
        if (
            str(asset.get("chainId")) == str(CHAIN_ID)
            and str(token.get("address", "")).lower() == POLYGON_USDC.lower()
        ):
            return asset
    raise RuntimeError("Polymarket Bridge does not currently list native Polygon USDC")


def _bridge_deposit_address(funder_address: str) -> str:
    payload = _post_json(
        f"{BRIDGE_URL}/deposit",
        {"address": funder_address},
    )
    address = payload.get("address", {}).get("evm")
    if not address:
        raise RuntimeError("Polymarket Bridge response did not contain an EVM address")
    return str(address)


def _existing_credentials(config: SetupConfig) -> ApiKeyCreds | None:
    if not config.api_key:
        return None
    return ApiKeyCreds(
        apiKey=config.api_key,
        secret=config.api_secret or "",
        passphrase=config.api_passphrase or "",
    )


def _model_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, dict):
        return value
    return {"value": str(value)}


def _write_runtime_config(
    config: SetupConfig,
    *,
    funder_address: str,
    credentials: ApiKeyCreds,
) -> None:
    config.env_path.touch(exist_ok=True)
    values = {
        "POLYMARKET_FUNDER_ADDRESS": funder_address,
        "POLYMARKET_SIGNATURE_TYPE": "3",
        "POLYMARKET_CLOB_API_KEY": credentials.key,
        "POLYMARKET_CLOB_API_SECRET": credentials.secret,
        "POLYMARKET_CLOB_API_PASSPHRASE": credentials.passphrase,
    }
    for key, value in values.items():
        set_key(config.env_path, key, value, quote_mode="never")


def setup_wallet(config: SetupConfig, *, apply: bool) -> dict[str, Any]:
    private_key = _normalized_private_key(config.private_key)
    signer_address = _signer_address(private_key)
    if not apply:
        return SetupResult(
            mode="preview",
            chain_id=CHAIN_ID,
            signer_address=signer_address,
        ).to_dict()

    relayer_address = str(config.relayer_api_key_address)
    if relayer_address.lower() != signer_address.lower():
        raise ValueError(
            "POLYMARKET_RELAYER_API_KEY_ADDRESS must match the signer address "
            "for Deposit Wallet setup"
        )
    geoblock = _get_json(GEOBLOCK_URL)
    if geoblock.get("blocked") is True:
        raise RuntimeError("Polymarket reports that this network location is blocked")
    deposit_asset = _polygon_usdc()

    relayer_key = RelayerApiKey(
        key=str(config.relayer_api_key),
        address=relayer_address,
    )
    with SecureClient.create(
        private_key=private_key,
        credentials=_existing_credentials(config),
        api_key=relayer_key,
    ) as client:
        funder_address = str(client.wallet)
        if str(client.wallet_type) != "DEPOSIT_WALLET":
            raise RuntimeError(f"Expected Deposit Wallet, got {client.wallet_type}")
        if (
            config.existing_funder_address
            and config.existing_funder_address.lower() != funder_address.lower()
        ):
            raise ValueError(
                "Existing funder address does not match the derived wallet"
            )

        client.setup_trading_approvals()
        collateral = client.get_balance_allowance(asset_type="COLLATERAL")
        bridge_address = _bridge_deposit_address(funder_address)
        _write_runtime_config(
            config,
            funder_address=funder_address,
            credentials=client.credentials,
        )

        return SetupResult(
            mode="applied",
            chain_id=CHAIN_ID,
            signer_address=signer_address,
            funder_address=funder_address,
            wallet_type=str(client.wallet_type),
            bridge_evm_address=bridge_address,
            deposit_asset=deposit_asset,
            collateral=_model_dict(collateral),
        ).to_dict()
