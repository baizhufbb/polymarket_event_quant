from __future__ import annotations

import json
from pathlib import Path

import pytest
from dotenv import dotenv_values
from eth_account import Account
from polymarket import ApiKeyCreds

from polymarket_bot.config import SetupConfig
from polymarket_bot import setup as setup_mod


PRIVATE_KEY = "0x" + "1".zfill(64)
SIGNER = Account.from_key(PRIVATE_KEY).address
RELAYER_OWNER = SIGNER
FUNDER = "0x" + "2" * 40
BRIDGE = "0x" + "3" * 40


def _config(tmp_path: Path, *, relayer_address: str = RELAYER_OWNER) -> SetupConfig:
    return SetupConfig(
        project_root=tmp_path,
        private_key=PRIVATE_KEY,
        existing_funder_address=None,
        api_key=None,
        api_secret=None,
        api_passphrase=None,
        relayer_api_key="relayer-key",
        relayer_api_key_address=relayer_address,
    )


def test_setup_preview_only_derives_public_address(tmp_path: Path) -> None:
    result = setup_mod.setup_wallet(_config(tmp_path), apply=False)

    assert result == {
        "mode": "preview",
        "chain_id": 137,
        "signer_address": SIGNER,
    }
    assert not (tmp_path / ".env.trading").exists()


def test_setup_apply_persists_runtime_values_without_printing_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_with: dict[str, object] = {}
    credentials = ApiKeyCreds(
        apiKey="clob-key",
        secret="clob-secret",
        passphrase="clob-passphrase",
    )

    class FakeBalance:
        def model_dump(self, **_: object) -> dict[str, str]:
            return {"balance": "0", "allowance": "1"}

    class FakeClient:
        wallet = FUNDER
        wallet_type = "DEPOSIT_WALLET"

        def __init__(self) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def setup_trading_approvals(self) -> None:
            return None

        def get_balance_allowance(self, **_: object) -> FakeBalance:
            return FakeBalance()

    class FakeSecureClient:
        @staticmethod
        def create(**kwargs: object) -> FakeClient:
            created_with.update(kwargs)
            return FakeClient()

    monkeypatch.setattr(setup_mod, "SecureClient", FakeSecureClient)
    monkeypatch.setattr(
        setup_mod,
        "_get_json",
        lambda url: (
            {"blocked": True, "country": "IE", "region": "L"}
            if "geoblock" in url
            else {}
        ),
    )
    monkeypatch.setattr(
        setup_mod,
        "_polygon_usdc",
        lambda: {
            "chainId": "137",
            "chainName": "Polygon",
            "token": {"symbol": "USDC", "address": setup_mod.POLYGON_USDC},
            "minCheckoutUsd": 2,
        },
    )
    monkeypatch.setattr(setup_mod, "_bridge_deposit_address", lambda _: BRIDGE)

    result = setup_mod.setup_wallet(_config(tmp_path), apply=True)
    values = dotenv_values(tmp_path / ".env.trading")

    assert result["funder_address"] == FUNDER
    assert result["bridge_evm_address"] == BRIDGE
    assert created_with["private_key"] == PRIVATE_KEY
    assert str(created_with["api_key"].address).lower() == RELAYER_OWNER.lower()
    rendered = json.dumps(result)
    assert PRIVATE_KEY not in rendered
    assert "clob-secret" not in rendered
    assert values["POLYMARKET_FUNDER_ADDRESS"] == FUNDER
    assert values["POLYMARKET_SIGNATURE_TYPE"] == "3"
    assert values["POLYMARKET_CLOB_API_KEY"] == "clob-key"
    assert values["POLYMARKET_CLOB_API_SECRET"] == "clob-secret"
    assert values["POLYMARKET_CLOB_API_PASSPHRASE"] == "clob-passphrase"


def test_setup_apply_rejects_api_restricted_country(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        setup_mod,
        "_get_json",
        lambda _: {"blocked": True, "country": "US", "region": "NY"},
    )

    with pytest.raises(RuntimeError, match="network location is blocked"):
        setup_mod.setup_wallet(_config(tmp_path), apply=True)


def test_setup_apply_rejects_relayer_key_for_another_address(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    network_called = False

    def unexpected_network_call(_: str) -> dict[str, object]:
        nonlocal network_called
        network_called = True
        return {}

    monkeypatch.setattr(setup_mod, "_get_json", unexpected_network_call)

    with pytest.raises(ValueError, match="must match the signer address"):
        setup_mod.setup_wallet(
            _config(tmp_path, relayer_address="0x" + "4" * 40),
            apply=True,
        )

    assert network_called is False
    assert not (tmp_path / ".env.trading").exists()
