from decimal import Decimal
from types import SimpleNamespace

from polymarket_bot.redemption import RedemptionWorker


class FakePaginator:
    def __init__(self, positions):
        self.positions = positions

    def iter_items(self):
        return iter(self.positions)


class FakeHandle:
    transaction_id = "transaction-id"

    def __init__(self, *, fail=False):
        self.fail = fail

    def wait(self):
        if self.fail:
            raise RuntimeError("relay failed")
        return SimpleNamespace(
            transaction_hash="0xtransaction",
            transaction_id=self.transaction_id,
        )


class FakeClient:
    def __init__(self, positions, *, fail=False):
        self.positions = positions
        self.fail = fail
        self.redeem_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def list_positions(self, **_kwargs):
        return FakePaginator(self.positions)

    def redeem_positions(self, *, condition_id, metadata):
        self.redeem_calls.append((condition_id, metadata))
        return FakeHandle(fail=self.fail)


def _config():
    return SimpleNamespace(
        private_key="private-key",
        api_key="api-key",
        api_secret="api-secret",
        api_passphrase="api-passphrase",
        relayer_api_key="relayer-key",
        relayer_api_key_address="0xaddress",
    )


def test_redeems_positive_positions_once_per_condition() -> None:
    positions = [
        SimpleNamespace(condition_id="0xwinner", current_value=Decimal("40")),
        SimpleNamespace(condition_id="0xwinner", current_value=Decimal("10")),
        SimpleNamespace(condition_id="0xloser", current_value=Decimal("0")),
    ]
    client = FakeClient(positions)
    worker = RedemptionWorker(_config(), 1800, client_factory=lambda: client)

    first = worker.run_once()
    second = worker.run_once()

    assert first.scanned_positions == 3
    assert first.eligible_conditions == 1
    assert first.redeemed[0].condition_id == "0xwinner"
    assert first.redeemed[0].payout == Decimal("50")
    assert first.errors == ()
    assert second.eligible_conditions == 0
    assert len(client.redeem_calls) == 1


def test_failed_redemption_is_not_blindly_retried() -> None:
    positions = [SimpleNamespace(condition_id="0xwinner", current_value=Decimal("50"))]
    client = FakeClient(positions, fail=True)
    worker = RedemptionWorker(_config(), 1800, client_factory=lambda: client)

    first = worker.run_once()
    second = worker.run_once()

    assert first.redeemed == ()
    assert first.errors[0].condition_id == "0xwinner"
    assert first.errors[0].transaction_id == "transaction-id"
    assert second.eligible_conditions == 0
    assert len(client.redeem_calls) == 1
