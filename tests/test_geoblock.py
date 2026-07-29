import pytest

from polymarket_bot.geoblock import GeoblockWorker, is_api_trading_blocked


class FakeExchange:
    def __init__(self, responses):
        self.responses = iter(responses)

    def geoblock(self):
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def test_network_failure_pauses_and_success_recovers() -> None:
    worker = GeoblockWorker(
        FakeExchange([OSError("network down"), {"blocked": False}]),
        interval_seconds=300,
        retry_seconds=5,
    )

    failed = worker.run_once()
    assert not failed.available
    assert failed.changed
    assert not worker.healthy

    recovered = worker.run_once()
    assert recovered.available
    assert recovered.recovered
    assert worker.healthy


def test_explicit_block_never_becomes_healthy() -> None:
    result = {"blocked": True, "country": "US", "region": "NY"}
    worker = GeoblockWorker(
        FakeExchange([result]), interval_seconds=300, retry_seconds=5
    )

    update = worker.run_once()

    assert update.available
    assert update.blocked
    assert not worker.healthy


@pytest.mark.parametrize("country", ["IE", "JP", "MT", "NL"])
def test_frontend_only_country_keeps_api_trading_healthy(country: str) -> None:
    result = {"blocked": True, "country": country, "region": ""}
    worker = GeoblockWorker(
        FakeExchange([result]), interval_seconds=300, retry_seconds=5
    )

    update = worker.run_once()

    assert update.available
    assert not update.blocked
    assert worker.healthy
    assert update.result == result


def test_unknown_blocked_country_remains_api_blocked() -> None:
    assert is_api_trading_blocked({"blocked": True, "country": "XX"})
