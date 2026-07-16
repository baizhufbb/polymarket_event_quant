from polymarket_bot.heartbeat import HeartbeatWorker


class FakeExchange:
    def __init__(self):
        self.fail = True

    def heartbeat(self):
        if self.fail:
            raise ConnectionError("offline")
        return {"status": "ok"}


def test_worker_reports_failure_and_recovery() -> None:
    exchange = FakeExchange()
    worker = HeartbeatWorker(exchange, interval_seconds=5)

    failed = worker.run_once()
    assert not failed.success
    assert failed.changed
    assert not worker.healthy

    exchange.fail = False
    recovered = worker.run_once()
    assert recovered.success
    assert recovered.recovered
    assert worker.healthy
