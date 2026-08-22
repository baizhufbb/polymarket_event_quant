from polymarket_bot.reconciliation import (
    ReconciliationWorker,
    TrackedOrderSnapshot,
)


class FakeExchange:
    def __init__(self):
        self.get_calls = []

    def open_orders(self):
        return [{"id": "open-order", "status": "live", "size_matched": "0"}]

    def get_order(self, order_id):
        self.get_calls.append(order_id)
        return {"id": order_id, "status": "cancelled", "size_matched": "0"}


def test_fetches_open_orders_once_and_queries_only_missing_orders() -> None:
    exchange = FakeExchange()
    worker = ReconciliationWorker(exchange)
    update = worker._fetch(
        (
            TrackedOrderSnapshot("open-order", "market-a", "100"),
            TrackedOrderSnapshot("closed-order", "market-b", "100"),
        )
    )

    assert update.batch_error is None
    assert [item.snapshot.order_id for item in update.orders] == [
        "open-order",
        "closed-order",
    ]
    assert exchange.get_calls == ["closed-order"]
    assert update.orders[0].raw["status"] == "live"
    assert update.orders[1].raw["status"] == "cancelled"


def test_missing_order_becomes_terminal_unknown(monkeypatch) -> None:
    import polymarket_bot.reconciliation as module

    monkeypatch.setattr(module, "MISSING_ORDER_RECHECK_SECONDS", 0.0)
    exchange = FakeExchange()
    calls = []
    exchange.get_order = lambda order_id: calls.append(order_id)
    worker = ReconciliationWorker(exchange)

    update = worker._fetch(
        (TrackedOrderSnapshot("missing-order", "market-a", "100"),)
    )

    assert update.batch_error is None
    assert update.orders[0].error is None
    assert update.orders[0].raw == {
        "id": "missing-order",
        "status": "terminal_unknown",
        "size_matched": "0",
    }
    # retired only after the re-read
    assert calls == ["missing-order", "missing-order"]


def test_fetch_rereads_a_missing_order_before_retiring_it(monkeypatch) -> None:
    import polymarket_bot.reconciliation as module

    monkeypatch.setattr(module, "MISSING_ORDER_RECHECK_SECONDS", 0.0)

    class LaggingExchange:
        def __init__(self):
            self.calls = 0

        def open_orders(self):
            return []

        def get_order(self, order_id):
            self.calls += 1
            if self.calls == 1:
                return None
            return {"id": order_id, "status": "live", "size_matched": "0"}

    exchange = LaggingExchange()
    worker = module.ReconciliationWorker(exchange)
    update = worker._fetch((module.TrackedOrderSnapshot("late-order", "slug", "100"),))

    assert exchange.calls == 2
    assert update.orders[0].raw["status"] == "live"


def test_fetch_pauses_once_per_round_and_skips_the_reread_when_stopping(monkeypatch) -> None:
    import polymarket_bot.reconciliation as module

    monkeypatch.setattr(module, "MISSING_ORDER_RECHECK_SECONDS", 0.0)

    class AbsentExchange:
        def __init__(self):
            self.calls = 0

        def open_orders(self):
            return []

        def get_order(self, order_id):
            self.calls += 1
            return None

    exchange = AbsentExchange()
    worker = module.ReconciliationWorker(exchange)
    snapshots = tuple(
        module.TrackedOrderSnapshot(f"order-{i}", "slug", "100") for i in range(3)
    )
    waits = []
    monkeypatch.setattr(
        worker._stop, "wait", lambda seconds: waits.append(seconds) or worker._stop.is_set()
    )

    update = worker._fetch(snapshots)

    assert waits == [0.0]
    assert exchange.calls == 6
    assert [o.raw["status"] for o in update.orders] == ["terminal_unknown"] * 3

    worker._stop.set()
    exchange.calls = 0
    update = worker._fetch(snapshots)

    assert exchange.calls == 3
    assert update.orders == ()
