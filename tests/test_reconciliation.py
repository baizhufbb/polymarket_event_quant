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


def test_fetch_stops_retiring_when_stop_lands_during_the_reread(monkeypatch) -> None:
    import polymarket_bot.reconciliation as module

    monkeypatch.setattr(module, "MISSING_ORDER_RECHECK_SECONDS", 0.0)
    worker = None

    class AbsentExchange:
        def __init__(self):
            self.calls = 0

        def open_orders(self):
            return []

        def get_order(self, order_id):
            self.calls += 1
            if self.calls == 4:  # first re-read of the second pass
                worker.stop()
            return None

    exchange = AbsentExchange()
    worker = module.ReconciliationWorker(exchange)
    snapshots = tuple(
        module.TrackedOrderSnapshot(f"order-{i}", "slug", "100") for i in range(3)
    )

    update = worker._fetch(snapshots)

    # the order whose re-read completed is retired; the rest are left alone
    assert exchange.calls == 4
    assert [o.snapshot.order_id for o in update.orders] == ["order-0"]
    assert update.orders[0].raw["status"] == "terminal_unknown"

def test_fetch_returns_what_the_venue_holds_even_with_nothing_tracked() -> None:
    """The round where we track nothing is the round an orphan shows up in.

    A placement that raises writes no row, so the reconciler has nothing to
    look up - and the early return meant it never asked the venue either, so
    an order resting behind a lost reply stayed invisible for the whole run.
    """
    import polymarket_bot.reconciliation as module

    class VenueWithAnOrder:
        def __init__(self):
            self.asked = 0

        def open_orders(self):
            self.asked += 1
            return [{"id": "orphan", "market": "0xcondition", "asset_id": "up-token"}]

        def get_order(self, order_id):
            raise AssertionError("nothing is tracked, so nothing should be looked up")

    exchange = VenueWithAnOrder()
    worker = module.ReconciliationWorker(exchange)

    update = worker._fetch(())

    assert exchange.asked == 1
    assert update.orders == ()
    assert [row["id"] for row in update.open_rows] == ["orphan"]


def test_fetch_carries_the_venue_rows_alongside_the_tracked_ones() -> None:
    """Both lists come back, so the service can compare them."""
    import polymarket_bot.reconciliation as module

    class Venue:
        def open_orders(self):
            return [
                {"id": "known", "status": "LIVE", "size_matched": "0"},
                {"id": "orphan", "status": "LIVE", "size_matched": "0"},
            ]

        def get_order(self, order_id):
            raise AssertionError("both rows are in the open list")

    worker = module.ReconciliationWorker(Venue())
    update = worker._fetch((module.TrackedOrderSnapshot("known", "slug", "100"),))

    assert [o.snapshot.order_id for o in update.orders] == ["known"]
    assert sorted(row["id"] for row in update.open_rows) == ["known", "orphan"]
