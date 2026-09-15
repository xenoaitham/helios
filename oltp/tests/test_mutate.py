"""Mutation loop tests: one tick really churns rows and advances the WAL,
inserted orders are internally consistent, and health state reports honestly.

Ticks use a tiny deterministic stub RNG instead of the `random` module so the
scenarios are reproducible; the stub is a test fixture, not crypto.
"""

from __future__ import annotations

from oltp import mutate as oltp_mutate
from tests.conftest import SMALL_ORDERS

_HEALTH_TEST_PORT = 18081  # literal: the health probe test uses a fixed local port


class StubRng:
    """Deterministic stand-in for random.Random (32-bit LCG; test fixture only)."""

    def __init__(self, seed: int = 1):
        self._state = (seed & 0x7FFFFFFF) or 1

    def _next(self) -> int:
        self._state = (1103515245 * self._state + 12345) & 0x7FFFFFFF
        return self._state

    def randint(self, a: int, b: int) -> int:
        return a + self._next() % (b - a + 1)

    def randrange(self, n: int) -> int:
        return self._next() % n

    def random(self) -> float:
        return self._next() / 0x7FFFFFFF


def test_tick_churns_rows_and_advances_wal(seeded):
    lsn_start = seeded.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    counters = oltp_mutate.run_tick(seeded, StubRng(123))
    lsn_end = seeded.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    delta = seeded.execute("SELECT pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)", (lsn_end, lsn_start)).fetchone()[0]
    assert counters["updated_users"] > 0
    assert counters["inserted_orders"] >= 1
    assert delta > 0, "a mutating tick must generate WAL"
    assert counters["inserted_items"] >= counters["inserted_orders"] * 3
    assert counters["inserted_payments"] == counters["inserted_orders"]


def test_tick_grows_orders_by_inserted_minus_deleted(seeded):
    before = seeded.execute("SELECT COUNT(*) FROM public.orders").fetchone()[0]
    counters = oltp_mutate.run_tick(seeded, StubRng(7))
    after = seeded.execute("SELECT COUNT(*) FROM public.orders").fetchone()[0]
    assert after == before + counters["inserted_orders"] - counters["deleted_orders"]


def test_inserted_order_total_matches_its_items_and_payment(seeded):
    counters = oltp_mutate.run_tick(seeded, StubRng(99))
    rows = seeded.execute("SELECT o.total_amount, COALESCE((SELECT SUM(oi.quantity * oi.unit_price) FROM public.order_items oi WHERE oi.order_id = o.order_id), 0), (SELECT p.amount FROM public.payments p WHERE p.order_id = o.order_id AND p.status = 'CAPTURED') FROM public.orders o WHERE o.order_id > %s", (SMALL_ORDERS,)).fetchall()
    assert len(rows) == counters["inserted_orders"]
    for total, items_sum, payment in rows:
        assert total == items_sum
        assert total == payment


def test_inserted_orders_start_new_with_cascadeable_children(seeded):
    counters = oltp_mutate.run_tick(seeded, StubRng(11))
    assert counters["inserted_orders"] > 0
    bad = seeded.execute("SELECT COUNT(*) FROM public.orders o WHERE o.order_id > %s AND (o.status <> 'NEW' OR NOT EXISTS (SELECT 1 FROM public.order_items oi WHERE oi.order_id = o.order_id) OR NOT EXISTS (SELECT 1 FROM public.payments p WHERE p.order_id = o.order_id))", (SMALL_ORDERS,)).fetchone()[0]
    assert bad == 0


def test_tick_survives_when_no_cancelled_orders_exist(seeded):
    seeded.execute("DELETE FROM public.orders WHERE status = 'CANCELLED'")
    counters = oltp_mutate.run_tick(seeded, StubRng(5))
    assert counters["deleted_orders"] == 0


def test_delete_pool_never_exceeds_available_cancelled(seeded):
    cancelled_before = seeded.execute("SELECT COUNT(*) FROM public.orders WHERE status = 'CANCELLED'").fetchone()[0]
    counters = oltp_mutate.run_tick(seeded, StubRng(31))
    assert counters["deleted_orders"] <= cancelled_before
    assert counters["cancelled_orders"] <= counters["updated_orders"]


def test_health_state_tracks_ok_and_error_paths():
    state = oltp_mutate.HealthState()
    assert state.snapshot()["fresh"] is False
    state.record_ok({"updated_orders": 2, "updated_users": 1})
    snap = state.snapshot()
    assert snap["fresh"] is True
    assert snap["ticks"] == 1
    assert snap["totals"]["updated_orders"] == 2
    state.record_error("boom")
    assert state.snapshot()["last_error"] == "boom"


def test_health_handler_serves_snapshot():
    import json
    import threading
    import urllib.request
    from http.server import ThreadingHTTPServer

    state = oltp_mutate.HealthState()
    state.record_ok({"updated_orders": 1})
    try:
        server = ThreadingHTTPServer(("127.0.0.1", _HEALTH_TEST_PORT), oltp_mutate._health_handler(state))
    except OSError:
        import pytest

        pytest.skip("health test port 18081 is busy")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen("http://127.0.0.1:18081/health", timeout=5) as resp:
            assert resp.status == 200
            body = json.loads(resp.read())
            assert body["fresh"] is True
    finally:
        server.shutdown()
        thread.join(timeout=5)
