"""Status report tests: counts/sizes/WAL probe on a small seeded database."""

from __future__ import annotations

from oltp import seed as oltp_seed
from oltp import status as oltp_status
from tests.conftest import SMALL_ORDERS, SMALL_USERS


def test_collect_counts_on_seeded_db(seeded):
    counts = oltp_status.collect_counts(seeded)
    expected_items = sum(oltp_seed.item_count(oid) for oid in range(1, SMALL_ORDERS + 1))
    expected_payments = sum(oltp_seed.payment_count(oid) for oid in range(1, SMALL_ORDERS + 1))
    assert counts == {
        "users": SMALL_USERS,
        "orders": SMALL_ORDERS,
        "order_items": expected_items,
        "payments": expected_payments,
    }


def test_collect_counts_reports_missing_schema(admin):
    """On a fresh database without the schema the report must say so, not crash.
    Uses its own throwaway database: the session testdb gets a schema from the
    other tests."""
    import psycopg

    from oltp.db import connect_settings

    admin.execute("DROP DATABASE IF EXISTS oltp_test_bare WITH (FORCE)")
    admin.execute("CREATE DATABASE oltp_test_bare")
    try:
        cfg = connect_settings()
        with psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname="oltp_test_bare") as conn:
            assert oltp_status.collect_counts(conn) is None
    finally:
        admin.execute("DROP DATABASE IF EXISTS oltp_test_bare WITH (FORCE)")


def test_collect_sizes_returns_positive_bytes(seeded):
    sizes = oltp_status.collect_sizes(seeded)
    assert all(value > 0 for value in sizes.values())


def test_wal_probe_reports_delta_and_cumulatives(seeded):
    wal = oltp_status.wal_probe(seeded, 1)
    assert wal["delta_bytes"] >= 0
    assert wal["wal_records_total"] >= 0
    assert wal["wal_bytes_total"] >= 0
