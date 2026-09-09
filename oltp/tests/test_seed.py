"""Seeder tests: exact counts from the pure functions, determinism, --if-empty
and reset semantics, totals agreeing with line items."""

from __future__ import annotations

from decimal import Decimal

from oltp import seed as oltp_seed
from tests.conftest import SMALL_ORDERS, SMALL_USERS


def _snapshot(conn) -> dict:
    counts = oltp_seed._counts(conn)
    row = conn.execute("SELECT COALESCE(SUM(total_amount), 0), COALESCE((SELECT SUM(quantity) FROM public.order_items), 0) FROM public.orders").fetchone()
    statuses = conn.execute("SELECT status, COUNT(*) FROM public.orders GROUP BY status ORDER BY status").fetchall()
    return {"counts": counts, "total_amount": row[0], "total_quantity": row[1], "statuses": statuses}


def test_seed_counts_match_the_pure_functions(seeded):
    counts = oltp_seed._counts(seeded)
    expected_items = sum(oltp_seed.item_count(oid) for oid in range(1, SMALL_ORDERS + 1))
    expected_payments = sum(oltp_seed.payment_count(oid) for oid in range(1, SMALL_ORDERS + 1))
    assert counts["users"] == SMALL_USERS
    assert counts["orders"] == SMALL_ORDERS
    assert counts["order_items"] == expected_items
    assert counts["payments"] == expected_payments


def test_seed_is_deterministic_across_resets(tconn):
    oltp_seed.seed_database(tconn, users=SMALL_USERS, orders=SMALL_ORDERS, reset=True)
    first = _snapshot(tconn)
    oltp_seed.seed_database(tconn, users=SMALL_USERS, orders=SMALL_ORDERS, reset=True)
    second = _snapshot(tconn)
    assert first == second


def test_if_empty_skips_and_does_not_duplicate(seeded):
    before = oltp_seed._counts(seeded)
    report = oltp_seed.seed_database(seeded, users=SMALL_USERS, orders=SMALL_ORDERS, if_empty=True)
    assert report["skipped"] is True
    assert oltp_seed._counts(seeded) == before


def test_seed_into_non_empty_requires_explicit_reset(seeded):
    import pytest

    with pytest.raises(RuntimeError):
        oltp_seed.seed_database(seeded, users=SMALL_USERS, orders=SMALL_ORDERS)


def test_reset_reload_leaves_no_duplicates(seeded):
    report = oltp_seed.seed_database(seeded, users=SMALL_USERS, orders=SMALL_ORDERS, reset=True)
    assert report["skipped"] is False
    counts = oltp_seed._counts(seeded)
    assert counts["users"] == SMALL_USERS and counts["orders"] == SMALL_ORDERS


def test_order_totals_agree_with_line_items(seeded):
    """The warehouse will rely on this: totals are derived from the same item
    specs, so SUM(quantity*unit_price) per order equals orders.total_amount."""
    mismatches = seeded.execute("SELECT COUNT(*) FROM public.orders o WHERE o.total_amount <> COALESCE((SELECT SUM(oi.quantity * oi.unit_price) FROM public.order_items oi WHERE oi.order_id = o.order_id), 0)").fetchone()[0]
    assert mismatches == 0


def test_refund_payments_are_negative_and_match_source_order(seeded):
    row = seeded.execute("SELECT p.amount, o.total_amount FROM public.payments p JOIN public.orders o ON o.order_id = p.order_id WHERE p.status = 'REFUNDED' AND p.order_id = 5").fetchone()
    assert row[0] == -row[1]
    assert isinstance(row[0], Decimal)


def test_status_distribution_uses_the_shared_vocabulary(seeded):
    statuses = {r[0] for r in seeded.execute("SELECT DISTINCT status FROM public.orders").fetchall()}
    assert statuses <= set(oltp_seed.ORDER_STATUSES)
    delivered = seeded.execute("SELECT COUNT(*) FROM public.orders WHERE status = 'DELIVERED'").fetchone()[0]
    assert delivered > SMALL_ORDERS // 2  # weighted 82%
