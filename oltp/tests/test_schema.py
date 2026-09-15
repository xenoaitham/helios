"""Schema contract tests: idempotent DDL, constraint enforcement, cascades."""

from __future__ import annotations

import psycopg
import pytest

from oltp import schema as oltp_schema


def _named_constraint_count(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM pg_constraint WHERE conname LIKE 'pk\\_%' OR conname LIKE 'fk\\_%' OR conname LIKE 'uq\\_%'").fetchone()[0]


def test_apply_schema_is_idempotent(tconn):
    oltp_schema.apply_schema(tconn)
    first = _named_constraint_count(tconn)
    oltp_schema.apply_schema(tconn)
    assert _named_constraint_count(tconn) == first
    assert first == 8  # 4 PK + 1 UNIQUE(email) + 3 FK


def test_seeded_schema_has_exactly_expected_constraints(seeded):
    assert _named_constraint_count(seeded) == 8


def test_fk_rejects_order_for_missing_user(seeded):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        seeded.execute("INSERT INTO public.orders (user_id, status, currency, total_amount, placed_at, updated_at) VALUES (999999, 'NEW', 'USD', 1.00, now(), now())")


def test_unique_email_enforced(seeded):
    email = seeded.execute("SELECT email FROM public.users WHERE user_id = 1").fetchone()[0]
    with pytest.raises(psycopg.errors.UniqueViolation):
        seeded.execute("INSERT INTO public.users (email, full_name, country_code, created_at, is_active) VALUES (%s, 'Dup User', 'DE', now(), TRUE)", (email,))


def test_deleting_order_cascades_items_and_payments(seeded):
    items_before = seeded.execute("SELECT COUNT(*) FROM public.order_items WHERE order_id = 1").fetchone()[0]
    payments_before = seeded.execute("SELECT COUNT(*) FROM public.payments WHERE order_id = 1").fetchone()[0]
    assert items_before > 0 and payments_before > 0
    seeded.execute("DELETE FROM public.orders WHERE order_id = 1")
    assert seeded.execute("SELECT COUNT(*) FROM public.order_items WHERE order_id = 1").fetchone()[0] == 0
    assert seeded.execute("SELECT COUNT(*) FROM public.payments WHERE order_id = 1").fetchone()[0] == 0


def test_identity_sequence_synced_after_seed(seeded):
    """seed_database syncs sequences past the max id, so plain INSERTs (the
    mutator's bread and butter) must not collide."""
    row = seeded.execute("INSERT INTO public.orders (user_id, status, currency, total_amount, placed_at, updated_at) VALUES (1, 'NEW', 'USD', 9.99, now(), now()) RETURNING order_id").fetchone()
    assert row[0] == 121  # seed used 120 explicit ids
