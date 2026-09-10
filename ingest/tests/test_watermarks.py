"""Watermark guard tests (ADR-006): cursors never regress, stamps move freely."""

from __future__ import annotations

from ingest.watermarks import MAX_CREATED_AT, advance_watermark, get_watermark


def test_missing_watermark_returns_none(tconn):
    assert get_watermark(tconn, "soap_orders") is None


def test_advance_creates_then_updates(tconn):
    assert advance_watermark(tconn, "soap_orders", "2026-09-01 00:00:00", MAX_CREATED_AT) is True
    assert get_watermark(tconn, "soap_orders") == ("2026-09-01 00:00:00", MAX_CREATED_AT)
    assert advance_watermark(tconn, "soap_orders", "2026-09-02 00:00:00", MAX_CREATED_AT) is True
    assert get_watermark(tconn, "soap_orders")[0] == "2026-09-02 00:00:00"


def test_cursor_never_regresses(tconn):
    advance_watermark(tconn, "soap_orders", "2026-09-10 00:00:00", MAX_CREATED_AT)
    assert advance_watermark(tconn, "soap_orders", "2026-09-04 00:00:00", MAX_CREATED_AT) is False
    assert get_watermark(tconn, "soap_orders")[0] == "2026-09-10 00:00:00"  # replay cannot move it back


def test_same_value_is_accepted_as_no_change(tconn):
    advance_watermark(tconn, "soap_orders", "2026-09-10 00:00:00", MAX_CREATED_AT)
    assert advance_watermark(tconn, "soap_orders", "2026-09-10 00:00:00", MAX_CREATED_AT) is True  # >= guard


def test_last_full_walk_stamp_moves_freely(tconn):
    advance_watermark(tconn, "rest_products", "2026-09-11T08:00:00+00:00", "last_full_walk")
    advance_watermark(tconn, "rest_products", "2026-09-11T09:00:00+00:00", "last_full_walk")
    assert get_watermark(tconn, "rest_products")[0] == "2026-09-11T09:00:00+00:00"
