"""Landing-layer tests against the scratch DB: the idempotency core (ADR-007)."""

from __future__ import annotations

import uuid

from ingest import loads
from ingest.landing import coalesce_rows, land_file_customers, land_quarantine, land_rest_products, record_file, files_ledger_hash


def _load_id():
    return uuid.uuid4()


def _customer(cid, name="Ada"):
    return {"customer_id": cid, "email": f"ada.{cid}@example.com", "full_name": name, "country_code": "DE", "signup_date": "2026-01-01", "tier": "gold"}


def test_first_landing_inserts_every_row(tconn):
    rows = [(1, _customer(1)), (2, _customer(2)), (3, _customer(3))]
    landed, unchanged = land_file_customers(tconn, rows, _load_id(), "customers-20260101.csv")
    assert (landed, unchanged) == (3, 0)
    assert tconn.execute("SELECT count(*) FROM raw.file_customers").fetchone()[0] == 3


def test_relanding_identical_content_is_a_physical_noop(tconn):
    load_a, load_b = _load_id(), _load_id()
    rows = [(1, _customer(1)), (2, _customer(2))]
    land_file_customers(tconn, rows, load_a, "f.csv")
    before = tconn.execute("SELECT customer_id, load_id, content_hash, payload, landed_at FROM raw.file_customers ORDER BY customer_id").fetchall()
    landed, unchanged = land_file_customers(tconn, rows, load_b, "f.csv")
    after = tconn.execute("SELECT customer_id, load_id, content_hash, payload, landed_at FROM raw.file_customers ORDER BY customer_id").fetchall()
    assert (landed, unchanged) == (0, 2)
    assert before == after  # load_id, landed_at — NOTHING moved; that is the no-op contract


def test_changed_content_updates_only_the_changed_row(tconn):
    land_file_customers(tconn, [(1, _customer(1)), (2, _customer(2))], _load_id(), "f.csv")
    landed, unchanged = land_file_customers(tconn, [(1, _customer(1, "Changed")), (2, _customer(2))], _load_id(), "f.csv")
    assert (landed, unchanged) == (1, 1)
    payload = tconn.execute("SELECT payload->>'full_name' FROM raw.file_customers WHERE customer_id = 1").fetchone()[0]
    assert payload == "Changed"


def test_within_batch_duplicate_keys_coalesce_last_wins(tconn):
    rows = [(7, _customer(7, "First")), (7, _customer(7, "Last")), (8, _customer(8))]
    coalesced, count = coalesce_rows(rows)
    assert (len(coalesced), count) == (2, 1)
    land_file_customers(tconn, coalesced, _load_id(), "f.csv")
    payload = tconn.execute("SELECT payload->>'full_name' FROM raw.file_customers WHERE customer_id = 7").fetchone()[0]
    assert payload == "Last"


def test_quarantine_replay_guard_inserts_once(tconn):
    rejects = [("file_customers", "customers-20260101.csv", "customers-20260101.csv", 5, "ragged_width", "1,2,3")]
    assert land_quarantine(tconn, rejects, _load_id()) == 1
    assert land_quarantine(tconn, rejects, _load_id()) == 0  # crash-replay re-quarantines nothing
    assert tconn.execute("SELECT count(*) FROM raw.ingest_quarantine").fetchone()[0] == 1


def test_file_ledger_roundtrip(tconn):
    record_file(tconn, "customers-20260101.csv", "hash-aaa", 100, _load_id(), 10, 10, 0)
    assert files_ledger_hash(tconn, "customers-20260101.csv") == "hash-aaa"
    assert files_ledger_hash(tconn, "customers-20260999.csv") is None
    record_file(tconn, "customers-20260101.csv", "hash-bbb", 120, _load_id(), 10, 1, 0)  # regenerated file
    assert files_ledger_hash(tconn, "customers-20260101.csv") == "hash-bbb"


def test_load_ledger_lifecycle_and_forensics(tconn):
    load_id = _load_id()
    stats = loads.RunStats(rows_read=10, rows_landed=8, rows_quarantined=2, units_done=1, units_total=4)
    loads.start_load(tconn, load_id, "file", "file", 4)
    status, finished = tconn.execute("SELECT status, finished_at FROM raw.ingest_loads WHERE load_id = %s", (load_id,)).fetchone()
    assert status == "running" and finished is None  # what a SIGKILL would leave behind
    stats.units_total = 0  # 0 must not clobber the discovered total (COALESCE guard)
    loads.update_load(tconn, load_id, stats)
    assert tconn.execute("SELECT units_total FROM raw.ingest_loads WHERE load_id = %s", (load_id,)).fetchone()[0] == 4
    loads.update_load(tconn, load_id, stats, status="succeeded")
    status, finished = tconn.execute("SELECT status, finished_at FROM raw.ingest_loads WHERE load_id = %s", (load_id,)).fetchone()
    assert status == "succeeded" and finished is not None


def test_rest_products_lands_by_natural_key(tconn):
    rows = [(1, {"id": 1, "sku": "SKU-00001", "price_cents": 199}), (2, {"id": 2, "sku": "SKU-00019", "price_cents": 812})]
    landed, unchanged = land_rest_products(tconn, rows, _load_id(), "rest_products")
    assert (landed, unchanged) == (2, 0)
    landed, unchanged = land_rest_products(tconn, rows, _load_id(), "rest_products")
    assert (landed, unchanged) == (0, 2)
