"""File extractor tests: dirt modes quarantine/reject, never corrupt (ADR-004/006/007).

All files are generated into pytest's tmp_path INSIDE the test container — the
real filedrop volume is never touched. Content is hand-built (no seeds), so the
expected landed/quarantined counts are exact.
"""

from __future__ import annotations

import uuid

from ingest import loads
from ingest.extract_file import _safe_path, discover, parse_feed, run_file
from ingest.extract_file import FEEDS

HEADER = b"customer_id,email,full_name,country_code,signup_date,tier"


def _row(cid, name="Ada Abbott"):
    return f"{cid},ada.{cid}@example.com,{name},DE,2025-03-0{cid % 9 or 1},gold".encode()


def _run(conn, drop_dir):
    load_id = uuid.uuid4()
    stats = loads.RunStats()
    run_file(conn, load_id, stats, drop_dir=str(drop_dir))
    return stats


def test_clean_file_lands_every_row(tconn, tmp_path):
    tmp_path.joinpath("customers-20260911.csv").write_bytes(b"\n".join([HEADER, _row(1), _row(2), _row(3)]) + b"\n")
    stats = _run(tconn, tmp_path)
    assert stats.rows_landed == 3 and stats.rows_quarantined == 0
    assert tconn.execute("SELECT count(*) FROM raw.file_customers").fetchone()[0] == 3
    assert tconn.execute("SELECT count(*) FROM raw.ingest_files").fetchone()[0] == 1


def test_dirt_modes_land_clean_rows_and_quarantine_the_rest(tconn, tmp_path):
    cp_row = "8,hans.8@example.com,Jürgen Müller,DE,2025-07-07,bronze"  # exists as cp1252 bytes below
    payload = b"\n".join([
        HEADER,
        _row(1), _row(2), _row(3), _row(4),
        _row(3),                                  # verbatim duplicate (dupes mode)
        b"6,frank.6@example.com,Frank Costa,US",  # ragged: short (4 fields)
        b"7,grace.7@example.com,Grace Duarte,GB,2025-05-05,silver,",  # ragged: long (7 fields)
        cp_row.encode("cp1252"),                  # encoding: cp1252 row inside the UTF-8 file
        _row(9),
    ]) + b"\n"
    tmp_path.joinpath("customers-20260911.csv").write_bytes(payload)

    stats = _run(tconn, tmp_path)

    # 5 distinct clean rows land; the duplicate coalesces; 3 rejects quarantine with reasons
    assert stats.rows_landed == 5
    assert stats.rows_coalesced == 1
    assert stats.rows_quarantined == 3
    assert stats.rows_read == 9  # 5 clean + 1 dupe + 3 rejects
    reasons = sorted(r[0] for r in tconn.execute("SELECT reason FROM raw.ingest_quarantine ORDER BY reason").fetchall())
    assert reasons == ["encoding_not_utf8", "ragged_width", "ragged_width"]

    # row numbers are PHYSICAL (header = row 1): ragged rows 7/8, cp1252 row 9
    rows = {r[0]: r[1] for r in tconn.execute("SELECT row_number, reason FROM raw.ingest_quarantine").fetchall()}
    assert rows == {7: "ragged_width", 8: "ragged_width", 9: "encoding_not_utf8"}


def test_rerun_after_ledger_is_a_full_noop(tconn, tmp_path):
    path = tmp_path.joinpath("customers-20260911.csv")
    path.write_bytes(b"\n".join([HEADER, _row(1), _row(2)]) + b"\n")
    _run(tconn, tmp_path)
    stats = _run(tconn, tmp_path)  # same bytes -> same hash -> ledger skip
    assert stats.rows_landed == 0 and stats.rows_read == 0
    assert tconn.execute("SELECT count(*) FROM raw.file_customers").fetchone()[0] == 2
    assert tconn.execute("SELECT count(*) FROM raw.ingest_quarantine").fetchone()[0] == 0


def test_regenerated_file_same_name_relands_changed_rows(tconn, tmp_path):
    path = tmp_path.joinpath("customers-20260911.csv")
    path.write_bytes(b"\n".join([HEADER, _row(1), _row(2)]) + b"\n")
    _run(tconn, tmp_path)
    path.write_bytes(b"\n".join([HEADER, _row(1), _row(2, "Grace Duarte")]) + b"\n")  # hash changed
    stats = _run(tconn, tmp_path)
    assert (stats.rows_landed, stats.rows_unchanged) == (1, 1)  # only the changed row writes
    name = tconn.execute("SELECT payload->>'full_name' FROM raw.file_customers WHERE customer_id = 2").fetchone()[0]
    assert name == "Grace Duarte"


def test_backdated_late_file_lands(tconn, tmp_path):
    tmp_path.joinpath("customers-20260911.csv").write_bytes(b"\n".join([HEADER, _row(1)]) + b"\n")
    _run(tconn, tmp_path)
    # the LATE batch: OLDER batch date in the name, arrives NOW — must land
    tmp_path.joinpath("customers-20260901.csv").write_bytes(b"\n".join([HEADER, _row(2)]) + b"\n")
    stats = _run(tconn, tmp_path)
    assert stats.rows_landed == 1
    assert tconn.execute("SELECT count(*) FROM raw.file_customers").fetchone()[0] == 2


def test_unknown_header_rejects_whole_file_without_ledger_row(tconn, tmp_path):
    tmp_path.joinpath("customers-20260911.csv").write_bytes(b"cola,colb,colc\n1,2,3\n")
    stats = _run(tconn, tmp_path)
    assert stats.rows_landed == 0 and stats.rows_quarantined == 1
    assert tconn.execute("SELECT reason FROM raw.ingest_quarantine").fetchone()[0] == "unknown_header"
    # no ledger row: a fixed regeneration must re-land on the next run
    assert tconn.execute("SELECT count(*) FROM raw.ingest_files").fetchone()[0] == 0


def test_unparseable_pk_is_quarantined(tconn, tmp_path):
    tmp_path.joinpath("customers-20260911.csv").write_bytes(b"\n".join([HEADER, b"not-an-int,ada@example.com,Ada,DE,2025-01-01,gold", _row(1)]) + b"\n")
    stats = _run(tconn, tmp_path)
    assert stats.rows_landed == 1 and stats.rows_quarantined == 1
    assert tconn.execute("SELECT reason FROM raw.ingest_quarantine").fetchone()[0] == "invalid_pk"


def test_products_feed_lands_by_sku(tconn, tmp_path):
    header = b"sku,name,category,price_cents,supplier_code"
    rows = b"SKU-00047,Kitchen Item 00047,kitchen,840,SUP-A\nSKU-00094,Tools Item 00094,tools,563,SUP-B\n"
    tmp_path.joinpath("products-20260911.csv").write_bytes(header + b"\n" + rows)
    stats = _run(tconn, tmp_path)
    assert stats.rows_landed == 2
    assert tconn.execute("SELECT count(*) FROM raw.file_products").fetchone()[0] == 2


def test_unknown_and_unsafe_names_are_skipped_not_fatal(tconn, tmp_path):
    tmp_path.joinpath("README.txt").write_text("not a feed")
    tmp_path.joinpath("customers-2026 0911.csv").write_bytes(HEADER + b"\n")  # spaces: unsafe name
    tmp_path.joinpath("customers-20260911.csv").write_bytes(b"\n".join([HEADER, _row(1)]) + b"\n")
    stats = _run(tconn, tmp_path)  # must not raise; only the valid feed processes
    assert stats.rows_landed == 1
    assert discover(str(tmp_path)) == [("customers-20260911.csv", FEEDS["customers"])]


def test_empty_file_is_header_quarantine(tconn, tmp_path):
    tmp_path.joinpath("customers-20260911.csv").write_bytes(b"")
    stats = _run(tconn, tmp_path)
    assert stats.rows_quarantined == 1 and stats.rows_landed == 0


def test_safe_path_contains_reads(tmp_path):
    assert _safe_path(str(tmp_path), "customers-20260911.csv") == tmp_path.resolve() / "customers-20260911.csv"
    import pytest
    with pytest.raises(ValueError):
        _safe_path(str(tmp_path), "../escape.csv")
    with pytest.raises(ValueError):
        _safe_path(str(tmp_path), "sub/dir/file.csv")
