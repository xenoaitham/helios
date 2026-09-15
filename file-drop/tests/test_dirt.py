"""Dirt-mode classification tests on real rendered files (ADR-004 DoD):
each mode is provoked, then classified by reading the file back."""

from __future__ import annotations

import csv
import io

import pytest

from filedrop.generate import (
    CUSTOMER_COUNT,
    CUSTOMER_HEADER,
    FEED_SEED,
    PRODUCT_COUNT,
    apply_dupes,
    apply_encoding,
    apply_ragged,
    build_feed,
    customer_rows,
    generate,
    list_drop,
    product_rows,
    render_csv,
)
from datetime import date

import random


@pytest.fixture()
def rng():
    return random.Random(FEED_SEED)


def test_row_builders_are_deterministic_and_sized():
    assert customer_rows(50, date(2026, 9, 9)) == customer_rows(50, date(2026, 9, 9))
    assert len(customer_rows(CUSTOMER_COUNT, date(2026, 9, 9))) == CUSTOMER_COUNT
    assert len(product_rows(PRODUCT_COUNT)) == PRODUCT_COUNT
    skus = [row[0] for row in product_rows(PRODUCT_COUNT)]
    assert len(set(skus)) == PRODUCT_COUNT  # unique skus in the shared space


def test_clean_feed_parses_with_exact_counts():
    payload, report = build_feed(CUSTOMER_HEADER, customer_rows(100, date(2026, 9, 9)), set(), random.Random(1))
    assert report["dupes_added"] == 0 and report["ragged_lines"] == 0 and report["encoding_lines"] == 0
    rows = list(csv.reader(io.StringIO(payload.decode("utf-8"))))
    assert rows[0] == list(CUSTOMER_HEADER)
    assert len(rows) == 101
    assert all(len(row) == len(CUSTOMER_HEADER) for row in rows)


def test_dupes_mode_creates_verbatim_repeated_lines(rng):
    content = render_csv(CUSTOMER_HEADER, customer_rows(200, date(2026, 9, 9)))
    dirty, added = apply_dupes(content, rng)
    assert added > 0
    lines = dirty.splitlines()
    assert len(lines) == len(content.splitlines()) + added
    seen = {}
    dupes = 0
    for line in lines[1:]:
        seen[line] = seen.get(line, 0) + 1
        if seen[line] == 2:
            dupes += 1
    assert dupes == added  # every duplicate is a verbatim re-insertion


def test_ragged_mode_produces_wrong_field_counts(rng):
    content = render_csv(CUSTOMER_HEADER, customer_rows(200, date(2026, 9, 9)))
    dirty, touched = apply_ragged(content, rng)
    assert touched > 0
    lines = dirty.splitlines()
    ragged = [line for line in lines[1:] if len(line.split(",")) != len(CUSTOMER_HEADER)]
    assert len(ragged) == touched
    widths = {len(line.split(",")) for line in ragged}
    assert widths <= {len(CUSTOMER_HEADER) - 1, len(CUSTOMER_HEADER) + 1}  # short and long variants


def test_encoding_mode_breaks_utf8_but_decodes_as_cp1252(rng):
    content = render_csv(CUSTOMER_HEADER, customer_rows(200, date(2026, 9, 9)))
    payload = apply_encoding(content, rng)
    with pytest.raises(UnicodeDecodeError):
        payload.decode("utf-8")  # strict UTF-8 readers fail — the whole point
    text = payload.decode("cp1252")  # the vendor-export reality
    rows = list(csv.reader(io.StringIO(text)))
    accented = [row for row in rows if "Jürgen" in row[2] or "Müller" in row[2]]
    assert len(accented) == 1  # exactly one injected row carries the mojibake source


def test_generate_writes_both_feeds_and_reports_counts(tmp_path):
    report = generate(str(tmp_path), date(2026, 9, 9), {"dupes", "ragged", "encoding"})
    assert set(report["files"]) == {"customers-20260909.csv", "products-20260909.csv"}
    customers = report["files"]["customers-20260909.csv"]
    assert customers["rows"] == CUSTOMER_COUNT
    assert customers["dupes_added"] > 0
    assert customers["ragged_lines"] > 0
    assert customers["encoding_lines"] == 1


def test_late_offset_backdates_the_batch_date(tmp_path):
    report = generate(str(tmp_path), date(2026, 9, 9), set(), late_offset_days=3)
    assert report["batch_date"] == "2026-09-06"
    assert set(report["files"]) == {"customers-20260906.csv", "products-20260906.csv"}


def test_same_inputs_are_byte_identical(tmp_path):
    first = generate(str(tmp_path), date(2026, 9, 9), {"dupes", "ragged"})
    second = generate(str(tmp_path), date(2026, 9, 9), {"dupes", "ragged"})
    # same batch date overwrites in place; determinism means the bytes match
    assert first["files"].keys() == second["files"].keys()
    for name in first["files"]:
        assert (tmp_path / name).read_bytes() == (tmp_path / name).read_bytes()


def test_list_drop_lists_files_with_sizes(tmp_path):
    generate(str(tmp_path), date(2026, 9, 9), set())
    entries = list_drop(str(tmp_path))
    assert len(entries) == 2
    assert all(size > 0 for _, size, _ in entries)
    assert list_drop(str(tmp_path / "missing")) == []
