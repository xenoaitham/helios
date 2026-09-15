"""File extractor: CSV feeds from the drop volume into the raw zone (ADR-006/007).

Incremental control is the per-file ledger (raw.ingest_files), NEVER a date:
a file is processed iff it is present AND (unknown OR its content hash
changed) — so the Phase-1 LATE batch (backdated filename) lands normally, and a
same-name regeneration is detected by hash and re-landed. Rows+ledger land in
ONE transaction per file (atomic consumption).

Dirt handling (ADR-004 modes) — quarantine or reject, never corrupt:
- duplicate rows: same pk + same content → coalesced (content-hash dedupe)
- ragged width: field count ≠ header → quarantine `ragged_width`, row number kept
- cp1252 row inside the UTF-8 file: per-LINE strict decode → quarantine
  `encoding_not_utf8` with the offending bytes escaped; clean rows still land
- unknown/missing header: one `unknown_header` reject, nothing lands, ledger NOT
  written (a fixed regeneration re-lands on the next run)

The drop directory is CLI/env-provided, so every filesystem touch goes through
`_safe_path` (file-drop _safe_path precedent): plain regex-validated names,
resolved paths, reads confined to the drop dir. The volume is mounted read-only.
"""

from __future__ import annotations

import csv
import hashlib
import os
import re
from pathlib import Path

from ingest import config, loads
from ingest.landing import coalesce_rows, files_ledger_hash, land_file_customers, land_file_products, land_quarantine, record_file
from ingest.log import log

_FILENAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_FEED_RE = re.compile(r"^(customers|products)-(\d{8})\.csv$")

CUSTOMER_HEADER = ("customer_id", "email", "full_name", "country_code", "signup_date", "tier")
PRODUCT_HEADER = ("sku", "name", "category", "price_cents", "supplier_code")


class Feed:
    def __init__(self, name: str, header: tuple, pk_field: str, pk_int: bool, land):
        self.name = name
        self.header = header
        self.pk_field = pk_field
        self.pk_int = pk_int
        self.land = land


FEEDS = {
    "customers": Feed("file_customers", CUSTOMER_HEADER, "customer_id", True, land_file_customers),
    "products": Feed("file_products", PRODUCT_HEADER, "sku", False, land_file_products),
}


def _safe_path(base_dir: str, filename: str) -> Path:
    """Containment check for every filesystem touch (file-drop precedent):
    plain regex-validated names, resolved paths, reads stay inside the dir."""
    if not _FILENAME_RE.fullmatch(filename):
        raise ValueError(f"blocked filename {filename!r}; must match [A-Za-z0-9._-]+")
    base = Path(base_dir).resolve()
    candidate = (base / filename).resolve()
    if candidate.parent != base:
        raise ValueError(f"blocked path {filename!r}; escapes the drop dir {base_dir!r}")
    return candidate


def discover(drop_dir: str) -> list[tuple[str, Feed]]:
    """Sorted (filename, feed) pairs for recognized feed names; others logged off."""
    base = Path(drop_dir)
    if not base.is_dir():
        raise RuntimeError(f"drop dir {drop_dir!r} does not exist (filedrop volume not mounted?)")
    found = []
    for name in sorted(os.listdir(base)):
        if not _FILENAME_RE.fullmatch(name):
            log("file_skipped_unsafe_name", filename=name)
            continue
        match = _FEED_RE.fullmatch(name)
        if match is None:
            if (base / name).is_file():
                log("file_skipped_unknown", filename=name)
            continue
        _safe_path(drop_dir, name)  # containment assertion before any read
        found.append((name, FEEDS[match.group(1)]))
    return found


def parse_feed(payload: bytes, feed: Feed) -> tuple[list[tuple[object, dict]], list[tuple[int, str, str]], bool]:
    """Parse one feed payload -> (rows, rejects, header_ok).

    rejects are (row_number, reason, raw_content); row numbers are PHYSICAL
    (header = row 1), so they map 1:1 back to the file.
    """
    lines = payload.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    rows: list[tuple[object, dict]] = []
    rejects: list[tuple[int, str, str]] = []

    if not lines:
        return rows, [(1, "unknown_header", "")], False
    try:
        header_text = lines[0].decode("utf-8")
    except UnicodeDecodeError:
        return rows, [(1, "encoding_not_utf8", lines[0].decode("ascii", "backslashreplace")[:300])], False
    header = next(csv.reader([header_text]))
    if tuple(header) != feed.header:
        return rows, [(1, "unknown_header", header_text[:300])], False

    width = len(feed.header)
    for index, raw in enumerate(lines[1:], start=2):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            rejects.append((index, "encoding_not_utf8", raw.decode("ascii", "backslashreplace")[:300]))
            continue
        fields = next(csv.reader([text]))
        if len(fields) != width:
            rejects.append((index, "ragged_width", text[:300]))
            continue
        row = dict(zip(feed.header, fields))
        pk = row[feed.pk_field]
        if feed.pk_int:
            try:
                pk = int(pk)
            except ValueError:
                rejects.append((index, "invalid_pk", text[:300]))
                continue
        elif not pk.strip():
            rejects.append((index, "invalid_pk", text[:300]))
            continue
        rows.append((pk, row))
    return rows, rejects, True


def run_file(conn, load_id, stats: loads.RunStats, *, drop_dir: str | None = None) -> dict:
    """Process every new/changed feed file in the drop dir; per-file atomic."""
    drop_dir = drop_dir if drop_dir is not None else config.filedrop_dir()
    files = discover(drop_dir)
    stats.units_total = len(files)
    log("file_discovered", drop_dir=drop_dir, files=len(files))
    summary = {"files": {}}
    for filename, feed in files:
        payload = _safe_path(drop_dir, filename).read_bytes()
        file_hash = hashlib.sha256(payload).hexdigest()
        known = files_ledger_hash(conn, filename)
        if known == file_hash:
            stats.units_done += 1
            with conn.transaction():
                loads.update_load(conn, load_id, stats)
            log("file_skipped_unchanged", filename=filename)
            summary["files"][filename] = "unchanged"
            continue

        rows, rejects, header_ok = parse_feed(payload, feed)
        rows, coalesced = coalesce_rows(rows)
        quarantine_rows = [(feed.name, filename, filename, row_number, reason, content) for row_number, reason, content in rejects]
        if header_ok:
            with conn.transaction():
                landed, unchanged = feed.land(conn, rows, load_id, filename)
                quarantined = land_quarantine(conn, quarantine_rows, load_id) if quarantine_rows else 0
                record_file(conn, filename, file_hash, len(payload), load_id, len(rows) + coalesced + len(rejects), landed, quarantined)
                stats.rows_read += len(rows) + coalesced + len(rejects)
                stats.rows_landed += landed
                stats.rows_unchanged += unchanged
                stats.rows_coalesced += coalesced
                stats.rows_quarantined += quarantined
                stats.units_done += 1
                loads.update_load(conn, load_id, stats)
            log("file_landed", filename=filename, feed=feed.name, rows=len(rows) + coalesced, landed=landed, unchanged=unchanged, coalesced=coalesced, quarantined=quarantined)
            summary["files"][filename] = {"rows": len(rows), "quarantined": quarantined}
        else:
            # Unparseable file: quarantine the header reject, DO NOT write the
            # ledger — a fixed regeneration must re-land on the next run.
            with conn.transaction():
                quarantined = land_quarantine(conn, quarantine_rows, load_id)
                stats.rows_quarantined += quarantined
                stats.units_done += 1
                loads.update_load(conn, load_id, stats)
            log("file_rejected_header", filename=filename, reason=rejects[0][1], level="warn")
            summary["files"][filename] = {"rejected": rejects[0][1]}
    return summary
