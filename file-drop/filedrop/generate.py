"""Nightly CSV feed generator for the SFTP-style drop point (ADR-004).

Feeds: customers-<batchdate>.csv (PII-carrying), products-<batchdate>.csv
(SKUs/prices coherent with the OLTP + rest-mock sources). Dirt is applied as
named, seeded, classified modes — `dupes`, `ragged`, `encoding` — and `--clean`
produces the parseable baseline. `--late-offset N` backdates the batch date in
the filename so an older batch lands *now* (late-arrival simulation).

Every file is byte-deterministic for a given (seed, batch date, dirt set).
The drop directory is CLI/env-provided, so every filesystem touch goes through
`_safe_path` (same containment posture as soap-service/check_contract.py):
plain regex-validated names, resolved paths, writes confined to the drop dir.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import random
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

CUSTOMER_COUNT = 5000
PRODUCT_COUNT = 2000
FEED_SEED = 42
DUP_RATE = 0.02
RAGGED_LINES = 4
DIRT_MODES = ("dupes", "ragged", "encoding")
_FILENAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")

CUSTOMER_HEADER = ("customer_id", "email", "full_name", "country_code", "signup_date", "tier")
PRODUCT_HEADER = ("sku", "name", "category", "price_cents", "supplier_code")

_FIRST_NAMES = ("ada", "bruno", "camila", "dmitri", "elif", "farid", "greta", "hiro", "ines", "jonas", "kira", "liam", "mara", "noor", "oskar", "pia", "ronja", "sven", "tara", "vera", "wren", "yara", "zane", "alba", "cleo", "emre", "freya", "borys")
_LAST_NAMES = ("abbott", "bauer", "costa", "duarte", "egan", "fischer", "gomez", "hartmann", "ibarra", "jensen", "keller", "moreau", "novak", "oliveira", "petrov", "richter", "santos", "tanaka", "vogel", "wagner", "yilmaz", "zhang", "alfaro", "cohen", "delfino", "farrow", "gustafsson", "lindqvist")
_COUNTRIES = ("DE", "GB", "US", "FR", "NL", "PL", "SE", "ES", "JP", "BR")
_TIERS = ("bronze", "silver", "gold", "platinum")
_CATEGORIES = ("garden", "kitchen", "electronics", "toys", "sports", "books", "tools", "apparel")
_SUPPLIERS = ("SUP-A", "SUP-B", "SUP-C", "SUP-D")
_ENCODING_NAME = ("Jürgen", "Müller")  # the accents exist only in cp1252 bytes on disk


def _safe_path(drop_dir: str, filename: str) -> Path:
    """Containment check for every filesystem touch (soap-precedent posture):
    plain regex-validated names, resolved paths, writes stay inside the dir."""
    if not _FILENAME_RE.fullmatch(filename):
        raise ValueError(f"blocked filename {filename!r}; must match [A-Za-z0-9._-]+")
    base = Path(drop_dir).resolve()
    candidate = (base / filename).resolve()
    if candidate != base and candidate.parent != base:
        raise ValueError(f"blocked path {filename!r}; escapes the drop dir {drop_dir!r}")
    return candidate


def customer_rows(count: int, batch_date: date) -> list[tuple]:
    rng = random.Random(FEED_SEED)
    rows = []
    for cid in range(1, count + 1):
        first = rng.choice(_FIRST_NAMES)
        last = rng.choice(_LAST_NAMES)
        signup = batch_date - timedelta(days=rng.randrange(0, 1400))
        rows.append((cid, f"{first}.{last}.{cid}@example.com", f"{first.capitalize()} {last.capitalize()}", rng.choice(_COUNTRIES), signup.isoformat(), rng.choice(_TIERS)))
    return rows


def product_rows(count: int) -> list[tuple]:
    rows = []
    for i in range(1, count + 1):
        sku_num = (i * 47) % 100000  # unique: gcd(47, 100000) == 1; same space as oltp/rest-mock
        category = _CATEGORIES[i % len(_CATEGORIES)]
        rows.append((f"SKU-{sku_num:05d}", f"{category.title()} Item {sku_num:05d}", category, 199 + (sku_num * 613) % 14999, _SUPPLIERS[i % len(_SUPPLIERS)]))
    return rows


def render_csv(header: tuple, rows: list[tuple]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue()


def apply_dupes(content: str, rng: random.Random) -> tuple[str, int]:
    """Re-insert ~DUP_RATE of data lines verbatim after their originals."""
    lines = content.splitlines()
    added = 0
    out = [lines[0]]
    for i in range(1, len(lines)):
        out.append(lines[i])
        if rng.random() < DUP_RATE:
            out.append(lines[i])
            added += 1
    return "\n".join(out) + "\n", added


def apply_ragged(content: str, rng: random.Random) -> tuple[str, int]:
    """Shorten some data lines (drop last field), lengthen others (extra empty
    field). Safe on generated feeds: fields contain no commas/quotes."""
    lines = content.splitlines()
    targets = rng.sample(range(1, len(lines)), min(RAGGED_LINES, len(lines) - 1))
    for n, i in enumerate(targets):
        if n % 2 == 0:
            fields = lines[i].split(",")
            lines[i] = ",".join(fields[:-1])
        else:
            lines[i] = lines[i] + ","
    return "\n".join(lines) + "\n", len(targets)


def apply_encoding(content: str, rng: random.Random) -> bytes:
    """Inject one accent-bearing row and encode THAT line as cp1252 inside the
    otherwise UTF-8 payload — strict UTF-8 decoding of the file fails."""
    lines = content.splitlines()
    position = rng.randrange(1, len(lines))
    fields = lines[position].split(",")
    fields[2] = f"{_ENCODING_NAME[0]} {_ENCODING_NAME[1]}"
    lines.insert(position, ",".join(fields))
    payload = b""
    for i, line in enumerate(lines):
        raw = (line + "\n").encode("utf-8")
        if i == position:
            raw = (line + "\n").encode("cp1252")
        payload += raw
    return payload


def build_feed(header: tuple, rows: list[tuple], dirt: set[str], rng: random.Random) -> tuple[bytes, dict]:
    """Render, apply the selected dirt modes, return (bytes, per-mode counts)."""
    content = render_csv(header, rows)
    report = {"rows": len(rows), "dupes_added": 0, "ragged_lines": 0, "encoding_lines": 0}
    if "dupes" in dirt:
        content, report["dupes_added"] = apply_dupes(content, rng)
    if "ragged" in dirt:
        content, report["ragged_lines"] = apply_ragged(content, rng)
    if "encoding" in dirt:
        payload = apply_encoding(content, rng)
        report["encoding_lines"] = 1
        return payload, report
    return content.encode("utf-8"), report


def generate(drop_dir: str, batch_date: date, dirt: set[str], late_offset_days: int = 0) -> dict:
    """Write both feeds for the (possibly backdated) batch date; returns a report."""
    base = Path(drop_dir)
    base.mkdir(parents=True, exist_ok=True)
    effective = batch_date - timedelta(days=late_offset_days)
    stamp = effective.strftime("%Y%m%d")
    rng = random.Random(FEED_SEED + late_offset_days)
    files = {}
    for name, header, rows in (
        ("customers", CUSTOMER_HEADER, customer_rows(CUSTOMER_COUNT, effective)),
        ("products", PRODUCT_HEADER, product_rows(PRODUCT_COUNT)),
    ):
        payload, counts = build_feed(header, rows, dirt, rng)
        filename = f"{name}-{stamp}.csv"
        _safe_path(drop_dir, filename).write_bytes(payload)
        files[filename] = counts
    return {"batch_date": effective.isoformat(), "late_offset_days": late_offset_days, "dirt": sorted(dirt), "files": files}


def list_drop(drop_dir: str) -> list[tuple]:
    base = Path(drop_dir).resolve()
    if not base.is_dir():
        return []
    listing = []
    for name in sorted(os.listdir(base)):
        path = _safe_path(str(base), name)
        if path.is_file():
            listing.append((name, path.stat().st_size, datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")))
    return listing


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="HELIOS nightly CSV feed generator (ADR-004)")
    parser.add_argument("--drop-dir", default=os.environ.get("FILEDROP_DROP_DIR", "/data/drop"))
    parser.add_argument("--for-date", default=date.today().strftime("%Y%m%d"), help="batch date, YYYYMMDD (default: today UTC)")
    parser.add_argument("--late-offset", type=int, default=0, help="backdate the batch date by N days (late-arrival simulation)")
    parser.add_argument("--dirt", default=",".join(DIRT_MODES), help="comma list of dirt modes: dupes,ragged,encoding")
    parser.add_argument("--clean", action="store_true", help="emit parseable feeds without any dirt")
    parser.add_argument("--list", action="store_true", help="list the drop directory and exit")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args(argv)

    if args.list:
        entries = list_drop(args.drop_dir)
        for name, size, modified in entries:
            print(f"[drop] {name}  {size} bytes  modified {modified}Z")
        print(f"[drop] {len(entries)} file(s) in {args.drop_dir}")
        return 0

    batch_date = datetime.strptime(args.for_date, "%Y%m%d").date()
    dirt = set() if args.clean else {mode.strip() for mode in args.dirt.split(",") if mode.strip()}
    unknown = dirt - set(DIRT_MODES)
    if unknown:
        parser.error(f"unknown dirt mode(s): {sorted(unknown)}; allowed: {list(DIRT_MODES)}")
    started = time.monotonic()
    report = generate(args.drop_dir, batch_date, dirt, args.late_offset)
    if args.report:
        tag = " (LATE)" if args.late_offset else ""
        print(f"[drop] batch_date={report['batch_date']}{tag} dirt={report['dirt'] or 'none'}")
        for filename, counts in report["files"].items():
            print(f"[drop] {filename}: rows={counts['rows']} dupes_added={counts['dupes_added']} ragged_lines={counts['ragged_lines']} encoding_lines={counts['encoding_lines']}")
        print(f"[drop] written in {time.monotonic() - started:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
