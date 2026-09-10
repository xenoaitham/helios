"""REST extractor tests: a local fake API (threaded, 127.0.0.1) serves the
rest-mock contract — cursor pages per endpoint, a scripted 429 with Retry-After
and a 500 — so the walk, the retry policy and idempotent re-walks are exercised
for real, in-container, without touching the live rest-mock.
"""

from __future__ import annotations

import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from ingest import loads
from ingest.extract_rest import ALLOWED_PATHS, build_url, run_rest


def _make_handler(products, promotions, flake):
    class Handler(BaseHTTPRequestHandler):
        seq = 0

        def log_message(self, *args):
            pass

        def do_GET(self):
            Handler.seq += 1
            injected = flake.get(Handler.seq)
            if injected == 429:
                body = b'{"detail": "rate limit exceeded"}'
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if injected == 500:
                body = b'{"detail": "transient upstream failure", "retryable": true}'
                self.send_response(500)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            path = urlparse(self.path).path
            catalog = Handler.products if path == "/products" else Handler.promotions
            query = parse_qs(urlparse(self.path).query)
            start = int(query.get("cursor", ["0"])[0])
            limit = int(query.get("limit", ["3"])[0])
            page = catalog[start : start + limit]
            next_cursor = str(start + limit) if start + limit < len(catalog) else None
            body = json.dumps({"data": page, "next_cursor": next_cursor, "total": len(catalog)}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    Handler.products = list(products)
    Handler.promotions = list(promotions)
    return Handler


@pytest.fixture()
def fake_rest():
    servers = []

    def start(products, promotions=(), flake=None):
        server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(products, promotions, flake or {}))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield start
    for server in servers:
        server.shutdown()


class RecordingSleep:
    def __init__(self):
        self.slept = []

    def __call__(self, seconds):
        self.slept.append(seconds)


def _products(n):
    return [{"id": i, "sku": f"SKU-{i * 19:05d}", "price_cents": 199 + i} for i in range(1, n + 1)]


def _promotions(n):
    return [{"id": i, "product_sku": "SKU-00019", "discount_percent": 5 + i, "starts_at": "2026-09-01", "ends_at": "2026-12-01", "description": f"promo {i}"} for i in range(1, n + 1)]


def _run(conn, base_url, sleep):
    load_id = uuid.uuid4()
    stats = loads.RunStats()
    run_rest(conn, load_id, stats, base_url=base_url, sleep=sleep)
    return stats


def test_full_walk_retries_429_and_500_and_lands_everything(tconn, fake_rest, monkeypatch):
    monkeypatch.setenv("INGEST_REST_PAGE_LIMIT", "3")
    base = fake_rest(_products(7), _promotions(2), flake={2: 429, 3: 500})  # req2: 429+Retry-After:1, req3: 500
    sleep = RecordingSleep()
    stats = _run(tconn, base, sleep)

    assert stats.rows_landed == 7 + 2
    assert sleep.slept[0] == 1.0  # Retry-After honored verbatim (the ADR-006 contract)
    assert len(sleep.slept) >= 2  # the 500 went through jittered backoff
    assert tconn.execute("SELECT count(*) FROM raw.rest_products").fetchone()[0] == 7
    assert tconn.execute("SELECT count(*) FROM raw.rest_promotions").fetchone()[0] == 2


def test_rewalk_is_a_noop_for_unchanged_catalog(tconn, fake_rest, monkeypatch):
    monkeypatch.setenv("INGEST_REST_PAGE_LIMIT", "5")
    base = fake_rest(_products(4), _promotions(2))
    first = _run(tconn, base, RecordingSleep())
    second = _run(tconn, base, RecordingSleep())
    assert first.rows_landed == 6
    assert second.rows_landed == 0
    assert second.rows_unchanged == 6
    kinds = {r[0]: r[1] for r in tconn.execute("SELECT source, watermark_kind FROM raw.ingest_watermarks").fetchall()}
    assert kinds == {"rest_products": "last_full_walk", "rest_promotions": "last_full_walk"}


def test_missing_pk_is_quarantined_clean_rows_still_land(tconn, fake_rest, monkeypatch):
    monkeypatch.setenv("INGEST_REST_PAGE_LIMIT", "10")
    products = [{"id": 1, "sku": "SKU-00001"}, {"no_id": True}, {"id": 3, "sku": "SKU-00057"}]
    base = fake_rest(products, _promotions(1))
    stats = _run(tconn, base, RecordingSleep())
    assert stats.rows_landed == 3 and stats.rows_quarantined == 1
    assert tconn.execute("SELECT reason FROM raw.ingest_quarantine WHERE source = 'rest_products'").fetchone()[0] == "missing_pk"


def test_build_url_blocks_off_allowlist_targets():
    assert build_url("http://rest-mock:8000", "/products", {"limit": 3}).startswith("http://rest-mock:8000/products?limit=3")
    with pytest.raises(RuntimeError, match="allowlist"):
        build_url("http://169.254.169.254", "/products", {"limit": 3})
    with pytest.raises(RuntimeError, match="blocked path"):
        build_url("http://rest-mock:8000", "/admin", {"limit": 3})
    assert "/health" in ALLOWED_PATHS
