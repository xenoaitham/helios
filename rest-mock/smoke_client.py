"""Smoke client for the running rest-mock service (`make smoke-rest`).

Walks EVERY page of /promotions and asserts the DoD invariant: all 2,500
promotions, no duplicates, no gaps, next_cursor terminates. Transient 500s and
429s are retried with backoff (that is the point of this source) and counted.

Security posture (mirrors soap-service/check_contract.py): fetch targets are
restricted to an explicit host/port allowlist — this client exists to talk to
the local rest-mock service, nothing else.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

BASE_URL = "http://127.0.0.1:8000"
ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1"})
ALLOWED_PORT = 8000
MAX_TRIES = 6


def validated_url(path: str) -> str:
    """Allowlist check on every request URL before it may be fetched."""
    raw = BASE_URL + path
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"blocked scheme {parsed.scheme!r}; only http/https allowed")
    if parsed.hostname not in ALLOWED_HOSTS or parsed.port != ALLOWED_PORT or parsed.username or parsed.password:
        raise ValueError(
            f"blocked target {parsed.hostname!r}:{parsed.port}; "
            f"allowed hosts {sorted(ALLOWED_HOSTS)} on port {ALLOWED_PORT}"
        )
    if not parsed.path.startswith("/promotions") and not parsed.path.startswith("/products") and parsed.path != "/health":
        raise ValueError(f"blocked path {parsed.path!r}; this client only walks catalog endpoints")
    return raw


def fetch(path: str) -> tuple[int, int, dict]:
    """GET with retry on 500/429; returns (retries_500, retries_429, body)."""
    retries_500 = 0
    retries_429 = 0
    for _ in range(MAX_TRIES):
        try:
            with urllib.request.urlopen(validated_url(path), timeout=10) as resp:
                return retries_500, retries_429, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            exc.read()
            if exc.code == 500:
                retries_500 += 1
                time.sleep(0.5)
            elif exc.code == 429:
                retries_429 += 1
                retry_after = float(exc.headers.get("Retry-After", "1"))
                time.sleep(min(2.0, max(0.1, retry_after)))
            else:
                raise
    raise RuntimeError(f"GET {path} still failing after {MAX_TRIES} tries")


def main() -> int:
    retries_500 = 0
    retries_429 = 0
    seen: list[int] = []
    cursor: str | None = None
    pages = 0
    total = None
    while True:
        path = "/promotions?limit=100" + (f"&cursor={cursor}" if cursor else "")
        used_500, used_429, payload = fetch(path)
        retries_500 += used_500
        retries_429 += used_429
        pages += 1
        total = payload["total"]
        seen.extend(item["id"] for item in payload["data"])
        cursor = payload["next_cursor"]
        if cursor is None:
            break
    dupes = len(seen) - len(set(seen))
    ascending = seen == sorted(seen)
    complete = set(seen) == set(range(1, total + 1))
    print(f"[smoke-rest] pages={pages} promotions_seen={len(seen)} total={total}")
    print(f"[smoke-rest] dupes={dupes} ascending={ascending} complete={complete}")
    print(f"[smoke-rest] transient hits retried: 500s={retries_500} 429s={retries_429}")
    if dupes or not ascending or not complete:
        print("[smoke-rest] FAIL — pagination walk broken")
        return 1
    print("[smoke-rest] PASS — /promotions pages through ALL data with no dupes or gaps")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
