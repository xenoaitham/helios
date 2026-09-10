"""REST extractor: full cursor walk of rest-mock /products + /promotions (ADR-006).

The catalog is static (ADR-003), so the incremental strategy is a full walk per
run; hash-guarded landing makes it a no-op for unchanged rows. The walk EXPECTS
429s once the token bucket drains (cap 30, refill 10/s) and ~5% deterministic
500s — both flow through the ADR-006 retry policy, whose retries are logged with
their slept seconds.

Security posture (Mimosa canon, cdc/register.py precedent): the host+path
ALLOWLIST lives in this module, immediately adjacent to the URL builder that
feeds the urllib Request build; the assembled URL (including its query string)
is re-validated against the allowlist before it is returned. The opaque cursor
comes from the API response and is returned to the API as a query parameter —
no URL is ever derived from user input.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from ingest import config, loads, watermarks
from ingest.landing import coalesce_rows, land_quarantine, land_rest_products, land_rest_promotions
from ingest.log import log
from ingest.retry import RetryExhausted, RetryPolicy, http_request

# Only this compose service (plus loopback for in-container test fakes), and
# only these fixed paths — validated here, in this module, before any Request.
ALLOWED_HOSTS = frozenset({"rest-mock", "localhost", "127.0.0.1"})
ALLOWED_PATHS = frozenset({"/products", "/promotions", "/health"})

LAST_FULL_WALK = "last_full_walk"


def build_url(base_url: str, path: str, params: dict) -> str:
    """Build an endpoint URL and validate the COMPLETE result (scheme, host,
    path) against this module's allowlist before returning it — cdc/register.py
    posture. The only dynamic part is the query string: a urlencoded limit int
    and the opaque cursor token handed back by the API itself."""
    base = config.validate_source_base_url(base_url, ALLOWED_HOSTS)
    query = urllib.parse.urlencode(params)
    url = f"{base}{path}?{query}"
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "http":
        raise RuntimeError(f"blocked scheme {parsed.scheme!r}; only http allowed")
    if parsed.hostname not in ALLOWED_HOSTS:
        raise RuntimeError(f"blocked host {parsed.hostname!r}; allowed hosts: {sorted(ALLOWED_HOSTS)}")
    if parsed.path not in ALLOWED_PATHS:
        raise RuntimeError(f"blocked path {parsed.path!r}; allowed paths: {sorted(ALLOWED_PATHS)}")
    return url


def urllib_transport(url: str, timeout: float = 30) -> tuple[int, dict, bytes]:
    """One real HTTP GET; non-2xx becomes a (status, headers, body) result for
    the retry policy to judge (urllib raises HTTPError for those)."""
    request = urllib.request.Request(url, method="GET")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers or {}), error.read()


def default_policy() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=config.retry_max_attempts(),
        backoff_base=config.retry_backoff_base(),
        backoff_factor=config.retry_backoff_factor(),
        backoff_max=config.retry_backoff_max(),
        retry_after_default=config.retry_after_default(),
    )


def _walk_pages(path: str, *, base_url: str, transport, policy: RetryPolicy, sleep):
    """Yield page data following the opaque cursor to the end."""
    cursor = None
    while True:
        params = {"limit": config.rest_page_limit()}
        if cursor is not None:
            params["cursor"] = cursor
        doc = http_request(build_url(base_url, path, params), transport=transport, policy=policy, sleep=sleep)
        data = doc.get("data")
        if not isinstance(data, list):
            raise RuntimeError(f"{path} returned non-list data: {type(data).__name__} (contract break)")
        yield data
        cursor = doc.get("next_cursor")
        if not cursor:
            return


def run_rest(conn, load_id, stats: loads.RunStats, *, base_url: str | None = None, transport=None, policy: RetryPolicy | None = None, sleep=time.sleep) -> dict:
    """Walk both endpoints, land every page in its own transaction.

    Returns a summary dict. Pages land as they arrive (crash = replay from the
    start; the hash guard absorbs re-landed pages). Watermark rows are
    observability stamps written after each walk's last commit (ADR-006).
    """
    base = base_url if base_url is not None else config.rest_base_url()
    transport = transport if transport is not None else urllib_transport
    policy = policy if policy is not None else default_policy()
    summary = {}
    for name, path, land in (
        ("rest_products", "/products", land_rest_products),
        ("rest_promotions", "/promotions", land_rest_promotions),
    ):
        pages = 0
        seen = 0
        for data in _walk_pages(path, base_url=base, transport=transport, policy=policy, sleep=sleep):
            rows, rejects = [], []
            for position, item in enumerate(data, start=1):
                pk = item.get("id") if isinstance(item, dict) else None
                if isinstance(pk, int):
                    rows.append((pk, item))
                else:
                    rejects.append((name, name, name, seen + position, "missing_pk", str(item)[:300]))
            rows, coalesced = coalesce_rows(rows)
            with conn.transaction():
                landed, unchanged = land(conn, rows, load_id, name)
                if rejects:
                    quarantined = land_quarantine(conn, rejects, load_id)
                else:
                    quarantined = 0
                stats.rows_read += len(data)
                stats.rows_landed += landed
                stats.rows_unchanged += unchanged
                stats.rows_coalesced += coalesced
                stats.rows_quarantined += quarantined
                stats.units_done += 1
                loads.update_load(conn, load_id, stats)
            seen += len(data)
            pages += 1
            if pages >= config.rest_max_pages():
                raise RuntimeError(f"{path}: exceeded rest_max_pages={config.rest_max_pages()} with a non-empty cursor (buggy cursor?)")
        with conn.transaction():
            watermarks.advance_watermark(conn, name, datetime.now(timezone.utc).isoformat(timespec="seconds"), LAST_FULL_WALK)
        summary[name] = {"pages": pages, "rows": seen}
        log("rest_walk_done", source=name, pages=pages, rows=seen)
    return summary
