"""Settings for the ingest lib, sourced from the environment only (ADR-006/007).

Credentials never have defaults — a missing variable fails fast with an actionable
message (cdc-sink config precedent). Source base URLs are validated against
explicit host allowlists here, before any request is built anywhere in this
package: the REST allowlist lives next to the urllib build in extract_rest, the
SOAP one next to the zeep client build in extract_soap, and both share the
validator below.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

# Hosts the two HTTP extractors may ever talk to: the compose-network service
# names plus loopback for local fakes in tests. Each extractor binds its own
# strict subset (extract_soap / extract_rest) next to the request build.
ALLOWED_HTTP_HOSTS = frozenset({"soap-service", "rest-mock", "localhost", "127.0.0.1"})


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"environment variable {name} is required (see .env.example / compose service env)"
        )
    return value


def _connect_settings(prefix: str, host_env: str, host_default: str) -> dict:
    return {
        "host": os.environ.get(host_env, host_default),
        "port": int(os.environ.get(f"{prefix}PGPORT", "5432")),
        "user": _require(f"{prefix}USER"),
        "password": _require(f"{prefix}PASSWORD"),
        "dbname": _require(f"{prefix}DB"),
    }


def warehouse_settings() -> dict:
    """psycopg kwargs for the warehouse (raw zone) connection."""
    return _connect_settings("WAREHOUSE_POSTGRES_", "INGEST_PGHOST", "warehouse-db")


def validate_source_base_url(url: str, allowed_hosts: frozenset[str]) -> str:
    """Return the canonical base URL if (and only if) it passes the allowlist."""
    parsed = urlparse(url)
    if parsed.scheme != "http":
        raise RuntimeError(f"source base URL scheme must be http, got: {parsed.scheme!r}")
    if parsed.username or parsed.password:
        raise RuntimeError("source base URL must not embed credentials; use the environment")
    if parsed.hostname not in allowed_hosts:
        raise RuntimeError(
            f"source base URL host {parsed.hostname!r} is not allowlisted "
            f"(allowed: {sorted(allowed_hosts)})"
        )
    port = parsed.port if parsed.port is not None else 80
    if not 1 <= port <= 65535:
        raise RuntimeError(f"source base URL port out of range: {port}")
    if parsed.path not in ("", "/"):
        raise RuntimeError(f"source base URL path must be empty, got: {parsed.path!r}")
    return f"http://{parsed.hostname}:{port}"


def soap_base_url() -> str:
    """Validated SOAP service base URL (in-network default)."""
    return validate_source_base_url(
        os.environ.get("INGEST_SOAP_BASE_URL", "http://soap-service:8000"),
        frozenset({"soap-service", "localhost", "127.0.0.1"}),
    )


def soap_auth() -> tuple[str, str]:
    """SOAP HTTP basic auth credentials from the environment only."""
    return _require("SOAP_BASIC_AUTH_USER"), _require("SOAP_BASIC_AUTH_PASSWORD")


def rest_base_url() -> str:
    """Validated REST mock base URL (in-network default)."""
    return validate_source_base_url(
        os.environ.get("INGEST_REST_BASE_URL", "http://rest-mock:8000"),
        frozenset({"rest-mock", "localhost", "127.0.0.1"}),
    )


def filedrop_dir() -> str:
    return os.environ.get("INGEST_FILEDROP_DIR", "/data/drop")


def soap_page_size() -> int:
    return int(os.environ.get("INGEST_SOAP_PAGE_SIZE", "500"))


def soap_overlap_days() -> int:
    """ADR-006: re-pull window behind the watermark; absorbs OFFSET-shift."""
    return int(os.environ.get("INGEST_SOAP_OVERLAP_DAYS", "7"))


def rest_page_limit() -> int:
    return int(os.environ.get("INGEST_REST_PAGE_LIMIT", "100"))


def rest_max_pages() -> int:
    """Safety bound for a cursor walk; a buggy cursor fails loudly, not forever."""
    return int(os.environ.get("INGEST_REST_MAX_PAGES", "1000"))


def soap_max_pages() -> int:
    """Safety bound for the GetOrders page walk."""
    return int(os.environ.get("INGEST_SOAP_MAX_PAGES", "100000"))


def retry_max_attempts() -> int:
    return int(os.environ.get("INGEST_RETRY_MAX_ATTEMPTS", "8"))


def retry_backoff_base() -> float:
    return float(os.environ.get("INGEST_RETRY_BACKOFF_BASE", "0.5"))


def retry_backoff_factor() -> float:
    return float(os.environ.get("INGEST_RETRY_BACKOFF_FACTOR", "2.0"))


def retry_backoff_max() -> float:
    return float(os.environ.get("INGEST_RETRY_BACKOFF_MAX", "8.0"))


def retry_after_default() -> float:
    """Fallback sleep when a 429 arrives without a usable Retry-After (ADR-006)."""
    return float(os.environ.get("INGEST_RETRY_AFTER_DEFAULT", "5"))
