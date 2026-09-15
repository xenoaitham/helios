"""SOAP extractor: windowed GetOrders pull with a max(created_at) cursor (ADR-006).

Each run pulls [watermark − overlap, now] page by page (page_size ≤ 500; the
service pages with OFFSET under the hood — ADR-001 residual), landing every page
in its own transaction. The watermark advances only AFTER the last landing
commit, in its own transaction: a crash in between replays the window, which the
content-hash guard absorbs as no-ops.

Interop notes (ADR-001 addendum): spyne emits named array wrappers that zeep
does NOT flatten — `page.orders.Order` is the repeated child, and a
single-order response comes back UNWRAPPED (the child is the object itself, not
a list). `orders_of()` handles both. Timestamps are naive UTC
'YYYY-MM-DD HH:MM:SS' strings at the source; they land verbatim in the payload.

Security posture: the base URL is validated against this module's allowlist
BEFORE the zeep client (which fetches the WSDL and posts SOAP calls) is built;
basic-auth credentials come from the environment only.

Endpoint pinning (Session 8 incident, 2026-09-12): the spyne-served WSDL's
soap:address alternates between http://localhost:8000/ and
http://soap-service:8000/ across requests (measured; independent of the
request's Host header), and zeep by default POSTs to the WSDL-declared
address — so an ingest container that drew the `localhost` variant died with
Connection refused. `build_client` therefore pins the service endpoint to the
validated base URL via zeep's public `create_service(binding, address)`; the
WSDL is only used for the contract (types/operations), never for routing.
"""

from __future__ import annotations

import datetime as dt

import requests
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport

from ingest import config, loads, watermarks
from ingest.landing import coalesce_rows, land_soap_orders
from ingest.log import log
from ingest.watermarks import MAX_CREATED_AT

# Only this compose service (plus loopback), validated before client build.
ALLOWED_HOSTS = frozenset({"soap-service", "localhost", "127.0.0.1"})

# Service binding from the frozen contract (soap-service/contract/…wsdl,
# ADR-001): <wsdl:binding name="OrderManagement"> in tns. The contract drift
# test guards a rename; used to pin the endpoint (module docstring).
BINDING_QNAME = "{urn:helios:soap:ordermanagement:v1}OrderManagement"

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
EPOCH = "1970-01-01 00:00:00"
SOURCE = "soap_orders"


def build_client(base_url: str | None = None, auth: tuple[str, str] | None = None):
    """Validated-base-URL zeep service proxy with the endpoint PINNED to base.

    The WSDL is fetched from `{base}/?wsdl` for the contract only; the returned
    proxy POSTs to `base` itself — never to the WSDL-declared soap:address,
    which spyne serves nondeterministically as localhost or soap-service.
    """
    base = config.validate_source_base_url(base_url or config.soap_base_url(), ALLOWED_HOSTS)
    user, password = auth if auth is not None else config.soap_auth()
    session = requests.Session()
    session.auth = HTTPBasicAuth(user, password)
    client = Client(f"{base}/?wsdl", transport=Transport(session=session))
    return client.create_service(BINDING_QNAME, base)


def window_start(watermark_value: str, overlap_days: int) -> dt.datetime:
    """Cursor minus the overlap that absorbs OFFSET-shift (ADR-006)."""
    current = dt.datetime.strptime(watermark_value, TS_FORMAT)
    return current - dt.timedelta(days=overlap_days)


def fmt(value) -> str:
    """Naive source datetime -> the wire/storage format, verbatim."""
    return value.strftime(TS_FORMAT)


def order_to_row(order) -> dict:
    """Map one zeep Order object to the landed payload (strings stay strings)."""
    return {
        "order_id": int(order.order_id),
        "customer_id": int(order.customer_id),
        "status": str(order.status),
        "total_amount": str(order.total_amount),
        "currency": str(order.currency),
        "created_at": fmt(order.created_at),
        "updated_at": fmt(order.updated_at),
    }


def orders_of(page) -> list:
    """Unwrap the spyne array wrapper (ADR-001): `page.orders.Order` is the
    repeated child; a single-order response arrives as the bare object."""
    wrapper = getattr(page, "orders", None)
    if wrapper is None:
        return []
    rows = getattr(wrapper, "Order", None)
    if rows is None:
        return []
    return rows if isinstance(rows, list) else [rows]


def run_soap(conn, load_id, stats: loads.RunStats, *, client=None, page_size: int | None = None, overlap_days: int | None = None, full: bool = False, max_pages: int | None = None) -> dict:
    """Pull the watermark window and land it page by page; then advance cursor.

    `full=True` re-pulls history from the epoch (backfill / refresh of old
    status changes — unchanged rows no-op through the hash guard). `client`
    is the pinned service proxy from `build_client()` (tests inject a stub
    exposing GetOrders directly).
    """
    client = client if client is not None else build_client()
    page_size = page_size if page_size is not None else config.soap_page_size()
    overlap_days = overlap_days if overlap_days is not None else config.soap_overlap_days()
    max_pages = max_pages if max_pages is not None else config.soap_max_pages()

    current = watermarks.get_watermark(conn, SOURCE)
    watermark_value = current[0] if current is not None else EPOCH
    date_from = dt.datetime.strptime(EPOCH, TS_FORMAT) if full or current is None else window_start(watermark_value, overlap_days)
    log("soap_window", watermark=watermark_value, date_from=date_from.strftime(TS_FORMAT), full=full)

    page_number = 1
    latest_total_pages = 1
    max_created = None
    while page_number <= latest_total_pages:
        if page_number > max_pages:
            raise RuntimeError(f"GetOrders exceeded max_pages={max_pages} (total_pages={latest_total_pages})")
        page = client.GetOrders(page=page_number, page_size=page_size, date_from=date_from)
        latest_total_pages = max(1, int(page.total_pages))
        stats.units_total = latest_total_pages
        rows = [order_to_row(o) for o in orders_of(page)]
        for row in rows:
            created = row["created_at"]
            if max_created is None or created > max_created:
                max_created = created
        rows, coalesced = coalesce_rows([(row["order_id"], row) for row in rows])
        with conn.transaction():
            landed, unchanged = land_soap_orders(conn, rows, load_id, SOURCE)
            stats.rows_read += len(rows) + coalesced
            stats.rows_landed += landed
            stats.rows_unchanged += unchanged
            stats.rows_coalesced += coalesced
            stats.units_done = page_number
            loads.update_load(conn, load_id, stats)
        log("soap_page_landed", page=page_number, total_pages=latest_total_pages, rows=len(rows), landed=landed, unchanged=unchanged)
        page_number += 1

    if max_created is not None:
        # ADR-006: advance AFTER the last landing commit — never-regress guard inside.
        with conn.transaction():
            advanced = watermarks.advance_watermark(conn, SOURCE, max_created, MAX_CREATED_AT)
        log("soap_watermark_advanced", watermark=max_created, advanced=advanced)
    return {"pages": page_number - 1, "max_created_at": max_created}
