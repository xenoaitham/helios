"""SOAP extractor tests: wrapper unwrapping (ADR-001 addendum), row mapping,
window math, and the full land-then-replay cycle against fake pages. The real
zeep round-trip runs live via `make ingest-soap` (VERIFIER evidence).
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from types import SimpleNamespace

from ingest import loads
from ingest.extract_soap import EPOCH, TS_FORMAT, order_to_row, orders_of, run_soap, window_start


def _order(oid, created, updated=None, status="NEW"):
    return SimpleNamespace(
        order_id=oid,
        customer_id=oid + 100,
        status=status,
        total_amount=Decimal("19.99"),
        currency="USD",
        created_at=dt.datetime.strptime(created, TS_FORMAT),
        updated_at=dt.datetime.strptime(updated or created, TS_FORMAT),
    )


def _page(orders, total_pages=1, total_results=None):
    wrapper = SimpleNamespace(Order=orders)
    count = len(orders) if isinstance(orders, list) else (1 if orders is not None else 0)
    return SimpleNamespace(page=1, page_size=count or 1, total_results=total_results if total_results is not None else count, total_pages=total_pages, orders=wrapper)


def test_unwrap_multi_order_wrapper():
    page = _page([_order(1, "2026-09-01 00:00:00"), _order(2, "2026-09-02 00:00:00")])
    assert len(orders_of(page)) == 2


def test_unwrap_single_order_arrives_bare():
    # ADR-001 addendum: single-order responses do NOT come back as a list
    page = _page(_order(1, "2026-09-01 00:00:00"))
    rows = orders_of(page)
    assert isinstance(rows, list) and len(rows) == 1


def test_unwrap_empty_and_missing():
    assert orders_of(SimpleNamespace(orders=SimpleNamespace(Order=[]))) == []
    assert orders_of(SimpleNamespace(orders=SimpleNamespace(Order=None))) == []
    assert orders_of(SimpleNamespace(orders=None)) == []


def test_order_to_row_maps_verbatim_strings():
    row = order_to_row(_order(7, "2026-09-10 08:00:00", "2026-09-10 09:00:00", "PROCESSING"))
    assert row == {
        "order_id": 7,
        "customer_id": 107,
        "status": "PROCESSING",
        "total_amount": "19.99",  # Decimal -> string; raw stays exact
        "currency": "USD",
        "created_at": "2026-09-10 08:00:00",
        "updated_at": "2026-09-10 09:00:00",
    }


def test_window_start_subtracts_overlap():
    start = window_start("2026-09-04 00:00:00", 7)
    assert start == dt.datetime(2026, 9, 4) - dt.timedelta(days=7)


class FakeClient:
    """GetOrders over an in-memory list, page_size pagination, created_at filter.

    Mimics the pinned zeep ServiceProxy from build_client(): GetOrders lives on
    the client object itself (endpoint pinning — see extract_soap module doc).
    """

    def __init__(self, orders):
        self.orders = orders
        self.seen_date_from = []

    def GetOrders(self, page=None, page_size=None, status=None, date_from=None, date_to=None):
        date_from_str = date_from.strftime(TS_FORMAT) if isinstance(date_from, dt.datetime) else date_from
        self.seen_date_from.append(date_from_str)
        window = [o for o in self.orders if o["created_at"] >= date_from_str]
        page_size = page_size or 500
        total_pages = max(1, -(-len(window) // page_size))
        chunk = window[(page - 1) * page_size : page * page_size]
        wrapped = SimpleNamespace(Order=[_order(o["order_id"], o["created_at"], o["updated_at"], o["status"]) for o in chunk])
        return SimpleNamespace(page=page, page_size=page_size, total_results=len(window), total_pages=total_pages, orders=wrapped)


def _orders(n):
    base = dt.datetime(2026, 9, 1)
    return [
        {"order_id": i, "created_at": (base + dt.timedelta(hours=i)).strftime(TS_FORMAT), "updated_at": (base + dt.timedelta(hours=i)).strftime(TS_FORMAT), "status": "NEW"}
        for i in range(1, n + 1)
    ]


def test_full_run_lands_all_pages_and_advances_watermark(tconn):
    client = FakeClient(_orders(10))
    stats = loads.RunStats()
    run_soap(tconn, uuid.uuid4(), stats, client=client, page_size=4, overlap_days=7)
    assert stats.rows_landed == 10 and stats.units_done == 3  # pages of 4/4/2
    assert tconn.execute("SELECT count(*) FROM raw.soap_orders").fetchone()[0] == 10
    wm = tconn.execute("SELECT watermark_value, watermark_kind FROM raw.ingest_watermarks WHERE source = 'soap_orders'").fetchone()
    assert wm == ("2026-09-01 10:00:00", "max_created_at")  # max created_at of order 10
    assert client.seen_date_from[0] == EPOCH  # no prior cursor -> full history window


def test_incremental_run_pulls_only_the_overlap_window(tconn):
    run_soap(tconn, uuid.uuid4(), loads.RunStats(), client=FakeClient(_orders(10)), page_size=4, overlap_days=7)
    incremental = FakeClient(_orders(12))
    stats = loads.RunStats()
    run_soap(tconn, uuid.uuid4(), stats, client=incremental, page_size=500, overlap_days=7)
    # window = 2026-09-01 10:00:00 - 7d = 2026-08-25 10:00:00 -> all 12 orders are >= that
    assert incremental.seen_date_from[0] == "2026-08-25 10:00:00"
    assert stats.rows_landed == 2  # the two new orders
    assert stats.rows_unchanged == 10  # the overlap re-pull no-ops through the hash guard


def test_replay_after_crash_between_land_and_advance_is_absorbed(tconn):
    # run 1 lands + advances; run 2 replays the SAME window (as a crash before
    # advance would cause): everything no-ops, the cursor cannot regress
    client = FakeClient(_orders(10))
    run_soap(tconn, uuid.uuid4(), loads.RunStats(), client=client, page_size=4, overlap_days=7)
    stats = loads.RunStats()
    run_soap(tconn, uuid.uuid4(), stats, client=FakeClient(_orders(10)), page_size=4, overlap_days=7)
    assert stats.rows_landed == 0 and stats.rows_unchanged == 10
    assert tconn.execute("SELECT count(*) FROM raw.soap_orders").fetchone()[0] == 10  # zero dupes
    assert tconn.execute("SELECT watermark_value FROM raw.ingest_watermarks WHERE source = 'soap_orders'").fetchone()[0] == "2026-09-01 10:00:00"
