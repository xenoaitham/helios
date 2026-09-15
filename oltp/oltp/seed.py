"""Deterministic bulk seeder for the HELIOS OLTP source (ADR-002).

Business attributes are pure functions of id; two fixed-seed RNG streams add
variety (users, order statuses). All four tables load via COPY inside ONE
transaction — a crash rolls back to a clean empty database, so `--if-empty`
can never observe a half-loaded state. Constraints and indexes are ensured
AFTER the load (fast path) and re-ensured on every run (self-healing), and
identity sequences are re-synced so post-seed INSERTs (the mutator) work.

Every SQL statement is a single static string literal at its call site,
fully parameterized where values vary (Mimosa source rule). The fixed-seed
`random` use here is synthetic-data determinism, not security.
"""

from __future__ import annotations

import argparse
import os
import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from oltp import schema
from oltp.db import connect

ORDER_STATUSES = ("DELIVERED", "CANCELLED", "SHIPPED", "PROCESSING", "NEW")
_STATUS_WEIGHTS = (82, 8, 5, 3, 2)
_STATUS_CUMULATIVE = tuple(sum(_STATUS_WEIGHTS[: i + 1]) for i in range(len(_STATUS_WEIGHTS)))

FIRST_NAMES = ("ada", "bruno", "camila", "dmitri", "elif", "farid", "greta", "hiro", "ines", "jonas", "kira", "liam", "mara", "noor", "oskar", "pia", "quintin", "ronja", "sven", "tara", "umar", "vera", "wren", "xavi", "yara", "zane", "alba", "borys", "cleo", "dario", "emre", "freya")
LAST_NAMES = ("abbott", "bauer", "costa", "duarte", "egan", "fischer", "gomez", "hartmann", "ibarra", "jensen", "keller", "lindqvist", "moreau", "novak", "oliveira", "petrov", "quintero", "richter", "santos", "tanaka", "vogel", "wagner", "xu", "yilmaz", "zhang", "alfaro", "brant", "cohen", "delfino", "eastwood", "farrow", "gustafsson")
COUNTRIES = ("DE", "GB", "US", "FR", "NL", "PL", "SE", "ES", "JP", "BR")
PAYMENT_METHODS = ("card", "paypal", "bank_transfer", "gift_card")

THREE_YEARS_SECONDS = 3 * 365 * 86400


def _draw_status(rng: random.Random) -> str:
    roll = rng.randrange(100)
    for status, ceiling in zip(ORDER_STATUSES, _STATUS_CUMULATIVE):
        if roll < ceiling:
            return status
    return "DELIVERED"


def item_count(order_id: int) -> int:
    """3..14 items per order (avg 8.5), pure function of the order id."""
    return 3 + (order_id * 2654435761) % 12


def item_spec(order_id: int, k: int) -> tuple[str, int, int]:
    """(sku, quantity, unit_price_cents) — pure function of (order_id, item index)."""
    sku_num = (order_id * 7919 + k * 104729) % 100000
    return f"SKU-{sku_num:05d}", 1 + (order_id + k) % 5, 199 + (sku_num * 613) % 14999


def order_total_cents(order_id: int) -> int:
    """Order total derived from the same item specs the items table gets —
    totals can never disagree with the line items."""
    return sum(qty * cents for _, qty, cents in (item_spec(order_id, k) for k in range(item_count(order_id))))


def payment_count(order_id: int) -> int:
    """Every order has one payment; every 5th also has a REFUNDED reversal."""
    return 2 if order_id % 5 == 0 else 1


def _placed_at(order_id: int, start: datetime, total_orders: int) -> datetime:
    """Uniformly spread over exactly 3 years, strictly increasing with order_id."""
    return start + timedelta(seconds=((order_id - 1) * THREE_YEARS_SECONDS) // total_orders)


def _user_rows(count: int, end: datetime):
    rng = random.Random(7)
    for uid in range(1, count + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        created = end - timedelta(days=1096 + rng.random() * 730)
        active = rng.random() > 0.05
        last_login = None if rng.random() < 0.10 else end - timedelta(days=rng.random() * 90)
        country = rng.choice(COUNTRIES)
        yield (uid, f"{first}.{last}.{uid}@example.com", f"{first.capitalize()} {last.capitalize()}", country, created, last_login, active)


def _counts(conn) -> dict:
    row = conn.execute("SELECT (SELECT COUNT(*) FROM public.users), (SELECT COUNT(*) FROM public.orders), (SELECT COUNT(*) FROM public.order_items), (SELECT COUNT(*) FROM public.payments)").fetchone()
    return {"users": row[0], "orders": row[1], "order_items": row[2], "payments": row[3]}


def _sync_sequences(conn) -> None:
    """Identity columns are BY DEFAULT (explicit ids during COPY); after the
    load the sequences must be past the max id or mutator INSERTs collide."""
    with conn.transaction():
        conn.execute("SELECT setval(pg_get_serial_sequence('public.users', 'user_id'), COALESCE((SELECT MAX(user_id) FROM public.users), 0) + 1, false)")
        conn.execute("SELECT setval(pg_get_serial_sequence('public.orders', 'order_id'), COALESCE((SELECT MAX(order_id) FROM public.orders), 0) + 1, false)")
        conn.execute("SELECT setval(pg_get_serial_sequence('public.order_items', 'order_item_id'), COALESCE((SELECT MAX(order_item_id) FROM public.order_items), 0) + 1, false)")
        conn.execute("SELECT setval(pg_get_serial_sequence('public.payments', 'payment_id'), COALESCE((SELECT MAX(payment_id) FROM public.payments), 0) + 1, false)")


def seed_database(conn, *, users: int, orders: int, if_empty: bool = False, reset: bool = False) -> dict:
    """Apply schema, (optionally reset), COPY all four tables in one transaction,
    ensure constraints/indexes, sync sequences, verify counts by SQL."""
    started = time.monotonic()
    schema.ensure_tables(conn)
    already = bool(conn.execute("SELECT EXISTS (SELECT 1 FROM public.orders)").fetchone()[0])
    if reset:
        with conn.transaction():
            conn.execute("TRUNCATE TABLE public.users, public.orders, public.order_items, public.payments RESTART IDENTITY CASCADE")
        already = False
    if already:
        if not if_empty:
            raise RuntimeError("oltp database already contains orders; pass if_empty=True or reset=True")
        t_heal = time.monotonic()
        schema.ensure_constraints(conn)
        schema.ensure_indexes(conn)
        _sync_sequences(conn)
        return {"skipped": True, "heal_seconds": round(time.monotonic() - t_heal, 2), "counts": _counts(conn), "elapsed_s": round(time.monotonic() - started, 2)}

    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(seconds=THREE_YEARS_SECONDS)
    t_user = time.monotonic()
    with conn.transaction():
        with conn.cursor().copy("COPY public.users (user_id, email, full_name, country_code, created_at, last_login_at, is_active) FROM STDIN") as cp:
            for row in _user_rows(users, end):
                cp.write_row(row)
        t_order = time.monotonic()
        with conn.cursor().copy("COPY public.orders (order_id, user_id, status, currency, total_amount, placed_at, updated_at) FROM STDIN") as cp:
            rng_status = random.Random(42)
            for oid in range(1, orders + 1):
                placed = _placed_at(oid, start, orders)
                cp.write_row((oid, 1 + (oid * 9973) % users, _draw_status(rng_status), "USD", Decimal(order_total_cents(oid)) / 100, placed, placed))
        t_items = time.monotonic()
        with conn.cursor().copy("COPY public.order_items (order_item_id, order_id, product_sku, quantity, unit_price) FROM STDIN") as cp:
            item_id = 0
            for oid in range(1, orders + 1):
                for k in range(item_count(oid)):
                    item_id += 1
                    sku, qty, cents = item_spec(oid, k)
                    cp.write_row((item_id, oid, sku, qty, Decimal(cents) / 100))
        t_pay = time.monotonic()
        with conn.cursor().copy("COPY public.payments (payment_id, order_id, method, amount, status, paid_at) FROM STDIN") as cp:
            payment_id = 0
            for oid in range(1, orders + 1):
                payment_id += 1
                paid = _placed_at(oid, start, orders) + timedelta(hours=oid % 72)
                method = PAYMENT_METHODS[oid % 4]
                total = Decimal(order_total_cents(oid)) / 100
                status = "PENDING" if oid % 97 == 0 else "CAPTURED"
                cp.write_row((payment_id, oid, method, total, status, paid))
                if oid % 5 == 0:
                    payment_id += 1
                    cp.write_row((payment_id, oid, method, -total, "REFUNDED", paid + timedelta(hours=24)))
    t_constraint = time.monotonic()

    schema.ensure_constraints(conn)
    schema.ensure_indexes(conn)
    _sync_sequences(conn)
    return {
        "skipped": False,
        "rows": {"users": users, "orders": orders, "order_items": item_id, "payments": payment_id},
        "counts": _counts(conn),
        "seconds": {
            "load": round(t_constraint - t_user, 2),
            "constraints_and_verify": round(time.monotonic() - t_constraint, 2),
            "total": round(time.monotonic() - started, 2),
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="HELIOS OLTP seeder (ADR-002)")
    parser.add_argument("--users", type=int, default=int(os.environ.get("OLTP_SEED_USERS", "50000")))
    parser.add_argument("--orders", type=int, default=int(os.environ.get("OLTP_SEED_ORDERS", "500000")))
    parser.add_argument("--if-empty", action="store_true", help="skip seeding when orders already exist (still self-heals constraints)")
    parser.add_argument("--reset", action="store_true", help="DESTRUCTIVE: truncate the OLTP source tables before loading")
    parser.add_argument("--report", action="store_true", help="print the per-table timing/count report")
    args = parser.parse_args(argv)

    with connect() as conn:
        report = seed_database(conn, users=args.users, orders=args.orders, if_empty=args.if_empty, reset=args.reset)
    if args.report:
        if report.get("skipped"):
            print(f"[seed] store already seeded ({report['counts']['orders']} orders); skipping (--if-empty)")
            print(f"[seed] self-heal check (constraints/indexes/sequences) in {report['heal_seconds']}s")
        else:
            rows = report["rows"]
            total_rows = sum(rows.values())
            print(f"[seed] loaded {total_rows} rows in {report['seconds']['load']}s (COPY, single transaction)")
            for table in ("users", "orders", "order_items", "payments"):
                print(f"[seed]   {table}={rows[table]}")
            print(f"[seed] constraints+indexes+sequences+verify in {report['seconds']['constraints_and_verify']}s")
            print(f"[seed] verified counts by SQL: {report['counts']}")
            print(f"[seed] wall clock {report['seconds']['total']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
