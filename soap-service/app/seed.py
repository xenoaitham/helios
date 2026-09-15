"""Deterministic seeder for the legacy OrderManagement store.

Generates ~3 years of order history ending yesterday (UTC) into the SQLite
store: a linear ramp of daily order volume, age-dependent status mix with
consistent state-machine transition chains, and per-product stable prices.
Deterministic for a given (rng_seed, days, first_day_orders, last_day_orders)
so evidence and tests are reproducible.

Usage (inside the container):
  python -m app.seed --if-empty --report      # no-op when already seeded
  python -m app.seed --reset --report         # DESTRUCTIVE: drops the SOAP store
"""
import argparse
import datetime as dt
import os
import random
import sqlite3
import time

from .db import connect, db_path, ensure_schema

CUSTOMER_COUNT = 5000
PRODUCT_COUNT = 2000
BATCH_ORDERS = 20000
DEFAULT_DAYS = 1096
DEFAULT_RNG_SEED = 20260909

_ITEM_COUNT_WEIGHTS = ((1, 45), (2, 25), (3, 15), (4, 10), (5, 5))

# Transition chains are consistent with ALLOWED_TRANSITIONS (models.py); the
# status for each order is drawn from an age-dependent mix, then expanded into
# the full chain that would have produced it.
_STEP_HOURS = {
    ("NEW", "PROCESSING"): (1, 12),
    ("NEW", "CANCELLED"): (1, 24),
    ("PROCESSING", "CANCELLED"): (1, 24),
    ("PROCESSING", "SHIPPED"): (6, 48),
    ("SHIPPED", "DELIVERED"): (24, 96),
}


def _status_for(age_days, rng):
    r = rng.random()
    if age_days > 30:
        if r < 0.90:
            return "DELIVERED"
        if r < 0.97:
            return "CANCELLED"
        return "SHIPPED"
    if age_days > 7:
        if r < 0.55:
            return "DELIVERED"
        if r < 0.85:
            return "SHIPPED"
        if r < 0.95:
            return "PROCESSING"
        return "CANCELLED"
    if r < 0.40:
        return "NEW"
    if r < 0.75:
        return "PROCESSING"
    if r < 0.90:
        return "SHIPPED"
    if r < 0.95:
        return "DELIVERED"
    return "CANCELLED"


def _path_for(status, rng):
    if status == "NEW":
        return ["NEW"]
    if status == "CANCELLED":
        return ["NEW", "CANCELLED"] if rng.random() < 0.5 else ["NEW", "PROCESSING", "CANCELLED"]
    if status == "PROCESSING":
        return ["NEW", "PROCESSING"]
    if status == "SHIPPED":
        return ["NEW", "PROCESSING", "SHIPPED"]
    return ["NEW", "PROCESSING", "SHIPPED", "DELIVERED"]


def _chain_timestamps(start, path, rng, now):
    stamps = [start]
    ts = start
    for prev, nxt in zip(path, path[1:]):
        low, high = _STEP_HOURS[(prev, nxt)]
        ts = ts + dt.timedelta(hours=rng.randint(low, high))
        if ts >= now:
            ts = now - dt.timedelta(minutes=1)
        if ts <= stamps[-1]:
            ts = stamps[-1] + dt.timedelta(minutes=1)
        stamps.append(ts)
    return stamps


def _fmt(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _flush(conn, order_rows, item_rows, history_rows):
    conn.executemany(
        "INSERT INTO orders (order_id, customer_id, status, total_cents, currency, created_at, updated_at) VALUES (?, ?, ?, ?, 'USD', ?, ?)",
        order_rows,
    )
    conn.executemany(
        "INSERT INTO order_items (order_id, line_no, product_id, quantity, unit_price_cents) VALUES (?, ?, ?, ?, ?)",
        item_rows,
    )
    conn.executemany(
        "INSERT INTO order_status_history (order_id, from_status, to_status, note, changed_at) VALUES (?, ?, ?, NULL, ?)",
        history_rows,
    )
    order_rows.clear()
    item_rows.clear()
    history_rows.clear()


def generate(conn, days=DEFAULT_DAYS, rng_seed=DEFAULT_RNG_SEED,
             first_day_orders=220, last_day_orders=480):
    """Load the full history; returns the number of orders inserted."""
    rng = random.Random(rng_seed)
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    product_prices = [rng.randint(100, 200_000) for _ in range(PRODUCT_COUNT)]
    populations = [qty for qty, _ in _ITEM_COUNT_WEIGHTS]
    weights = [w for _, w in _ITEM_COUNT_WEIGHTS]

    end_date = (now - dt.timedelta(days=1)).date()
    start_date = end_date - dt.timedelta(days=days - 1)
    span = max(days - 1, 1)

    # Bulk-load pragmas; WAL + NORMAL are restored afterwards for the service.
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")

    order_rows, item_rows, history_rows = [], [], []
    order_id = 0

    for day_index in range(days):
        date = start_date + dt.timedelta(days=day_index)
        base = first_day_orders + (last_day_orders - first_day_orders) * day_index // span
        orders_today = max(0, base + rng.randint(-40, 40))
        age_days = (end_date - date).days

        for _ in range(orders_today):
            order_id += 1
            created = dt.datetime.combine(
                date,
                dt.time(rng.randrange(24), rng.randrange(60), rng.randrange(60)),
            )
            customer_id = rng.randint(1, CUSTOMER_COUNT)
            n_items = rng.choices(populations, weights=weights)[0]
            chosen = rng.sample(range(PRODUCT_COUNT), n_items)

            total_cents = 0
            for line_no, product_index in enumerate(chosen, start=1):
                price = product_prices[product_index]
                quantity = rng.randint(1, 3)
                total_cents += price * quantity
                item_rows.append((order_id, line_no, product_index + 1, quantity, price))

            status = _status_for(age_days, rng)
            path = _path_for(status, rng)
            stamps = _chain_timestamps(created, path, rng, now)
            order_rows.append(
                (order_id, customer_id, status, total_cents, _fmt(stamps[0]), _fmt(stamps[-1]))
            )
            history_rows.append((order_id, None, path[0], _fmt(stamps[0])))
            for step in range(1, len(path)):
                history_rows.append((order_id, path[step - 1], path[step], _fmt(stamps[step])))

        if len(order_rows) >= BATCH_ORDERS:
            _flush(conn, order_rows, item_rows, history_rows)
            conn.commit()

    _flush(conn, order_rows, item_rows, history_rows)
    conn.commit()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return order_id


def report(conn):
    total = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
    items = conn.execute("SELECT count(*) FROM order_items").fetchone()[0]
    history = conn.execute("SELECT count(*) FROM order_status_history").fetchone()[0]
    revenue = conn.execute("SELECT coalesce(sum(total_cents), 0) FROM orders").fetchone()[0]
    window = conn.execute("SELECT min(created_at), max(created_at) FROM orders").fetchone()
    print(f"[seed][report] orders={total} order_items={items} status_history={history}")
    print(f"[seed][report] gross_total_usd={revenue / 100:.2f}")
    print(f"[seed][report] history_window={window[0]} .. {window[1]}")
    for row in conn.execute("SELECT status, count(*) FROM orders GROUP BY status ORDER BY status"):
        print(f"[seed][report] status {row[0]}={row[1]}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Seed the OrderManagement store.")
    parser.add_argument("--db", default=db_path())
    parser.add_argument("--if-empty", action="store_true",
                        help="skip when the store already has orders (container boot path)")
    parser.add_argument("--reset", action="store_true",
                        help="DESTRUCTIVE: delete the SOAP store file, then reseed")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--rng-seed", type=int, default=DEFAULT_RNG_SEED)
    parser.add_argument("--first-day-orders", type=int, default=220)
    parser.add_argument("--last-day-orders", type=int, default=480)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args(argv)

    if args.reset:
        for suffix in ("", "-wal", "-shm"):
            path = args.db + suffix
            if os.path.exists(path):
                os.remove(path)
                print(f"[seed] removed {path}")

    conn = connect(args.db)
    ensure_schema(conn)
    existing = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
    if existing and args.if_empty:
        print(f"[seed] store already seeded ({existing} orders); skipping (--if-empty)")
        return 0
    if existing and not args.reset:
        print(f"[seed] ERROR: store already has {existing} orders; use --reset to reseed",
              file=__import__("sys").stderr)
        return 1

    started = time.perf_counter()
    count = generate(
        conn,
        days=args.days,
        rng_seed=args.rng_seed,
        first_day_orders=args.first_day_orders,
        last_day_orders=args.last_day_orders,
    )
    elapsed = time.perf_counter() - started
    print(f"[seed] inserted {count} orders in {elapsed:.1f}s")
    if args.report:
        report(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
