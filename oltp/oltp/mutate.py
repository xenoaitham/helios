"""Continuous mutation loop for the HELIOS OLTP source (ADR-002).

Makes CDC (Phase 2) meaningful: every ~2 s one transaction walks a batch of
orders through the status machine, touches user logins, inserts 1–3 new
orders (with items + payments), and deletes the oldest CANCELLED orders
(cascading items/payments, which bounds growth). A tiny HTTP /health on
:8081 lets the container healthcheck verify liveness.

Every SQL statement is a single static string literal at its call site,
fully parameterized (Mimosa source rule). The loop RNG is plain `random`
(row-picking only — no security use).
"""

from __future__ import annotations

import json
import os
import random
import signal
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from oltp.db import connect
from oltp.seed import PAYMENT_METHODS

_HEALTH_PORT_DEFAULT = 8081
_HEALTH_STALE_SECONDS = 90


class HealthState:
    """Thread-safe liveness accumulator shared with the HTTP health thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ticks = 0
        self._last_ok_epoch = 0.0
        self._last_error = None
        self._totals = {
            "updated_orders": 0,
            "cancelled_orders": 0,
            "updated_users": 0,
            "inserted_orders": 0,
            "inserted_items": 0,
            "inserted_payments": 0,
            "deleted_orders": 0,
        }

    def record_ok(self, counters: dict) -> None:
        with self._lock:
            self._ticks += 1
            self._last_ok_epoch = time.time()
            for key, value in counters.items():
                self._totals[key] = self._totals.get(key, 0) + value

    def record_error(self, message: str) -> None:
        with self._lock:
            self._last_error = message

    def record_schema_missing(self) -> None:
        with self._lock:
            self._last_error = "schema not present yet"

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "ticks": self._ticks,
                "last_ok_epoch": self._last_ok_epoch,
                "last_error": self._last_error,
                "totals": dict(self._totals),
                "fresh": self._last_ok_epoch > 0 and (time.time() - self._last_ok_epoch) < _HEALTH_STALE_SECONDS,
            }


def _schema_ready(conn) -> bool:
    row = conn.execute("SELECT to_regclass('public.orders') IS NOT NULL").fetchone()
    return bool(row and row[0])


def run_tick(conn, rng: random.Random) -> dict:
    """One mutation transaction. Deterministic helpers are NOT used here: the
    loop must look like operational churn, not like the seed. Returns counters."""
    now = datetime.now(timezone.utc)
    counters = {
        "updated_orders": 0,
        "cancelled_orders": 0,
        "updated_users": 0,
        "inserted_orders": 0,
        "inserted_items": 0,
        "inserted_payments": 0,
        "deleted_orders": 0,
    }
    with conn.transaction():
        max_order_id = conn.execute("SELECT COALESCE(MAX(order_id), 0) FROM public.orders").fetchone()[0]
        max_user_id = conn.execute("SELECT COALESCE(MAX(user_id), 0) FROM public.users").fetchone()[0]

        if max_order_id:
            anchor = rng.randint(1, max_order_id)
            rows = conn.execute("SELECT order_id, status FROM public.orders WHERE order_id BETWEEN %s AND %s AND status IN ('NEW', 'PROCESSING', 'SHIPPED') LIMIT 20", (anchor, anchor + 400)).fetchall()
            order_ids = [r[0] for r in rows]
            if order_ids:
                cancel = rng.random() < 0.05
                updated = conn.execute("UPDATE public.orders SET status = CASE status WHEN 'NEW' THEN CASE WHEN %s THEN 'CANCELLED' ELSE 'PROCESSING' END WHEN 'PROCESSING' THEN 'SHIPPED' WHEN 'SHIPPED' THEN 'DELIVERED' ELSE status END, updated_at = %s WHERE order_id = ANY(%s)", (cancel, now, order_ids))
                counters["updated_orders"] = updated.rowcount
                if cancel:
                    counters["cancelled_orders"] = sum(1 for _, status in rows if status == "NEW")

        if max_user_id:
            user_anchor = rng.randint(1, max_user_id)
            user_ids = [r[0] for r in conn.execute("SELECT user_id FROM public.users WHERE user_id BETWEEN %s AND %s LIMIT 40", (user_anchor, user_anchor + 200)).fetchall()]
            if user_ids:
                touched = conn.execute("UPDATE public.users SET last_login_at = %s WHERE user_id = ANY(%s)", (now, user_ids))
                counters["updated_users"] = touched.rowcount

        for _ in range(rng.randint(1, 3)):
            if not max_user_id:
                break
            specs = []
            total_cents = 0
            for _ in range(rng.randint(3, 8)):
                sku_num = rng.randrange(100000)
                qty = rng.randint(1, 5)
                cents = 199 + rng.randrange(14999)
                total_cents += qty * cents
                specs.append((f"SKU-{sku_num:05d}", qty, Decimal(cents) / 100))
            new_order_id = conn.execute("INSERT INTO public.orders (user_id, status, currency, total_amount, placed_at, updated_at) VALUES (%s, 'NEW', 'USD', %s, %s, %s) RETURNING order_id", (rng.randint(1, max_user_id), Decimal(total_cents) / 100, now, now)).fetchone()[0]
            inserted = conn.execute("INSERT INTO public.order_items (order_id, product_sku, quantity, unit_price) SELECT %s, s.sku, s.qty, s.price FROM unnest(%s::text[], %s::int[], %s::numeric[]) AS s(sku, qty, price)", (new_order_id, [s[0] for s in specs], [s[1] for s in specs], [s[2] for s in specs]))
            counters["inserted_items"] += inserted.rowcount
            conn.execute("INSERT INTO public.payments (order_id, method, amount, status, paid_at) VALUES (%s, %s, %s, 'CAPTURED', %s)", (new_order_id, PAYMENT_METHODS[rng.randrange(4)], Decimal(total_cents) / 100, now))
            counters["inserted_orders"] += 1
            counters["inserted_payments"] += 1

        delete_limit = rng.randint(0, 2)
        if delete_limit:
            deleted = conn.execute("DELETE FROM public.orders WHERE order_id IN (SELECT order_id FROM public.orders WHERE status = 'CANCELLED' ORDER BY placed_at ASC LIMIT %s)", (delete_limit,))
            counters["deleted_orders"] = deleted.rowcount

    return counters


def _health_handler(state: HealthState):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib naming
            if self.path != "/health":
                self.send_error(404)
                return
            snap = state.snapshot()
            body = json.dumps(snap, default=str).encode()
            self.send_response(200 if snap["fresh"] else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep container logs about data, not probes
            pass

    return Handler


def main(argv=None) -> int:
    tick_seconds = float(os.environ.get("OLTP_MUTATE_TICK_SECONDS", "2"))
    port = int(os.environ.get("MUTATOR_HEALTH_PORT", str(_HEALTH_PORT_DEFAULT)))
    state = HealthState()
    server = ThreadingHTTPServer(("0.0.0.0", port), _health_handler(state))
    threading.Thread(target=server.serve_forever, daemon=True, name="health").start()

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    print(f"[mutate] started: tick={tick_seconds}s health=http://0.0.0.0:{port}/health", flush=True)
    conn = None
    rng = random.Random()
    while not stop.is_set():
        try:
            if conn is None or conn.closed:
                # autocommit: baseline reads (e.g. the schema check) must not open
                # an implicit transaction, or run_tick's transaction() would become
                # a savepoint inside one never-committed transaction (CRITIC finding).
                conn = connect(autocommit=True)
            if not _schema_ready(conn):
                state.record_schema_missing()
                print("[mutate] waiting for schema (run make seed-oltp)", flush=True)
                stop.wait(30)
                continue
            counters = run_tick(conn, rng)
            state.record_ok(counters)
            if state.snapshot()["ticks"] % 15 == 0:
                print(f"[mutate] tick done: {counters}", flush=True)
            stop.wait(tick_seconds)
        except Exception as exc:  # the loop must survive transient DB issues
            state.record_error(repr(exc))
            print(f"[mutate] tick failed, retrying: {exc!r}", flush=True)
            try:
                if conn is not None and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None
            stop.wait(5)
    server.shutdown()
    if conn is not None and not conn.closed:
        conn.close()
    print("[mutate] stopped cleanly", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
