"""`python -m cdc.verify <stage>` — live DoD checks for ADR-005 (real runs only).

Stages (driven by scripts/cdc-verify.sh):
  wait-catchup    — block until connector RUNNING and sink-group lag is 0
  baseline-compare— per-table row counts (+ money checksums) oltp vs raw
  marker          — INSERT/UPDATE/DELETE a marker user on oltp; measure the
                    wall-clock latency until each lands in the raw zone
  replay-record   — print the marker's raw-zone state as JSON (pre-replay)
  replay-assert   — compare the marker's raw-zone state after a forced Kafka
                    offset reset + full re-consumption; any drift fails

The OLTP marker writes need the mutator to be STOPPED (controlled window) —
the driver script handles that. Every SQL statement is a single static
string literal at its call site, fully parameterized (Mimosa source rule).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import psycopg

from cdc import config, lag, register

MARKER_ID_DEFAULT = 987654321
MARKER_EMAIL_V1 = "cdc-verify-v1@example.invalid"
MARKER_EMAIL_V2 = "cdc-verify-v2@example.invalid"
SOURCE_TABLES = ("users", "orders", "order_items", "payments")


def _log(**fields) -> None:
    print(json.dumps(fields), flush=True)


def _oltp_conn() -> psycopg.Connection:
    cfg = config.oltp_settings()
    return psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True)


def _wh_conn() -> psycopg.Connection:
    cfg = config.warehouse_settings()
    return psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"], autocommit=True)


# ---------------------------------------------------------------------------
# wait-catchup
# ---------------------------------------------------------------------------

def stage_wait_catchup(timeout_s: float) -> int:
    started = time.monotonic()
    deadline = started + timeout_s
    last_progress = 0.0
    while time.monotonic() < deadline:
        try:
            status = register.connector_status()
            states = [status.get("connector", {}).get("state", "?")]
            states += [t.get("state", "?") for t in status.get("tasks", [])]
            if not all(s == "RUNNING" for s in states):
                _log(stage="wait-catchup", connector=states)
            else:
                total, per_topic = lag.group_lag()
                if total == 0:
                    _log(stage="wait-catchup", verdict="caught_up", waited_s=round(time.monotonic() - started, 1), per_topic_lag=per_topic)
                    return 0
                if time.monotonic() - last_progress >= 30:
                    last_progress = time.monotonic()
                    _log(stage="wait-catchup", total_lag=total, per_topic_lag=per_topic)
        except Exception as exc:
            _log(stage="wait-catchup", error=repr(exc))
        time.sleep(5)
    _log(stage="wait-catchup", verdict="timeout", timeout_s=timeout_s)
    return 1


# ---------------------------------------------------------------------------
# baseline-compare (mutator must be stopped)
# ---------------------------------------------------------------------------

def _oltp_counts(conn: psycopg.Connection) -> dict:
    result = {}
    result["users"] = (conn.execute("SELECT count(*) FROM public.users").fetchone()[0],)
    result["orders"] = tuple(conn.execute("SELECT count(*) AS n, COALESCE(sum(total_amount), 0)::text AS total FROM public.orders").fetchone())
    result["order_items"] = (conn.execute("SELECT count(*) FROM public.order_items").fetchone()[0],)
    result["payments"] = tuple(conn.execute("SELECT count(*) AS n, COALESCE(sum(amount), 0)::text AS total FROM public.payments").fetchone())
    return result


def _raw_counts(conn: psycopg.Connection) -> dict:
    result = {}
    result["users"] = (conn.execute("SELECT count(*) FROM raw.cdc_users WHERE op <> 'd'").fetchone()[0],)
    result["orders"] = tuple(conn.execute("SELECT count(*) AS n, COALESCE(sum((after->>'total_amount')::numeric), 0)::text AS total FROM raw.cdc_orders WHERE op <> 'd'").fetchone())
    result["order_items"] = (conn.execute("SELECT count(*) FROM raw.cdc_order_items WHERE op <> 'd'").fetchone()[0],)
    result["payments"] = tuple(conn.execute("SELECT count(*) AS n, COALESCE(sum((after->>'amount')::numeric), 0)::text AS total FROM raw.cdc_payments WHERE op <> 'd'").fetchone())
    return result


def stage_baseline_compare() -> int:
    with _oltp_conn() as oltp, _wh_conn() as wh:
        source = _oltp_counts(oltp)
        landed = _raw_counts(wh)
    verdict = "match"
    for table in SOURCE_TABLES:
        ok = source[table] == landed[table]
        verdict = verdict if ok else "mismatch"
        _log(stage="baseline-compare", table=table, oltp=source[table], raw=landed[table], ok=ok)
    _log(stage="baseline-compare", verdict=verdict)
    return 0 if verdict == "match" else 1


# ---------------------------------------------------------------------------
# marker INSERT/UPDATE/DELETE latency
# ---------------------------------------------------------------------------

def _marker_pk(marker_id: int) -> str:
    return json.dumps({"user_id": marker_id})


def _wait_raw_marker(wh: psycopg.Connection, marker_id: int, predicate, timeout_s: float, poll_s: float = 2.0):
    started = time.monotonic()
    while time.monotonic() - started < timeout_s:
        row = wh.execute("SELECT op, lsn, ts_ms, after->>'email' AS email FROM raw.cdc_users WHERE pk = %s::jsonb", (_marker_pk(marker_id),)).fetchone()
        if row is not None and predicate(row):
            return row, round(time.monotonic() - started, 2)
        time.sleep(poll_s)
    return None, round(time.monotonic() - started, 2)


def stage_marker(marker_id: int, budget_s: float) -> int:
    verdicts = []
    with _oltp_conn() as oltp, _wh_conn() as wh:
        oltp.execute("DELETE FROM public.users WHERE user_id = %s", (marker_id,))

        oltp.execute("INSERT INTO public.users (user_id, email, full_name, country_code, created_at, last_login_at, is_active) VALUES (%s, %s, %s, %s, now(), now(), TRUE) ON CONFLICT (user_id) DO NOTHING", (marker_id, MARKER_EMAIL_V1, "CDC Verify Marker", "ZZ"))
        row_i, latency_i = _wait_raw_marker(wh, marker_id, lambda r: r[3] == MARKER_EMAIL_V1 and r[0] in ("r", "c", "u"), budget_s)
        _log(stage="marker", op="INSERT", latency_s=latency_i, raw_row=row_i, verdict="ok" if row_i else "timeout")
        verdicts.append(row_i is not None)

        oltp.execute("UPDATE public.users SET email = %s, last_login_at = now() WHERE user_id = %s", (MARKER_EMAIL_V2, marker_id))
        row_u, latency_u = _wait_raw_marker(wh, marker_id, lambda r: r[3] == MARKER_EMAIL_V2 and r[0] == "u", budget_s)
        _log(stage="marker", op="UPDATE", latency_s=latency_u, raw_row=row_u, verdict="ok" if row_u else "timeout")
        verdicts.append(row_u is not None)

        oltp.execute("DELETE FROM public.users WHERE user_id = %s", (marker_id,))
        row_d, latency_d = _wait_raw_marker(wh, marker_id, lambda r: r[0] == "d", budget_s)
        _log(stage="marker", op="DELETE", latency_s=latency_d, raw_row=row_d, verdict="ok" if row_d else "timeout")
        verdicts.append(row_d is not None)

    ok = all(verdicts)
    _log(stage="marker", verdict="ok" if ok else "timeout", budget_s=budget_s)
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# replay-record / replay-assert
# ---------------------------------------------------------------------------

def _marker_state(wh: psycopg.Connection, marker_id: int) -> dict | None:
    row = wh.execute("SELECT op, lsn, ts_ms, after->>'email' AS email FROM raw.cdc_users WHERE pk = %s::jsonb", (_marker_pk(marker_id),)).fetchone()
    if row is None:
        return None
    return {"op": row[0], "lsn": row[1], "ts_ms": row[2], "email": row[3]}


def stage_replay_record(marker_id: int) -> int:
    with _wh_conn() as wh:
        state = _marker_state(wh, marker_id)
    print(json.dumps(state))
    return 0 if state is not None else 1


def stage_replay_assert(marker_id: int, expected_json: str) -> int:
    expected = json.loads(expected_json)
    with _wh_conn() as wh:
        state = _marker_state(wh, marker_id)
        dupes = wh.execute("SELECT count(*) FROM raw.cdc_users WHERE pk = %s::jsonb", (_marker_pk(marker_id),)).fetchone()[0]
    ok_state = state == expected
    ok_dupes = dupes == 1
    _log(stage="replay-assert", expected=expected, actual=state, state_ok=ok_state, rows_for_pk=dupes, dupes_ok=ok_dupes)
    ok = ok_state and ok_dupes
    _log(stage="replay-assert", verdict="ok" if ok else "drift")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="CDC DoD live checks (ADR-005)")
    sub = parser.add_subparsers(dest="stage", required=True)

    p_wait = sub.add_parser("wait-catchup")
    p_wait.add_argument("--timeout", type=float, default=2400)

    sub.add_parser("baseline-compare")

    p_marker = sub.add_parser("marker")
    p_marker.add_argument("--budget", type=float, default=90)
    p_marker.add_argument("--marker-id", type=int, default=int(os.environ.get("CDC_VERIFY_MARKER_ID", MARKER_ID_DEFAULT)))

    p_rec = sub.add_parser("replay-record")
    p_rec.add_argument("--marker-id", type=int, default=int(os.environ.get("CDC_VERIFY_MARKER_ID", MARKER_ID_DEFAULT)))

    p_assert = sub.add_parser("replay-assert")
    p_assert.add_argument("--marker-id", type=int, default=int(os.environ.get("CDC_VERIFY_MARKER_ID", MARKER_ID_DEFAULT)))
    p_assert.add_argument("--expect", required=True)

    args = parser.parse_args()
    if args.stage == "wait-catchup":
        return stage_wait_catchup(args.timeout)
    if args.stage == "baseline-compare":
        return stage_baseline_compare()
    if args.stage == "marker":
        return stage_marker(args.marker_id, args.budget)
    if args.stage == "replay-record":
        return stage_replay_record(args.marker_id)
    if args.stage == "replay-assert":
        return stage_replay_assert(args.marker_id, args.expect)
    return 2


if __name__ == "__main__":
    sys.exit(main())
