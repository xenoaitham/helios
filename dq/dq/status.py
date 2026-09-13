"""Quarantine status (ADR-011 D6/D8): the dead-letter table IS the alert
surface — this makes it queryable at a glance. Informational: exits 0; the
GATE is what blocks (nonzero), never the status report.
"""

from __future__ import annotations

import psycopg2

from dq.config import Config
from dq.ensure import ensure_dq_schema
from dq.log import log


def run_status(cfg: Config) -> dict:
    conn = psycopg2.connect(**cfg.dsn())
    conn.autocommit = True
    cur = conn.cursor()
    try:
        ensure_dq_schema(cur)
        cur.execute(
            "SELECT status, count(*) FROM dq.dq_quarantine GROUP BY status ORDER BY status"
        )
        by_status = cur.fetchall()
        cur.execute(
            "SELECT suite, data_asset, count(*) FROM dq.dq_quarantine WHERE resolved_at IS NULL GROUP BY suite, data_asset ORDER BY suite, data_asset"
        )
        open_by_suite = cur.fetchall()
        cur.execute(
            "SELECT quarantine_id, suite, data_asset, expectation, source_pk, failure_reason, run_id, landed_at FROM dq.dq_quarantine WHERE resolved_at IS NULL ORDER BY landed_at DESC LIMIT 20"
        )
        open_rows = cur.fetchall()
    finally:
        conn.close()

    counts = {status: int(n) for status, n in by_status}
    summary = {
        "open": counts.get("open", 0),
        "resolved": counts.get("resolved", 0),
        "open_by_suite": {suite: int(n) for suite, _asset, n in open_by_suite},
        "recent_open": [
            {
                "quarantine_id": row[0],
                "suite": row[1],
                "data_asset": row[2],
                "expectation": row[3],
                "source_pk": row[4],
                "failure_reason": row[5][:200],
                "run_id": row[6],
                "landed_at": str(row[7]),
            }
            for row in open_rows
        ],
    }
    log("dq.status", **summary)
    print("[dq] quarantine status: open={} resolved={}".format(summary["open"], summary["resolved"]))
    for suite, _asset, n in open_by_suite:
        print("[dq]   open in {}: {}".format(suite, n))
    return summary
