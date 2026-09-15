"""Replay (ADR-011 D7): resolve open dead-letter incidents whose underlying
violation no longer reproduces against the current frozen build.

The fix always happens at the SOURCE (file: corrected CSV re-lands via the
hash ledger; REST: refreshed walk; CDC: corrected row upserts through the
normal path; SOAP: the documented --full replay) and the warehouse is
REBUILT first — replay never edits warehouse data by hand. Replay then:
  1. runs the full gate against the current staging/marts (writing/refreshing
     incidents for whatever still fails),
  2. resolves every open incident whose (data_asset, check_digest,
     source_pk_hash) is NOT among this run's failures — the violation is gone
     from the warehouse, the incident is history,
  3. reports and exits nonzero if any incident remains open (an operator
     tool that cannot half-succeed).

Resolved history is never deleted: it is the audit trail of what the gate
caught and when it cleared.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg2

from dq.config import Config
from dq.gate import GateReport, format_summary, new_run_id, run_gate
from dq.log import log


@dataclass
class ReplayReport:
    run_id: str
    resolved: int
    remaining: int
    gate: GateReport


def compute_resolution(open_rows: list, failure_keys: set) -> tuple[list[int], int]:
    """Pure diff: open incidents (quarantine_id, data_asset, check_digest,
    source_pk_hash) vs THIS gate run's failure keys -> (ids to resolve,
    count remaining open)."""
    resolve_ids = [row[0] for row in open_rows if (row[1], row[2], row[3]) not in failure_keys]
    return resolve_ids, len(open_rows) - len(resolve_ids)


def run_replay(cfg: Config, run_id: str | None = None) -> ReplayReport:
    run_id = run_id or new_run_id("dqr-")
    conn = psycopg2.connect(**cfg.dsn())
    conn.autocommit = True
    cur = conn.cursor()
    try:
        gate_report = run_gate(cfg, run_id=run_id, cur=cur)

        cur.execute(
            "SELECT quarantine_id, data_asset, check_digest, source_pk_hash FROM dq.dq_quarantine WHERE resolved_at IS NULL"
        )
        rows = cur.fetchall()
        open_count = len(rows)
        resolve_ids, remaining = compute_resolution(rows, gate_report.failure_keys)

        if resolve_ids:
            cur.execute(
                "UPDATE dq.dq_quarantine SET status = %s, resolved_at = now(), resolved_run_id = %s WHERE resolved_at IS NULL AND quarantine_id = ANY(%s::bigint[])",
                ("resolved", run_id, resolve_ids),
            )
        log("dq.replay", run_id=run_id, open_before=open_count,
            resolved=len(resolve_ids), remaining=remaining,
            gate_success=gate_report.success)
        return ReplayReport(run_id=run_id, resolved=len(resolve_ids),
                            remaining=remaining, gate=gate_report)
    finally:
        conn.close()


def format_replay_summary(report: ReplayReport) -> str:
    return "[dq] replay {}: run_id={} open_before={} resolved={} remaining={} | {}".format(
        "CLEAN" if report.remaining == 0 else "OPEN-REMAIN",
        report.run_id, report.resolved + report.remaining, report.resolved,
        report.remaining, format_summary(report.gate),
    )
