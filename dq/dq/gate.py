"""The gate (ADR-011 D3): run every suite over the FROZEN staging/marts
relations, dead-letter every offending row with provenance, then exit
nonzero on any failure — after ALL suites have run. The gate reports
completely, then blocks: one task, one nonzero exit, every failure visible
in the same run (ADR-010 D3's contract).

A suite that cannot even be evaluated (metric exception) is a LOUD failure,
never a silent pass — an unevaluatable expectation must block the close like
a violated one (the vacuous-PASS-is-worse-than-a-FAIL rule).

GX coupling is confined here and in dq/suites.py: ephemeral data context,
one Postgres datasource, per-suite whole-table batch. Zero filesystem writes
(ADR-011 D9, probe-verified); the only durable outputs are dq.dq_quarantine
rows and stdout JSON.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field

import great_expectations as gx
import psycopg2

from dq import quarantine as qmod
from dq.config import Config
from dq.ensure import ensure_dq_schema
from dq.log import log
from dq.suites import build_suites

_BASE_RESULT_FORMAT = {
    "result_format": "COMPLETE",
    "include_unexpected_rows": True,
    "return_unexpected_index_query": False,
}


@dataclass
class SuiteReport:
    suite: str
    data_asset: str
    expectations: int
    passed: int
    failed: int
    metric_exceptions: int
    beyond_cap: int


@dataclass
class GateReport:
    run_id: str
    success: bool
    suites: list[SuiteReport] = field(default_factory=list)
    inserted: int = 0
    refreshed: int = 0
    # identity keys of every incident dead-lettered THIS run — what replay
    # diffs open incidents against (ADR-011 D7)
    failure_keys: set = field(default_factory=set)


def new_run_id(prefix: str = "dq-") -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return prefix + stamp + "-" + uuid.uuid4().hex[:8]


def run_gate(cfg: Config, run_id: str | None = None, today: dt.date | None = None, cur=None) -> GateReport:
    """Run every suite; write dead-letter rows; return the report. When `cur`
    is supplied (replay reuses one cursor/connection) the caller owns it."""
    run_id = run_id or new_run_id()
    report = GateReport(run_id=run_id, success=True)
    own_conn = None
    if cur is None:
        own_conn = psycopg2.connect(**cfg.dsn())
        own_conn.autocommit = True
        cur = own_conn.cursor()
    try:
        ensure_dq_schema(cur)
        suites = build_suites(today)
        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_postgres(
            name="helios_warehouse", connection_string=cfg.connection_string()
        )

        pending_rows: list[qmod.QuarantineRow] = []
        log("dq.gate_started", run_id=run_id, suites=len(suites),
            expectations=sum(len(b.expectations) for b in suites))

        for built in suites:
            spec = built.spec
            sr = SuiteReport(suite=spec.name, data_asset=spec.data_asset,
                             expectations=len(built.expectations), passed=0,
                             failed=0, metric_exceptions=0, beyond_cap=0)
            asset = datasource.add_table_asset(
                name=spec.data_asset.replace(".", "_"),
                table_name=spec.table,
                schema_name=spec.schema_name,
            )
            batch_def = asset.add_batch_definition_whole_table(spec.data_asset + "_whole")
            gx_suite = context.suites.add(gx.ExpectationSuite(name=spec.name))
            for expectation in built.expectations:
                gx_suite.add_expectation(expectation)
            validation = context.validation_definitions.add(
                gx.ValidationDefinition(name=spec.name + "_vd", data=batch_def, suite=gx_suite)
            )
            result = validation.run(
                result_format={**_BASE_RESULT_FORMAT, "unexpected_index_column_names": [spec.pk_column]}
            )

            for er in result.results:
                if er.success:
                    sr.passed += 1
                    continue
                sr.failed += 1
                exc = getattr(er, "exception_info", None) or {}
                kwargs = dict(er.expectation_config.kwargs)
                etype = er.expectation_config.type
                if not er.result or exc.get("raised_exception"):
                    # unevaluatable rule: blocks the gate, nothing to dead-letter
                    sr.metric_exceptions += 1
                    report.success = False
                    log("dq.metric_exception", level="error", run_id=run_id,
                        suite=spec.name, expectation=etype,
                        message=(exc.get("exception_message") or "empty result")[:500])
                    continue
                rows, beyond_cap = qmod.rows_from_result(
                    suite_name=spec.name,
                    data_asset=spec.data_asset,
                    pk_column=spec.pk_column,
                    expectation_type=etype,
                    kwargs=kwargs,
                    result=dict(er.result),
                    max_rows=cfg.quarantine_max_rows,
                )
                sr.beyond_cap += beyond_cap
                for row in rows:
                    report.failure_keys.add(
                        (row.data_asset, row.check_digest, row.source_pk_hash)
                    )
                pending_rows.extend(rows)

            if sr.failed:
                report.success = False
            report.suites.append(sr)
            log("dq.suite_result", run_id=run_id, suite=spec.name,
                data_asset=spec.data_asset, expectations=sr.expectations,
                passed=sr.passed, failed=sr.failed,
                metric_exceptions=sr.metric_exceptions, beyond_cap=sr.beyond_cap)

        report.inserted, report.refreshed = qmod.write_quarantine(cur, pending_rows, run_id)

        if report.success:
            log("dq.gate_passed", run_id=run_id, suites=len(report.suites),
                expectations=sum(s.expectations for s in report.suites),
                quarantined_inserted=report.inserted, quarantined_refreshed=report.refreshed)
        else:
            log("dq.gate_failed", level="error", run_id=run_id,
                suites=len(report.suites),
                failed_expectations=sum(s.failed for s in report.suites),
                metric_exceptions=sum(s.metric_exceptions for s in report.suites),
                quarantined_inserted=report.inserted,
                quarantined_refreshed=report.refreshed,
                beyond_cap=sum(s.beyond_cap for s in report.suites))
        return report
    finally:
        if own_conn is not None:
            own_conn.close()


def format_summary(report: GateReport) -> str:
    """Compact human line for the DAG/make log, next to the JSON events."""
    status = "PASS" if report.success else "FAIL"
    return (
        "[dq] gate {}: run_id={} suites={} expectations={} failed={} "
        "dead-lettered(inserted={}, refreshed={})".format(
            status, report.run_id, len(report.suites),
            sum(s.expectations for s in report.suites),
            sum(s.failed for s in report.suites),
            report.inserted, report.refreshed,
        )
    )
