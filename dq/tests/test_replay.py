"""Replay resolution diff + report formatting (ADR-011 D7)."""

from dq.gate import GateReport, SuiteReport, format_summary
from dq.quarantine import source_pk_hash
from dq.replay import ReplayReport, compute_resolution, format_replay_summary


def open_row(qid, asset, digest, pk):
    return (qid, asset, digest, source_pk_hash(pk))


def test_all_open_incidents_resolve_when_gate_is_clean():
    rows = [open_row(1, "staging.stg_payments", "d", {"payment_id": 1})]
    resolve, remaining = compute_resolution(rows, set())
    assert resolve == [1] and remaining == 0


def test_re_detected_incident_stays_open():
    key = ("staging.stg_payments", "d", source_pk_hash({"payment_id": 1}))
    rows = [open_row(1, key[0], key[1], {"payment_id": 1})]
    resolve, remaining = compute_resolution(rows, {key})
    assert resolve == [] and remaining == 1


def test_mixed_resolution_partition():
    still_failing = ("staging.stg_payments", "d1", source_pk_hash({"payment_id": 1}))
    rows = [
        open_row(1, still_failing[0], still_failing[1], {"payment_id": 1}),
        open_row(2, "staging.stg_payments", "d2", {"payment_id": 2}),
        open_row(3, "marts.fct_orders", "d3", {"order_id": 3}),
    ]
    resolve, remaining = compute_resolution(rows, {still_failing})
    assert resolve == [2, 3] and remaining == 1


def test_format_summary_reports_pass_and_counts():
    report = GateReport(run_id="dq-1", success=True, suites=[
        SuiteReport(suite="s1", data_asset="a", expectations=4, passed=4,
                    failed=0, metric_exceptions=0, beyond_cap=0),
        SuiteReport(suite="s2", data_asset="b", expectations=6, passed=6,
                    failed=0, metric_exceptions=0, beyond_cap=0),
    ], inserted=0, refreshed=0)
    text = format_summary(report)
    assert "PASS" in text and "dq-1" in text and "expectations=10" in text


def test_format_summary_reports_fail_and_dead_letter_counts():
    report = GateReport(run_id="dq-2", success=False, inserted=2, refreshed=1)
    text = format_summary(report)
    assert "FAIL" in text and "dead-lettered(inserted=2, refreshed=1)" in text


def test_format_replay_summary_states_remaining():
    gate = GateReport(run_id="dq-3", success=True)
    clean = ReplayReport(run_id="dqr-1", resolved=2, remaining=0, gate=gate)
    assert "CLEAN" in format_replay_summary(clean)
    open_ = ReplayReport(run_id="dqr-2", resolved=1, remaining=1, gate=gate)
    assert "OPEN-REMAIN" in format_replay_summary(open_)
