"""Dead-letter mapping, digests and idempotent-write split (ADR-011 D6)."""

import pytest

from dq.quarantine import (
    QuarantineRow,
    canonical_json,
    check_digest,
    rows_from_result,
    source_pk_hash,
    write_quarantine,
)


class FakeConn:
    """psycopg2 cursor contract: execute() returns None (NOT the cursor —
    the regression this fake guards against; psycopg3 returns it, psycopg2
    does not); results come from fetchall() on the same object."""

    def __init__(self, results=None):
        self.calls = []
        self._results = list(results or [])

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self._results.pop(0) if self._results else []


def test_source_pk_hash_is_stable_and_order_insensitive():
    a = source_pk_hash({"payment_id": 7})
    b = source_pk_hash({"payment_id": 7})
    assert a == b and len(a) == 64
    assert a != source_pk_hash({"payment_id": 8})


def test_check_digest_ignores_runtime_kwargs():
    base = {"column": "amount", "min_value": 0.01}
    assert check_digest("expect_column_values_to_be_between", base) == check_digest(
        "expect_column_values_to_be_between", dict(base, batch_id="abc123")
    )


def test_check_digest_separates_distinct_rules():
    a = check_digest("expect_column_values_to_be_between", {"column": "amount", "min_value": 0.01})
    b = check_digest("expect_column_values_to_be_between", {"column": "amount", "max_value": -0.01})
    c = check_digest("expect_column_values_to_match_regex", {"column": "amount", "regex": "^x$"})
    assert len({a, b, c}) == 3


def test_canonical_json_is_deterministic():
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})


RESULT = {
    "unexpected_count": 2,
    "unexpected_rows": [
        {"payment_id": 11, "amount": -5.0, "status": "CAPTURED"},
        {"payment_id": 12, "amount": -9.0, "status": "CAPTURED"},
    ],
    "unexpected_index_list": [{"payment_id": 11}, {"payment_id": 12}],
}
KWARGS = {"column": "amount", "min_value": 0.01, "row_condition": 'col("status") == "CAPTURED"', "batch_id": "b"}


def test_rows_from_result_maps_provenance():
    rows, beyond = rows_from_result(
        suite_name="stg_payments.money_sanity",
        data_asset="staging.stg_payments",
        pk_column="payment_id",
        expectation_type="expect_column_values_to_be_between",
        kwargs=KWARGS,
        result=RESULT,
        max_rows=10000,
    )
    assert beyond == 0
    assert [r.source_pk for r in rows] == [{"payment_id": 11}, {"payment_id": 12}]
    assert rows[0].payload["amount"] == -5.0
    assert rows[0].suite == "stg_payments.money_sanity"
    assert rows[0].data_asset == "staging.stg_payments"
    assert rows[0].column_name == "amount"
    assert rows[0].row_condition == 'col("status") == "CAPTURED"'
    assert "expect_column_values_to_be_between" in rows[0].failure_reason
    assert "batch_id" not in rows[0].failure_reason
    assert rows[0].check_digest == check_digest("expect_column_values_to_be_between", KWARGS)


def test_rows_from_result_caps_and_records_shortfall():
    rows, beyond = rows_from_result(
        suite_name="s", data_asset="staging.stg_payments", pk_column="payment_id",
        expectation_type="expect_column_values_to_be_between", kwargs=KWARGS,
        result=RESULT, max_rows=1,
    )
    assert (len(rows), beyond) == (1, 1)
    assert "beyond cap" in rows[0].failure_reason and "+1 more" in rows[0].failure_reason


def test_rows_from_result_falls_back_to_payload_pk():
    result = {"unexpected_count": 1, "unexpected_rows": [{"payment_id": 42, "amount": -1.0}], "unexpected_index_list": []}
    rows, beyond = rows_from_result(
        suite_name="s", data_asset="staging.stg_payments", pk_column="payment_id",
        expectation_type="e", kwargs={"column": "amount"}, result=result, max_rows=10,
    )
    assert beyond == 0
    assert rows[0].source_pk == {"payment_id": 42}


def test_rows_from_result_handles_empty_unexpected_rows():
    result = {"unexpected_count": 3, "unexpected_rows": [], "unexpected_index_list": []}
    rows, beyond = rows_from_result(
        suite_name="s", data_asset="a", pk_column="pk",
        expectation_type="e", kwargs={"column": "c"}, result=result, max_rows=10,
    )
    assert rows == [] and beyond == 3


def test_write_quarantine_no_rows_is_a_noop():
    conn = FakeConn()
    assert write_quarantine(conn, [], "run-1") == (0, 0)
    assert conn.calls == []


def test_write_quarantine_splits_new_and_refreshed():
    row = QuarantineRow(
        suite="s", data_asset="staging.stg_payments", expectation="e", check_digest="d1",
        column_name="amount", row_condition=None, source_pk={"payment_id": 1},
        source_pk_hash=source_pk_hash({"payment_id": 1}), payload={"payment_id": 1},
        failure_reason="r1",
    )
    fresh = QuarantineRow(
        suite="s", data_asset="marts.fct_orders", expectation="e", check_digest="d2",
        column_name="total_amount", row_condition=None, source_pk={"order_id": 2},
        source_pk_hash=source_pk_hash({"order_id": 2}), payload={"order_id": 2},
        failure_reason="r2",
    )
    # pre-fetch sees the stg_payments incident as already open -> refresh; the
    # fct_orders one is new
    conn = FakeConn(results=[[("staging.stg_payments", "d1", row.source_pk_hash)]])
    inserted, refreshed = write_quarantine(conn, [row, fresh], "run-9")
    assert (inserted, refreshed) == (1, 1)
    assert len(conn.calls) == 3
    select_sql, select_params = conn.calls[0]
    assert select_sql.startswith("SELECT data_asset, check_digest, source_pk_hash FROM dq.dq_quarantine")
    assert "staging.stg_payments" in select_params[0] and "marts.fct_orders" in select_params[0]
    update_sql, update_params = conn.calls[1]
    assert update_sql.startswith("UPDATE dq.dq_quarantine")
    assert update_params[0] == "run-9"
    insert_sql, insert_params = conn.calls[2]
    assert insert_sql.startswith("INSERT INTO dq.dq_quarantine")
    assert insert_params[-1] == ["open"]
    assert insert_params[-2] == ["run-9"]


def test_write_quarantine_every_statement_is_fully_parameterized():
    row = QuarantineRow(
        suite="s", data_asset="a", expectation="e", check_digest="d",
        column_name="c", row_condition=None, source_pk={"pk": 1},
        source_pk_hash=source_pk_hash({"pk": 1}), payload={"pk": 1},
        failure_reason="r",
    )
    conn = FakeConn()
    write_quarantine(conn, [row], "run-1")
    for sql, params in conn.calls:
        assert "%s" in sql
        assert "{" not in sql and "}" not in sql  # no interpolation artifacts
        assert params is not None
