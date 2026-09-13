"""GX API probe for the pinned great-expectations line (ADR-011 D1).

Not part of the gate path — a bring-up utility that validates, against the
EXACT installed GX version inside this image, the API surface the gate
depends on: ephemeral context, SQL datasource + table asset signatures,
row_condition compilation on SQL, result-object iteration shape,
unexpected_rows type, and the zero-filesystem-write property (ADR-011 D9).
Part 1 runs on an in-container throwaway SQLite db (no warehouse needed);
part 2 runs read-only against the live warehouse's smallest staging table.

Run: docker compose run --rm dq python -m dq.tools.probe_gx
"""

from __future__ import annotations

import os
import sqlite3

PROBE_TODAY = "2026-09-13"


def _sqlite_part() -> None:
    print("== part 1: sqlite (result-shape probe) ==")
    before = sorted(os.listdir("."))
    import great_expectations as gx
    from great_expectations import expectations as gxe

    print("gx.__version__ =", gx.__version__)
    ctx = gx.get_context(mode="ephemeral")
    print("context:", type(ctx).__name__)

    con = sqlite3.connect("/tmp/probe.db")
    con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amt REAL, status TEXT, cc TEXT, sd TEXT)")
    con.executemany(
        "INSERT INTO t VALUES (?,?,?,?,?)",
        [
            (1, 10.0, "CAPTURED", "US", "2026-01-01"),
            (2, -5.0, "CAPTURED", "us", "2030-01-01"),
            (3, -7.0, "REFUNDED", "DE", "2025-05-05"),
            (4, 9.0, "REFUNDED", "US", "2026-02-02"),
        ],
    )
    con.commit()
    con.close()

    ds = ctx.data_sources.add_sqlite(name="probe_src", connection_string="sqlite:////tmp/probe.db")
    asset = ds.add_table_asset(name="t", table_name="t")
    bd = asset.add_batch_definition_whole_table("whole_table")

    # condition-grammar candidates (parsed at construction time, no DB hit)
    from great_expectations.expectations.legacy_row_conditions import parse_great_expectations_condition
    for candidate in (
        'col("status") == "REFUNDED"',
        '(col("status") == "CAPTURED") | (col("status") == "PENDING")',
        'col("status") in ["CAPTURED", "PENDING"]',
    ):
        try:
            parse_great_expectations_condition(candidate)
            print("condition PARSES:", candidate)
        except Exception as exc:  # noqa: BLE001 — probe reports everything
            print("condition REJECTED:", candidate, "|", type(exc).__name__, str(exc)[:80])

    suite = ctx.suites.add(gx.ExpectationSuite(name="probe_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="amt", min_value=0.01,
        row_condition='col("status") == "CAPTURED"',
        condition_parser="great_expectations__experimental__",
    ))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="amt", max_value=-0.01,
        row_condition='col("status") == "REFUNDED"',
        condition_parser="great_expectations__experimental__",
    ))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="cc", regex="^[A-Z]{2}$"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="sd", max_value=PROBE_TODAY))
    vd = ctx.validation_definitions.add(gx.ValidationDefinition(name="probe_vd", data=bd, suite=suite))
    res = vd.run(result_format={
        "result_format": "COMPLETE",
        "include_unexpected_rows": True,
        "unexpected_index_column_names": ["id"],
        "return_unexpected_index_query": False,
    })
    print("result type:", type(res).__name__)
    print("result attrs:", [a for a in dir(res) if not a.startswith("_")])
    results = _iter_results(res)
    for er in results:
        _dump_expectation_result(er)
    created = [f for f in sorted(os.listdir(".")) if f not in before]
    print("files created in cwd:", created)
    assert not created, "GX wrote files — ephemeral-context contract broken"


def _iter_results(validation_result):
    for attr in ("expectations", "results"):
        if hasattr(validation_result, attr):
            cand = list(getattr(validation_result, attr) or [])
            if cand:
                print(f"iteration via res.{attr} ({len(cand)} expectation results)")
                return cand
    raise AssertionError("no expectation-result iterable found — inspect result attrs above")


def _dump_expectation_result(er) -> None:
    print("---")
    print("type:", type(er).__name__, "| success:", er.success)
    cfg = er.expectation_config
    print("expectation:", cfg.type, "| kwargs keys:", sorted(cfg.kwargs.keys()))
    r = er.result
    print("result keys:", sorted(r.keys()))
    print("unexpected_count:", r.get("unexpected_count"))
    rows = r.get("unexpected_rows")
    print("unexpected_rows:", type(rows).__name__ if rows is not None else None,
          getattr(rows, "shape", None))
    if rows is not None and hasattr(rows, "to_dict"):
        print("unexpected_rows sample:", rows.to_dict("records")[:2])
    print("unexpected_index_list:", r.get("unexpected_index_list"))


def _postgres_part() -> None:
    print("== part 2: postgres (live read-only probe) ==")
    import great_expectations as gx
    from great_expectations import expectations as gxe

    from dq.config import load_config

    cfg = load_config()
    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_postgres(name="probe_wh", connection_string=cfg.connection_string())
    import inspect
    print("add_table_asset sig:", str(inspect.signature(ds.add_table_asset))[:300])
    asset = ds.add_table_asset(name="stg_file_customers", table_name="stg_file_customers", schema_name="staging")
    bd = asset.add_batch_definition_whole_table("whole_table")
    suite = ctx.suites.add(gx.ExpectationSuite(name="probe_pg_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="country_code", regex="^[A-Z]{2}$"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="signup_date", max_value=PROBE_TODAY))
    vd = ctx.validation_definitions.add(gx.ValidationDefinition(name="probe_pg_vd", data=bd, suite=suite))
    res = vd.run(result_format={
        "result_format": "COMPLETE",
        "include_unexpected_rows": True,
        "unexpected_index_column_names": ["customer_id"],
        "return_unexpected_index_query": False,
    })
    print("success:", res.success)
    for er in _iter_results(res):
        _dump_expectation_result(er)


if __name__ == "__main__":
    _sqlite_part()
    _postgres_part()
    print("PROBE OK")
