"""The suite inventory IS the dbt-vs-GE contract in executable form
(ADR-011 D2/D5): exact suites, exact expectations, zero dbt-owned classes,
deterministic construction."""

import datetime as dt

import great_expectations.expectations as gxe

from dq.suites import CONDITION_PARSER, SUITE_SPECS, build_suites

TODAY = dt.date(2026, 9, 13)


def fields(expectation) -> dict:
    """Semantic view of an expectation instance (pydantic v1 fields)."""
    return expectation.dict()


def suite(name):
    return next(s for s in SUITE_SPECS if s.name == name)


def test_inventory_matches_adr_011_d5():
    assert [(s.data_asset, s.pk_column) for s in SUITE_SPECS] == [
        ("staging.stg_payments", "payment_id"),
        ("staging.stg_orders", "order_id"),
        ("staging.stg_order_items", "order_item_id"),
        ("staging.stg_file_customers", "customer_id"),
        ("staging.stg_soap_orders", "order_id"),
        ("marts.fct_orders", "order_id"),
    ]


def test_expectation_count_is_ten():
    built = build_suites(TODAY)
    assert sum(len(b.expectations) for b in built) == 10
    # every spec builds non-empty
    for b in built:
        assert b.expectations


def test_asset_names_are_unique():
    assets = [s.data_asset for s in SUITE_SPECS]
    assert len(assets) == len(set(assets))


def test_money_sanity_rules_and_conditions():
    built = next(b for b in build_suites(TODAY) if b.spec.name == "stg_payments.money_sanity")
    # GX parses the condition string into a structured condition at construction
    # (probe, 1.22.0): {'type': 'comparison', 'column': {'name': 'status'}, ...}
    conditions = [fields(e).get("row_condition") for e in built.expectations]
    assert [c["parameter"] for c in conditions] == ["CAPTURED", "PENDING", "REFUNDED"]
    assert all(c["operator"] == "==" and c["column"]["name"] == "status" for c in conditions)
    assert [type(e).__name__ for e in built.expectations] == ["ExpectColumnValuesToBeBetween"] * 3
    assert all(fields(e).get("column") == "amount" for e in built.expectations)
    mins = [fields(e).get("min_value") for e in built.expectations]
    maxes = [fields(e).get("max_value") for e in built.expectations]
    assert mins == [0.01, 0.01, None]
    assert maxes == [None, None, -0.01]
    assert all(fields(e).get("condition_parser") == CONDITION_PARSER for e in built.expectations)


def test_signup_date_ceiling_is_suite_construction_today():
    built = next(b for b in build_suites(TODAY) if b.spec.name == "stg_file_customers.feed_sanity")
    between = next(e for e in built.expectations if type(e).__name__ == "ExpectColumnValuesToBeBetween")
    assert fields(between).get("column") == "signup_date"
    # GX coerces the ISO string into a date object at construction (probe, 1.22.0)
    assert fields(between).get("max_value") == TODAY
    regex = next(e for e in built.expectations if type(e).__name__ == "ExpectColumnValuesToMatchRegex")
    assert fields(regex).get("regex") == "^[A-Z]{2}$"


def test_soap_pair_expectation_shape():
    built = next(b for b in build_suites(TODAY) if b.spec.name == "stg_soap_orders.temporal_coherence")
    (pair,) = built.expectations
    assert type(pair).__name__ == "ExpectColumnPairValuesAToBeGreaterThanB"
    k = fields(pair)
    assert (k.get("column_A"), k.get("column_B"), k.get("or_equal")) == ("updated_at", "created_at", True)


def test_no_dbt_owned_classes_smuggled_in():
    """The dbt contract (ADR-011 D2) reserves nulls/uniqueness/enums/hash-format
    for dbt. The suite set must never re-assert them as GX noise."""
    dbt_owned = {
        "ExpectColumnValuesToNotBeNull",
        "ExpectColumnValuesToBeUnique",
        "ExpectColumnValuesToBeInSet",
        "ExpectColumnPairValuesToBeEqual",
    }
    for built in build_suites(TODAY):
        for expectation in built.expectations:
            assert type(expectation).__name__ not in dbt_owned


def test_build_is_deterministic_for_a_fixed_day():
    a = build_suites(TODAY)
    b = build_suites(TODAY)
    for x, y in zip(a, b):
        assert x.spec.name == y.spec.name
        for ex, ey in zip(x.expectations, y.expectations):
            assert fields(ex) == fields(ey)


def test_all_expectation_classes_exist_in_gx_core():
    # guards against silent GX-version drift in the pinned line
    for built in build_suites(TODAY):
        for expectation in built.expectations:
            assert hasattr(gxe, type(expectation).__name__)
