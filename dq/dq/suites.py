"""The GE suite set (ADR-011 D2/D5): small, curated, business-semantic.

This module is the ONLY GX-coupled surface of the gate — expectations are
Python objects built here, no SQL is authored by us (GX compiles its own
validation SQL; row_conditions use GX's own condition grammar, probed 1.22.0:
single comparisons only, see dq/tools/probe_gx.py). Every rule encodes a
measured business policy and none duplicates a dbt-owned class (the full
dbt-vs-GE contract table lives in ADR-011 D2; the dbt inventory is
dbt/project/models/**/_.yml + dbt/project/tests/).

Bounds provenance (probed 2026-09-13, see EVIDENCE/phase-4-dq.md):
- stg_payments sign policy: CAPTURED/PENDING min +34.51 (628,222 rows),
  REFUNDED max -235.82 (91,923 rows) — 0 violations, 0 sign crossings.
- quantity floor is semantic (a line item is >= 1 unit; seed draws 1-5 and
  the ceiling is deliberately loose so the gate never echoes the generator).
- signup_date ceiling is "today" at suite-construction time (ADR-011 D4.4).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from great_expectations import expectations as gxe

# GX 1.22 condition grammar (probed): single `col("x") OP value` comparisons.
# Two-value rules therefore split into one expectation per value — explicit,
# and it keeps the suite set free of any SQL condition strings.
CONDITION_PARSER = "great_expectations__experimental__"


@dataclass(frozen=True)
class SuiteSpec:
    """One curated suite over one frozen staging/marts relation."""

    name: str
    schema_name: str
    table: str
    pk_column: str
    doc: str
    factories: tuple = ()  # callables(today) -> [expectation, ...]

    @property
    def data_asset(self) -> str:
        return f"{self.schema_name}.{self.table}"


def _captured_payment_positive(today: dt.date) -> list:
    return [
        gxe.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0.01,
            row_condition='col("status") == "CAPTURED"',
            condition_parser=CONDITION_PARSER,
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0.01,
            row_condition='col("status") == "PENDING"',
            condition_parser=CONDITION_PARSER,
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="amount", max_value=-0.01,
            row_condition='col("status") == "REFUNDED"',
            condition_parser=CONDITION_PARSER,
        ),
    ]


def _order_total_non_negative(today: dt.date) -> list:
    return [gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0)]


def _line_item_sanity(today: dt.date) -> list:
    return [
        gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=10000),
        gxe.ExpectColumnValuesToBeBetween(column="unit_price", min_value=0),
    ]


def _file_customer_feed_sanity(today: dt.date) -> list:
    return [
        gxe.ExpectColumnValuesToBeBetween(column="signup_date", max_value=today.isoformat()),
        gxe.ExpectColumnValuesToMatchRegex(column="country_code", regex="^[A-Z]{2}$"),
    ]


def _soap_temporal_coherence(today: dt.date) -> list:
    return [
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="updated_at", column_B="created_at", or_equal=True,
        ),
    ]


def _published_order_total_non_negative(today: dt.date) -> list:
    return [gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0)]


SUITE_SPECS: tuple[SuiteSpec, ...] = (
    SuiteSpec(
        name="stg_payments.money_sanity",
        schema_name="staging",
        table="stg_payments",
        pk_column="payment_id",
        doc="Payment sign policy: captured/pending money is positive, refunds carry the negated amount (measured source contract).",
        factories=(_captured_payment_positive,),
    ),
    SuiteSpec(
        name="stg_orders.order_sanity",
        schema_name="staging",
        table="stg_orders",
        pk_column="order_id",
        doc="An order total is never negative (totals derive from positive line items).",
        factories=(_order_total_non_negative,),
    ),
    SuiteSpec(
        name="stg_order_items.line_sanity",
        schema_name="staging",
        table="stg_order_items",
        pk_column="order_item_id",
        doc="Line items are 1..10000 units at a non-negative unit price.",
        factories=(_line_item_sanity,),
    ),
    SuiteSpec(
        name="stg_file_customers.feed_sanity",
        schema_name="staging",
        table="stg_file_customers",
        pk_column="customer_id",
        doc="Batch feed semantics: no future signup dates, ISO-3166 alpha-2 country shape.",
        factories=(_file_customer_feed_sanity,),
    ),
    SuiteSpec(
        name="stg_soap_orders.temporal_coherence",
        schema_name="staging",
        table="stg_soap_orders",
        pk_column="order_id",
        doc="A SOAP order cannot be updated before it was created.",
        factories=(_soap_temporal_coherence,),
    ),
    SuiteSpec(
        name="fct_orders.published_money_sanity",
        schema_name="marts",
        table="fct_orders",
        pk_column="order_id",
        doc="Published fact: order totals non-negative across both sources.",
        factories=(_published_order_total_non_negative,),
    ),
)


@dataclass(frozen=True)
class BuiltSuite:
    """A SuiteSpec instantiated for one gate run (today fixed)."""

    spec: SuiteSpec
    expectations: tuple


def build_suites(today: dt.date | None = None) -> list[BuiltSuite]:
    """Instantiate every suite; `today` anchors the one time-relative rule."""
    today = today or dt.date.today()
    return [
        BuiltSuite(
            spec=spec,
            expectations=tuple(e for factory in spec.factories for e in factory(today)),
        )
        for spec in SUITE_SPECS
    ]
