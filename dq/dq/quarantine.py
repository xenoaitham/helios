"""Dead-letter row identity + pure adapters (ADR-011 D6)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from dq.log import log

_RUNTIME_KWARGS = frozenset({"batch_id"})


@dataclass(frozen=True)
class QuarantineRow:
    suite: str
    data_asset: str
    expectation: str
    check_digest: str
    column_name: str | None
    row_condition: str | None
    source_pk: dict
    source_pk_hash: str
    payload: dict
    failure_reason: str


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def source_pk_hash(source_pk: dict) -> str:
    return hashlib.sha256(canonical_json(source_pk).encode()).hexdigest()


def check_digest(expectation_type: str, kwargs: dict) -> str:
    semantic = {k: v for k, v in kwargs.items() if k not in _RUNTIME_KWARGS}
    return hashlib.sha256(canonical_json([expectation_type, semantic]).encode()).hexdigest()


def _describe(kwargs: dict) -> str:
    semantic = {k: v for k, v in kwargs.items() if k not in _RUNTIME_KWARGS}
    return ", ".join(k + "=" + json.dumps(v, default=str) for k, v in sorted(semantic.items()))


def rows_from_result(
    *,
    suite_name: str,
    data_asset: str,
    pk_column: str,
    expectation_type: str,
    kwargs: dict,
    result: dict,
    max_rows: int,
) -> tuple[list[QuarantineRow], int]:
    """Pure adapter: one failed GX expectation result -> dead-letter rows.

    Returns (rows, beyond_cap). beyond_cap = unexpected rows beyond the
    per-expectation cap (DQ_QUARANTINE_MAX_ROWS): the cap keeps a catastrophic
    violation from exploding the dead-letter, and the shortfall is recorded in
    every affected failure_reason (ADR-011 D5/D6).
    """
    column = kwargs.get("column")
    # GX 1.22 normalizes row_condition into a structured dict at construction
    # (probe); the dead-letter column stores its canonical JSON form so the
    # audit value survives and psycopg2 never sees a bare dict.
    raw_condition = kwargs.get("row_condition")
    row_condition = canonical_json(raw_condition) if isinstance(raw_condition, dict) else raw_condition
    digest = check_digest(expectation_type, kwargs)
    unexpected_count = int(result.get("unexpected_count") or 0)
    unexpected_rows = list(result.get("unexpected_rows") or [])[:max_rows]
    index_list = list(result.get("unexpected_index_list") or [])
    rule = _describe(kwargs)
    beyond_cap = max(0, unexpected_count - len(unexpected_rows))

    rows: list[QuarantineRow] = []
    for i, payload in enumerate(unexpected_rows):
        payload = dict(payload)
        index_entry = index_list[i] if i < len(index_list) else {}
        source_pk = {pk_column: index_entry.get(pk_column, payload.get(pk_column))}
        observed = json.dumps(payload, default=str)
        reason = "{}({}) violated by {} row(s); observed {}".format(
            expectation_type, rule, unexpected_count, observed
        )
        if beyond_cap:
            reason += "; +{} more beyond cap (DQ_QUARANTINE_MAX_ROWS={})".format(beyond_cap, max_rows)
        rows.append(
            QuarantineRow(
                suite=suite_name,
                data_asset=data_asset,
                expectation=expectation_type,
                check_digest=digest,
                column_name=column,
                row_condition=row_condition,
                source_pk=source_pk,
                source_pk_hash=source_pk_hash(source_pk),
                payload=payload,
                failure_reason=reason,
            )
        )
    return rows, beyond_cap


def write_quarantine(cur, rows: list[QuarantineRow], run_id: str) -> tuple[int, int]:
    """Land open incidents; returns (inserted, refreshed).

    Shape mirrors the passing ingest/landing.py upserts: parameter lists
    hoisted into named locals, one static SQL literal per call site, every
    value a bound parameter. The pre-fetch computes the inserted-vs-refreshed
    split; the partial unique index (WHERE resolved_at IS NULL) backstops any
    concurrent run loudly instead of silently duplicating.
    """
    if not rows:
        return 0, 0

    batch_keys = {(r.data_asset, r.check_digest, r.source_pk_hash) for r in rows}
    fetch_assets = [k[0] for k in batch_keys]
    fetch_digests = [k[1] for k in batch_keys]
    cur.execute(
        "SELECT data_asset, check_digest, source_pk_hash FROM dq.dq_quarantine WHERE resolved_at IS NULL AND data_asset = ANY(%s::text[]) AND check_digest = ANY(%s::text[])",
        (fetch_assets, fetch_digests),
    )
    fetched = cur.fetchall()
    existing_keys = {(row[0], row[1], row[2]) for row in fetched}
    new_rows = [r for r in rows if (r.data_asset, r.check_digest, r.source_pk_hash) not in existing_keys]
    refresh_rows = [r for r in rows if (r.data_asset, r.check_digest, r.source_pk_hash) in existing_keys]

    if refresh_rows:
        refresh_assets = [r.data_asset for r in refresh_rows]
        refresh_digests = [r.check_digest for r in refresh_rows]
        refresh_pks = [r.source_pk_hash for r in refresh_rows]
        refresh_payloads = [canonical_json(r.payload) for r in refresh_rows]
        refresh_reasons = [r.failure_reason for r in refresh_rows]
        cur.execute(
            "UPDATE dq.dq_quarantine AS d SET run_id = %s, landed_at = now(), failure_reason = q.failure_reason, payload = q.payload::jsonb FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::text[]) AS q(data_asset, check_digest, source_pk_hash, payload, failure_reason) WHERE d.resolved_at IS NULL AND d.data_asset = q.data_asset AND d.check_digest = q.check_digest AND d.source_pk_hash = q.source_pk_hash",
            (run_id, refresh_assets, refresh_digests, refresh_pks, refresh_payloads, refresh_reasons),
        )

    if new_rows:
        new_suites = [r.suite for r in new_rows]
        new_assets = [r.data_asset for r in new_rows]
        new_expectations = [r.expectation for r in new_rows]
        new_digests = [r.check_digest for r in new_rows]
        new_columns = [r.column_name for r in new_rows]
        new_conditions = [r.row_condition for r in new_rows]
        new_source_pks = [canonical_json(r.source_pk) for r in new_rows]
        new_pk_hashes = [r.source_pk_hash for r in new_rows]
        new_payloads = [canonical_json(r.payload) for r in new_rows]
        new_reasons = [r.failure_reason for r in new_rows]
        new_run_ids = [run_id] * len(new_rows)
        new_statuses = ["open"] * len(new_rows)
        cur.execute(
            "INSERT INTO dq.dq_quarantine (suite, data_asset, expectation, check_digest, column_name, row_condition, source_pk, source_pk_hash, payload, failure_reason, run_id, status) SELECT q.suite, q.data_asset, q.expectation, q.check_digest, q.column_name, q.row_condition, q.source_pk::jsonb, q.source_pk_hash, q.payload::jsonb, q.failure_reason, q.run_id, q.status FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::text[], %s::text[], %s::jsonb[], %s::text[], %s::jsonb[], %s::text[], %s::text[], %s::text[]) AS q(suite, data_asset, expectation, check_digest, column_name, row_condition, source_pk, source_pk_hash, payload, failure_reason, run_id, status)",
            (new_suites, new_assets, new_expectations, new_digests, new_columns, new_conditions, new_source_pks, new_pk_hashes, new_payloads, new_reasons, new_run_ids, new_statuses),
        )

    log("dq.quarantine_write", run_id=run_id, inserted=len(new_rows), refreshed=len(refresh_rows))
    return len(new_rows), len(refresh_rows)
