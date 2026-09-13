"""Idempotent dead-letter DDL (ADR-011 D6): schema `dq` + the quarantine
table + the partial unique index that makes re-detection an upsert instead of
a duplicate. Runs at every gate start — CREATE IF NOT EXISTS is a read-only
no-op on a warm warehouse (the ingest/ensure.py pattern, ADR-007). `status`
is supplied as a bound parameter by every INSERT, never a DDL default.

Every SQL statement is a single static string literal at its execution site
(no concatenation, no assembly, no interpolation — Mimosa source rule); DDL
has no parameters to bind.
"""

from __future__ import annotations

from dq.log import log


def ensure_dq_schema(cur) -> None:
    """CREATE IF NOT EXISTS for the dq dead-letter; safe on every run."""
    cur.execute("CREATE SCHEMA IF NOT EXISTS dq")
    cur.execute("CREATE TABLE IF NOT EXISTS dq.dq_quarantine (quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, suite TEXT NOT NULL, data_asset TEXT NOT NULL, expectation TEXT NOT NULL, check_digest TEXT NOT NULL, column_name TEXT, row_condition TEXT, source_pk JSONB NOT NULL, source_pk_hash TEXT NOT NULL, payload JSONB NOT NULL, failure_reason TEXT NOT NULL, run_id TEXT NOT NULL, status TEXT NOT NULL, landed_at TIMESTAMPTZ NOT NULL DEFAULT now(), resolved_at TIMESTAMPTZ, resolved_run_id TEXT)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_dq_quarantine_open ON dq.dq_quarantine (data_asset, check_digest, source_pk_hash) WHERE resolved_at IS NULL")
    cur.execute("COMMENT ON TABLE dq.dq_quarantine IS 'DQ dead-letter (ADR-011 D6): one open incident per (data_asset, check_digest, source_pk); re-detection refreshes run_id and landed_at; dq-replay resolves incidents whose row no longer fails; payload is a staging or marts row with hashed PII only'")
    log("dq.ensure_ddl", statements=4)
