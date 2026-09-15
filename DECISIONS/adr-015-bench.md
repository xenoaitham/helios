# ADR-015 — bench: what a "5M-row full load" honestly means on a never-touch platform, the per-stage measurement plan, the nightly-collision policy, and the evidence shape

Date: 2026-09-14 · Status: accepted · Phase: 5 (item 14)
Decides: the meaning of the item-14 mandate ("timed 5M-row full load, rows/sec
per stage") for a platform whose sources are living corpora that must never be
reseeded; whether the bench measures the shipped pipeline or a dedicated rig;
which measured number is the numerator and which measured wall the denominator
for every stage; how bench legs avoid the 05:00 UTC nightly; what the honest
hardware disclosure contains; the re-runnability/loudness contract; the
evidence shape (`EVIDENCE/metrics.md` + `EVIDENCE/bench-*.log`) and the
smoke-test impact (zero).

## Context

Phase 5 item 13 (chaos-test, ADR-014) is closed and CRITIC-passed. Item 14
mandates real throughput numbers with date + hardware in
`EVIDENCE/metrics.md` — every rows/sec computed from a measured row count ÷
a measured wall, never estimated, never copied from an earlier
different-code-different-machine-era phase (the Session-12 boot prompt names
that copy-the-old-numbers move as trap (a), and a synthetic generator
pretending to be "the 5M load" as trap (b)).

The constraints that shape the design:

- **Never-touch**: `oltp` (5.4M+ rows) and `soap` (382k orders) are living
  corpora — reseeding is forbidden; the mutator never stops (pausing it for a
  "clean" CDC number would measure a lie); the CDC connector/slot is never
  re-set-up; the 8 RESOLVED dq incidents and the snapshot invariant
  (50,001 rows | min(dbt_valid_from) 2026-09-11 05:40:37.528841) must survive.
- **The platform is incremental by design at ingest** (watermarks + hash
  ledger, ADR-006/007; lsn-guarded CDC, ADR-005). There is no operational
  "re-ingest 5M rows" button to time, and building one just for the bench
  would be a rig, not the platform.
- The nightly `daily_close` fires 05:00 UTC (`max_active_runs=1`,
  `catchup=False`); a bench leg that collides with it measures the queue,
  not the stage.
- The chaos suite (`make chaos-test`, ~53 min, destructive) stays separate;
  smoke-test must not grow by a single check.

## D1 — What "5M-row full load" means here, and the A/B decision

The measured fact that dissolves the paradox: **every dbt build IS a full
load.** All 14 models are `+materialized: table` (dbt_project.yml), so each
build re-materializes the entire published corpus from the raw envelope:

- staging (9 tables): ~7.35M rows measured 2026-09-14, of which
  `stg_order_items` = 5,399,261 — **the mandate's "5M-row" table, measured,
  live**;
- marts (5 tables): ~6.57M rows (`fct_order_items` alone 5.40M);
- plus the SCD2 snapshot scan (50,001 rows, in-place update strategy).

So the honest "5M-row full load" = one real `dbt build` under the dbt-ol
wrapper (the same invocation `make dbt-build` and the DAG's `dbt_build` task
run), timed, with rows/sec computed from post-build measured `count(*)`s ÷
measured walls. Around it, the OTHER stages get their own real legs: the SOAP
full-walk replay (the one genuinely cold-ish full read: epoch→now, 382k rows),
the CDC drain rate on the churning source, the GE gate scan, and one full
orchestrated close end-to-end.

| Option | What it measures | Verdict |
|---|---|---|
| **A: time the REAL pipeline per stage on the real corpus** (SOAP full walk + CDC drain window + real dbt full re-materialization + real GE gate + one real orchestrated close) | The shipped platform, on the data it actually serves, re-runnable any day, zero new state | **DECIDED** |
| B: a dedicated bench database/schema (own volume, own DDL, COPY-based 5M-row load through the real tooling) | A rig: synthetic generator + synthetic DDL, measuring a thing the platform never does at that layer | Rejected — trap (b) verbatim; the ingest layer is incremental by design, and a COPY bench schema would measure the rig's DDL, not HELIOS; fails the earn bar (new volume, new state, new code for a number the real pipeline already produces honestly) |

Honest labeling of A's edges (recorded here, repeated in metrics.md): (1) the
dbt full load re-materializes ~13.9M rows per build — bigger than 5M — because
the corpus GREW since the mandate was written; the 5.4M items table is reported
as its own row. (2) The SOAP full walk lands 0 rows (hash-guard idempotence) —
its honest rate is a READ rate, labeled as such. (3) The CDC leg measures the
APPLIED event rate on a churning source (the production-honest number), not a
paused-source number. (4) File+REST ingest re-runs are zero-work by the
idempotence contract; their walls are reported with landed counts, and no rate
is fabricated for a zero-work leg.

## D2 — The per-stage measurement plan (numerator ÷ denominator, clocked how)

One table per leg; every rate = measured count ÷ measured wall, both printed
in the same transcript, computed with awk from `date +%s.%N` pairs. Parse
failures abort the leg (a rate that cannot be traced to both its numerator and
its wall is a FAIL, not a print).

| Leg (script) | Stage | Numerator (measured how) | Denominator (measured how) |
|---|---|---|---|
| `00-env` | environment | — (hardware + pre-state inventory: lscpu/free/df/lsblk/kernel/docker-context, 17-container health, baseline counts, quarantine 0 OPEN, snapshot invariant) | — |
| `01-ingest` | batch ingest, SOAP full walk | `rows_read` parsed from the run's own `run_completed` JSON event (assert ≥ 380,000) | wall of `docker compose run --rm ingest python -m ingest.run --source soap --full` |
| `01-ingest` | batch ingest, file + REST | `rows_read`/`rows_landed` from the same JSON events (zero-work legs reported as walls + landed counts, honestly labeled) | wall of the one-shot runs |
| `02-cdc` | CDC applied drain | Δ `count(*)` of `raw.cdc_users ∪ cdc_orders ∪ cdc_order_items ∪ cdc_payments` between two `cdc-status`/SQL endpoints (mutator running — no pause), asserted lag TOTAL=0 at the window end | the measured wall between the endpoint probes (600 s window) |
| `03-dbt` | **the full load**: staging materialize | Σ post-build `count(*)` of the 9 staging tables | Σ per-model walls parsed from dbt's own `OK created … in N.NNs` output |
| `03-dbt` | snapshot scan | `count(*)` of `snapshots.customers_snapshot` (50,001 — invariant re-asserted post-build) | the snapshot's parsed wall |
| `03-dbt` | marts materialize | Σ post-build `count(*)` of the 5 marts | Σ per-model walls, same parse |
| `03-dbt` | dbt total | Σ all materialized rows + snapshot | the end-to-end `dbt build` wall; `PASS=160` asserted (1 hook + 1 snapshot + 14 tables + 144 tests) |
| `04-dq` | GE semantic gate | Σ `count(*)` of the 6 suite tables (stg_payments, stg_orders, stg_order_items, stg_file_customers, stg_soap_orders, fct_orders), measured pre-gate | wall of `docker compose run --rm dq python -m dq gate`, exit 0 asserted |
| `05-close` | orchestrated daily_close end-to-end | staging+marts rows materialized by THIS close (measured post-close), plus the per-task walls read from `task_instance` start/end in the airflow metadata DB (server-side clocks — the honest cross-proof of legs 03/04) | dag-run `start_date → end_date` (server-side) AND the client-observed wall of `scripts/run-etl.sh`, both printed |

Per-model rates for the big tables (stg_order_items, fct_order_items) are
printed individually in the leg transcript — the "5M rows/sec" headline is the
items pipeline: 5.40M rows staged + 5.40M published per build.

## D3 — The nightly-collision policy (05:00 UTC)

The ADR-014 D4 preflight pattern, restated for bench: before EVERY leg, query
`dag_run` for any `daily_close`/`ingest_*` run in `running`/`queued` state and
abort loudly if found. Additionally, because a bench leg STARTED at 04:55
would straddle the fire even with a clean preflight, each preflight also
computes the seconds to the next 05:00 UTC boundary and aborts before starting
any leg inside a 30-minute guard window (the longest leg is the close, < 10
min warm). A leg that collides anyway (host suspend, etc.) is detectable after
the fact: each transcript prints the UTC start time and the measured distance
to the fire. Session 13 runs 2026-09-14 ~22:40 UTC → next fire 2026-09-15
05:00 UTC, > 6 h away: no collision is possible, the guard is still exercised
on every leg.

## D4 — The hardware disclosure shape (measured fresh, every pass)

`00-env` records, as command output (never prose): UTC date; `lscpu` model +
cores; `free -h` total; `df -h /`; `lsblk` ROTA flag of the root device;
kernel; `docker context show` + the rootless facts (slirp4netns/fuse-overlayfs,
no cgroup limits, published ports loopback-only — RUNBOOK §5); the 17 healthy
containers. `EVIDENCE/metrics.md` opens with this header verbatim, labeled
"personal laptop, rootless Docker — numbers are THIS machine's, not production
benchmarks" per MASTER_PROMPT §2 rule 4.

## D5 — Re-runnability, loudness, variance

`make bench [BENCH_LABEL=<label>]` runs legs 00→05 in order; each leg tees a
full transcript to `EVIDENCE/bench-<label>-NN-<stage>.log` (default label =
UTC timestamp, so repeated passes never overwrite each other — the ≥2-pass
variance mandate). Leg scripts are individually runnable. Every setup step's
exit code is checked; every parse is asserted; a vacuous PASS is impossible
(the chaos/lib.sh and smoke precedents). The bench touches NO state it does
not own: no reseed, no `--full-refresh`, no connector changes, no network
verbs, no container kills, no mutator pause — every write it triggers is an
idempotent pipeline action the platform already runs daily (a close, a
gate). Variance policy: the suite is run ≥ 2× end-to-end; `metrics.md` prints
both passes' numbers and the relative delta, stated, not hidden.

## D6 — Scope fences

Bench does NOT: run or grow smoke-test (delta exactly 0, git-proven at close);
run chaos scenarios; pause the mutator; create volumes/schemas/tables; author
new HTTP client code (host `curl` against the already-documented local
Prometheus API for the read-only dagrun-duration cross-proof only); add new
test counts (bash orchestration — the repo test convention stays pytest 195 /
dbt 144 / dq 32). The four regression guards (dq-status 0 OPEN,
lineage-verify, metrics-verify, smoke-test) close the session, after the
bench has finished stressing the stack — never concurrent with it.

## D7 — Evidence shape

Per-leg full transcripts: `EVIDENCE/bench-<label>-00-env.log` …
`-05-close.log` (every command, every measured number). The roll-up:
**`EVIDENCE/metrics.md`** — the exact file the backlog names — ONE table:
stage | rows | wall | rows/sec | how measured (command + log file), under the
D4 hardware/date header, plus the two-pass variance table. No separate
phase-5-bench.md: metrics.md IS the deliverable. Devlog-012 may reference the
Grafana duration panels as bench-adjacent color, but every number in
metrics.md traces to a bench log.
