# ADR-010 — Airflow orchestration: invocation mechanism, DAG topology, DQ-gate placement

Date: 2026-09-12 · Status: accepted · Phase: 3 (item 9, phase close)
Decides: how the Airflow scheduler (2.10.5, LocalExecutor, running DAG-less since
Phase 0) actually invokes the existing one-shot TOOL containers (`ingest`, `dbt`);
the DAG topology (per-source DAGs + master `daily_close`); where the DAG-level
DQ gate sits and where Phase 4 (Great Expectations) plugs in; the semantics of
`make run-etl` and `make backfill`; the rootless-docker socket/uid fix.

## Context

Airflow has been in the stack since Phase 0 (webserver :8080 + scheduler +
metadata db, healthy, DAG-less). The pipeline units it must orchestrate already
exist and are proven: `docker compose run --rm ingest python -m ingest.run
--source {soap|file|rest}` (ADR-006/007) and `make dbt-build` = `docker compose
build dbt && docker compose run --rm dbt build` (ADR-008/009; ONE `dbt build`
covering staging → snapshot → marts + 144 tests; NEVER `--full-refresh`).

Measured fact base this ADR relies on (all re-measurable; see
EVIDENCE/phase-3-airflow.md):

- **The scheduler image already ships a docker CLI** (apache/airflow
  2.10.5-python3.11 → `/usr/bin/docker`, client 27.5.1); only the **compose
  plugin** is missing. The host's compose plugin
  (`/usr/libexec/docker/cli-plugins/docker-compose`, v5.5.1) is a **static ELF** —
  copyable into the image without dependency risk, giving the scheduler byte-exact
  the same compose parser `make` uses (host CLI is v5.5.1; client/server 29.8.0 on
  the daemon side).
- **Docker is rootless**; the daemon socket is
  `/run/user/1000/docker.sock`, `srw-rw---T potato:100121`. Group `100121` is an
  **unnamed rootlesskit allocation** (no `/etc/group` entry) — not a contractual,
  restart-stable gid. The socket *owner* (uid 1000, the user running the rootless
  daemon) is the only stable access signal.
- `.env` sets `AIRFLOW_UID=50000` (image default) — an Airflow container running
  as 50000 matches neither socket owner nor group.
- **Airflow REST basic auth works out of the image** (curl
  `-u $AIRFLOW_WWW_USER:… http://localhost:8080/api/v1/health` → 200).
- Ingest CLIs accept exactly `--source {soap,file,rest,all}` and (soap only)
  `--full`. There are **no historical window parameters**: SOAP's incremental
  window is `max(created_at) − 7 d` overlap by design (ADR-006 D1), REST is a
  full cursor walk, file is hash-ledger driven.
- `ingest/ensure.py` runs shared `CREATE TABLE IF NOT EXISTS` DDL (ledgers,
  quarantine, per-source tables) on **every** extractor start, with no advisory
  lock — three concurrent first-ever extractor runs on a *fresh* warehouse race
  on `pg_class`'s relname unique index. On a warm warehouse the relations exist
  and the check is read-only (no race).
- dbt `build` interleaves tests into the resource DAG: an error-severity test
  blocks every downstream node (staging tests gate marts inside ONE invocation;
  marts tests gate the published layer — ADR-008 D3 / ADR-009 D6).

## Decisions

### D1 — Invocation mechanism: **BashOperator → docker CLI + compose plugin → rootless socket → the SAME one-shot containers make runs**

The scheduler speaks to the **same rootless daemon** make uses and issues the
**same commands the make targets use**, verbatim:

- `ingest`: `cd /helios && docker compose run --rm ingest python -m ingest.run
  --source <s>` — identical to `make ingest-soap/-file/-rest`.
- `dbt`: `cd /helios && docker compose build dbt && docker compose run --rm dbt
  build` — identical to `make dbt-build` (image rebuild first, so a project edit
  can never be missed by a run; `build` is a cached no-op when unchanged).

One pipeline unit, zero logic duplication, no fetcher re-implementation, no HTTP
in DAG code, no credentials in DAG code (tool containers get theirs from compose
`.env` interpolation, exactly as make does).

**Image extension (not a new service):** `airflow/Dockerfile` =
`FROM apache/airflow:2.10.5-python3.11` + `COPY docker-compose` into
`/usr/local/lib/docker/cli-plugins/`. The plugin binary is **copied from the
host at build time** by `make airflow-image` (staged into `airflow/`, a
gitignored build artifact) — the plugin version must match the host's compose
parser (v5.5.1), and a host-static binary is the only dependency-free way to
guarantee that. The in-image CLI (27.5.1) API-negotiates with the daemon
(29.8.0) — verified live. A fresh clone rebuilds with *its own* host plugin,
which is exactly the parity we want; the target aborts loudly when no host
plugin is found.

**Socket + permissions (the deliberate fix — with one measured false start):**
the scheduler mounts the rootless socket at `/var/run/docker.sock`
(`DOCKER_HOST` set explicitly) and the airflow services run as **`user: "0:0"`**
— the daemon's **user-namespace root, which IS the host user running the
rootless daemon** (potato) and therefore the socket's owner. The first attempt
(`AIRFLOW_UID=1000` = host uid, owner-match) failed empirically with
`permission denied`: under rootlesskit the container uid namespace maps
container uid 0 → host user and uid ≥ 1 → the subuid range
(`potato:100000:65536` measured) — container uid 1000 is host uid 101000, which
matches neither the socket owner (host 1000) nor its group (host 100121 → in-
namespace gid 122). The socket *group* is additionally an unnamed rootlesskit
allocation (`getent group 100121` is empty) — not a restart-stable signal, so
`group_add` would be doubly fragile (subgid arithmetic × ephemeral gid).
`user: "0:0"` has exactly one stable anchor: container root = host daemon user,
which is the whole point of rootless docker — **no privilege exists in the
container beyond what that host user already holds** (bind mounts stay
read-only; the socket grants exactly the daemon this user already controls).
Cost: the `airflow_logs` volume (dirs previously created by container uid
50000) needs a **one-time idempotent re-own** — `make airflow-image` performs
`chown -R 0:0` via a throwaway container and is safe to re-run; documented in
RUNBOOK §4b.
Consequence accepted and documented: the scheduler holds a docker socket = root
equivalent on the local daemon; this is a single-user local portfolio stack, and
the scheduler already runs arbitrary task code by design. The repo is mounted
read-only at `/helios` (compose build contexts are read; nothing in the project
dir is written); `.env` (mode 664, owner 1000) is readable for compose
interpolation and contains only the committed local-dev defaults.

**Alternatives weighed honestly:**

| Option | Verdict |
|---|---|
| dbt inside the scheduler venv | Rejected — breaks the pinned-image story (ADR-008 D5: `helios/dbt:latest` with pinned dbt-core/dbt-postgres is the one pipeline unit), double dependency install, drift between "what make runs" and "what the DAG runs". |
| Docker operator provider (`apache-airflow-providers-docker`) | Rejected — same socket requirement underneath, plus a provider dependency, per-task image/env API surface to maintain, and it would *re-declare* the tool containers' env in DAG code instead of reusing compose. |
| Trigger ingest/dbt via HTTP | Rejected — reimplements orchestration of already-fetching containers and puts HTTP into DAG code (banned shape); the containers exist to own their protocols. |
| make/cron on the host | Rejected — defeats the orchestrator: no UI, no retries/SLAs/task docs, no run history, split-brain with the Airflow install that has been running since Phase 0. |
| `group_add: [122]` (in-namespace gid of the socket group) | Rejected — depends on BOTH the subgid arithmetic (host 100121 → in-namespace 122) AND the socket group being a rootlesskit-ephemeral allocation (`getent group 100121` is empty; could churn on daemon restart). `user: "0:0"` has one stable anchor. |
| Run as `AIRFLOW_UID=1000` (host-uid owner match) | **Tried first, failed measured**: container uid 1000 = host uid 101000 (subuid range) — `permission denied` on the socket. Rootless userns makes container uid 0 the only owner match. |

### D2 — DAG topology: **3 per-source DAGs (schedule=None) + master `daily_close` composing them via TriggerDagRunOperator**

- `ingest_soap` / `ingest_file` / `ingest_rest`: one BashOperator task each,
  `schedule=None` (the master triggers them; standalone manual runs remain
  possible for ops), `max_active_runs=1` (extractors are uncoordinated one-shots
  — two concurrent runs of one source are prevented at the DAG level).
- `daily_close`: `schedule="0 5 * * *"` (UTC), `catchup=False`,
  `max_active_runs=1` (a full-refresh-rebuilt marts layer + the snapshot are not
  concurrency-safe), topology
  `ingest_file >> [ingest_soap, ingest_rest] >> dbt_build`:
  - `ingest_file` runs **first on purpose**: every extractor start executes the
    shared ingest DDL (ledgers/quarantine — D-context above); running file first
    (the smallest feed, ~seconds, idempotent) warms the DDL before soap ∥ rest
    fan out, structurally removing the cold-start `CREATE TABLE IF NOT EXISTS`
    race. On a warm warehouse this is a no-op re-order; the two heavy walkers
    (soap window, rest cursor walk) still run concurrently and touch disjoint
    raw tables.
  - `dbt_build` is ONE task: `build dbt` (image currency) + `dbt build`
    (staging → snapshot → marts + tests, single invocation — no separate dbt
    step; snapshot never sees `--full-refresh`).
- Every task carries the DoD surfaces: `retries` (ingest 2, exponential backoff,
  15 min cap; trigger 1), `retry_delay`, `execution_timeout`, `sla`,
  `doc_md` on task and DAG, `owner`, `tags`. SLA misses surface in the UI
  (local stack: no SMTP — documented; alerting arrives with Phase 4 item 12).
- `TriggerDagRunOperator(wait_for_completion=True, poke_interval=30,
  deferrable=False)` — LocalExecutor has no triggerer; the waiter task is an
  ordinary slot. Child failure fails the trigger task (default
  allowed/failed-state semantics), failing the master run.
- DAG code embeds **no SQL at all** (the preferred Mimosa shape: DAGs orchestrate
  containers; SQL lives in dbt files and the smoke script), **no HTTP**, **no
  credential literals**. `DAGS_ARE_PAUSED_AT_CREATION=true` stays the compose
  policy; `make run-etl` unpauses explicitly (idempotent) so a fresh stack is
  triggerable, and `catchup=False` makes unpause safe.

### D3 — DQ-gate placement: **the single `dbt build` task IS the DAG-level gate; Great Expectations (item 10) plugs in as a dedicated task downstream of it**

- `dbt build` already implements "staging tests gate marts": tests run inline in
  the resource DAG, error-severity tests block every downstream node
  (warn-tolerant staging per ADR-008 D3; strict published layer per ADR-009 D6),
  and the task's nonzero exit is the gate the DAG reacts to (retry → fail the
  run). A DAG-level split `--select staging` then `--select marts+` would
  re-select the graph across two invocations (staging rebuilt twice or fragile
  selection sets to maintain) and buy exactly the gating semantics the
  interleaved build already has.
- **Where GE goes (Phase 4)**: expectation suites + quarantine + replay are
  semantic DQ on top of the structural DQ dbt owns; they land as an explicit
  task **between `dbt_build` and publish/exit** (suites over staging + marts,
  failures quarantine rows and fail the run — same nonzero-exit gate seam), so
  the orchestration contract stays "one task, one nonzero exit, one gate". If
  item 10 needs a raw-side gate too, it inserts as a second task between the
  ingest fan-in and `dbt_build` — the single-dbt-task shape leaves both seams
  free. Decision deferred to item 10; the seam is reserved here.

### D4 — `make run-etl`: **unpause → trigger (known run_id) → poll to terminal state → nonzero exit on failure**

`scripts/run-etl.sh` drives the **airflow CLI inside the scheduler container**
(`docker compose exec -T airflow-scheduler airflow …`): no REST call, no
credential handling in any script (the CLI path is the control plane the
scheduler itself trusts; REST basic-auth remains available to humans at
:8080 — verified working). Sequence: preflight (scheduler container healthy) →
unpause the four DAGs (abort loudly on failure) → `airflow dags trigger
daily_close -r etl-<UTC ts> -e <UTC ts>` (abort loudly on failure) → poll
`airflow dags state daily_close <exec_date>` every 10 s until
`success|failed` or a 45 min deadline (`RUN_ETL_TIMEOUT_S` overridable) → on
`failed`, print the failed task instances (read-only query of the metadata db)
and exit 1; on timeout, say so and exit 1. A vacuous PASS is impossible: every
setup step's exit code is checked, **and success is only accepted when the run
has task instances and every one of them is `success`** — the guard exists
because of a measured trap: Airflow's `DagRun.verify_integrity` only creates
task instances with `task.start_date <= execution_date`, and the scheduler
skips verify_integrity when the run's dag_hash matches the current
serialization — so the first trigger of this session (exec date 23:50:27Z,
before the initial `start_date` of 2026-09-12T00:00Z) executed ZERO tasks and
was marked `success` in 0.03 s. Fixed by pinning `START_DATE` safely in the
past (2026-09-10) + the run-etl task-instance guard; recorded here so the trap
is knowledge, not folklore. An already-running `daily_close` (e.g. from the
schedule) simply queues ours behind `max_active_runs=1` — the deadline covers
it.

### D5 — `make backfill`: **honest replay semantics — the watermarks+ledger already ARE the backfill**

Measured contract first: the ingest CLIs accept no historical window parameters
(`--source` + soap `--full` only). The pipeline's backfill happened at first
landing and is *kept correct by construction*: SOAP pulled 3 years of history
(382,179 orders, 2023-09 → 2026-09), REST walks the full catalog every run, file
feeds land every file the ledger hasn't seen. Therefore:

- `make backfill` = semantics banner + a **full master-DAG replay**
  (`daily_close`, run_id `backfill-<ts>`) + the run ledger tail via
  `make ingest-status`. The replay's *proof value* is the DoD's idempotency
  claim: the hash ledger makes unchanged content a **physical no-op**
  (`rows_unchanged` accounting, zero new writes), the snapshot records zero new
  versions, and mart counts stay bit-stable modulo attributed CDC drift.
- The one place window mechanics *do* exist — SOAP `--full` (epoch→now replay,
  hash-guarded, ~2.5 min measured in Phase 2) — stays a documented manual
  operation (`docker compose run --rm ingest python -m ingest.run --source soap
  --full`), not a DAG path: wiring it through the DAG would add conf-branching to
  the topology for zero daily value. VERIFIER records one real `--full` replay
  as the window-mechanics evidence.
- Airflow-native `dags backfill -s -e` over 90 logical dates is **deliberately
  not wired**: with watermark+ledger extractors and full-refresh-rebuilt tables,
  90 runs of the same idempotent commands produce 90× the work and zero data
  difference. `catchup=False` encodes that at the DAG level. An honest replay
  proof beats a fake window; the coverage claim (3 years landed) is re-asserted
  from the live warehouse in EVIDENCE, not promised by a flag.

### D6 — `make smoke-test` Stage 2: **a real orchestrated run + the published-layer contract asserted against the live warehouse**

Stage 2 (Stage 1 stays byte-identical, 19/19):

1. `bash scripts/run-etl.sh` — the E2E path IS the orchestrator (no parallel
   "smoke-only" invocation to drift from production).
2. `airflow dags list-import-errors` == 0.
3. Mart row-count parity vs staging (mirrors the dbt singular `marts_row_parity`
   contract, re-asserted from the live warehouse via psql single-line static
   SQL): `fct_orders = stg_orders + stg_soap_orders`; `fct_order_items =
   stg_order_items`; `dim_product = distinct(rest ∪ file) SKUs`;
   `dim_customer current = stg_users`.
4. SCD2 checks: exactly one current row per customer; windows contiguous
   (`valid_to = next valid_from`, no gaps/overlaps, only the last version open);
   unknown-member discipline both directions (`customer_sk IS NULL ⇔
   source_type='soap'`; OLTP rows always resolve).

Between the dbt build and the assertions the only writer is the live CDC sink —
staging/marts are frozen tables, so the parity counts are stable during the
check window. Stage 2 failure exits nonzero (make fails loudly).

## Consequences / known limits

- The scheduler container holds the rootless docker socket (root-equivalent on
  the local daemon) and a read-only view of the repo — accepted for a local
  single-user portfolio stack; called out in RUNBOOK §4b. Do not point this at a
  multi-tenant host without a socket proxy. The airflow services run as
  container uid 0, which under rootless docker IS the host daemon user — no
  privilege beyond that user's own daemon.
- The extended image builds only when a host compose plugin exists (make target
  aborts loudly otherwise); the gitignored `airflow/docker-compose` artifact must
  never be committed (~65 MB binary).
- `AIRFLOW_UID` (`.env`/`.env.example`) is now **only the host-side socket path
  knob** (`/run/user/<AIRFLOW_UID>/docker.sock`); the containers run as
  `user: "0:0"`. The logs volume needs its one-time re-own after a uid change
  (`make airflow-image`, idempotent). Fresh clones (new volume) need nothing.
- In-image docker CLI (27.5.1) vs daemon (29.8.0): API negotiation verified live
  during bring-up; if a future daemon drops old API versions, bump the base
  image (one-line FROM change) — recorded as a known limit.
- SLA misses are UI/log-only until Phase 4 adds alerting (no SMTP in the stack).
- The master DAG's `max_active_runs=1` serializes *DAG-initiated* dbt builds;
  a human running `make dbt-build` while `daily_close` is mid-build is NOT
  protected (two concurrent full-refresh builds) — documented op hazard, same
  class as the pre-Airflow era.
- Backfill semantics are replay-based by design (D5); a future extractor
  windowing feature (e.g. SOAP date-range parameters) would extend, not replace,
  this contract.
