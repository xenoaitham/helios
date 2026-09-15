# ADR-012 — OpenLineage + Marquez: version pins, the emit boundary, service topology & storage, the non-fatal lineage contract

Date: 2026-09-13 · Status: accepted · Phase: 4 (item 11)
Decides: the Marquez 2026 research verdict and the exact pins that follow from
it; the emit boundary (what the lineage graph honestly shows and what it
cannot); the Airflow-side wiring (provider kept at the constraint-pinned
version, config env-only); the dbt-side wiring (the `dbt-ol` wrapper as the
ONE invocation path); the Marquez service topology, storage decision and wipe
story; the non-fatal lineage contract and how it is proven; the smoke-test
impact; the child-DAG (TriggerDagRunOperator) lineage boundary.

## Context

Phase 3 closed with `daily_close` = ingest_file → (ingest_soap ∥ ingest_rest)
→ dbt_build (ADR-010 D1/D2); Phase 4 item 10 appended `dq_gate` (ADR-011 D3).
Item 11 mandates OpenLineage events from Airflow/dbt rendering table+column
lineage source→mart in the Marquez UI — **measured events, not config
claims** — with the pipeline proven non-fatal when Marquez is down.

This is the first NEW long-running container(s) since Phase 0. ADR-012 must
earn them before code. Fact base (researched 2026-09-13, primary sources
fetched and recorded in EVIDENCE/phase-4-lineage.md):

- **Marquez status (verified 2026-09-13, github.com/MarquezProject/Marquez +
  Docker Hub registry API):** NOT archived (`archived: false` via the GitHub
  API), but effectively stalled upstream: last release **0.51.1
  (2025-03-27)**; since then only four minor commits on `main` through
  2026-04-12 (docs fixes, React 18/19 bumps, one perf fix); the README still
  claims "active development" and the CHANGELOG stops at 0.50.0. **No dated,
  citable maintenance-mode or archival announcement was found** (OpenLineage
  blog, openlineage.io docs, repo issues/discussions all checked) — the
  rumor is recorded as a rumor, and the *measurable* stall is recorded as
  the fact. openlineage.io/docs still calls Marquez "the reference
  implementation of the OpenLineage API" (fetched 2026-09-13).
- **Images:** `marquezproject/marquez:0.51.1` (api; exposes 5000 lineage API
  + 5001 Dropwizard admin with `/healthcheck`) and
  `marquezproject/marquez-web:0.51.1` (static UI, finds the api via
  `MARQUEZ_HOST`/`MARQUEZ_PORT`, serves :3000). Upstream's own compose runs
  the api against **its own Postgres** (`POSTGRES_HOST=db`,
  `migrateOnStartup: true`); H2 is only a dev/test mode. `latest` == 0.51.1
  today; upstream publishes nothing newer.
- **Column lineage capability (Marquez CHANGELOG):** column-level lineage
  representation + `GET /api/v1/column-lineage` since **0.27.0** (2022);
  dedicated column-level UI page since **0.45.0** (2024-03). 0.51.1 has both.
- **Airflow side (verified against the RUNNING image, not docs):**
  `apache-airflow-providers-openlineage==2.0.0`, `openlineage-python==1.27.0`,
  `openlineage-sql==1.27.0` are already installed in
  `apache/airflow:2.10.5-python3.11` (the openlineage extra is in the base
  image's default `AIRFLOW_EXTRAS`, pinned by the official
  constraints-2.10.5). `airflow.providers.openlineage.conf` probed live:
  `is_disabled()` returns True with no transport configured (the integration
  is OFF until `[openlineage] transport` exists); `namespace()` defaults to
  `default`; the listener plugin and the client's `HttpTransport` are
  present. Version-boundary research: provider **2.8.0 is the newest that
  supports Airflow 2.10.x** (2.9.0, 2025-11-27, bumps to
  `apache-airflow>=2.11.0`; latest 2.20.1 requires 2.11+ too).
- **Provider failure semantics (2.8.0 source, same architecture in 2.0.0):**
  every listener hook is wrapped in `try/except` and emissions run in a
  forked process with a 10 s timeout — an unreachable backend logs a warning
  and the task's execution status is untouched ("This has no impact on
  actual task execution status").
- **dbt side (PyPI + OpenLineage repo source, 2026-09-13):**
  `openlineage-dbt` is **alive, not deprecated** — 1.53.0 released 2026-09-01
  with an active release cadence through 2026; requires `dbt-core>=1.0.0`
  (1.9.11 satisfies it) and ships manifest-v12 fixes (the manifest schema
  dbt 1.9 emits). Invoked as `dbt-ol`, which wraps the dbt invocation
  (dbtRunner), then parses `target/manifest.json` + `run_results.json`
  (+ `catalog.json` when present) and emits per-node events. **Column-level
  lineage is real and mechanical**: `get_column_lineage()` parses the
  compiled SQL with the native `openlineage_sql` Rust parser and emits
  `ColumnLineageDatasetFacet`. **Failure semantics verified in source:**
  each `client.emit` is `except Exception`-wrapped, the outer handler logs
  "OpenLineage failed to process dbt execution. This does not make dbt
  execution fail…", and the wrapper returns **the underlying dbt exit
  code** — Marquez down cannot fail a dbt run.
- **Config contracts:** client 1.53.0 resolves config
  `OPENLINEAGE_DISABLED` → explicit → `OPENLINEAGE_CONFIG` file →
  `OPENLINEAGE__*` env → plain `OPENLINEAGE_URL` → fallback console
  transport (events printed, never lost silently as "sent"). Airflow 2.10
  has an `[openlineage]` config section keyed
  `AIRFLOW__OPENLINEAGE__*` (there is no `__ENABLED`; presence of a
  transport is the switch).
- **The graph's subject matter is fixed by the codebase:** dbt's
  `models/staging/_sources.yml` declares the raw tables as sources; every
  staging model reads raw, every mart reads staging (and the snapshot). The
  extractors (SOAP/file/REST) and the CDC sink are Python tooling that
  lands rows via SQL upserts — they are NOT dbt resources and emit no SQL
  that a parser could attribute.
- **Mimosa canon** (cumulative, Sessions 2–10): source files via Write/Edit
  only; no credential-looking literals anywhere (env only); filesystem
  writes through containment; HTTP clients we author need same-module
  host+path allowlists.

## Decisions

### D1 — Version pin: Marquez `0.51.1` frozen; Airflow provider kept at the constraint-pinned `2.0.0`; dbt wrapper `openlineage-dbt==1.53.0` + `openlineage-python==1.53.0`

**Marquez `:0.51.1` (exact tag, never `latest`), api + web both.** It is the
final upstream release; `latest` is identical today but an unpinned tag on a
stalled repo is a silent-drift hazard with no upside. The ADR-011 N-1
discipline does not apply (there is no newer line to fall back to) — the
discipline here is *freeze + migration path*:

- **Risk, honestly stated:** upstream is stalled since 2025-03-27 (D-facts
  above); bug fixes and OpenLineage-spec evolutions will not land in Marquez.
- **Mitigation 1 — the spec is the contract.** We emit standard OpenLineage
  JSON over HTTP to `/api/v1/lineage` from two independent emitters; Marquez
  is a replaceable *backend*. Any OL-compatible backend (or a newer fork —
  the ecosystem already has one advertising API compatibility) is a
  transport-URL change, not an emitter rewrite. The events themselves are
  portable by construction.
- **Mitigation 2 — the pin is baked into compose, and the whole graph is
  re-derivable** (D5 wipe story): the backend holds *derived* metadata; a
  backend swap or wipe costs one pipeline re-run, never source data.
- **Mitigation 3 — upgrade/exit path documented here:** Airflow provider
  2.8.0 (newest for 2.10.x) and 2.9.0+ (needs Airflow ≥2.11) are the
  measured boundary table; openlineage-dbt 1.53.0 tracks dbt Fusion caveats
  upstream (issue #4253) — both recorded so the next session inherits the
  research, not the search.

**Airflow provider: keep `2.0.0` — zero image delta.** The provider is
already installed and constraint-pinned in the 2.10.5 base image (probed
live on the running scheduler: 2.0.0 / openlineage-python 1.27.0 /
openlineage-sql 1.27.0). Wiring it is pure environment (D3). Alternatives
considered and rejected:

- *Install 2.8.0* (newest 2.10-compatible): requires a pip install inside
  the extended image **without** the official constraints file (the
  constraint file pins 2.0.0) — i.e. an unconstrained dependency
  resolution on top of a webserver image, purely to gain ~15 months of
  provider fixes we have no use for on this item's scope (the listener,
  BashExtractor and soft-failure semantics all exist in 2.0.0). Cost/risk
  without benefit; recorded as the *migration path* instead.
- *Custom extractor:* nothing to extract — our tasks are BashOperator
  one-shots whose datasets are unknowable from the command line (D2).
  Building a custom extractor would manufacture fake dataset edges. The
  ADR-010 D1 "no new mechanism without a job it does" rule applies.

**dbt: `openlineage-dbt==1.53.0` with `openlineage-python==1.53.0` pinned
explicitly** alongside the unchanged exact pins `dbt-core==1.9.11` /
`dbt-postgres==1.9.1` (the wrapper's `dbt-core>=1.0.0` is satisfied; the
exact dbt pins prevent pip from walking dbt-core upward behind the
wrapper's back). The dbt image is a hermetic one-shot tool (ADR-008 D5):
exact pins, baked image, upstream churn cannot move a shipped emitter.

### D2 — THE EMIT BOUNDARY (verbatim; UI evidence must not oversell this)

> **The lineage graph HELIOS ships is exactly the dbt graph: raw source
> tables (as declared in `models/staging/_sources.yml`) → staging models →
> marts, with column-level lineage wherever the `openlineage_sql` parser
> resolves column provenance through the compiled SQL — evidenced for at
> least one mart. The ingest→raw hop exists only as job-level Airflow
> task/run events (no dataset edges): the true sources (SOAP, REST, file
> drops, CDC) sit outside SQL, and the ingest tools' upsert SQL is
> library-generated — no honest dataset-level edge into raw can be
> manufactured from it, and none is. "source→mart" therefore means *dbt's
> raw sources → marts*; column-level lineage stops at the dbt graph's
> edges. The Airflow layer contributes job/run/task facets — the task
> graph including `dq_gate`, run timings, statuses — and `dq_gate` appears
> in Marquez as an Airflow job with no dataset edges (the GE gate emits no
> lineage events of its own; ADR-011 stands: dq is untouched). Airflow's
> `dbt_build` task and the dbt-ol-emitted dbt run are different jobs in
> different mechanisms: the dataset graph is drawn by the dbt-ol events
> from inside the one-shot dbt container.**

Consequences of the boundary, accepted: Marquez will show the
raw→staging→marts dataset graph plus unconnected Airflow job nodes in the
same namespace; that is the honest picture of a stack whose movement layer
is partly non-SQL. A PR would be welcome to declare the ingest output
tables as *outputs* of the ingest Airflow tasks via custom extractors —
rejected here (D1) as fabricated semantics.

### D3 — Airflow wiring: env-only, in `x-airflow-common`, no DAG changes

Config lives in compose environment (never in DAG code — ADR-010 rule):

- `AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url":
  "http://marquez-api:5000", "endpoint": "api/v1/lineage"}'` — in-compose
  topology (service name + the api container's own listener port), same
  policy as `CDC_CONNECT_URL`/`INGEST_SOAP_BASE_URL`. No literals in DAG
  or tool code.
- `AIRFLOW__OPENLINEAGE__NAMESPACE=helios` — the same namespace the dbt
  side emits into (D4), so jobs and datasets from both emitters render in
  one Marquez namespace. (Provider default is `default`; explicit wins.)
- Presence of the transport switches the integration on (probed:
  `is_disabled()` True today, because no transport exists). No `__ENABLED`
  key exists on 2.10 — documented so nobody hunts for one.
- **BashOperator events:** the built-in `BashExtractor` emits job/run/task
  facets (command as source-code facet, run timings, terminal states) and
  deliberately no datasets — matching D2. `dbt_build`, `dq_gate` and the
  three trigger tasks all emit; **`dq_gate`'s events come from Airflow
  only** (D8).
- **Child-DAG boundary — honest gap:** the TriggerDagRunOperator-triggered
  child DAG runs (`ingest_file/soap/rest`) emit their own task/run events
  (they are ordinary DAG runs in the same scheduler; the listener fires on
  every task instance). What does NOT happen on provider 2.0.0 is automatic
  parent→child run linkage (the trigger task's run referencing the child
  DAG run as a parent facet). Auto-injection of parent info into
  TriggerDagRunOperator conf arrived in provider **2.9.0** (PR apache/airflow#58672),
  which requires Airflow ≥2.11 — outside the 2.10.5 stack. Documented gap
  with a migration path; no custom extractor (D1). In the Marquez graph the
  three child-DAG ingest jobs are first-class jobs in namespace `helios`,
  visually adjacent to (not linked by a run-edge to) the `daily_close`
  trigger tasks.

### D4 — dbt wiring: the wrapper IS the invocation path (build-then-run convention preserved)

`dbt/Dockerfile`: `ENTRYPOINT ["dbt-ol"]` (CMD stays `["build"]`), plus the
D1 pins in `dbt/requirements.txt`. Every existing entry point is thereby
wrapped by the ONE command: `make dbt-build` runs `docker compose run --rm
dbt build` and the `dbt_build` DAG task runs the identical compose command
(ADR-010 D1 + ADR-011 D3 build-then-run convention) — **no DAG change, no
make-target change, no second invocation path to keep in sync** (the
mandate's one-invocation-path requirement). `dbt-ol` passes arguments
through to dbt, so `dbt test`, `dbt source freshness` and the smoke-driven
invocations behave identically, just lineage-wrapped.

- Env (compose `dbt` service): `OPENLINEAGE_URL=http://marquez-api:5000`,
  `OPENLINEAGE_NAMESPACE=helios`. Client resolution order verified (D-facts):
  `OPENLINEAGE_URL` is the documented simple knob; without any config the
  client falls back to a *console* transport — so a lost env var degrades to
  visible no-op logging, never a crash.
- **PASS=160 semantics unchanged** (1 hook + 1 snapshot + 14 tables + 144
  tests): the wrapper invokes the same dbt core with the same project;
  VERIFIER re-proves the count under the wrapper. NEVER `--full-refresh`
  (grep-proven surfaces unchanged — no new command anywhere); the snapshot
  invariant (50,001 rows | min(dbt_valid_from) = 2026-09-11
  05:40:37.528841) must never move.
- **Column-level lineage** arrives via `openlineage_sql` parsing the
  compiled SQL of each model (mechanical, D-facts) — VERIFIER proves it by
  querying the Marquez column-lineage **API** for a mart field, not by
  looking at the graph picture.

### D5 — Topology & storage: three services, dedicated Postgres, named volume, wipe story

Added to compose (house patterns throughout: healthchecks, restart policy,
env-with-defaults, `container_name: helios-*`):

1. **`marquez-db`** — `postgres:16-alpine` (house standard; the official
   Marquez compose uses postgres:14, but Marquez is a JDBI/Dropwizard app
   on vanilla PostgreSQL; 16 is its supported line). Named volume
   `marquez_db_data`. Credentials via `MARQUEZ_POSTGRES_*` env
   (`*_local_dev` defaults, same policy as every other DB in the stack).
2. **`marquez-api`** — `marquezproject/marquez:0.51.1`. Env:
   `POSTGRES_HOST=marquez-db`, `POSTGRES_PORT=5432`, `MARQUEZ_PORT=5000`,
   `MARQUEZ_ADMIN_PORT=5001`, `SEARCH_ENABLED=false` (upstream's search
   option would drag in OpenSearch — no job justifies it). Host port knob
   `MARQUEZ_API_PORT` (:5000). Healthcheck: the admin port's
   `/healthcheck` (Dropwizard, upstream-documented) via the same
   `/dev/tcp` probe pattern the cdc-connect healthcheck uses — the api
   image is a JRE image with no curl/wget guarantee. `restart:
   unless-stopped`. `migrateOnStartup` runs Marquez's own schema
   migrations into its database — Marquez owns its storage end to end.
3. **`marquez-web`** — `marquezproject/marquez-web:0.51.1`. Env:
   `MARQUEZ_HOST=marquez-api`, `MARQUEZ_PORT=5000`. Host port knob
   `MARQUEZ_WEB_PORT` (:3000). No in-container healthcheck (static-server
   image without a probe tool — verified at build); smoke checks it from
   the host over the mapped port.

**Storage decision — why a dedicated Postgres and not something cheaper:**

- *In-container H2:* rejected — dies with the container; every restart
  would blank the lineage history this item exists to accumulate.
- *A named volume under the api container (H2 file):* rejected — same
  freeze, and it puts a non-Postgres storage engine into a stack whose
  every other store speaks house-standard SQL.
- *A `marquez` database inside `warehouse-db` or `airflow-db`:* rejected —
  couples an observability tool's lifecycle to a data-plane / control-plane
  store, and entangles `make clean` semantics with pipeline data (the
  opposite of D6's "observability is never on the write path").
- **Chosen: dedicated `marquez-db` + named volume.** The official topology,
  isolated lifecycle, one more small Postgres is cheap; the graph
  accumulates across restarts like every other volume.

**Wipe story (documented, honest):** `make down` preserves
`marquez_db_data` (all volumes preserved). `make clean` deletes it — and
that is *fine by design*: lineage metadata is **derived, re-derivable
state**. A wiped Marquez graph is rebuilt by one `make run-etl` (+ a
`make dbt-build` for the dbt graph) — no source data touched. This is the
same re-derivability argument that makes `warehouse_data`'s raw zone
rebuildable, applied to observability metadata.

### D6 — The non-fatal lineage contract: observability is never on the write path

The pipeline must go green with Marquez **stopped** and resume emitting when
it returns. Mechanism per emitter (both verified in source, D-facts):

- Airflow provider: listener hooks are try/except-wrapped; emission runs in
  a forked process with a 10 s execution timeout; a dead backend logs a
  warning — task states are untouched.
- dbt wrapper: every emit is exception-guarded; the outer handler logs
  "This does not make dbt execution fail"; `dbt-ol` returns **dbt's own
  exit code**.

This is the lineage twin of ADR-011 D4's drift-proofing: **an
observability outage that can fail the close is a bug.** No retry-storm
risk accepted either: the HTTP transport's bounded retry/timeout is the
client library's own; we add no bespoke retry layer.

**Proven, not assumed (VERIFIER):** `docker compose stop marquez-api` → a
full `daily_close` run green (and a dbt build green) with emission warnings
in logs → `docker compose start marquez-api` → the next build/run emits
again (new events visible with post-restart timestamps via the API).

### D7 — Smoke impact: exactly what this item justifies, nothing more

- **Stage 1: 19 → 22 checks** — the three new containers join the health
  roll (`marquez-db` healthy + answers SQL, `marquez-api` healthy,
  `marquez-web` healthy), justified BY THIS ITEM as the mandate allows.
- **Stage 2: one lineage-presence block** — after the orchestrated run,
  assert via the Marquez REST API (host curl against the mapped port):
  (a) namespace `helios` carries the dbt datasets (staging + marts
  present), (b) the `daily_close` task jobs exist (incl. `dbt_build` and
  `dq_gate`), (c) the column-lineage API returns a non-empty downstream
  graph for a fixed mart field (the API-level proof of column lineage —
  nodeIds pinned to the MEASURED dataset naming after first verification,
  with the naming scheme recorded in EVIDENCE). Wholesale growth is not
  taken; every added line is a claim this item's DoD requires.

### D8 — dq: untouched

The GE gate emits no lineage events and needs none; `dq_gate` surfaces in
Marquez via Airflow's own task events (D2/D3). No `dq/` file changes; the
32-test suite and the exit-code contract are out of scope. (If a future
need appears — e.g. recording the gate's verdict as a custom run facet —
it gets its own ADR.)

### D9 — HTTP egress & secrets audit (Mimosa alignment)

- **No HTTP client code is authored.** The only new egress is the
  OpenLineage client's HTTP POSTs to the Marquez lineage endpoint —
  library code, configured from env (`OPENLINEAGE_URL` /
  `AIRFLOW__OPENLINEAGE__TRANSPORT`), zero URL literals in any repo file
  that executes. The Mimosa host+path-allowlist rule binds code we write;
  we write none, and the ADR records that as the control (CRITIC will
  grep for literal URLs anyway).
- dq still speaks **no HTTP** (unchanged). The dbt image gains exactly one
  egress target (the Marquez API URL from env); the airflow services gain
  the same single target.
- New credentials: `MARQUEZ_POSTGRES_USER/PASSWORD` (marquez-db, env with
  `*_local_dev` defaults). Marquez itself runs unauthenticated on the
  compose network — local-dev, like every other in-net service; nothing
  new is host-exposed except the two documented ports. `.env.example`
  stays in sync (MARQUEZ section: ports + db credentials).

## Consequences

**Positive.** The Marquez UI renders the real dbt graph — raw sources →
staging → marts — with column-level lineage into marts, backed by API/DB
evidence, not screenshots alone. Every DAG task (including `dq_gate` and
the trigger tasks) leaves job/run/task events with timings and statuses.
The pipeline is hardened against observability outages by construction and
by drill. Zero DAG-code changes, zero make-target changes for existing
flows, zero airflow-image changes; the only code delta is the dbt image
(requirements + entrypoint) and compose/env/script additions. The whole
lineage store is re-derivable state with a one-command rebuild.

**Negative / costs.** +3 long-running containers (11 → 14) — earned by this
item, recorded here; +1 postgres to babysit (migrateOnStartup runs Marquez's
own migrations on boot). dbt invocations gain the wrapper's overhead
(sub-second warm) and one more pinned dependency set in a baked image. The
Marquez pin is a frozen 2025 release on a stalled upstream — accepted with
the D1 mitigations and migration paths. Smoke grows by this item's
justified checks only (D7).

**Residual risks (honest).** (1) Marquez upstream is stalled; a security
issue found in 0.51.1 will not be fixed upstream — mitigations in D1;
the exposure is a host-mapped UI/API on a local dev machine. (2) Column
lineage is only as good as the `openlineage_sql` parser's SQL coverage —
complex SQL (windows, UDFs) can leave column edges unresolved; the DoD
asks for one mart and the evidence records exactly what resolved. (3) The
child-DAG runs appear without parent-run linkage on provider 2.0.0 (D3
gap; migration path = provider 2.9.0 + Airflow 2.11). (4) Airflow's
`dbt_build` job and the dbt-ol dbt run are separate job nodes — the graph
shows datasets connecting through the dbt-ol job; this is the boundary
speaking (D2), not a wiring bug. (5) The nightly 05:00 UTC schedule fires
for real: a Marquez stopped for days only loses events for that window
(they are emitted and dropped by the client) — the pipeline itself is
unaffected (D6); attribution discipline (`make cdc-status` first, Session-8
protocol) applies to every drill as before.
