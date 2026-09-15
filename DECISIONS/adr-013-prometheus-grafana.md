# ADR-013 — Prometheus + Grafana: metric sources, service topology & ports, dashboards as code, the honest alerting ceiling, the non-fatal metrics contract

Date: 2026-09-13 · Status: accepted · Phase: 4 (item 12)
Decides: the Airflow 2.10.5 metrics surface as *measured in the installed
image* (not blog metric names); the metrics-source decision (StatsD for
durations/failures, dashboard-side SQL pulls for row counts — custom exporter
and pushgateway rejected); the 2026 version pins and the honest churn
statement (the ADR-012/D5 twin: one risk profile frozen, one alive); the
topology, host ports (including two real collisions) and storage/wipe story;
dashboards-as-code provisioning; the honest alerting ceiling reconciling
ADR-011 D8; the non-fatal metrics contract and its drills; the smoke-test
impact and the new make targets.

## Context

Phase 4 items 10 (GE gates, ADR-011) and 11 (OpenLineage+Marquez, ADR-012)
are closed and CRITIC-passed. Item 12 mandates pipeline duration / failure /
row-count metrics with dashboards and alerting rules — **measured scrapes,
not config claims** — with the pipeline proven non-fatal when Prometheus AND
Grafana are down, and a fire-drill proving a rule genuinely fires.

Fact base (researched 2026-09-13; Airflow facts verified against the source
installed in the RUNNING `helios-airflow-scheduler` container — the primary
source for what 2.10.5 actually emits, not docs or blogs):

- **Airflow 2.10.5 has NO native Prometheus surface.** `airflow/metrics/`
  contains exactly three backends — `statsd_logger.py`, `datadog_logger.py`,
  `otel_logger.py` — and no Prometheus backend; `[metrics]` in
  `config.yml` has no prometheus keys; the webserver registers no `/metrics`
  route. (`prometheus_client` the *library* is installed as a transitive
  dependency, but nothing in Airflow exposes it.) The StatsD path is
  therefore not a choice among equals — it is the only built-in egress.
- **The StatsD client is already in the base image** (`import statsd`
  succeeds in the running scheduler; the package ships in the 2.10.5
  constraints) → wiring metrics is env-only, zero image delta.
- **Config keys** (`airflow/config_templates/config.yml`, section
  `[metrics]`): `statsd_on` (default False), `statsd_host` (localhost),
  `statsd_port` (8125), `statsd_prefix` (airflow).
  `statsd_influxdb_enabled` defaults False → **tags are DROPPED on the
  plain-statsd wire**. Every metric is deliberately emitted twice (source:
  comments in `taskinstance.py`/`dagrun.py`): once *name-encoded* and once
  *tagged*. On plain StatsD only the name-encoded forms reach the wire.
- **The real metric names** (grep of every `Stats.timing/incr/gauge` emit
  site in the installed package, unique list recorded in
  EVIDENCE/phase-4-metrics/):
  - `dagrun.duration.{state}.{dag_id}` — StatsD **timer (ms)**, emitted when
    a dag run reaches a terminal state (`success`/`failed`). Plus the global
    `dagrun.duration.{state}` and `dagrun.first_task_scheduling_delay`.
  - `ti.finish.{dag_id}.{task_id}.{state}` — **counter**, zero-initialized
    for ALL states at task start (the `count=0` loop in
    `taskinstance.py:257-266`) and incremented at finish → after any task
    runs, `…daily_close.dq_gate.failed:0|c` EXISTS at 0. A failure-rate rule
    can be written against a series that is always present.
  - `ti.start.{dag_id}.{task_id}` counter; global `ti_successes` /
    `ti_failures`; `task_instance_created_{task_type}`.
  - `dag.{dag_id}.{task_id}.{metric_name}` timers exist ONLY for
    `queued_duration` / `scheduled_duration` — **queue waits, NOT runtime**.
  - Scheduler-side: `scheduler_heartbeat`, `triggerer_heartbeat`,
    `dag_processing.last_duration.{file}`, `dagbag_size`,
    `pool.{open,queued,running,deferred}_slots.{pool_name}`,
    `zombies_killed`, `sla_missed`, `dag.callback_exceptions`.
- **The duration boundary (measured, stated verbatim):** Airflow 2.10.5
  emits *dag-run* duration over StatsD but **no per-task runtime timer**
  (task runtime is a `task_instance.duration` DB column, rendered by the
  Airflow UI, not emitted as a metric). The dashboard gets task durations
  from the Airflow metadata DB via a read-only SQL datasource — not from an
  invented metric name.
- **Versions (Docker Hub registry API, fetched 2026-09-13):**
  `prom/prometheus` **v3.13.3** (pushed 2026-09-07; v3 line GA 2024-11);
  `grafana/grafana` **13.0.8** (pushed 2026-09-01); `prom/statsd-exporter`
  **v0.31.0** (pushed 2026-09-02). All ALIVE with active release cadences.
  All three images probed: each ships `sh` + `wget` → container healthchecks
  work without adding tooling; the exporter's flags (`--statsd.mapping-config`,
  `--statsd.listen-udp=:9125`, `--web.listen-address=:9102`) verified via
  `--help` on the pinned image.
- **Port collisions (measured on this host, 2026-09-13):** host `:9090` is
  taken by ANOTHER project's `busforge-prometheus`; host `:3000` is taken by
  our own `marquez-web` (ADR-012). `:9091` and `:3001` are free.
- **Grafana provisioning (docs, fetched 2026-09-13):** `apiVersion: 1`
  datasource files support env-var substitution (`$VAR`) in values —
  credentials can be injected without literals; dashboard file providers
  poll a repo-mounted path; Postgres datasources provision via
  `jsonData`/`secureJsonData`.
- **No SMTP/Slack/webhook receiver exists in this stack and none may be
  invented** (the ADR-011 D8 honesty rule).
- **Mimosa canon** (cumulative, Sessions 2–11): source files via Write/Edit
  only; no credential-looking literals anywhere including configs and
  dashboards; TSDB/storage in named volumes; verify scripts abort loudly.

## Decisions

### D1 — Version pins: `prom/prometheus:v3.13.3`, `grafana/grafana:13.0.8`, `prom/statsd-exporter:v0.31.0` — exact tags on an ALIVE ecosystem

Exact `==`-style tags pinned in compose (never `latest`), current stable as
of 2026-09-13 per the registry API. **The honest churn statement, and the
contrast with ADR-012's frozen Marquez — two risk profiles, one ADR:**

- *Prometheus* releases ~every 6 weeks on the v3 line; config/API surface
  used here (scrape configs, rule files, `/api/v1/*`, `/-/healthy`) is the
  stable core. Churn risk: moderate; migration cost for a pin bump: read one
  release-notes page.
- *Grafana* moves fastest (major ~yearly, patch monthly; dashboard JSON
  schema drifts most). Mitigation: dashboards are code (D5) — a version
  bump's blast radius is a re-render of one JSON file, and the smoke's
  metrics-presence assertions catch a broken provisioning at boot.
- *statsd-exporter* is slow-moving; the mapping config shape is validated
  empirically at boot (the exporter fails LOUDLY on an unknown mapping key —
  a bad mapping cannot silently misname metrics; VERIFIER asserts the mapped
  names exist in the exporter's own `/metrics`). **Measured at boot:** on
  v0.31.0 the per-mapping timer field is `observer_type` — both `timer_type`
  and `timers_type` are REJECTED as unknown keys (the boot failure found the
  name; the correct one was recovered by extracting the yaml struct tags
  from the pinned image's binary).

A scraped-in Prometheus makes version drift *visible* (target-down alerts
fire) rather than silent — the opposite of the frozen-Marquez problem, where
drift was invisible because the upstream was dead.

### D2 — Metrics sources: StatsD→statsd-exporter for durations/failures; dashboards PULL row counts via Postgres datasources; custom exporter and pushgateway REJECTED

The row-count decision space, settled:

- **(a) Grafana Postgres datasource panels — CHOSEN.** The dashboard pulls
  counts from `warehouse-db` (and task durations from `airflow-db`) over
  read-only SELECTs at render/refresh time. Zero new containers, zero
  pipeline coupling, values real by construction (they ARE the tables),
  credentials via env interpolation (D5). This is the same
  "observability pulls, never pushes" posture as Prometheus itself.
- **(b) A custom row-count exporter — REJECTED.** A new long-running
  container whose only job is to duplicate (a): it would need its own tests,
  allowlist, healthcheck and scraping, and it adds a warehouse polling
  client we'd have to babysit — failing the ADR-012 D5 "every new container
  individually earned" bar. *Earned alternative noted:* if alerting on row
  counts is ever required, the honest path is Grafana unified alerting with
  a Postgres query (no new container), decided then, not now.
- **(c) Pushgateway — REJECTED.** The pipeline would PUSH metrics to it:
  push code sits on the write path (exactly the smell the non-fatal
  contract forbids), and pushgateway's batch-expiry semantics add state to
  babysit. StatsD's UDP fire-and-forget (D7) achieves the same decoupling
  with zero push-side code.

**StatsD → statsd-exporter mapping (repo file
`observability/statsd-exporter/mappings.yml`):** only name-encoded patterns
are mapped (tags are dropped on the wire, Context). Final Prometheus names —
the contract dashboards and rules are written against:

| StatsD name (prefix `airflow.`) | Prometheus name | Type/labels |
|---|---|---|
| `ti.finish.*.*.*` | `airflow_task_finish_total` | counter {dag_id, task_id, state} |
| `ti.start.*.*` | `airflow_task_start_total` | counter {dag_id, task_id} |
| `dagrun.duration.success.*` / `.failed.*` | `airflow_dagrun_duration_seconds` | histogram {dag_id, status} |
| `dag_processing.last_duration.*` | `airflow_dag_processing_last_duration_seconds` | histogram {dag_file} |
| `scheduler_heartbeat` | `airflow_scheduler_heartbeat` | gauge |
| `ti_failures` / `ti_successes` | `airflow_ti_failures_total` / `airflow_ti_successes_total` | counters |

Timers: StatsD sends **milliseconds**; statsd-exporter observes **seconds**
in the histogram — VERIFIER sanity-checks the magnitude (a ~200 s close must
not appear as 200,000). Unmapped scheduler gauges fall through to the
exporter's default sanitization (`airflow_dagbag_size` etc.) — available,
not dashboard-critical; a deliberate choice to keep the mapping file to
exactly the metrics the DoD names.

### D3 — Airflow wiring: four env keys in `x-airflow-common`, zero image delta, no DAG change

```
AIRFLOW__METRICS__STATSD_ON: "True"
AIRFLOW__METRICS__STATSD_HOST: statsd-exporter
AIRFLOW__METRICS__STATSD_PORT: "9125"
AIRFLOW__METRICS__STATSD_PREFIX: airflow
```

In-compose topology (`statsd-exporter` service name + the exporter's
in-net UDP listener) — same policy as `CDC_CONNECT_URL`/`OPENLINEAGE_URL`.
The client ships in the base image (Context) → no `airflow/Dockerfile`
change, no DAG change, no new `depends_on` (D7). The `dbt`, `dq`, `ingest`
tools are untouched: their work surfaces through Airflow's own task metrics
and the warehouse tables the dashboards read.

### D4 — Topology, ports, storage: +3 long-running containers (14 → 17), each individually earned

1. **`statsd-exporter`** (`prom/statsd-exporter:v0.31.0`) — *earned because
   it is the ONLY bridge from the measured 2.10.5 StatsD surface to
   Prometheus (D-facts: no native endpoint exists).* UDP+TCP :9125 in-net
   only (the sole emitter is in-net airflow; no host exposure, no job for
   one), `/metrics` :9102 scraped by Prometheus. Healthcheck: `wget` on
   :9102/metrics (probe-verified present in the image). Mapping config
   mounted ro from the repo.
2. **`prometheus`** (`prom/prometheus:v3.13.3`) — TSDB in named volume
   `prometheus_data`; config + rule files mounted ro from
   `observability/prometheus/`. Host port knob `PROMETHEUS_PORT=9091`
   (**:9090 is taken by another local project's prometheus — measured**).
   Healthcheck: `wget` on `/-/healthy`. Scrape jobs: `prometheus` (self),
   `statsd-exporter`, `grafana`. **Deliberately NOT scraped:** the
   databases (no postgres_exporter — a fourth container whose only
   justification would be DB-internal metrics the DoD does not ask for;
   row counts are covered by (a)), Kafka/Debezium (no JMX exporter wired —
   same bar), cdc-sink/soap/rest (no Prometheus-format endpoints exist).
   The honest scrape list is the three observability services; everything
   pipeline-side enters as Airflow *application* metrics.
3. **`grafana`** (`grafana/grafana:13.0.8`) — dashboards + datasources
   provisioned from the repo (D5); sqlite state in named volume
   `grafana_data`. Host port knob `GRAFANA_PORT=3001` (**:3000 is taken by
   marquez-web**). Healthcheck: `wget` on `/api/health`.
   `GF_SECURITY_ADMIN_USER/PASSWORD` from env (`*_local_dev` defaults).

**Deliberate absences:** no Alertmanager (D6), no postgres_exporter (above),
no exporters for Kafka/Debezium (the DoD asks for pipeline metrics, which
enter via StatsD; JMX exporters would be containers without a job this item
does). The observability services carry **no `depends_on` in either
direction** with the pipeline — leaves by construction (D7).

**Storage & wipe story (honest, and NOT the ADR-012 D5 story):** dashboards,
datasources, scrape config and alert rules are **code in the repo** — a
wiped volume costs nothing. The TSDB and Grafana sqlite are **derived
ephemeral state that is NOT re-derivable**: metrics history for a window
that never reached the scraper is gone forever (dropped-not-queued, D7) —
unlike lineage, which one run re-derives. `make clean` deletes both volumes;
`make down` preserves them. Documented here so nobody promises "replayable
metrics".

### D5 — Dashboards as code: provisioning YAML + dashboard JSON in the repo

Repo layout (mounted ro into the containers):

```
observability/
  prometheus/prometheus.yml          # scrape config (D4)
  prometheus/rules/helios.yml        # alerting rules (D6)
  statsd-exporter/mappings.yml       # D2 mapping
  grafana/provisioning/datasources/helios.yml
  grafana/provisioning/dashboards/helios.yml
  grafana/dashboards/pipeline.json   # the dashboard
```

`helios.yml` datasources (env-interpolated values — zero credential
literals): `Helios-Prometheus` → `http://prometheus:9090` (default);
`Helios-Warehouse` → `warehouse-db:5432`, user `$WAREHOUSE_POSTGRES_USER`,
`secureJsonData.password: $WAREHOUSE_POSTGRES_PASSWORD`; `Helios-Airflow` →
`airflow-db:5432` with `AIRFLOW_DB_PASSWORD`. Read-only pull at render time
(D2a); the datasources' DB users are the existing service users — the
local-dev credentials policy is unchanged (documented residual: a dedicated
read-only Postgres role is the production hardening, out of scope here).

`pipeline.json` panels (all fed by REAL surfaces — the mapped names from
D2, or SQL against real tables):

1. **Dag-run duration** — histogram quantiles of
   `airflow_dagrun_duration_seconds` by `dag_id`/`status` (the DoD's
   "pipeline duration" at close granularity).
2. **Task outcomes** — `sum by (state) (increase(airflow_task_finish_total[1h]))`
   and the 24h failed-tasks stat (the DoD's "failure").
3. **Row counts** — warehouse SQL stat panels: `marts.fct_orders`,
   `marts.fct_order_items`, `raw.cdc_orders`, current `dim_customer`,
   **`dq.dq_quarantine` open incidents** with a threshold color (the
   ADR-011 dead-letter surface, finally visible outside psql).
4. **Task durations** — `Helios-Airflow` SQL panel over
   `task_instance.duration` for the latest `daily_close` runs (the measured
   duration boundary: runtime lives in the metadata DB, Context).
5. **ALERTS** — `ALERTS{alertstate="firing"}` table (the rule surface, D6).

### D6 — Alerting: Prometheus rule file is the honest ceiling; no Alertmanager; ADR-011 D8 reconciled

**No Alertmanager — rejected.** No SMTP/Slack/webhook receiver exists and
none may be invented; Alertmanager without a receiver adds a container that
only deduplicates what the `ALERTS` series and `/alerts` UI already expose.
**The ceiling, stated exactly:** Prometheus evaluates rules from
`observability/prometheus/rules/helios.yml`; results are visible as the
`ALERTS` metric series, Prometheus's `/api/v1/alerts` + `/alerts` UI, and
the Grafana ALERTS panel. A rule firing is *provable* (the drill, D7) —
what we do NOT claim is any push notification. If a future receiver exists,
Alertmanager gets its own ADR.

Rules (all three written against real, mapped series):

- `HeliosScrapeTargetDown` — `up{job=~"statsd-exporter|grafana"} == 0`,
  `for: 30s`. A stopped container records `up=0` on the next failed scrape —
  this is the drill rule. (Bootstrap limit, documented in the rule's
  annotation: if Prometheus itself dies, nothing alerts — the standard
  self-monitoring gap, mitigated in prod by external healthchecks, honest
  here as a residual.)
- `HeliosAirflowTaskFailure` — `sum(increase(airflow_task_finish_total{state="failed"}[10m])) > 0`,
  `for: 1m`. **The ADR-011 D8 reconciliation:** "the alert IS the gate"
  stays true (a `dq_gate` failure still reddens the DAG, the quarantine
  table and the exit code — no fake channel was invented there); what item
  12 adds is that the *same event* now also fires a real PromQL alert,
  because the task-failure counter exists. The dead-letter open count stays
  a dashboard panel, not a rule — Prometheus cannot evaluate SQL, and
  saying so is the honesty.
- `HeliosDailyCloseStale` —
  `absent_over_time(airflow_dagrun_duration_seconds_count{dag_id="daily_close",status="success"}[26h])`.
  No successful close recorded in 26 h (schedule = daily 05:00 UTC) → the
  staleness alert the scheduler's SLA mechanisms can't give across a statsd
  outage. It will legitimately fire before the first post-wiring run — the
  truth, not noise.

### D7 — The non-fatal metrics contract: UDP fire-and-forget, pull-only reads, proven by drills — with the destination-caching amendment

- **Airflow → statsd-exporter is UDP**: datagrams are *dropped-not-queued*
  when the exporter is down (the same honesty as lineage emissions during
  the Marquez outage — the window's metrics are lost, nothing buffers,
  nothing retries, nothing fails). Airflow's own fallback line, captured
  during the drill: `Could not configure StatsClient: [Errno -3] Temporary
  failure in name resolution, using NoStatsLogger instead` — the metrics
  path degrades to a no-op logger; task execution is untouched.
- **The destination-caching amendment (found by the drills, 2026-09-13):**
  the `statsd` pip client resolves the host **once per process**
  (`statsd/client/udp.py`: `getaddrinfo` → cached numeric `_addr`, every
  send reuses it). An exporter IP change therefore silently orphans every
  long-lived Airflow process's metrics — measured twice at boot (scheduler/
  webserver recreated before the exporter, and an exporter stop/start cycle
  that shifted the IP): heartbeat datagrams stopped arriving while the
  stack looked healthy. **Fix (wired):** a dedicated `metrics-net` network
  with a fixed subnet (172.31.0.0/24) gives the exporter the STATIC address
  `172.31.0.9`, and `AIRFLOW__METRICS__STATSD_HOST` targets that address —
  the cached destination survives ANY exporter restart. Proven: exporter
  restart → same IP → `airflow_scheduler_heartbeat` resumed with 0 s sample
  age, no airflow restart needed.
- **No `depends_on`** binds any pipeline service to any observability
  service; Prometheus and Grafana are pure pulls (Prometheus scrapes the
  exporter, Grafana pulls Prometheus and the two Postgres read-only
  datasources).
- **Drill 1 (the DoD drill):** `docker compose stop prometheus grafana
  statsd-exporter` → `make run-etl` fully green (measured: TWO runs green —
  etl-20260913T101806Z, etl-20260913T102000Z) → `start` → the scrape picks
  back up (evidence: the `airflow_scheduler_heartbeat` series shows the
  outage as a sample gap — 6 samples in a 10 m window across the outage —
  and the drill runs' dagrun durations are absent FOREVER, the honest
  dropped-not-queued shape).
- **Drill 2 (the fire-drill):** `make metrics-drill` — stop the exporter,
  `HeliosScrapeTargetDown` genuinely FIRES, restart, the alert resolves.

### D8 — Smoke impact and make targets: exactly what this item justifies

- **Stage 1: 22 → 26** — the three new containers join the health roll
  (`statsd-exporter`, `prometheus`, `grafana` healthy) + one "prometheus
  answers" probe (`/-/ready` via the mapped port — the SQL-probe analog
  from the marquez-db check). No other growth.
- **Stage 2: one metrics-presence block** after the orchestrated run, via
  the Prometheus HTTP API (host curl, mapped port): (a)
  `up{job="statsd-exporter"} == 1` (the scrape is REAL); (b)
  `sum(airflow_task_finish_total{dag_id="daily_close"}) >= 1` (a known
  metric with a real value produced by THIS run); (c)
  `airflow_dagrun_duration_seconds_count{dag_id="daily_close",status="success"} >= 1`
  (the duration surface, measured). Wholesale growth is not taken; the
  lineage assertions stay untouched as the regression guard.
- **`make metrics-verify`** (`scripts/metrics-verify.sh`, the
  lineage-verify bar): non-destructive, API-asserted, aborts loudly,
  dumps evidence JSON — targets all `up`, rules loaded, mapped metric names
  present IN THE EXPORTER'S OWN `/metrics`, Prometheus query results,
  Grafana datasources + dashboard provisioned (authenticated via env),
  everything to `EVIDENCE/phase-4-metrics/`.
- **`make metrics-drill`** (`scripts/metrics-drill.sh`): the scripted
  fire-drill — stop statsd-exporter, assert `HeliosScrapeTargetDown` goes
  FIRING via `/api/v1/alerts`, dump evidence, restart, assert recovery;
  nonzero exit on any step. The non-fatal drills (D7) are VERIFIER runs
  recorded in EVIDENCE with their exact commands (they drive `make
  run-etl` and are documented in RUNBOOK).

### D9 — HTTP egress & secrets audit (Mimosa alignment)

- **No HTTP client code is authored.** New egress: Prometheus's scrapes
  (config-driven pulls), Grafana's datasource pulls (config-driven),
  airflow's UDP datagrams (library client, env-configured). The scripts use
  host `curl` for APIs already exposed — read-only.
- **Credentials:** new knobs only `GRAFANA_ADMIN_USER/PASSWORD` and the two
  port numbers, in `.env`/`.env.example` with `*_local_dev` defaults
  (`.env` never committed; the operator `.env` is synced in the same edit).
  Datasource DB passwords are env-interpolated by Grafana provisioning —
  no literals in YAML, dashboards, or configs (CRITIC greps). TSDB + Grafana
  storage in named volumes — no repo binds for state (containment rule).
- Prometheus and Grafana run unauthenticated in-net / admin-basic-auth on
  the host — local-dev parity with every other service (documented in ADR-012 D9's spirit: nothing beyond a local machine is exposed).

## Consequences

**Positive.** The pipeline's duration/failure reality is measurable in
Prometheus with named, labeled series — no invented metric names anywhere
(every name traces to an emit site in the installed 2.10.5 source). The
Grafana dashboard renders close durations, task outcomes, warehouse row
counts and the dq dead-letter surface from real tables. Alerting exists and
is *proven to fire* by a drill, with the honest ceiling (no push channel)
documented instead of faked. The pipeline is non-fatal against the whole
metrics stack by construction (UDP + pulls + no depends_on) and by drill.
Dashboards/rules/configs are code; state volumes are honest ephemera.

**Negative / costs.** +3 long-running containers (14 → 17) — each earned in
D4. The scheduler/webserver now emit StatsD (negligible UDP volume — a
dozen series, 15 s scrapes). Grafana dashboard JSON is the most
churn-prone artifact in the repo (D1 mitigation). Metrics history is
non-replayable — the wipe story says so in ink (D4). Smoke grew by this
item's justified checks only (26 Stage-1 checks, 3 Stage-2 assertions).

**Residual risks (honest).** (1) A dead Prometheus alerts nothing — the
self-monitoring bootstrap gap (D6 annotation; external liveness checks are
the prod answer). (2) Rule evaluation against a *fresh* stack fires
`HeliosDailyCloseStale` until the first post-wiring run completes — true,
by design. (3) Grafana provisioning env interpolation is
values-only — a future datasource key that needs a structural env cannot be
provisioned this way. (4) The exporter's default fall-through names
(unmapped scheduler gauges) are unpinned upstream behavior — mapped names
are the contract; unmapped ones are best-effort. (5) The nightly 05:00 UTC
schedule fires for real: drills that stop containers must respect a running
close — attribution discipline (`make cdc-status`, DAG-run check) applies,
Session-8 protocol. (6) Task durations arrive via the Airflow metadata DB
panel, not Prometheus — a statsd outage dims duration *history* while the
metadata DB stays complete (and vice versa: `make clean` erases grafana
state but not the Airflow run history). (7) **The UDP destination-caching gap — found by boot verification, FIXED
by the D7 amendment:** the statsd client caches its destination for the
process lifetime; before the amendment an exporter IP change (recreate, or
a stop/start that shifted the IP) silenced long-lived Airflow processes
until restart. The static `172.31.0.9` on `metrics-net` removes the
failure mode (proven by an exporter restart with the flow surviving); the
residual is the added network + pinned-address topology itself — a
compose-level constant, and a scrape-target-down alert fires if the
exporter ever stops for longer than 30 s regardless.
