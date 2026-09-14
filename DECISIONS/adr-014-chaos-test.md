# ADR-014 — chaos-test: the scenario inventory, the named safety mechanisms per act, the nightly-collision policy, the alerting cross-link, re-runnability & evidence shape

Date: 2026-09-13 · Status: accepted · Phase: 5 (item 13)
Decides: which of the repo's existing drills become scripted chaos scenarios
vs which are genuinely new; the degrade-safely mechanism and the recovery
assertion for every act BEFORE any code; what "kill the Airflow worker" means
on LocalExecutor and why the scheduler itself is NOT scripted; the nightly
05:00 UTC collision policy; how the alerting chain (ADR-013) is crossed with
pipeline failures and the measured alert latency expectation; re-runnability
of destructive acts; the evidence shape and the smoke-test impact (zero).

## Context

Phase 4 is closed (items 10–12, CRITIC-passed). Item 13 mandates: *scripted
scenarios (kill Airflow worker mid-DAG, kill DB mid-load, poison CSV, schema
drift, API outage) with documented recovery → EVIDENCE/chaos-*.log*, where
each scenario asserts the platform DEGRADES SAFELY — quarantine / alert /
resumable, not merely "it crashed" — and every scenario ends with RECOVERY
EVIDENCE measured, not narrated.

The repo already contains most of these drills as one-off precedents. The
version trap this ADR exists to defuse is re-inventing proven mechanisms as
new ones. The inventory below maps each mandated scenario to its precedent
and states the delta the chaos version adds.

## D1 — The scenario inventory: precedent → scripted chaos

| # | Scenario (make chaos-test SCENARIO=…) | Precedent (already drilled) | New in the chaos version |
|---|---|---|---|
| 01 | `kill_worker` | Session 3 SIGKILL-dbts-mid-run: retry converged (EVIDENCE/phase-3-airflow.md) | Scripted end-to-end: pre-state → SIGKILL `helios-dbt` mid-build → measured task-retry path → in-run convergence → HeliosAirflowTaskFailure firing |
| 02 | `kill_warehouse_midbuild` | None scripted (warehouse outage only implied) | Stop `warehouse-db` during dbt_build: attempt-1 fails loudly, marts stay frozen-old (measured), restart → retry converges in-run → parity |
| 03 | `kill_oltp_midcdc` | Session 1 SIGKILL recovery; CDC slot/offset replay (ADR-005) | Stop `oltp-db` mid-churn: connector degrades (measured), retained WAL peak measured at restart, slot drains → `cdc-status` lag TOTAL=0 |
| 04 | `poison_cdc` | Session 9 DRILL 1 (payment 764768 → −777.77, EVIDENCE/phase-4-dq.md) | Same injection, scripted; NEW: HeliosAirflowTaskFailure FIRING measured via the Prometheus API with recorded latency; alert RESOLVE latency measured |
| 05 | `poison_csv` | Session 9 DRILL 2 (file signup 2031 → row-level quarantine, fix-and-reland) | Same injection class, scripted (filename derived from run date); the same alert cross-link asserted |
| 06 | `schema_drift` | **None — never done** (ADR-011 explicitly says GE is not the schema-drift detector) | Act A additive drift (ADD COLUMN) proven INVISIBLE by measurement; Act B rename probe with the mutator blast radius measured and reverted; the honest detection gap documented as a first-class finding |
| 07 | `api_outage` | 429/500 retry logic (ADR-003/006); never a TOTAL outage drill | Stop rest-mock AND soap-service pre-trigger: attempt-1 failures measured, sources restored, in-run retries converge, raw counts bit-identical |

Each scenario is one bash script under `scripts/chaos/` with the same five
movement shape (D5), orchestrated by `scripts/chaos-test.sh`.

## D2 — What "kill the Airflow worker" means on LocalExecutor (and the scheduler decision)

On LocalExecutor there is no worker fleet: task *work* runs inside one-shot
tool containers (`helios-dbt`, `helios-ingest`, `helios-dq`) that the
scheduler spawns through the rootless docker socket (ADR-010 D1). The
scheduler container itself is the control plane — it runs the LocalExecutor
in-process. Therefore:

- **Scripted (01): the worker act = SIGKILL of the one-shot tool container
  mid-build.** The BashOperator's `docker compose run --rm dbt build` client
  observes the killed container exit (137), the task attempt fails, and
  Airflow's own retry loop (`dbt_build`: retries=2, retry_delay=2 min) spawns
  a fresh one-shot. Recovery = the DAG retry path, and the convergence proof
  is the same run finishing green with a parity measurement. The kill target
  is addressed by its compose service label
  (`com.docker.compose.project=helios` + `service=dbt`), NOT by a container
  name: `docker compose run` (v2) appends `-run-<hash>` and ignores
  `container_name` for run — found-by-verification on the first chaos-01
  attempt (2026-09-13: the fixed name never existed, the poll missed the
  whole build, the run converged untouched). `--rm` guarantees the retried
  `compose run` cannot collide with the corpse (asserted in-script: the
  killed container is gone before the retry window closes).
- **Not scripted, decided here: SIGKILL of the scheduler itself.** Its
  failure mode is a different class — the compose restart policy resurrects
  the control plane, but the in-flight task instances become scheduler-state
  zombies resolved by Airflow's orphan/zombie handling only after
  `zombie_task_threshold` (default minutes-scale), so the honest wall time of
  that drill is dominated by waiting out the zombie sweep rather than by any
  HELIOS mechanism, and the one safety mechanism it would exercise (compose
  restart policy) is already proven by every `make up` health gate. This is a
  bounded scope decision, recorded here, not an oversight: the mandate's
  worker-kill surface is the one-shot container, and that is what 01 scripts.
  The zombie-handling residual is listed in the roll-up as future work.

## D3 — Per-scenario safety mechanisms and recovery assertions (named BEFORE the chaos acts)

| Act | Degrade-safely mechanism (named before the act) | Recovery / convergence assertion (measured) |
|---|---|---|
| SIGKILL dbt one-shot mid-build (01) | Airflow task retry (retries=2, 2 min) over an idempotent rebuild: hash-ledgered raw landing + lsn-guarded CDC + snapshot-safe dbt build never double-count; the failed attempt exits nonzero (loud) | The SAME dag run reaches `success` with all TIs green (try_number ≥ 2 on dbt_build measured); marts row counts within attributed-CDC delta of pre-state; HeliosAirflowTaskFailure fires on the failed attempt |
| Stop warehouse-db mid-dbt_build (02) | The build fails LOUDLY (connection refused → nonzero exit); the published layer is never partial — dbt replaces each target only at per-model commit, so every staging/mart table is a COMPLETE replacement (the previous build's, or this attempt's if the outage lands after that model committed — measured both ways on 2026-09-13) — and during the outage the warehouse is UNREACHABLE (measured DOWN), never silently wrong; HeliosScrapeTargetDown must NOT fire (negative check) | warehouse-db restarted → healthy; the same run's retry rebuilds fully; parity re-measured (monotone growth by attributed churn); `cdc-status`/sink healthy again |
| Stop oltp-db mid-CDC (03) | The replication slot pins retained WAL while the source is unavailable (bounded by disk; 116 GB free) — nothing is lost: Debezium resumes from the slot; the sink consumes nothing while there is nothing. **Measured during verification: the FAILED task does not self-recover when the source returns — the scripted recovery includes a Connect-API task restart (`POST …/tasks/0/restart`, 204; config/offsets/slot untouched, never `cdc-setup`)** | oltp-db restarted → connector task RUNNING (self-recovery waited out first, then the scripted restart) → churn resumes → retained WAL measured at peak (recovery start) and drained after → `cdc-status` lag TOTAL=0 measured |
| CDC payment poison (04) | The SEMANTIC gate: `dq_gate` exits nonzero → DAG red AT THE GATE (ADR-011 D8) + the offending row dead-lettered to `dq.dq_quarantine` with provenance + HeliosAirflowTaskFailure FIRING (ADR-013 rule) | Source fixed at the OLTP row → CDC carries the correction → next close green 5/5 → `make dq-replay` resolves the incident (0 OPEN, measured) → alert resolves when the 10 m increase window rolls off (latency measured once, documented) |
| File signup poison (05) | Same semantic gate + ROW-LEVEL dead-letter (the clean control row 99990002 must remain untouched — row-level targeting re-proven) | Fix-and-reland (new file hash → ledger relands; hash-guarded upserts no-op the unchanged control row) → next close green → `dq-replay` resolves → 0 OPEN |
| Schema drift Act A: ADD COLUMN (06) | **None exists — that is the finding.** The JSONB raw landing (`raw.cdc_users.before/after` are full payloads) makes additive drift non-breaking and invisible end-to-end; the compensating control is forensic: full after-images are retained, so nothing is destroyed | The close is green (drift silent — measured, not assumed): after-images contain the new key, staging has no such column, zero tests failed, zero alerts; the gap is documented with the natural future detector named (dbt source-level column contract) |
| Schema drift Act B: RENAME probe (06) | Bounded blast radius by design: the renamed column (`last_login_at`) is NOT referenced by any dbt not_null test and is excluded from the SCD2 check columns (ADR-009), so the warehouse cannot corrupt before the probe reverts; the app-level blast radius (mutator ticks fail — its SQL names the column) is the measured degradation | Rename reverted → mutator ticks succeed again (measured via its health snapshot / tick counters); CDC keeps flowing throughout; close green |
| API outage (07) | The child ingest tasks fail LOUDLY and Airflow retries (retries=2, 5 min exp backoff); watermarks cannot skip — the overlapped windows are hash-guarded, so a failed walk leaves nothing half-landed. **Measured design corrections (two scripted attempts): container-level source outages SELF-HEAL through the tool-container dependency contract — `compose stop` is reversed instantly (compose run starts its deps; children succeeded 8 s in), `compose pause` stalls the pipeline ~10 min (the one-shot blocks on dependency health) and then proceeds, never failing. The outage that HOLDS is a network partition: `docker network disconnect` leaves the containers healthy (in-container healthchecks) but unreachable — compose run cannot heal that; an EXIT-trap guarantees reconnection on every path** | Networks reconnected → the same run's retries succeed → run `success` with try_number ≥ 2 measured on both child tasks; raw row counts bit-identical to pre-run (nothing new could land while partitioned — the idempotence proof) |

## D4 — The nightly-collision policy (05:00 UTC)

Every chaos act that starts, stops, kills, or triggers is gated by a
pre-flight check querying the airflow metadata DB directly (the run-etl SQL
pattern): if any `daily_close` dag run is in `running`/`queued` state, the
scenario aborts LOUDLY before acting (a queued nightly behind
`max_active_runs=1` would be silently starved by a triggered run otherwise).
Because the nightly fires at 05:00 UTC and sessions do not span it silently,
the check is re-evaluated before every scenario, not once per suite. The
2026-09-13 missed-fire precedent (stack booted after 05:00; catchup=False)
is the honest-attribution template: what the schedule did not do is recorded,
not assumed. Session 12 runs 2026-09-13 ~12:00 UTC → next fire 2026-09-14
05:00 UTC, ~17 h away: no collision is possible this session, and the
pre-flight guard is still exercised on every scenario.

## D5 — The five-movement shape every scenario script implements

1. **Pre-state measurement** (numbers, e.g. marts row counts, `cdc-status`
   lag, quarantine open count, container health).
2. **Chaos act** (one docker verb: `kill` / `stop`, or a poison write).
3. **Degrade-safely assertions** — the mechanism from D3, asserted with
   measured values while degraded (a scenario that cannot observe its own
   degradation aborts with a FAIL, it never "passes by luck").
4. **Recovery** (restart / fix / replay).
5. **Convergence proof** — a post-state measurement that matches the
   pre-state baseline (or the documented attributed delta), printed.

Every assert exits nonzero on failure (the smoke/verify precedent — a
vacuous PASS is worse than a FAIL). Every scenario is individually runnable:
`make chaos-test` runs all seven in order; `make chaos-test
SCENARIO=kill_worker` (or `SCENARIO=01`, name or number) runs exactly one
with the same pre-flight gating. A scenario refuses to run if a prior
scenario's recovery failed — the orchestrator aborts loudly on the first
nonzero scenario exit.

## D6 — Alerting cross-link: expectations and the measured-latency contract

- `HeliosAirflowTaskFailure` = `sum(increase(airflow_task_finish_total{state="failed"}[10m])) > 0`, `for: 1m`
  (ADR-013 D6). The counter surface was verified live before this ADR
  (`airflow_task_finish_total{state="failed"}` exists, currently 0). Expected
  fire latency after a task failure: scrape (≤15 s) + evaluation (≤15 s) +
  `for: 1m` ≈ 75–120 s. Scenarios 04/05 (the poison gates) exhaust their
  retries (`dq_gate` retries=1) and reach TERMINAL failure → both assert the
  alert FIRING via `/api/v1/alerts` with the measured latency recorded.
- **Measured rule semantics (found-by-verification, first chaos-01 attempt,
  2026-09-13): an ABSORBED retry never emits `state="failed"` at all.** The
  killed dbt attempt emitted `airflow_task_finish_total{state="up_for_retry"}`
  (+1, measured on the live counter) because Airflow records the TI as
  up_for_retry while retries remain; `failed` emits only when retries are
  exhausted. The scenarios whose failures converge in-run (01 kill-worker,
  02 warehouse-stop, 07 api-outage) therefore assert the measured
  `up_for_retry` counter increment instead — that is the honest observability
  surface of the degraded path — and the evidence logs carry the note that
  the terminal-failure alert stays quiet for absorbed retries BY RULE DESIGN
  (alerting on every absorbed transient would be noise; the DAG UI and the
  counter carry them).
- Resolve semantics: `increase(...[10m])` decays only when the failed
  attempts roll out of the 10-minute window → the alert auto-resolves
  ≈10–11 min after the LAST failure. That is the documented rule semantics;
  the resolve latency is measured ONCE (scenario 04) rather than waited out
  in every scenario.
- Cross-tripping risk assessed: none of the stopped containers in scenarios
  02/03/07 (warehouse-db, oltp-db, rest-mock, soap-service) is a scraped
  target — the scrape list is exactly prometheus, statsd-exporter, grafana
  (ADR-013 D4). Grafana's DB panels will error while warehouse-db is down;
  the grafana TARGET stays up, so HeliosScrapeTargetDown must NOT fire —
  asserted in scenario 02 as a negative check.
- The statsd-exporter is never stopped by chaos-test (the UDP drop window is
  `metrics-drill`'s owned drill; ADR-013 D7).

## D6a — Measured during verification: cross-table landing lag after a connector-task restart (the straddle, made real)

Scenario 05's first scripted run surfaced a real degradation chain that the
design documents but had never fired (ADR-010: "unreached in builds to date"):

1. Chaos-03's recovery restarted the Debezium task (the measured
   no-self-recovery finding). The mutator's cascade deletes kept churning.
2. **Same-transaction cascade deletes land NON-atomically across raw
   topics**: order 617350 and its 5 items were deleted in ONE source
   transaction (identical ts_ms 1789329460911), yet the order tombstone
   landed in raw at 19:57:42.26 while the item tombstones landed at
   20:07:12.38 — **9 min 30 s later** — in one catch-up batch. Any dbt build
   whose cut fell inside that window had items whose parent order was gone.
3. The safety mechanism worked exactly as designed: `fct_items_order_integrity`
   (the strict mart test, ADR-009 D6) FAILED LOUDLY with 114 violations
   (attempt 1), the build never published unreconciled rows, Airflow retried
   — and every windowed attempt was caught the same way.
4. Consequence for the chaos scripts: a poison close can fail BEFORE the
   gate on this straddle (run failed at dbt_build, dq_gate upstream_failed).
   The poison scenarios therefore drive closes with
   `chaos_close_until_gate_red` / `chaos_close_until_green`: every pre-gate
   failure is attributed with its measured violation count
   (`chaos_straddle_violations`) and retried — the DAG's own run-level retry
   contract, now scripted.

## D7 — Re-runnability (chaos that leaves the stack broken on a second run is a bug)

- Kills and stops are stateless; restarts wait for health, not for hope.
- The poison targets are deterministic: payment 764768 (DELIVERED order →
  mutator-inert; the fix restores 377.12 exactly as Session 9 did) and the
  file signup row 99990001 with control 99990002 (the fix-and-reland re-lands
  by content-hash; the control row is a measured no-op).
- The file poison filename is derived from the run date
  (`customers-YYYYMMDD.csv` matching the extractor's contract), so each
  poison→fix cycle is a new ledger entry; re-running re-lands by hash change.
- `ADD COLUMN IF NOT EXISTS` and a rename guarded on the original name's
  existence make scenario 06 idempotent.
- Quarantine grows, never shrinks: each poison cycle ADDS one incident and
  RESOLVES it — the two Session-9 RESOLVED incidents are never touched;
  `dq.dq_quarantine` must end 0 OPEN (smoke asserts).
- A second full-suite run is part of the plan (the CRITIC idempotency probe):
  same green end-state, more resolved audit rows, wall time re-measured.

## D8 — Runtime honesty and evidence shape

- A warm daily_close is ~4 min (dbt_build ~2 m 13 s). With retries and the
  red-close legs, the full suite is estimated at 60–90 min; every scenario
  prints and logs its wall time, and the roll-up reports the measured total.
- Evidence: one full transcript per scenario at
  `EVIDENCE/chaos-01-kill-worker.log` … `chaos-07-api-outage.log` (the act,
  the degrade-safely assertions with measured values, the recovery, the
  convergence proof), written by the orchestrator via `tee` so a FAILING
  scenario still leaves its log. A roll-up `EVIDENCE/phase-5-chaos.md`
  tabulates the measured numbers (per-scenario wall, alert latencies, WAL
  peak/drain, row-count baselines) and carries the honest findings (the
  schema-drift gap, the scheduler-kill scope decision).
- Smoke-test impact: **zero** (the contract in the boot mandate). Chaos is
  destructive by design and stays out of the green-state guard.

## D9 — Never-touch compliance in chaos terms

No reseed of soap/oltp (poisons are row-level UPDATEs / drop-volume CSVs on
existing rows — the mutator's own write path); no `--full-refresh` (grepped
in the scripts); resolved dq incidents never deleted (only `dq-replay` runs,
which resolve OPEN incidents that no longer reproduce); the sim-/busforge-/
meridian containers are not addressed; `make clean` never runs mid-session;
the mutator is NEVER paused by chaos-test (poison targets are mutator-inert
rows; lag attribution is by measurement, the Session-10 protocol).

## Consequences

- `make chaos-test` (+ `SCENARIO=`) becomes the item-13 proof target; the
  four regression guards (`dq-status` 0 OPEN, `lineage-verify`,
  `metrics-verify`, `smoke-test`) must be green at close.
- The schema-drift finding will be recorded as a documented detection gap
  with a named future detector — an honest limitation, not a fake control.
- The scheduler-kill zombie sweep stays future work with its wall-time
  rationale recorded here.
- Alert-resolve latency is documented as rule semantics (~10 min), preventing
  a future misreading of the alert panel after any failed-task drill.
