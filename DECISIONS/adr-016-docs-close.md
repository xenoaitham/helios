# ADR-016 — docs close: the recover-from-scratch drill decision, the docs-verify contract, and the final documentation outline

Date: 2026-09-15 · Status: accepted · Phase: 6 (item 15 — the final item)
Decides: what "RUNBOOK complete (recover-from-scratch drill)" honestly means on
a platform whose living corpora must never be wiped; which doc carries what in
the final package; the mechanical docs-verify contract (`scripts/docs-verify.sh`
+ `make docs-verify`, smoke grows by exactly 0); the honesty rules every final
doc sentence must satisfy; and the recorded correction of one over-claim the
docs sweep caught in the Phase-5 canon.

## Context

Phases 0–5 are closed and CRITIC-passed (14/14 items before this one). Item 15
is the packaging item: RUNBOOK drill, DATA_DICTIONARY refresh,
INTERVIEW_DEFENSE (40+ Q&A, ESB mapping, AliCloud study notes, known
limitations), resume bullets from EVIDENCE only, final CRITIC sweep. The docs
surfaces already exist in pieces — RUNBOOK 415 lines (Phase-3-era in places),
DATA_DICTIONARY 273 lines (Phase-3-era), INTERVIEW_DEFENSE a 26-line stub,
README 298 lines (Phase-5-era status table). The trap list from the session
mandate: docs narrating numbers without a metrics.md/log citation; a drill
described as if re-executed today when it was not; marketing adjectives; stale
claims (container counts, the 15 s Marquez timeout era, single-run bench
numbers instead of bands).

## D1 — The drill decision (options table, ADR-015 precedent)

"Recover-from-scratch drill" can mean re-executing `make clean && make up` on
this living stack, or documenting the drill to walk-able completeness. What
`make clean` actually deletes on THIS machine: the living SOAP corpus (382k
orders) and OLTP corpus (5.4M+ rows, reseeds ~200 s + ~44 s), the raw envelope
(~7.3M+ rows), the SCD2 snapshot history — `min(dbt_valid_from)
2026-09-11 05:40:37.528841` RESETS, invalidating the bit-identical invariant
that six bench legs and every chaos scenario asserted — the 8 RESOLVED dq
incidents (the drill/war-story artifacts themselves), the CDC slot and
connector offsets (require `make cdc-setup` + a fresh ~877 s snapshot
re-drain), and the chaos-06 residue (below). It would re-prove exactly what
Phase 0 already proved at compose level (`EVIDENCE/phase-0.md`: `make clean &&
make up` → all healthy).

| Option | What it does | Verdict |
|---|---|---|
| **B: DOCUMENT the drill completely** — step-by-step with expected outputs and evidence pointers, citing the Phase-0 execution as its proof, with an explicit "not re-executed on the living stack, and why" note and a named future work (re-verify on a scratch machine) | Makes the interviewer's real question — "can I clone this and run it?" — walk-able, without burning measured history to re-prove Phase 0 | **DECIDED** |
| A1: re-execute `make clean && make up` on the living stack today | Destroys measured portfolio history (corpora, snapshot invariant, 8 RESOLVED incidents, CDC slot) to re-prove a Phase-0 result; every "bit-identical since 2026-09-11" claim in EVIDENCE dies with it | Rejected — the cost is permanent and the information gain is zero |
| A2: scratch-machine / scratch-volume re-verification (clone to a second compose project with its own volumes + port overrides, or a second host) | The honest A-adjacent drill: exercises the fresh-clone path END-TO-END (seed → cdc-setup → first drain → first close → smoke) on today's 17-container code state | Named FUTURE WORK in the RUNBOOK drill section — not required to close docs, and a second live stack on this host would muddy the "17 healthy" invariant the guards assert |

Consequence of B: the RUNBOOK's drill section must be complete enough that a
stranger succeeds on a fresh clone WITHOUT this laptop — procedure, expected
outputs at every step, known slow steps (seeds, first CDC drain ~877 s),
failure pointers — and must state plainly that the recorded execution proof is
Phase-0-era (8 containers; the platform has since grown to 17) with the A2
re-verification as future work. No sentence may imply the drill re-ran today.

## D2 — The docs outline (what lives where)

- `docs/RUNBOOK.md` — §4 becomes THE drill: preconditions, the exact sequence
  (`make env` → `make up` → seeds → `make cdc-setup` → first drain →
  `make run-etl` → `make smoke-test`) with expected outputs and per-step
  evidence pointers; the data-loss table for `make clean` on a used stack;
  ADR-016 D1's decision note; A2 future work. Plus the consistency pass: the
  header stops saying "currently Phase 3"; the smoke-test rows describe the
  GREEN Stage 2 (26 Stage-1 checks + the E2E assertions, exit 0); `dbt build`
  is described as the 14-model/144-test build it is (PASS=160 shape); the
  Marquez section gains the /jobs payload-growth residual (2.6 MB / ~16 s
  measured 2026-09-15; 45 s client timeouts in lineage-verify + smoke since
  the Session-13 repair; pruning = future work).
- `docs/DATA_DICTIONARY.md` — verified against the LIVE schemas
  (information_schema queried 2026-09-15, not memory). Adds the missing
  `users.loyalty_tier` row (see D4), a raw batch-landing section (the ADR-007
  payload tables + ledgers the Phase-3-era doc never enumerated), column
  enumerations for staging/marts/dq verified against the dump, and "as of"
  labels on living counts.
- `docs/INTERVIEW_DEFENSE.md` — the stub becomes the pack: 40+ Q&A each
  citing its grounding (ADR-NNN / devlog-NNN / EVIDENCE file); the ESB
  mapping table; the AliCloud table labeled "study notes, not operational
  claims"; known-limitations (harvested from every ADR's residuals); the
  resume bullets (§10.2 — metrics.md numbers only).
- `README.md` — status table closes Phases 5–6; the mermaid diagram shows the
  17-container topology (marquez ×3, airflow-db, statsd-exporter, prometheus,
  grafana included); quickstart unchanged (`make up` → healthy); every linked
  path exists (docs-verify enforces).
- `scripts/docs-verify.sh` + `make docs-verify` — D3.

## D3 — The docs-verify contract (mechanical, loud, abort on first miss)

A NEW make target; `scripts/smoke-test.sh` untouched (delta exactly 0,
git-proven at close). Pure bash + grep over repo files — no SQL, no HTTP, no
docker. Checks, in order, each printing what it proves and exiting non-zero on
the first miss (the smoke/chaos/bench loud-assert precedent; a vacuous PASS is
worse than a FAIL — the script must fail loudly if a check list itself is
empty):

1. Every `make <target>` referenced in README.md + docs/*.md exists in the
   Makefile.
2. Every relative repo path referenced in README.md + docs/*.md (markdown
   links and backticked paths under known repo roots) exists on disk.
3. Every `EVIDENCE/…` reference resolves to a real file (the mandate's
   explicit check — subsumed by 2, asserted as its own named check).
4. No credential-shaped literal in README.md + docs/*.md (assignment-shaped
   secrets, PEM blocks, cloud-key shapes, `user:password@` DSNs). Env-var
   NAMES and `…`-elided placeholders are the only allowed shapes.
5. The resume-bullet number anchors: every number the bullets cite appears
   verbatim in `EVIDENCE/metrics.md` (or via its declared anchor token), and
   the bullets carry the "Personal project" framing.

Evidence: the sweep transcript lands in `EVIDENCE/docs-verify.log`.

## D4 — Found at docs time: the loyalty_tier over-claim (recorded correction)

Verifying the dictionary against `information_schema` surfaced
`users.loyalty_tier` — the chaos-06 Act-A probe column — still live in the
source (added 2026-09-14; all 50,000 values NULL; 14,922 `raw.cdc_users`
after-images carry the key; `staging.stg_users` has no such column; every
close since has been green). The SCRIPT is correct and by-design (Act A is
add-and-leave — no `DROP COLUMN` exists in `scripts/chaos/06-schema-drift.sh`;
only the Act-B rename is reverted, EXIT-trap guarded). But the Phase-5 canon
over-claims: `EVIDENCE/phase-5-chaos.md` and STATE.md said "**both** reverted
in-scenario". Correction handling: a dated correction note appended to
`EVIDENCE/phase-5-chaos.md` (the metrics.md ROTA-note precedent — records are
corrected by dated annotation, never silent rewrites); the dictionary gains
the column with its provenance; the defense pack's war story gets its best
ending — the drift column is queryable in the live source TODAY, which is
stronger interview evidence than a revert. Session-12's CRITIC missed it; the
docs-verify-era sweep caught it — that is the found-by-verification story of
devlog-013.

## D5 — Honesty rules for every final doc sentence

- Every number cites its file: `EVIDENCE/metrics.md` bands for bench numbers
  (never a single run), phase evidence files for phase numbers. A number
  without a checkable home does not ship.
- Test-count convention: pytest 195 · dbt data tests 144 · dq unit tests 32 —
  cited exactly; no new test numbers (docs-verify is bash orchestration).
- Personal-project labeling everywhere; no employment-voice wording.
- The AliCloud section is labeled "study notes, not operational claims".
- No marketing adjectives (`production-grade`, `blazing`, `enterprise-ready`
  as praise) — demonstrable facts only (MASTER_PROMPT §2.2/§6.8).
- INTERVIEW_DEFENSE Q&A each cite their grounding so a reader can verify.
- The four regression guards (dq-status 0 OPEN · lineage-verify ·
  metrics-verify · smoke-test) re-run green at close — docs touch no pipeline
  state, and the guards prove it.
