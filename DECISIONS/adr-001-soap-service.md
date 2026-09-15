# ADR-001 — Legacy SOAP OrderManagement service (spyne + owned SQLite store + frozen WSDL contract)

Status: accepted (2026-09-09, Phase 1, Session 2)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-000 (compose baseline), BACKLOG item 1

## Context

Phase 1 needs the "legacy enterprise" side of HELIOS: a SOAP OrderManagement service that
Phase 2's batch extractor will pull via a `zeep` client. Host constraints from Session 1:
rootless Docker, ~3.6 GB free disk (slim images mandatory), no sudo. Debezium CDC will read
`helios-oltp-db` separately; the SOAP service is a *different* source system and must not
share that database.

## Options considered

**Storage for order history**
1. In-memory dicts — fast, but history evaporates on restart; makes "3 years of order
   history" a lie and `GetOrders` totals meaningless across restarts.
2. Own Postgres instance — most "enterprise", but +1 container/+volume for a service that
   is a *source*, not infrastructure, and disk is at 100%.
3. SQLite on a named volume — durable across restarts, zero extra service, and it makes
   the story explicit: the legacy system owns its datastore, exactly like a real
   mainframe-adjacent system would.

Decision: **3**.

**Contract approach**
1. Strict WSDL-first (hand-authored XSD/WSDL, generated skeleton) — spyne has no server
   codegen; the hand-written WSDL would be a dead artifact nobody enforces.
2. Code-first with spyne + **frozen golden WSDL artifact** — the WSDL served at `/?wsdl`
   is the contract; a committed copy under `soap-service/contract/OrderManagement.wsdl`
   is compared (namespace-normalized canonical form) by `tests/test_wsdl_golden.py`, and
   `make smoke-soap` asserts the served WSDL still exposes the 4 operations. Contract
   drift fails loudly.

Decision: **2**, documented honestly here: spyne is code-first in reality; the frozen
artifact + enforcement is what makes the contract real.

**Auth**
1. None — unrealistic for a "legacy enterprise" service.
2. WS-Security headers — heavyweight, partial spyne support, disproportionate for a local
   portfolio stack.
3. HTTP basic auth at a WSGI middleware — mandated by BACKLOG; simple, unit-testable;
   credentials from env only (`SOAP_BASIC_AUTH_USER/PASSWORD`), boot fails fast if unset,
   no literals in source or tests.

Decision: **3**. `/health` is deliberately unauthenticated (container healthcheck); every
SOAP path and the WSDL itself require auth.

**Money representation** — floats vs integer cents. Decision: cents in storage,
`Decimal` (2 dp, enforced at the edge) in the SOAP contract. No float money.

**CreateOrder idempotency** — optional `client_reference` (client-supplied idempotency
key). Unique partial index (NULLs are distinct in SQLite) + sha256 payload hash:
same ref + same payload → returns the *original* order with `idempotent_replay=true`
(retries are side-effect-free); same ref + different payload → `ClientReferenceConflict`
fault. Rubric §1/§2 apply to the source system itself, not just the ETL.

## Decision (as built)

- Image: `python:3.11-slim`, deps pinned: `spyne==2.14.0` (2.14.5 does not exist on
  PyPI), `zeep`, `gunicorn`, `pytest` (tests run in-container via `make test-soap`).
- SOAP 1.1 (`Soap11(validator="lxml")`), tns `urn:helios:soap:ordermanagement:v1`,
  gunicorn 2 workers × 4 threads behind a WSGI stack: router → `/health` open, everything
  else → basic-auth middleware → spyne WsgiApplication.
- Operations (all faults are `soap:Client` subclasses):
  - `CreateOrder(customer_id, items[OrderItem{product_id, quantity, unit_price}],
    client_reference?) -> CreateOrderResult{order_id, status, created_at, idempotent_replay}`
  - `GetOrders(page?, page_size?, status?, date_from?, date_to?) ->
    OrderPage{orders[], page, page_size, total_results, total_pages}` (1-based, max 500)
  - `GetOrderStatus(order_id) -> OrderStatus{order_id, status, updated_at}`
  - `UpdateOrderStatus(order_id, new_status, note?) -> OrderStatus`
  - State machine: NEW → PROCESSING → SHIPPED → DELIVERED; NEW/PROCESSING → CANCELLED.
    Illegal transitions raise `InvalidStateTransition`; unknown orders raise
    `OrderNotFound`; bad payloads raise `ValidationError`.
- Seed (`python -m app.seed`): deterministic (`--rng-seed 20260909`), ~1096 days ending
  yesterday, ramp 220→480 orders/day (~380k orders, ~800k lines, ~1.1M history rows),
  age-dependent status mix with consistent transition chains. Runs automatically on first
  container boot (`--if-empty`), `make reseed-soap` for a destructive reset (SOAP store
  only).
- Timestamps: naive UTC `YYYY-MM-DD HH:MM:SS` strings — lexicographically comparable so
  date filters are plain SQL; documented in the data dictionary.

## Consequences

+ Source isolation: Phase 2 CDC (oltp-db WAL) and SOAP extraction pull from two
  independent systems, which is the honest enterprise topology.
+ Contract drift (column/operation changes) is detected by tests + smoke, not by an
  interviewer finding it.
+ Deterministic seed → reproducible evidence; row counts are assertable.
− spyne is code-first: the frozen artifact compensates, but a regenerated WSDL must be
  re-frozen via `make contract-freeze` and reviewed like any interface change.
− SQLite is single-writer: mutation throughput caps around 1k writes/s. Fine for a legacy
  source; this is a documented limit (§6.9 scale honesty), not a defect.
− `pytest` and test files ship in the runtime image (acceptable for local dev; splitting
  a test image is listed as prod-hardening, see STATE.md residuals).
− Timestamps are naive UTC by contract; a tz-aware consumer must normalize (zeep returns
  naive datetimes).

## Addendum — empirical findings during build (2026-09-09)

1. **ComplexModel namespaces**: without an explicit `__namespace__`, spyne places WSDL
   complex types in the *Python module's* namespace (`app.models`) — a code-structure
   leak into the contract. All models now declare `__namespace__ = TNS`.
2. **spyne↔zeep array interop**: spyne emits named array wrapper types
   (`OrderItemArray` with a repeated, nillable child). zeep does NOT auto-flatten such
   doc/lit wrappers (its array support is for `soap:encoding arrayType` only), so the
   canonical zeep call shapes are: request `items={"OrderItem": [...]}`, response
   `page.orders.Order`. Java/.NET clients consume the same WSDL natively. This is the
   kind of friction real legacy SOAP produces and is deliberately documented rather
   than papered over.
3. **WSDL element ordering**: spyne serializes schema types in object-set iteration
   order (id-based → ASLR), so the served WSDL is one of a few *semantically identical*
   orderings even with `PYTHONHASHSEED=0` (which only fixes string hashing). The
   contract artifact is therefore stable per process, and the drift test
   (`tests/test_wsdl_golden.py`) compares order-insensitively: locations pinned,
   definitions/types/schema children sorted before comparison.
4. **Security hardening applied during build** (Mimosa gate feedback): all SQL is
   inline, single-statement, fully parameterized (optional filters via
   `(? IS NULL OR col = ?)` guards); `check_contract.py` restricts fetch targets to an
   explicit host allowlist and `--emit` output to the contract directory (no SSRF /
   path traversal surface); the WSDL comparison parser disables DTD/entity resolution.
