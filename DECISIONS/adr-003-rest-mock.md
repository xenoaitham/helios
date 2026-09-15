# ADR-003 — rest-mock: Pricing & Promotions API (cursor pagination, token bucket, deterministic flakes)

Status: accepted (2026-09-09, Phase 1, Session 3)
Deciders: ORCHESTRATOR / ARCHITECT personas
Related: ADR-001 (soap), ADR-002 (oltp), BACKLOG item 3

## Context

Phase 1 needs a REST source whose *failure modes* are real: pagination a Phase-2
extractor must walk, rate limiting that actually answers 429 (+`Retry-After`), and
intermittent 500s so retry/backoff logic is built against a moving target. DoD adds:
`/promotions` must page through ALL data, and both 429s and 500s must be reproducible
under test. Constraints carried from ADR-001/002: slim images (disk ~1.9 GB free),
env-only configuration, no credential literals, in-memory only (no extra volume).

## Options considered

**Pagination**
1. Offset/limit — simple, but rows shift under inserts and deep offsets degrade; the
   SOAP service already carries that honest limitation (ADR-001), so repeating it adds
   no coverage.
2. Keyset pagination with an **opaque base64url cursor** (encodes the last seen id,
   tamper-evident via 400 on undecodable content) — chosen. Forward-only is enough: a
   Phase-2 extractor walks forward with watermarks; documented as such.
3. Cursor in an HMAC-signed envelope — rejected: no trust boundary inside the compose
   network; a mock API need not authenticate its own cursors.

**Rate limiting**
1. Global lock-free counter — unfair, untestable.
2. **Token bucket per API key** (`X-API-Key`, anonymous bucket when absent) with an
   injectable clock — chosen: `try_acquire(now=...)` makes 429s unit-testable without
   sleeping, and `Retry-After` is computed from the refill math, not guessed.
   Capacity/refill from env (`REST_MOCK_RATE_CAPACITY=30`, `REST_MOCK_RATE_REFILL_PER_SEC=10`).

**Flaky 500s**
1. `random.random()` — nondeterministic; a flaky test suite is worse than no test.
2. **Seeded deterministic flaker**: `sha256(f"{seed}:{endpoint}:{counter}")[:8] % 100 <
   FLAKE_PERCENT` — chosen. The decision function is a pure function of (seed, endpoint,
   request counter): a fresh app with `FLAKE_PERCENT=100` fails the first data request,
   `0` never fails, and the default (5%) is stationary across restarts of the sequence.
   `/health` and 404 paths never flake; flakes are 500 with a `retryable: true` body.

**Data**
1. Query a real DB — rejected: this is a *pricing edge API* in the story; in-memory
   deterministic catalog (5,000 products, 2,500 promotions) built from fixed seeds keeps
   the image stateless and disk usage zero.
2. **Coherence with oltp**: product SKUs reuse the OLTP sku space/format and the same
   price formula (`199 + (sku_num * 613) % 14999` cents), so Phase-3 warehouse joins
   between promotions and OLTP order items are plausible. Money crosses the API as
   integer `price_cents` + `currency` — no float money anywhere in HELIOS.

## Decision (as built)

- FastAPI + uvicorn on python:3.11-slim (`helios/rest-mock:latest`), port
  `${REST_MOCK_PORT:-8001}` host → 8000 container. Routes:
  `GET /health` (never rate-limited, never flakes), `GET /products`,
  `GET /promotions`, `GET /promotions/{promo_id}`. Responses carry
  `data / next_cursor / total` (+`X-RateLimit-Remaining`).
- App is built by a factory `create_app(...)` with injectable seed/flake/rate params —
  tests construct their own instances; compose supplies env defaults.
- compose service `rest-mock` (restart: unless-stopped, /health healthcheck) added to
  `wait-healthy.sh` (9 long-running containers); `make test-rest` (pytest in the image)
  and `make smoke-rest` (full pagination walk: no dupes, no missing ids, flakes retried
  and reported). `smoke-test` script stays untouched (Stage-1 contract 15/15).

## Consequences

- The catalog is static at runtime — this source models *read* flakiness, not change
  capture; the mutation story lives in oltp (ADR-002) and soap (ADR-001). Stated in the
  data dictionary so nobody expects CDC from this API.
- The flake counter is per-process: after a restart the deterministic sequence replays
  from zero — reproducible, but not unique across reboots; acceptable for a mock and
  called out in known-limitations.
- Anonymous clients share one bucket: a chatty extractor without an API key can starve
  itself — by design; the extractor (Phase 2) will send a key.
