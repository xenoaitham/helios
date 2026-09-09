# RUNBOOK — operating HELIOS

Grows each phase (currently Phase 1). Audience: a stranger with the repo and Docker.

## 1. Daily driver commands

| Command | Effect |
|---|---|
| `make up` | Start platform; block until every container is healthy (fails loudly with logs otherwise) |
| `make ps` | Container status |
| `make logs` | Follow all service logs |
| `make smoke-test` | Stage 1: infra health incl. SOAP WSDL + auth checks (must pass). Stage 2: E2E ETL — fails loudly until Phase 3 |
| `make down` | Stop platform; named data volumes preserved |
| `make clean` | **DESTRUCTIVE**: stop + delete all data volumes (warehouse re-inits schemas; SOAP store re-seeds on next `make up`) |

Phase 1 additions (soap-service):

| Command | Effect |
|---|---|
| `make seed-soap` | Seed SOAP order history if empty; prints row counts + revenue (idempotent) |
| `make reseed-soap` | **DESTRUCTIVE to the SOAP store only**: drop + reseed its SQLite file |
| `make smoke-soap` | zeep round-trip against the running service (create → replay → status → legal/illegal transition → pagination) |
| `make test-soap` | pytest suite (35 tests) in a throwaway container |
| `make contract-freeze` | Re-capture `soap-service/contract/OrderManagement.wsdl` after a deliberate contract change |

Phase 1 additions (oltp source, ADR-002):

| Command | Effect |
|---|---|
| `make seed-oltp` | Apply schema + seed 5.4M rows via one COPY transaction if empty (idempotent; re-ensures constraints/indexes/sequences on every run) |
| `make reseed-oltp` | **DESTRUCTIVE to the OLTP source only**: `TRUNCATE` all four tables + reload |
| `make oltp-status` | Row counts, per-table on-disk size, and a 15 s measured WAL-delta report |
| `make test-oltp` | pytest suite (26 tests) in a throwaway container against a dedicated `oltp_test` db |
| `make mutator-logs` | Follow the continuous mutation loop's log |

Notes: fresh clones seed automatically on `make up` (the one-shot `oltp-seed` service
runs before `oltp-mutator` starts; ~3.5 min at default scale — `WAIT_TIMEOUT` defaults
to 900 s to cover it). The mutator writes continuously (updates + bounded inserts/
deletes) precisely so Phase-2 CDC sees WAL churn; `make oltp-status` is the instrument.

Phase 1 additions (rest-mock, ADR-003):

| Command | Effect |
|---|---|
| `make test-rest` | pytest suite (34 tests) in a throwaway container |
| `make smoke-rest` | Walks every `/promotions` page against the running service; retries real 429s/500s; asserts no dupes/gaps |

Tuning knobs in `.env`: `REST_MOCK_FLAKE_PERCENT` (default 5; set 100 to force failures
while debugging an extractor, 0 to silence), `REST_MOCK_RATE_CAPACITY` /
`REST_MOCK_RATE_REFILL_PER_SEC` (default 30 / 10 per second). Send an `X-API-Key`
header to get an isolated rate bucket.

Phase 1 additions (file-drop, ADR-004):

| Command | Effect |
|---|---|
| `make drop-generate` | Emit today's `customers-`/`products-<date>.csv` with all dirt modes into the drop volume |
| `make drop-generate-late` | Late-arrival simulation: backdated batch (2 days) lands now |
| `make drop-ls` | List drop volume: files, sizes, arrival timestamps |
| `make test-drop` | 15 dirt-classification + CLI tests |

Notes: the drop point is the `filedrop_data` volume (SFTP-style, outside the repo);
same inputs give byte-identical files; re-running the same batch date overwrites in
place. The drop volume grows until cleaned — targeted `docker compose run --rm
filedrop-tools sh -c "rm /data/drop/<file>"` or `make down -v` (destroys ALL data).

## 2. Endpoints & credentials

All credentials live in `.env` (defaults in `.env.example`). Currently surfaced:

- **Airflow UI**: http://localhost:8080 — `AIRFLOW_WWW_USER` / `AIRFLOW_WWW_PASSWORD`
- **OLTP Postgres**: `localhost:${OLTP_PORT}` db `oltp`
- **Warehouse Postgres**: `localhost:${WAREHOUSE_PORT}` db `warehouse`,
  schemas `raw` / `staging` / `marts`
- **Kafka (from host)**: `localhost:${KAFKA_HOST_PORT}` (in-network: `kafka:9092`)
- **SOAP OrderManagement**: `http://localhost:${SOAP_PORT}/?wsdl` — HTTP basic auth
  (`SOAP_BASIC_AUTH_USER` / `SOAP_BASIC_AUTH_PASSWORD`) required on every path including
  the WSDL; `/health` is the only unauthenticated endpoint (container healthcheck).
  401 + `WWW-Authenticate` on missing/bad credentials.

Quick SOAP checks from host:

```bash
make smoke-soap                                        # full zeep round trip
curl -su "$SOAP_BASIC_AUTH_USER:$SOAP_BASIC_AUTH_PASSWORD" \
  "http://localhost:${SOAP_PORT}/?wsdl" | head -5      # peek at the WSDL
```

## 3. Failure playbook

| Symptom | Diagnosis | Recovery |
|---|---|---|
| `make up` timeout on a container | `docker logs helios-<svc>` (wait-healthy.sh prints tail) | Fix env/port, `make down && make up` |
| Port already in use | `ss -ltn \| grep <port>` | Change `*_PORT` in `.env`, `make down && make up` |
| `smoke-test` Stage 1 fails | Some container unhealthy or not answering | `make down && make up`; if persists, `docker logs helios-<svc>` |
| Warehouse missing schemas | Volume was created before init script existed | `make clean && make up` (destroys data — Phase 0 has none worth keeping) |
| Airflow UI 502 / not up yet | webserver start_period ~30-60s | Re-run `make ps`; check `helios-airflow-init` exited 0 |
| First `make up` slow on soap-service | First boot seeds ~382k orders (~45 s); healthcheck `start_period` 150 s covers it | Nothing — subsequent boots skip (store non-empty) |
| First `make up` slow on oltp-seed | Fresh volume: schema + 5.4M-row COPY seed (~3.5 min); `make up` waits via oltp-mutator's dependency chain | Nothing — subsequent boots hit the `--if-empty` skip (~1 s) |
| `oltp-mutator` unhealthy / crash-looping | `make mutator-logs`; it retries politely while the schema is missing | After a schema/db fix it self-recovers; `docker compose restart oltp-mutator` to force |
| `oltp-seed` fails mid-load (e.g. disk full) | Seed is one transaction — a crash rolls back to an empty schema | Free disk (`docker builder prune`), re-run `make seed-oltp` |
| Need OLTP rows/types reference | `docs/DATA_DICTIONARY.md` (OLTP section) | — |
| SOAP 401 in scripts | Credentials missing in env; the service refuses to boot without `SOAP_BASIC_AUTH_*` | Set them in `.env`, `make up` |
| `test_served_wsdl_matches_golden` fails | The contract changed (spyne type set) | Review the WSDL diff like an API change, then `make contract-freeze` and commit both |

## 4. Recovery-from-scratch drill (Phase 0)

```bash
make clean && make up && make ps   # expect: all healthy, schemas re-created
```

This is the exact drill `EVIDENCE/phase-0.md` records.

## 5. Environment notes (this repo's dev machine)

The baseline was built on a host where the system Docker daemon is disabled and sudo is
unavailable. Docker runs **rootless** under the dev user; the bootstrap (already applied)
was:

1. Static `slirp4netns` + `fuse-overlayfs` binaries into `~/.local/bin`.
2. User systemd unit `~/.config/systemd/user/docker.service` running
   `dockerd-rootless.sh` (Type=notify, NotifyAccess=all).
3. `systemctl --user enable --now docker.service`.
4. `docker context create rootless --docker host=unix:///run/user/1000/docker.sock &&
   docker context use rootless`.

On any normal machine with the system daemon running, none of this is needed —
`make up` works unchanged (that is the point of the context indirection).

Rootless caveats: published ports bind on host loopback (localhost URLs only), no cgroup
resource limits, images live in `~/.local/share/docker` (watch disk — this host is tight).
