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
