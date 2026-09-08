# RUNBOOK — operating HELIOS

Phase 0 edition. Grows each phase. Audience: a stranger with the repo and Docker.

## 1. Daily driver commands

| Command | Effect |
|---|---|
| `make up` | Start platform; block until every container is healthy (fails loudly with logs otherwise) |
| `make ps` | Container status |
| `make logs` | Follow all service logs |
| `make smoke-test` | Stage 1: infra health (must pass). Stage 2: E2E ETL — fails loudly until Phase 3 |
| `make down` | Stop platform; named data volumes preserved |
| `make clean` | **DESTRUCTIVE**: stop + delete all data volumes (warehouse re-inits schemas on next `make up`) |

## 2. Endpoints & credentials

All credentials live in `.env` (defaults in `.env.example`). Phase 0 surfaces:

- **Airflow UI**: http://localhost:8080 — `AIRFLOW_WWW_USER` / `AIRFLOW_WWW_PASSWORD`
- **OLTP Postgres**: `localhost:${OLTP_PORT}` db `oltp`
- **Warehouse Postgres**: `localhost:${WAREHOUSE_PORT}` db `warehouse`,
  schemas `raw` / `staging` / `marts`
- **Kafka (from host)**: `localhost:${KAFKA_HOST_PORT}` (in-network: `kafka:9092`)

Quick psql from host:

```bash
docker exec -it helios-warehouse-db psql -U warehouse -d warehouse
```

## 3. Failure playbook

| Symptom | Diagnosis | Recovery |
|---|---|---|
| `make up` timeout on a container | `docker logs helios-<svc>` (wait-healthy.sh prints tail) | Fix env/port, `make down && make up` |
| Port already in use | `ss -ltn \| grep <port>` | Change `*_PORT` in `.env`, `make down && make up` |
| `smoke-test` Stage 1 fails | Some container unhealthy or not answering | `make down && make up`; if persists, `docker logs helios-<svc>` |
| Warehouse missing schemas | Volume was created before init script existed | `make clean && make up` (destroys data — Phase 0 has none worth keeping) |
| Airflow UI 502 / not up yet | webserver start_period ~30-60s | Re-run `make ps`; check `helios-airflow-init` exited 0 |

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
