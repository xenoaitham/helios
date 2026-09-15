# Screenshots

Real captures from the running stack (17 containers, one `make up`). Nothing
here is mocked or staged: these are the live Airflow, Grafana and Marquez UIs
serving the data the pipeline actually processed. Dates and counters you can
see line up with `EVIDENCE/metrics.md` and the make targets in the README.

| File | What it shows |
|---|---|
| `airflow-daily-close-grid.png` | The `daily_close` DAG grid view: 25 runs of the nightly close (schedule 05:00 UTC), 5 tasks per run. The red squares are real history: terminal gate failures from the chaos drills (`make chaos-test`, scenarios 04/05), every one recovered and converged; the rest are green. |
| `grafana-helios-pipeline.png` | The `helios-pipeline` dashboard (uid `helios-pipeline`, dashboards as code in `observability/grafana/`): dag-run duration quantiles, task outcomes by state, failed tasks, dead-letter incidents at 0, and live row counts read-only from the warehouse (fct_orders ~1M, fct_order_items ~6M, raw CDC ~791k, dim_customer 50k). |
| `marquez-jobs.png` | Marquez overview: 24 datasets, 70 jobs, 2,350 lineage events over 24h; `daily_close` (2m51s last run), its `dbt_build` (1m32s) and `dq_gate` (0m15s) tasks all COMPLETED. |
| `marquez-lineage-fct-orders.png` | Table-level lineage graph for `warehouse.marts.fct_orders` in Marquez: raw sources -> staging -> the fact table, depth 2. The column-level view is reachable from the same page ("VIEW" links). |

To see all of this live yourself: `make up`, then Airflow at :8080, Grafana at
:3001, Marquez at :3000 (ports and local-dev credentials in `.env.example`;
the RUNBOOK has the full tour).
