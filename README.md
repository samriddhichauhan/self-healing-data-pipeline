# Self-Healing Data Pipeline

A local, Docker-native data engineering platform: a real Airflow pipeline that detects data-quality failures, diagnoses them with a local AI (Ollama, with a deterministic rule-based fallback — no external paid API), decides through a Policy Gate whether to auto-remediate or escalate, executes and verifies the fix, and reports real results to a Postgres-backed dashboard.

## Quickstart

```bash
docker compose up -d
docker compose ps                       # everything should be healthy/running

python scripts/verify_environment.py
python scripts/demo_healthy_run.py       # Manager Demo 1
python scripts/demo_self_healing_run.py  # Manager Demo 2
```

- Airflow UI: http://localhost:8080 (`airflow` / `airflow`)
- Dashboard: http://localhost:5173
- Backend API / docs: http://localhost:8000/docs

See `docs/runbook.md` for day-2 operations and `docs/architecture.md` for how the self-healing loop actually works.

## What's real here

- **Real Airflow DAG** (`self_healing_pipeline`, 8 tasks) running only inside Docker — never a manual UI click, never a direct DAG import on the host.
- **Real data-quality engine**: row-count bounds, null tolerance, primary-key uniqueness, referential integrity, and freshness SLAs, enforced from `airflow/config/pipeline_config.yaml`.
- **Real fault injection** (`scripts/fault-injection/inject_fault.py`): duplicates, schema drift, null spikes, volume drops, referential breaks, staleness — each a measurable, traceable problem.
- **Real AI diagnosis**: Ollama (`llama3.2`) primary, deterministic rule-based fallback if Ollama is unreachable. The project never depends on Ollama being present.
- **Real Policy Gate**: only `DUPLICATE_INGESTION` (idempotent, low-risk) is ever eligible for `AUTO_FIX`. Everything else — schema drift, null spikes, referential breaks, volume anomalies, staleness — is `ESCALATE`-only, by policy, always.
- **Real remediation + independent verification**: the fix executes, then a separate verifier re-reads the data from disk and only then confirms it's clean.
- **Real database**: a `pipeline` Postgres database (separate from Airflow's own metadata DB, same container) with `pipeline_runs`, `quality_results`, `incidents`, `orders`, `events`, `aggregated_metrics` tables, written by `load_data`/`agent_monitoring` and read live by the backend.
- **Real dashboard**: the React frontend polls the FastAPI backend, which reads Postgres and the Airflow REST API — no simulated `setTimeout` state, no hardcoded run history.

## Repository layout

```
src/                 # all pipeline logic: ingestion, validation, transformation,
                      #   remediation, agent (AI diagnosis + policy gate), database, monitoring
airflow/dags/self_healing_pipeline.py   # thin Airflow task wiring, imports from src/
airflow/config/pipeline_config.yaml     # the data-quality contract
apps/backend/         # FastAPI dashboard API
apps/frontend/        # React + Vite dashboard
data/                 # raw (canonical fixture) / staging / processed / incidents
scripts/               # demo_healthy_run.py, demo_self_healing_run.py, verify_environment.py,
                       #   reset_demo.py, fault-injection/, data/generate_data.py,
                       #   generate_daily_data.py (new daily batch for any date)
tests/                # pytest — 54 tests covering ingestion, schema, quality, duplicate
                       #   detection + self-heal, remediation, policy, diagnosis, verification,
                       #   transformation, database, incidents, DAG structure, backend API,
                       #   daily data generator
docker-compose.yml     # postgres, airflow-webserver, airflow-scheduler, airflow-init, backend, frontend
```

## Tests

```bash
pip install -r requirements.txt
pytest
```

54 passed (0 skipped when Postgres is up via `docker compose up -d postgres`).

Some tests (`test_database.py`, `test_pipeline_scenarios.py`) write `test_*` rows into the live pipeline Postgres on `localhost:5432`, so the dashboard's "latest run" shows a test run until the next real DAG run.

## Canonical demo date

The manager demo scripts (`demo_healthy_run.py`, `demo_self_healing_run.py`, `reset_demo.py`) run against the fixed date **2026-06-01**. The DAG has no cron schedule (`schedule=None`): every run is explicitly triggered (demo scripts, the dashboard's trigger button, or `airflow dags trigger`) with an explicit logical date. To run any other date, see below.

> Note: the demo scripts call `reset_demo_state()`, which clears **all** of `data/staging`, `data/processed` and `data/incidents` (not just 2026-06-01). `data/raw` is never touched.

## Daily Data Generation and Pipeline Demo

Ingestion is date-aware on purpose: a run for logical date `D` reads exactly `data/raw/orders/orders_D.csv` and `data/raw/events/events_D.jsonl`, and fails loudly (`FileNotFoundError`, listing the available files) if the batch for `D` is missing. It never falls back to another date. To run a new date, first generate that date's batch.

All commands are for **Windows CMD**. The containers are `internship1-postgres-1`, `internship1-airflow-scheduler-1`, `internship1-airflow-webserver-1`, `internship1-backend-1` and `internship1-frontend-1`. Airflow uses the LocalExecutor, so there is **no worker container**: tasks run inside `internship1-airflow-scheduler-1`, and that is where task logs are.

```bat
cd "C:\Users\samri\OneDrive\Documents\Internship 1"
docker compose up -d
docker ps --format "{{.Names}}"
```

### 1. Generate a date

```bat
python scripts\generate_daily_data.py --date 2026-09-27
```

Other options:

| Command | Effect |
|---|---|
| `--date today` or `--date tomorrow` | Uses the current UTC date, the same timezone Airflow uses, so no source change is needed. |
| `--start-date 2026-09-23 --end-date 2026-09-27` | Generates a range. |
| `--force` | Overwrites an existing batch. See the note below. |

How the data is generated:

- Each day gets 300 orders and 1200 events, built with the same generators as `scripts\data\generate_data.py`. That means the same status and event-type mix, quantity 1–5, `order_total = price × quantity`, and every timestamp inside day `D`.
- `customers.csv` and `products.csv` are static dimension tables and are never regenerated. Every generated `customer_id` and `product_id` is taken from them, so foreign keys are always valid.
- `order_id` continues the global `ORD…` sequence after the highest existing raw order ID, and `event_id` is a UUID. Both are global primary keys in Postgres (loads upsert on them), so a reused ID would overwrite another date's rows.
- Output is deterministic: each date is seeded from `(--seed, D)`.
- Existing files are left untouched (`already exists - left untouched`) unless you pass `--force`.

**Note on `--force`:** it only replaces the raw files. If `data\staging\stg_orders_D.csv` already exists, ingestion keeps using it, so delete that staging file if the new raw batch should be picked up.

### 2. Verify the files (host and container)

```bat
dir data\raw\orders
dir data\raw\events
docker exec internship1-airflow-scheduler-1 ls -la /opt/airflow/data/raw/orders
docker exec internship1-airflow-scheduler-1 ls -la /opt/airflow/data/raw/events
```

`.\data` is bind-mounted at `/opt/airflow/data` (see `docker-compose.yml`), so both listings match and no restart is needed.

### 3. Test ingestion

```bat
docker exec internship1-airflow-scheduler-1 airflow tasks test self_healing_pipeline ingest_orders 2026-09-27
docker exec internship1-airflow-scheduler-1 airflow tasks test self_healing_pipeline ingest_events 2026-09-27
```

Expect `[DATA PROFILER] Orders: 300 rows, 0.0% nulls, 0.0% duplicates.` (and the same for events with 1200 rows), then `Marking task as SUCCESS`.

- **Staging is idempotent:** once `data\staging\stg_orders_D.csv` exists, re-runs print `already present — preserving existing staging`.
- **Don't use `tasks test` for `load_data` or `agent_monitoring`.** It writes a `__airflow_temporary_run_…__` row to `pipeline_runs`, which then shows as the latest run.

### 4. Trigger the full DAG

```bat
docker exec internship1-airflow-scheduler-1 airflow dags trigger self_healing_pipeline -e 2026-09-27T00:00:00+00:00 -r batch__2026-09-27
```

- **One run per execution date.** Airflow allows only one DAG run per exact execution timestamp, and a second trigger fails with "already exists". To re-run a date, use another time on the same day plus a new run ID, for example `-e 2026-09-27T03:00:00+00:00 -r batch__2026-09-27__v2`. Tasks only use the date part (`ds`), and loads upsert on primary keys, so a re-run doesn't duplicate rows.
- **Future logical dates don't run yet.** A triggered run stays `queued`, and `tasks test` reports `Execution date … is in the future`. Generate tomorrow's batch whenever you like, but trigger it after 00:00 UTC that day.

**Troubleshooting: run stays `queued` and every task shows `None`.** Airflow 2.9.3 only starts a queued run when the DAG is unpaused **and** `execution_date <= now()` (see `get_queued_dag_runs_to_set_running` in `airflow/models/dagrun.py`). Task instances are created only once a run starts, so until then every task reads `None`. Check:

```bat
docker exec internship1-airflow-scheduler-1 date -u
docker exec internship1-airflow-scheduler-1 airflow dags list
```

- **`--exec-date` later than the container's UTC time:** the run starts on its own once that moment passes. Trigger with a past time instead, for example `-e 2026-09-27T06:30:00+00:00`.
- **`is_paused` is `True`:** run `airflow dags unpause self_healing_pipeline`. `airflow dags delete` re-creates the DAG paused (`dags_are_paused_at_creation=True`) and also erases all run history, so avoid it.

**Troubleshooting: `{"detail":"Not Found"}` from the API.** Check the exact URL. A trailing `\` (sent as `%5C`), a missing `/v1`, or port `8080` (that's Airflow, not the backend) all return 404. The endpoint is `http://localhost:8000/api/v1/pipeline/status`.

### 5. Inspect task states

```bat
docker exec internship1-airflow-scheduler-1 airflow dags list-runs -d self_healing_pipeline
docker exec internship1-airflow-scheduler-1 airflow tasks states-for-dag-run self_healing_pipeline 2026-09-27
docker exec internship1-airflow-scheduler-1 airflow tasks states-for-dag-run self_healing_pipeline batch__2026-09-27__v2
```

In Airflow 2.9.3 the last argument is `EXECUTION_DATE_OR_RUN_ID`:

- A bare date means midnight UTC (`2026-09-27T00:00:00+00:00`).
- For a run at another time, pass the run ID shown by `list-runs`, or the full timestamp such as `2026-09-27T03:00:00+00:00`.

All 8 tasks should be `success`. For task-level problems, run `docker logs internship1-airflow-scheduler-1 --tail 200`.

### 6. Inspect pipeline status and incidents

```bat
curl http://localhost:8000/api/v1/pipeline/status
curl http://localhost:8000/api/v1/pipeline/runs
curl http://localhost:8000/api/v1/incidents
dir data\incidents\reports
```

`/pipeline/status` returns the most recently started row in `pipeline_runs`, which holds one row per real DAG run:

| Status | Meaning |
|---|---|
| `HEALTHY` | Clean run. |
| `RECOVERED` | A fault was auto-fixed and verified in this run. |
| `FAILED` | A task failed after its retries, for example an escalated fault. Written by the DAG's failure hook. `row_counts` is `null` because nothing was loaded. |

`incident_count`, `auto_fix_count` and `escalation_count` count only incidents filed for that run. Note that `pytest` also inserts `test_*` rows here (see Tests).

### 7. Demonstrate self-healing (auto-fix)

Use a past date whose batch exists. Fault injection (`scripts\fault-injection\inject_fault.py`) rewrites only **staging** (`data\staging\stg_orders_D.csv`). It builds that file from the raw batch plus the fault. Raw data and the database are not touched. Ingestion then preserves the injected staging file.

```bat
python scripts\fault-injection\inject_fault.py --fault duplicate_ingestion --execution-date 2026-09-24
docker exec internship1-airflow-scheduler-1 airflow dags trigger self_healing_pipeline -e 2026-09-24T01:00:00+00:00 -r selfheal__2026-09-24
docker exec internship1-airflow-scheduler-1 airflow tasks states-for-dag-run self_healing_pipeline selfheal__2026-09-24
curl http://localhost:8000/api/v1/pipeline/status
curl http://localhost:8000/api/v1/incidents?limit=1
```

**Expected:** all 8 tasks succeed. The status is `RECOVERED` with `incident_count 1` and `auto_fix_count 1`, and the incident shows `DUPLICATE_INGESTION`, `action AUTO_FIX`, `status REMEDIATED` and `verification_status PASSED`.

What happens: `validate_quality` detects 50 duplicate `order_id` values, the AI adapter diagnoses them (Ollama, or the rule-based fallback if Ollama is offline), and the Policy Gate allows `AUTO_FIX`. The executor dedupes staging, the verifier re-reads it from disk, and the run continues. The healed staging file equals the original clean batch.

### 8. Demonstrate escalation

```bat
copy data\staging\stg_orders_2026-09-23.csv %TEMP%\stg_orders_2026-09-23.csv
python scripts\fault-injection\inject_fault.py --fault null_spike --execution-date 2026-09-23
docker exec internship1-airflow-scheduler-1 airflow dags trigger self_healing_pipeline -e 2026-09-23T01:00:00+00:00 -r escalation__2026-09-23
```

Wait about 5 minutes: `validate_quality` retries twice at 2-minute intervals before failing. Then check:

```bat
docker exec internship1-airflow-scheduler-1 airflow tasks states-for-dag-run self_healing_pipeline escalation__2026-09-23
curl http://localhost:8000/api/v1/pipeline/status
curl http://localhost:8000/api/v1/incidents?limit=1
```

**Expected:** `validate_quality` is `failed` and the downstream tasks are `upstream_failed`. The status is `FAILED` with `incident_count 1` and `escalation_count 1`, and the incident shows `NULL_SPIKE`, `action ESCALATE` and `status ESCALATED`. Only `DUPLICATE_INGESTION` is ever auto-fixed; every other fault is escalated by policy.

**Human fix, then re-run:** ingestion preserves staging, so restore the clean file (or delete it so it re-stages from raw) before re-running the date.

```bat
copy /Y %TEMP%\stg_orders_2026-09-23.csv data\staging\stg_orders_2026-09-23.csv
docker exec internship1-airflow-scheduler-1 airflow dags trigger self_healing_pipeline -e 2026-09-23T02:00:00+00:00 -r batch__2026-09-23__after_fix
```

For a `schema_drift` drill, the corrupted file is `data\staging\stg_products.csv` instead. Delete it afterwards so it re-stages from `data\raw\products.csv`.
