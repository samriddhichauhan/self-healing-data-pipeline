# Runbook

## Start everything

```bash
docker compose up -d
docker compose ps        # all services should show healthy/running
```

Services: `postgres`, `airflow-webserver` (:8080, user/pass `airflow`/`airflow`), `airflow-scheduler`, `backend` (:8000), `frontend` (:5173).

## Verify the environment

```bash
python scripts/verify_environment.py
```

## Run the two manager demos

```bash
python scripts/demo_healthy_run.py
python scripts/demo_self_healing_run.py
```

Both trigger a real DAG run inside the Airflow container, poll the real scheduler, and independently verify results via `airflow tasks states-for-dag-run` and the `pipeline` Postgres database. Neither ever marks a task successful manually or fabricates output.

## Reset demo state

```bash
python scripts/reset_demo.py
```

Clears `data/staging/`, `data/processed/`, and incident/status files. Regenerates the canonical `data/raw/` dataset only if it's missing — it never overwrites an intact canonical fixture.

## Inspect a specific run

```bash
docker exec <scheduler-container> airflow dags list-runs -d self_healing_pipeline
docker exec <scheduler-container> airflow tasks states-for-dag-run self_healing_pipeline <RUN_ID>
docker exec <scheduler-container> find /opt/airflow/logs/dag_id=self_healing_pipeline/run_id=<RUN_ID> -type f
```

(Find the container name with `docker ps --filter label=com.docker.compose.service=airflow-scheduler`.)

## Recovering from a restart

```bash
docker compose down
docker compose up -d
```

Postgres data persists in the `postgres-db-volume` named volume. Schema (both Airflow's and the pipeline's `pipeline` database + tables) is created idempotently — no manual repair needed.

## Tests

```bash
pip install -r requirements.txt
pytest
```

Tests that need the real Postgres `pipeline` database (`tests/test_database.py`, some of `tests/test_pipeline_scenarios.py` and `tests/integration/test_backend_api.py`) auto-skip if it isn't reachable; run `docker compose up -d postgres` first to exercise them for real.
