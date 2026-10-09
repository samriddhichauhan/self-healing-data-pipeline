# Architecture

## Stack

- **Airflow 2.9.3** (LocalExecutor) — orchestrates the `self_healing_pipeline` DAG, runs only inside Docker.
- **PostgreSQL 15** — one container, two databases: `airflow` (Airflow's own metadata) and `pipeline` (this project's business data — `pipeline_runs`, `quality_results`, `incidents`, `orders`, `events`, `aggregated_metrics`).
- **FastAPI backend** (`apps/backend`) — reads the `pipeline` database and incident/log files, exposes REST endpoints, can trigger DAG runs via the Airflow REST API.
- **React + Vite frontend** (`apps/frontend`) — polls the backend; no client-side simulation.
- **Ollama** (optional, local, host-side) — primary AI diagnosis engine. If unreachable, a deterministic rule-based engine takes over automatically. No external paid API is used anywhere.

## Code layout

```
src/
  ingestion/      # locate + stage raw batches for the Airflow logical date; fail loud on missing data
  validation/      # schema.py (column/type contract), quality.py (row-count/null/duplicate/referential/
                   #   freshness checks + the embedded duplicate-ingestion self-heal loop)
  transformation/ # dedupe, normalize, derive KPIs -> aggregated_metrics
  remediation/     # executor.py (deterministic dedupe), verifier.py (independent post-fix re-check)
  agent/           # policy_gate.py, diagnosis_engine.py (rule-based), ollama_adapter.py (primary AI),
                   #  incident.py (model), profiler.py, logger.py
  database/        # schema.py (DDL), loader.py (writes), queries.py (reads, used by the backend)
  monitoring/      # agent_monitoring: computes the real final run status
airflow/dags/self_healing_pipeline.py   # thin task wiring only — imports from src/
```

## DAG

```
ingest_dimensions ─┐
ingest_orders ──────┼──> validate_schema ──> validate_quality ──> transform_data ──> load_data ──> agent_monitoring
ingest_events ─────┘
```

Retries: 2, 2-minute delay, on every task. `on_failure_callback=on_task_failure` on every task (true-failure path: diagnoses and records an incident, escalates for human review — it never auto-remediates).

## Self-healing design

Airflow's `PythonOperator` model makes it awkward to resume a downstream task automatically after a mid-DAG fix, so self-healing for `DUPLICATE_INGESTION` is embedded **inside** `validate_quality` itself:

1. Duplicate `order_id`s detected -> build evidence -> `OllamaDiagnosticAdapter.analyze_incident()` (Ollama primary, rule-based fallback).
2. `PolicyGate.evaluate()` — the sole authority on AUTO_FIX vs ESCALATE. Only `DUPLICATE_INGESTION` (and the symmetric `VOLUME_ANOMALY_SPIKE`) are `LOW risk + idempotent`, so only those can ever get `AUTO_FIX`.
3. If `AUTO_FIX`: `RemediationExecutor.deduplicate_dataset()` -> `RemediationVerifier.verify()` re-reads the file independently and only then flags it verified.
4. If verified, `validate_quality` re-checks the in-memory frame and returns normally — Airflow sees a real task success, not a hidden failure.
5. Every other fault category (`SCHEMA_DRIFT`, `NULL_SPIKE`, `VOLUME_ANOMALY_DROP`, `REFERENTIAL_BREAK`, `STALENESS`) is `ESCALATE`-only by policy — the task raises for real and the DAG run fails, which is the correct, safe outcome.

Every incident (healed or escalated) is persisted as JSON+Markdown under `data/incidents/reports/` and mirrored into the `incidents` Postgres table.

## Database

`load_data` is the hard boundary for "database unavailable -> clear failure": if Postgres can't be reached, this task raises and the run fails — it never silently succeeds with stale/fake data. `agent_monitoring` computes the real final status (`HEALTHY` if no incident this run, `RECOVERED` if an incident was auto-fixed and verified this run) and writes it to `pipeline_runs`.

## Idempotent staging

Ingestion stages a partition once per logical date and **preserves** an existing staged file rather than re-copying from raw. This is what lets a fault injected directly into staging survive the ingestion tasks that run ahead of `validate_quality` in the same DAG run.
