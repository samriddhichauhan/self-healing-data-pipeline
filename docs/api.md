# Backend API

Base URL: `http://localhost:8000`. Interactive docs: `http://localhost:8000/docs`.

All endpoints below read real state (Postgres `pipeline` database, or files actually written by the Airflow tasks). None return hardcoded placeholder data — if the database is unreachable, endpoints return `503` with the real error, not a fake 200.

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Service + live DB connectivity status |
| GET | `/api/v1/pipeline/status` | Latest `pipeline_runs` row |
| GET | `/api/v1/pipeline/runs` | Recent `pipeline_runs` rows |
| GET | `/api/v1/pipeline/runs/{run_id}` | One run + its `quality_results` and `aggregated_metrics` |
| GET | `/api/v1/pipeline/runs/{run_id}/airflow-state` | Live task-instance states from the Airflow REST API for that run |
| GET | `/api/v1/pipeline/summary` | Aggregate counts across all runs |
| POST | `/api/v1/pipeline/trigger` | Triggers a real `self_healing_pipeline` DAG run via the Airflow REST API |
| GET | `/api/v1/incidents` | Real incidents from the `incidents` table |
| GET | `/api/v1/incidents/{incident_id}` | One incident, including full diagnosis/remediation/verification payload |
| POST | `/api/v1/incidents/{incident_id}/remediate` | Manually re-runs remediation + verification for an incident |
| GET | `/api/v1/data/profiles` | Data-profiling reports written during ingestion |
| POST | `/api/v1/simulation/inject-fault` | Runs the real fault-injection CLI against staged data |
| GET | `/api/v1/logs`, `/api/v1/logs/{filename}` | Real log files from `logs/` |

CORS is restricted to explicit origins via the `CORS_ORIGINS` env var (default: `http://localhost:5173,http://127.0.0.1:5173`) — never a wildcard with credentials.
