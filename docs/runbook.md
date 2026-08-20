# Operations Runbook — Self-Healing Data Pipeline

This document guides engineers on running, monitoring, and troubleshooting the containerized Airflow pipeline.

---

## 1. Environment Startup

1. Confirm Docker Desktop is running.
2. Initialize the metadata database (runs schema migrations and creates the default admin user):
   ```bash
   docker compose up airflow-init
   ```
3. Start the services in the background:
   ```bash
   docker compose up -d
   ```
4. Check running containers:
   ```bash
   docker compose ps
   ```

---

## 2. Ingesting & Running the Pipeline

- Access the Airflow UI at `http://localhost:8080` (credentials: `airflow` / `airflow`).
- Locate the DAG `self_healing_pipeline` and toggle the **Active/Inactive** switch to enable it.
- To trigger a manual run, click the **Trigger DAG** play button in the top right.

---

## 3. Monitoring & Health Checks

### Local Logs
- Task run logs are mapped locally to `./airflow/logs/`.
- Docker service logs can be viewed via:
   ```bash
   docker compose logs -f
   ```

### BigQuery Target State
- Transactional tables are saved under dataset `pipeline_intern_<handle>`:
  - `dim_customers` (Dimension)
  - `dim_products` (Dimension)
  - `fct_orders` (Fact, partitioned by day)
  - `fct_events` (Fact, partitioned by day)

---

## 4. Diagnostics & Remediation

When a task fails:
1. **Agent Diagnostic Hook**: Airflow's `on_failure_callback` fires the AI Agent (`apps/agent`).
2. **Incident Report**: The Agent creates a markdown incident report under `incidents/reports/inc_<incident_id>.md`.
3. **Remediation States**:
   - **Auto-Fixed**: Low-risk faults (Duplicate Ingestion, Volume Anomalies spike) are fixed automatically.
   - **Escalated**: Structural or high-risk issues (Schema Drift, Null Spikes, Referential Breaks) await manual engineer confirmation. Check the dashboard or run:
     ```bash
     # To review open incident reports
     cat incidents/reports/
     ```
