# Self-Healing Data Pipeline — Demo Readiness Report

This document provides a comprehensive readiness, architecture, policy rule, API, and limitation report for the **Self-Healing Data Pipeline**.

---

## 1. Project Overview & Architecture

The Self-Healing Data Pipeline is an enterprise-grade, resilient data engineering framework designed to ingest, validate, transform, and monitor multi-source e-commerce data. If a data quality or schema failure occurs, an integrated **AI Diagnostic Engine** analyzes raw log/dataset evidence, calculates confidence and blast radius, and hands off a recommendation to a safety-first **Policy Gate**.

### Pipeline Lineage & Execution Flow
```
DATA SOURCES (Orders CSV, Events JSONL, Customer & Product CSVs)
   │
   ▼
1. INGESTION (ingest_dimensions, ingest_orders, ingest_events)
   │
   ▼
2. SCHEMA VALIDATION (validate_schema — Contracts, Column Types)
   │
   ▼
3. QUALITY VALIDATION (validate_quality — Freshness SLA, Null Spikes, Duplicates, FK Integrity)
   │
   ▼
4. TRANSFORMATION (transform_data — Cleaning, Denormalization, Metrics)
   │
   ▼
5. LOAD & MONITORING (load_data, agent_monitoring — Analytics Staging & Health Reports)
```

---

## 2. Self-Healing Control Flow Architecture

```
INCIDENT TRIGGERED (Validation Failure / Pipeline Exception)
   │
   ▼
EVIDENCE COLLECTION (Log Snippets, Row Count Diffs, Schema Diffs, Metric Outliers)
   │
   ▼
AI DIAGNOSTIC ENGINE (Ollama Local AI / Gemini API / Rule-Based Engine)
   │
   ▼
DIAGNOSIS PAYLOAD (Root Cause, Hypothesis, Confidence Score [0.0-1.0], Blast Radius [1-10], Action)
   │
   ▼
POLICY GATE (Check: Confidence >= 0.85 AND Risk == LOW AND Idempotent == True)
   ├── APPROVED  ──► AUTO_FIX ──► REMEDIATION EXECUTOR ──► REMEDIATION VERIFIER ──► RESOLVED
   └── REJECTED  ──► ESCALATE ──► HUMAN REVIEW (Execution safe-stopped, No auto-modification)
```

---

## 3. AI Implementation Classification: HYBRID SYSTEM

The diagnostic layer (`apps/agent/agent/diagnosis/`) uses a tiered hybrid adapter pattern:

1. **Local Open-Weights LLM Adapter (`OllamaDiagnosticAdapter`):** Attempts connection to local Ollama instance (`http://localhost:11434/api/generate`) using models like `qwen2.5:coder` or `llama3.2`.
2. **Cloud AI Adapter (`LLMDiagnosticAdapter`):** Connects to Google Gemini / OpenAI APIs if `GEMINI_API_KEY` is present.
3. **Deterministic Fallback Engine (`DiagnosticEngine`):** Pattern-matches log snippets and quality metrics if LLMs are unconfigured or offline.

> [!NOTE]
> The AI layer provides **recommendations only**. It does **NOT** execute arbitrary terminal commands, modify database schemas directly, or bypass the Policy Gate.

---

## 4. Policy Gate Rules & Fault Truth Table

Hardcoded Policy Threshold: `AUTO_FIX_CONFIDENCE_THRESHOLD = 0.85` in [`apps/agent/agent/policies/policy_gate.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/policies/policy_gate.py).

| Fault Type | Observed Symptom | AI Diagnosis Hypothesis | Required Confidence | Risk Level | Policy Decision | Allowed Remediation Action |
| :--- | :--- | :--- | :---: | :---: | :---: | :--- |
| `DUPLICATE_INGESTION` | Duplicate primary keys in `stg_orders` | Upstream scheduler retry or duplicate file delivery | `>= 0.85` | `LOW` | **`AUTO_FIX`** | `DEDUPLICATE_DATASET` (SQL Window Deduplication) |
| `VOLUME_ANOMALY_SPIKE`| Row count exceeds +50% upper bound | Ingestion retry bursting / surge in event delivery | `>= 0.85` | `LOW` | **`AUTO_FIX`** | `DEDUPLICATE_DATASET` |
| `SCHEMA_DRIFT` | Data type mismatch / altered column | Upstream API contract alteration in `products` | N/A | `HIGH` | **`ESCALATE`** | None (Safe Stop — Human Review Required) |
| `VOLUME_ANOMALY_DROP` | Row count below -50% lower bound | Partial file export or batch job truncation | N/A | `HIGH` | **`ESCALATE`** | None (Safe Stop — Human Review Required) |
| `NULL_SPIKE` | Null ratio exceeds tolerance in `customer_id` | Source database extraction query failure | N/A | `HIGH` | **`ESCALATE`** | None (Safe Stop — Human Review Required) |
| `REFERENTIAL_BREAK` | Missing foreign keys in dimension lookup | Stale dimension sync or orphan records | N/A | `HIGH` | **`ESCALATE`** | None (Safe Stop — Human Review Required) |
| `STALENESS` | Order timestamps > 26 hours old | Upstream pipeline delay / stale batch file | N/A | `MEDIUM` | **`ESCALATE`** | None (Safe Stop — Human Review Required) |

---

## 5. Remediation & Verification Integrity

- **Remediation Executor ([`apps/agent/agent/remediation/executor.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/remediation/executor.py)):** Idempotently deduplicates staged CSV/JSONL files by primary key (`order_id`). Running remediation multiple times on the same dataset produces identical, clean output without data corruption.
- **Remediation Verifier ([`apps/agent/agent/verification/verifier.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/verification/verifier.py)):** Executes a post-remediation query asserting `duplicate_count == 0` and `remaining_rows > 0`. Incident status is updated to `RESOLVED` **only** after verification passes.

---

## 6. Incident Generation & Storage Paths

When a validation failure occurs, structured incident reports are generated and persisted to:

- **Status Reports:** `incidents/status/status_<date>.json`
- **Incident Artifacts:** `incidents/reports/INC-<timestamp>.json` & `incidents/reports/INC-<timestamp>.md`
- **Persistent Central Logs:** `logs/pipeline_execution.log`, `logs/validate_quality.log`, `logs/ollama_ai_agent.log`, `logs/remediation_executor.log`, `logs/verification.log`

---

## 7. Supported Fault Injection Scenarios

Controlled fault injector CLI: [`scripts/fault-injection/inject_fault.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/scripts/fault-injection/inject_fault.py)

1. `schema_drift`: Modifies `products.price` in staged data to string format (`"$14.99"`).
2. `volume_drop`: Truncates `orders` to 120 rows (below 240 row minimum bound).
3. `null_spike`: Injects 15% NULL values into `customer_id`.
4. `duplicate_ingestion`: Duplicates 50 order records (350 rows total).
5. `referential_break`: Sets 10 orders to non-existent `customer_id` (`"CUST_ORPHAN_999"`).
6. `staleness`: Sets order timestamps 30 days into the past (`"2026-01-01"`).

---

## 8. Synthetic Data Generation

Command:
```powershell
python scripts/data/generate_data.py --out ./data --start-date 2026-09-07 --days 1
```

Generates:
- `data/customers.csv` (500 records)
- `data/products.csv` (60 records)
- `data/orders/orders_2026-09-07.csv` (300 orders)
- `data/events/events_2026-09-07.jsonl` (1,200 events)

---

## 9. Apache Airflow Configuration

- **DAG ID:** `self_healing_pipeline`
- **Schedule:** `0 2 * * *` (Daily at 02:00 UTC)
- **DAG File:** [`airflow/dags/pipeline_dag_starter.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/airflow/dags/pipeline_dag_starter.py)
- **Task Retries:** 2 retries per task with 2-minute retry delay
- **Failure Callback:** `on_task_failure` logs incidents and triggers AI Diagnostic Agent hook.

---

## 10. Backend API Endpoints

FastAPI server located at [`apps/backend/main.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/backend/main.py):

- `GET /health` — Service health check
- `GET /api/v1/pipeline/status` — Current pipeline run status & row counts
- `GET /api/v1/pipeline/runs` — Pipeline execution history
- `GET /api/v1/incidents` — List all generated incidents
- `GET /api/v1/incidents/{incident_id}` — Incident details, diagnosis, & verification report
- `GET /api/v1/logs` — List central log files in `logs/`
- `GET /api/v1/logs/{filename}` — Stream central log file contents
- `POST /api/v1/simulation/inject-fault` — Trigger fault injection via API

---

## 11. Frontend Dashboard

React UI in `apps/frontend` compiled with Vite:
- Real-time pipeline lineage status visualization
- Incident list with root cause, confidence score, blast radius, Policy Gate decision, and verification status
- Production build command: `cd apps/frontend && npm run build` (Passed cleanly in 1.44s)

---

## 12. Environment Variables & Required Credentials

Configured in `.env`:
- `AIRFLOW_UID=50000`
- `PORT=8000`
- `OLLAMA_HOST=http://localhost:11434` (Optional for local LLM inference)
- `GEMINI_API_KEY=your_gemini_api_key_here` (Optional for cloud LLM inference)

---

## 13. System Boundaries & Overclaiming Protections

1. **BigQuery Cloud Execution:** BigQuery schema adapters and partitioning logic are fully implemented (`apps/agent/agent/adapters/bigquery_adapter.py`). Live cloud execution requires GCP service account credentials. Current environment runs in **Local Sandbox Mode**.
2. **AI Layer:** System uses a **Hybrid Diagnostic Engine**. If Ollama or Gemini keys are missing, the system gracefully falls back to deterministic rule-based pattern matching.

---

## 14. Verification Commands

```powershell
# 1. Full Pytest Suite (36/36 passed)
pytest

# 2. 5-Case Test Harness
python scripts/test_pipeline_5_cases.py

# 3. Airflow DAG Health Check
python scripts/check_airflow_health.py

# 4. Airflow DAG Verification
python scripts/verify_airflow_dag.py

# 5. Frontend Build
cd apps/frontend
npm run build
```
