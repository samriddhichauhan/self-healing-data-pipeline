# Operations & Self-Healing Runbook — Data Engineering Platform

This operational runbook documents the architecture, diagnostic logic, Policy Gate rules, auto-remediation procedures, and troubleshooting workflows for the Self-Healing Data Pipeline.

---

## 1. Environment & Architecture Overview

* **Orchestration**: Apache Airflow DAG (`self_healing_pipeline`) with 5 TaskGroups (`ingestion`, `validation`, `transformation`, `load`, `monitoring`).
* **API Backend**: FastAPI service running at `http://localhost:8000`.
* **Frontend**: React + Vite pipeline dashboard running at `http://localhost:5173`.
* **Agent System**: 5-stage diagnostic reasoning engine with local Ollama LLM primary adapter (`apps/agent/agent/diagnosis/ollama_adapter.py`) and deterministic rule-based fallback (`apps/agent/agent/diagnosis/engine.py`).
* **AI Runtime Connectivity**: Airflow containers connect to host Ollama instance via `http://host.docker.internal:11434` with model `llama3.2:latest`.

---

## 2. Policy Gate Decision Matrix

| Fault Category | Severity | Policy Action | Rationale | Remediation Executed |
| :--- | :---: | :---: | :--- | :--- |
| **`DUPLICATE_INGESTION`** | `MEDIUM` | **`AUTO_FIX`** | Idempotent deduplication by primary key is safe and deterministic. | `RemediationExecutor.deduplicate_dataset` |
| **`VOLUME_ANOMALY_SPIKE`**| `MEDIUM` | **`AUTO_FIX`** | Duplication spikes are resolved via primary key deduplication. | `RemediationExecutor.deduplicate_dataset` |
| **`SCHEMA_DRIFT`** | `HIGH` | **`ESCALATE`** | Altered data types require schema migration and engineering review. | Human Escalation |
| **`NULL_SPIKE`** | `HIGH` | **`ESCALATE`** | Non-nullable key nulls indicate upstream database extraction failure. | Human Escalation |
| **`VOLUME_ANOMALY_DROP`** | `HIGH` | **`ESCALATE`** | Missing records suggest upstream job truncation or network loss. | Human Escalation |
| **`REFERENTIAL_BREAK`** | `HIGH` | **`ESCALATE`** | Foreign key orphans require dimension synchronization. | Human Escalation |
| **`STALENESS`** | `MEDIUM` | **`ESCALATE`** | Freshness SLA breach requires upstream cron inspection. | Human Escalation |

---

## 3. Diagnostic & Auto-Healing Sequence

```
1. Task Failure -> Airflow on_task_failure Callback
2. Incident Serialization -> data/incidents/reports/inc_*.json
3. AI Diagnostic Reasoning -> Hypothesis, Confidence, Blast Radius
4. Policy Gate Safeguard -> AUTO_FIX or ESCALATE
5. If AUTO_FIX -> RemediationExecutor -> Verification -> Pipeline Continued
6. If ESCALATE -> Incident Marked PENDING_HUMAN_REVIEW -> Safe Stop
```

---

## 4. Operational Commands & Diagnostic Verification

### Execute Automated 5-Case Test Harness
```bash
python scripts/test_pipeline_5_cases.py
```

### Execute System Health Diagnostic
```bash
python scripts/check_airflow_health.py
```

### Verify DAG Lineage & Task Configuration
```bash
python scripts/verify_airflow_dag.py
```

### Run Full Test Suite
```bash
pytest
```
