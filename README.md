# Self-Healing Data Pipeline

A containerized, resilient production data engineering platform featuring automated anomaly detection, AI-powered diagnostic reasoning, policy-based self-healing, and interactive pipeline monitoring.

---

## 🏗️ Architecture & Component Topology

```
                    ┌───────────────────┐
                    │   DATA SOURCES    │
                    │ (CSV / JSONL / DB)│
                    └─────────┬─────────┘
                              ↓
                    ┌───────────────────┐
                    │ APACHE AIRFLOW    │
                    │ (5 TaskGroups)    │
                    └─────────┬─────────┘
                              ↓
            ┌─────────────────┴─────────────────┐
            ↓                                   ↓
  ┌───────────────────┐               ┌───────────────────┐
  │ INGESTION & DATA  │               │ SCHEMA & QUALITY  │
  │ PROFILING LAYER   │               │ VALIDATION ENGINE │
  └─────────┬─────────┘               └─────────┬─────────┘
            └─────────────────┬─────────────────┘
                              ↓ (On Validation Failure)
                    ┌───────────────────┐
                    │ AIRFLOW FAILURE   │
                    │ HOOK (INCIDENT)   │
                    └─────────┬─────────┘
                              ↓
                    ┌───────────────────┐
                    │  AI DIAGNOSTICS   │
                    │  REASONING LAYER  │
                    └─────────┬─────────┘
                              ↓
                    ┌───────────────────┐
                    │    POLICY GATE    │
                    └─────────┬─────────┘
                      ↙               ↘
              AUTO_FIX                 ESCALATE
                 ↓                        ↓
        REMEDIATION EXECUTOR         HUMAN REVIEW
                 ↓
        REMEDIATION VERIFIER
                 ↓
         PIPELINE RESOLVED
```

---

## 📁 Repository Directory Layout

```
├── apps/
│   ├── frontend/                # React + Vite pipeline monitoring dashboard
│   ├── backend/                 # FastAPI REST server & scenario endpoints
│   └── agent/                   # AI Diagnostic Engine (Ollama llama3.2), Policy Gate, Remediation, Verification
├── airflow/
│   ├── dags/                    # pipeline_dag_starter.py (5 TaskGroups, 8 Tasks, AI on_failure callback)
│   ├── config/                  # pipeline_config.yaml (schema contracts & SLAs)
│   └── plugins/                 # Custom Airflow hooks and operators
├── data/                        # Persistent analytical storage (raw, staging, processed, incidents)
├── docs/                        # Architecture & runbook documentation
├── scripts/
│   ├── fault-injection/         # Controlled fault injection CLI
│   ├── test_pipeline_5_cases.py # 5-case pipeline scenario test harness
│   ├── check_airflow_health.py  # Health diagnostic script
│   └── verify_airflow_dag.py    # DAG verification script
└── tests/                       # Automated unit & integration tests (36 passing tests)
```

---

## 🤖 AI Self-Healing Engine (Ollama llama3.2 Integration)

* **Primary Reasoning Engine**: `OllamaDiagnosticAdapter` running locally (`llama3.2:latest`) via host gateway connectivity.
* **Deterministic Fallback**: `DiagnosticEngine` seamlessly engages if the local model is offline or timing out.
* **Policy Gate Authorization**: Every AI action passes through strict policy evaluation (`AUTO_FIX` for duplicate ingestion, `ESCALATE` for schema drifts and null spikes).
* **Closed-Loop Verification**: Post-remediation verification checks ensure clean state before continuing or marking incidents resolved.

---

## 🚀 Quickstart & Manager Demonstration

### 1. Run Automated 5-Case Test Suite
```bash
python scripts/test_pipeline_5_cases.py
```

### 2. Launch FastAPI Backend
```bash
python -m uvicorn apps.backend.main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Launch React Dashboard
```bash
cd apps/frontend
npm run dev
```
Open your browser to `http://localhost:5173`.

### 4. Run Pytest Suite
```bash
pytest
```
