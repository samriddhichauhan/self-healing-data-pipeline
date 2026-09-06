# System Architecture — Self-Healing Data Pipeline

This document details the high-level architecture, component responsibilities, data profiling, incident lifecycle, policy gate, and implementation topology of the Self-Healing Data Pipeline project.

---

## 1. System Topology

The diagram below outlines the flow of data, control, profiling, and diagnostics across the system:

```mermaid
graph TD
    subgraph Client Layer
        FE["Frontend (React / Vite Dashboard)"]
    end

    subgraph Service Layer
        BE["Backend API (FastAPI)"]
    end

    subgraph Ingestion & Profiling Layer
        AF["Apache Airflow (LocalExecutor)"]
        Raw["Raw Data (CSV/JSONL)"]
        Prof["Data Profiler (Nulls / Dups / Bounds)"]
        Target["Local Staging & Storage"]
    end

    subgraph Diagnostics & Healing Layer
        Agent["AI Diagnostic Reasoning Engine"]
        Policy["Policy Gate (Auto-Fix vs Escalate)"]
        Remediation["Remediation Executor"]
        Verification["Remediation Verifier"]
    end

    %% Data flow
    Raw -->|1. Ingest & Profile| AF
    AF -->|2. Non-blocking Profile| Prof
    AF -->|3. Validate & Load| Target

    %% Control / Diagnostics flow
    AF -->|4. On Task Failure Hook| Agent
    Agent -->|5. Baseline Compare & Reason| Policy
    Policy -->|6. AUTO_FIX| Remediation
    Policy -->|6. ESCALATE| FE
    Remediation -->|7. Post-Fix Verification| Verification
    Verification -->|8. Mark RESOLVED| AF
    
    %% Management API flow
    FE <-->|User Interaction| BE
    BE <-->|Read Metrics / Profiles / Runs| AF
    BE <-->|Read Incidents / Trigger Auto-Fix| Agent
```

---

## 2. Component Directory

| Component | Responsibility | Status |
| :--- | :--- | :--- |
| **Frontend** | Interactive React/Vite dashboard displaying Live Pipeline Flow, DAG Node Canvas, Telemetry Metrics, Data Profiling, and Incident Inspector. | **COMPLETE** |
| **Backend API** | FastAPI service exposing REST endpoints for pipeline status, summary runs, incident listings, profiling reports, fault injection, and remediation calls. | **COMPLETE** |
| **Airflow Orchestrator** | Coordinates 5 TaskGroups (`ingestion`, `validation`, `transformation`, `load`, `monitoring`) and 8 tasks with detailed `doc_md` documentation and failure callbacks. | **COMPLETE** |
| **Data Profiler** | Non-blocking data health calculator evaluating row counts, null %, duplicate %, min/max bounds, column distributions, and freshness SLAs. | **COMPLETE** |
| **AI Diagnostic Engine** | Structured reasoning engine processing anomalies (`OBSERVED → EVIDENCE → POSSIBLE ROOT CAUSES → CONFIDENCE → BLAST RADIUS → RECOMMENDED ACTION`). | **COMPLETE** |
| **Policy Gate** | Safeguard engine enforcing `AUTO_FIX` for low-risk idempotent actions vs `ESCALATE` for high-risk anomalies (`SCHEMA_DRIFT`, `NULL_SPIKE`, `REFERENTIAL_BREAK`). | **COMPLETE** |
| **Remediation & Verifier** | Executes deterministic deduplication / quarantine actions followed by post-fix verification assertions before marking incidents `RESOLVED`. | **COMPLETE** |

---

## 3. Incident Lifecycle

```
DETECTED (Validation Assert Failure)
   ↓
SERIALIZED (Incident Report JSON & Markdown Created)
   ↓
DIAGNOSED (AI Engine & Historical Baseline Comparison)
   ↓
POLICY GATE EVALUATION
   ├── LOW-RISK (DUPLICATE / STALENESS) → AUTO_FIX → REMEDIATION → VERIFICATION → RESOLVED
   └── HIGH-RISK (SCHEMA / NULL / REF / DROP) → ESCALATE → HUMAN REVIEW → RESOLVED
```
