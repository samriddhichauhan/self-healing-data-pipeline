# Self-Healing Data Pipeline — Technical Audit

**Audit Date:** September 10, 2026  
**Auditor:** Senior Data Engineering + AI/ML + Full-Stack Systems Architect  
**Repository:** `samriddhichauhan/self-healing-data-pipeline`  
**Target Environment:** Apache Airflow 2.9.3 | FastAPI | React 19 + Vite | Python 3.12 | Google BigQuery / Local Sandbox  

---

## 1. Executive Summary

This technical audit provides a rigorous, ground-truth inspection of the **Self-Healing Data Pipeline** repository. Every module—including Airflow DAG definitions, data validation routines, AI diagnostic adapters, the Policy Gate engine, remediation scripts, FastAPI backend, React dashboard, tests, Docker definitions, and security parameters—was inspected, statically analyzed, and verified against actual command execution.

### High-Level Verdict:
The project demonstrates **strong foundational data engineering concepts**, an **exceptionally well-designed safety architecture (Policy Gate + Verification loop)**, and **complete, working test harnesses (36/36 passing pytest unit & integration tests)**. The core diagnostic reasoning flow correctly discriminates between safe, idempotent auto-fixes (`DUPLICATE_INGESTION`) and dangerous, high-risk structural faults (`SCHEMA_DRIFT`, `NULL_SPIKE`, `VOLUME_ANOMALY_DROP`, `REFERENTIAL_BREAK`, `STALENESS`).

However, the audit identified **several architectural disconnects, hardcoded/simulated components, and containerization gaps** that must be understood and addressed before presenting to technical leadership:

1. **Frontend-Backend Disconnect (CRITICAL):** The React dashboard (`apps/frontend/src/App.tsx`) is 100% simulated on client-side React state and `setTimeout` timers. It makes **zero HTTP network requests** to the FastAPI backend (`apps/backend/main.py`).
2. **Airflow Runtime vs. Test Harness Decoupling (HIGH):** The auto-healing loop (Failure → AI Diagnosis → Policy Gate → Remediation → Verification → Re-run) is fully implemented and tested in the standalone Python harness (`scripts/test_pipeline_5_cases.py`), but in the native Airflow DAG (`pipeline_dag_starter.py`), `on_task_failure` only writes the incident report to disk—it does not automatically trigger the remediation executor or resume downstream tasks inside the Airflow scheduler.
3. **Fault Injection Overwrite Vulnerability (HIGH):** Injected faults target `data/staging/` files. If a user runs a full Airflow DAG from the beginning, the upstream `ingest_orders` / `ingest_dimensions` tasks read from `data/raw/` and **overwrite** the injected staging files before validation runs.
4. **Ollama / LLM Fallback Behavior (MEDIUM):** The diagnostic engine uses a tiered hybrid model (Ollama → Gemini → Rule-Based Engine). In local development without an active Ollama instance or API key, it seamlessly falls back to the rule-based engine where confidence scores (`0.95`, `0.92`, `0.89`), hypotheses, and recovery times (`3.5s`) are static templates.
5. **Docker Compose Incompleteness (MEDIUM):** `docker-compose.yml` only defines Airflow webserver, scheduler, and Postgres. The FastAPI backend and React frontend are omitted from Docker.

---

## 2. Critical Issues

### Issue 1: Frontend Dashboard is 100% Client-Side Simulated (No Backend Integration)
- **Severity:** `CRITICAL`
- **File:** [`apps/frontend/src/App.tsx`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/frontend/src/App.tsx#L1-L1732)
- **Problem:** The entire React dashboard runs on synthetic local state. There are zero `fetch()`, `axios`, or WebSocket calls to `http://localhost:8000`.
- **Evidence:** Searching for network calls in `apps/frontend/src` yields 0 network requests. Functions `runPipelineSimulation()` (lines 512–653), `handleInjectFault()` (lines 655–699), and `handleApproveRemediation()` (lines 701–752) use hardcoded `setTimeout` sequences to simulate DAG execution, log creation, and incident resolution in React memory. Metrics (Success rate 98.2%, 18 successful runs, 14.8 MB storage, run duration 135s) are hardcoded static constants.
- **Impact:** If a manager asks to see live data from a real pipeline run or FastAPI endpoint in the dashboard, the UI will only display simulated dummy animations unrelated to database or backend state.
- **Recommended Fix:** Wire `useEffect` hooks and API handlers to fetch live data from FastAPI endpoints: `GET /api/v1/pipeline/status`, `GET /api/v1/incidents`, `GET /api/v1/logs`, `POST /api/v1/simulation/inject-fault`, and `POST /api/v1/incidents/{id}/remediate`.

---

### Issue 2: Fault Injection is Wiped Out If Ingestion Stage Precedes Validation
- **Severity:** `HIGH`
- **File:** [`scripts/fault-injection/inject_fault.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/scripts/fault-injection/inject_fault.py#L12-L75) & [`airflow/dags/pipeline_dag_starter.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/airflow/dags/pipeline_dag_starter.py#L264-L347)
- **Problem:** `inject_fault.py` modifies files in `data/staging/` (`stg_orders_...csv`, `stg_products.csv`). When a standard Airflow DAG run begins, tasks `ingest_dimensions`, `ingest_orders`, and `ingest_events` read from `data/raw/` and copy clean files into `data/staging/`, immediately overwriting the injected fault.
- **Evidence:** In `pipeline_dag_starter.py`, line 274: `pd.read_csv(cust_raw).to_csv(cust_stg, index=False)` and line 304: `orders_df.to_csv(stg_path, index=False)`.
- **Impact:** If someone runs `python scripts/fault-injection/inject_fault.py` and triggers the DAG from the Airflow UI, the DAG passes with green status because ingestion overwrote the corrupted staging file before `validate_schema` or `validate_quality` could inspect it.
- **Recommended Fix:** Either have `inject_fault.py` modify `data/raw/` batch files (or a dedicated `data/raw_corrupted/` path), or add an ingestion bypass flag when evaluating staged fault injection.

---

### Issue 3: Airflow Failure Callback Does Not Trigger Auto-Remediation or Task Resumption
- **Severity:** `HIGH`
- **File:** [`airflow/dags/pipeline_dag_starter.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/airflow/dags/pipeline_dag_starter.py#L147-L262)
- **Problem:** When an Airflow task fails, `on_task_failure(context)` creates the incident report JSON and calls `LLMDiagnosticAdapter`. However, it does **not** call `RemediationExecutor` or `RemediationVerifier`, nor does it clear/resume downstream Airflow tasks.
- **Evidence:** Lines 200–262 in `pipeline_dag_starter.py` instantiate `LLMDiagnosticAdapter`, diagnose the incident, and write JSON/MD files to disk. The auto-remediation execution and retry logic only exists in `scripts/test_pipeline_5_cases.py` (lines 260–280).
- **Impact:** In a real Airflow deployment, self-healing cannot complete automatically without external intervention; the Airflow DAG remains in a `FAILED` state.
- **Recommended Fix:** In `on_task_failure`, if `diag_report.action == "AUTO_FIX"`, invoke `RemediationExecutor` and `RemediationVerifier`, and optionally use Airflow's internal task instance retry or trigger a DAG run re-trigger via Airflow REST API / CLI.

---

### Issue 4: Backend Endpoint `/api/v1/pipeline/runs` Returns Hardcoded Static Mock Data
- **Severity:** `MEDIUM`
- **File:** [`apps/backend/main.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/backend/main.py#L81-L88)
- **Problem:** The pipeline runs endpoint returns 3 static fake runs (`run-001`, `run-002`, `run-003`) instead of inspecting actual execution logs or status files in `data/incidents/status/`.
- **Evidence:**
  ```python
  @app.get("/api/v1/pipeline/runs")
  def get_pipeline_runs():
      return [
          {"run_id": "run-001", "execution_date": "2026-06-01", "status": "SUCCESS", "duration_seconds": 12.4},
          {"run_id": "run-002", "execution_date": "2026-05-31", "status": "SUCCESS", "duration_seconds": 11.8},
          {"run_id": "run-003", "execution_date": "2026-05-30", "status": "FAILED", "duration_seconds": 4.2},
      ]
  ```
- **Impact:** Backend API consumers receive static mock data for run history.
- **Recommended Fix:** Read history dynamically from `data/incidents/status/status_*.json` or parse Airflow log metadata.

---

### Issue 5: Missing Dependency `google-generativeai` in Agent Requirements
- **Severity:** `MEDIUM`
- **File:** [`apps/agent/requirements.txt`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/requirements.txt) & [`apps/agent/agent/diagnosis/llm_adapter.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/diagnosis/llm_adapter.py#L80)
- **Problem:** `llm_adapter.py` imports `import google.generativeai as genai` when `GEMINI_API_KEY` is present. However, `google-generativeai` is omitted from `apps/agent/requirements.txt`.
- **Evidence:** `apps/agent/requirements.txt` contains only: `pandas>=2.0.0`, `pydantic>=2.0.0`, `pytest>=7.0.0`, `google-cloud-bigquery>=3.10.0`.
- **Impact:** If a user configures `GEMINI_API_KEY` in `.env` without separately installing `google-generativeai`, the Gemini branch throws an unhandled `ImportError` inside the try/except block and silently drops back to the rule-based engine.
- **Recommended Fix:** Add `google-generativeai>=0.7.0` and `pyyaml>=6.0` to `apps/agent/requirements.txt`.

---

### Issue 6: Insecure Wildcard CORS Configuration with Credentials
- **Severity:** `MEDIUM`
- **File:** [`apps/backend/main.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/backend/main.py#L27-L33)
- **Problem:** FastAPI CORS middleware is configured with `allow_origins=["*"]` combined with `allow_credentials=True`.
- **Evidence:**
  ```python
  app.add_middleware(
      CORSMiddleware,
      allow_origins=["*"],
      allow_credentials=True,
      allow_methods=["*"],
      allow_headers=["*"],
  )
  ```
- **Impact:** According to W3C CORS specifications, browsers will block credentialed cross-origin requests when `Access-Control-Allow-Origin` is `*`.
- **Recommended Fix:** Set `allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"]` or load allowed origins from environment configuration.

---

### Issue 7: Inaccurate Documentation References to Non-Existent Adapter Path
- **Severity:** `LOW`
- **File:** [`docs/DEMO_READINESS.md`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/docs/DEMO_READINESS.md#L175)
- **Problem:** Documentation claims BigQuery adapters exist at `apps/agent/agent/adapters/bigquery_adapter.py`. This directory and file do not exist; the actual implementation is in `apps/agent/agent/tools/bq_tool.py`.
- **Evidence:** `apps/agent/agent/adapters/` directory is not present in the workspace.
- **Impact:** Confuses engineers reviewing documentation against source files.
- **Recommended Fix:** Update doc references to point to `apps/agent/agent/tools/bq_tool.py`.

---

## 3. AI/LLM Audit

### Complete Flow Trace:
$$\text{FAILURE} \longrightarrow \text{INCIDENT} \longrightarrow \text{AI DIAGNOSIS} \longrightarrow \text{POLICY GATE} \longrightarrow \text{AUTO\_FIX / ESCALATE} \longrightarrow \text{REMEDIATION} \longrightarrow \text{VERIFICATION} \longrightarrow \text{RESOLVED}$$

| Step in Flow | Executing Code / File | Real vs Mocked / Simulated | Notes & Limitations |
| :--- | :--- | :--- | :--- |
| **1. Trigger / Hook** | [`airflow/dags/pipeline_dag_starter.py:on_task_failure`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/airflow/dags/pipeline_dag_starter.py#L147) | **REAL** | Extracts exception message, determines category, generates incident ID. |
| **2. AI Adapter Selection** | [`apps/agent/agent/diagnosis/llm_adapter.py:analyze_incident`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/diagnosis/llm_adapter.py#L23) | **REAL** | Tiers: 1) Local Ollama, 2) Gemini API, 3) Rule-Based Fallback. |
| **3. Ollama Integration** | [`apps/agent/agent/diagnosis/ollama_adapter.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/diagnosis/ollama_adapter.py#L48) | **REAL** | Queries `http://localhost:11434/api/tags` and `/api/generate` with structured JSON output. |
| **4. Gemini Integration** | [`apps/agent/agent/diagnosis/llm_adapter.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/diagnosis/llm_adapter.py#L80) | **REAL** | Configures `gemini-1.5-flash` with diagnostic prompt when `GEMINI_API_KEY` is provided. |
| **5. Rule-Based Fallback** | [`apps/agent/agent/diagnosis/engine.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/diagnosis/engine.py#L40) | **DETERMINISTIC FALLBACK** | Used when Ollama and Gemini are offline/unconfigured. |
| **6. Confidence Scoring** | `engine.py` (line 68, 78, 88, 98, 108, 118) | **HARDCODED in Fallback** | Hardcoded: `0.95` (Duplicates), `0.92` (Schema), `0.89` (Nulls), `0.88` (Ref), `0.90` (Volume Drop), `0.91` (Staleness). (Dynamic when LLM active). |
| **7. Blast Radius** | `engine.py` (lines 69, 79, 89, 99, 109) | **HARDCODED in Fallback** | String template mapping per category. (Dynamic when LLM active). |
| **8. Recommended Action** | `engine.py` / `llm_adapter.py` / `policy_gate.py` | **RULE-ENFORCED** | Generates proposed action; Policy Gate strictly governs execution. |
| **9. Policy Gate** | [`apps/agent/agent/policies/policy_gate.py:evaluate`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/policies/policy_gate.py#L21) | **REAL** | Asserts `confidence >= 0.85`, `risk == LOW`, and `idempotent == True`. |
| **10. Remediation Execution** | [`apps/agent/agent/remediation/executor.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/remediation/executor.py#L33) | **REAL** | Real pandas deduplication (`drop_duplicates`) and row quarantine. |
| **11. Post-Fix Verification** | [`apps/agent/agent/verification/verifier.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/verification/verifier.py#L21) | **REAL** | Inspects post-fix staging file on disk, asserts `duplicate_count == 0` and `rows > 0`. |
| **12. Incident Persistence** | [`apps/agent/agent/models/incident.py:save`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/apps/agent/agent/models/incident.py#L52) | **REAL** | Writes real JSON and Markdown reports to `data/incidents/reports/`. |

---

## 4. Airflow Audit

- **DAG Structure:**
  - DAG ID: `self_healing_pipeline`
  - Schedule: `0 2 * * *` (Daily 02:00 UTC)
  - Tasks (8 total): `ingest_dimensions`, `ingest_orders`, `ingest_events`, `validate_schema`, `validate_quality`, `transform_data`, `load_data`, `agent_monitoring`
  - TaskGroups (5 total): `ingestion`, `validation`, `transformation`, `load`, `monitoring`
  - Explicit Dependencies: Ingestion (3 parallel tasks) $\rightarrow$ `validate_schema` $\rightarrow$ `validate_quality` $\rightarrow$ `transform_data` $\rightarrow$ `load_data` $\rightarrow$ `agent_monitoring`
- **Windows / Standalone Compatibility Handling:**
  - `pipeline_dag_starter.py` contains safe workarounds for Windows environment execution:
    - Mocks missing Unix `fcntl` module.
    - Replaces local Windows drive-letter SQLite strings (`C:\...`) with POSIX-safe connection strings (`sqlite:////tmp/airflow.db`).
    - Provides fallback `MockDAG`, `MockTaskGroup`, and `MockOperator` classes if Airflow is imported in a local environment lacking native Airflow binaries.
- **Airflow Retries & Callbacks:**
  - Default retries: `retries: 2`, `retry_delay: 2 minutes`.
  - Failure callback: `on_task_failure` assigned to `default_args` and attached to all 8 tasks.
- **Airflow Audit Findings:**
  - `verify_airflow_dag.py` and `check_airflow_health.py` execute cleanly and validate complete DAG topology.
  - DAG execution on Windows produces standard Airflow runtime warnings regarding non-POSIX operating system and log symlinking.

---

## 5. Data Quality Audit

Data quality rules in `airflow/config/pipeline_config.yaml` and `pipeline_dag_starter.py` enforce strict operational assertions:

| Validation Dimension | Target Dataset | Configured Assertion | Enforcement Code |
| :--- | :--- | :--- | :--- |
| **Schema Structure** | `customers`, `products`, `orders`, `events` | Strict column presence, names, and types (`string`, `float`, `integer`, `timestamp`, `date`) | `validate_schema` $\rightarrow$ `check_dataframe_schema()` |
| **Volume Bounds** | `orders` | $240 \le \text{row count} \le 360$ (Nominal 300 $\pm 20\%$) | `validate_quality` |
| **Volume Bounds** | `events` | $900 \le \text{row count} \le 1500$ (Nominal 1200) | `validate_quality` |
| **Volume Bounds** | `customers` | $480 \le \text{row count} \le 520$ (Nominal 500) | `validate_quality` |
| **Volume Bounds** | `products` | $55 \le \text{row count} \le 65$ (Nominal 60) | `validate_quality` |
| **Null Tolerance** | `orders.order_total` | Null rate $\le 2.0\%$ | `validate_quality` |
| **Null Tolerance** | `orders.customer_id` | Null rate $== 0.0\%$ (Zero tolerance) | `validate_quality` |
| **Primary Key Uniqueness** | `orders.order_id`, `events.event_id` | 0 Duplicate Primary Keys | `validate_quality` |
| **Referential Integrity** | `orders.customer_id` $\rightarrow$ `customers.customer_id` | 0 Orphan Foreign Keys | `validate_quality` |
| **Referential Integrity** | `orders.product_id` $\rightarrow$ `products.product_id` | 0 Orphan Foreign Keys | `validate_quality` |
| **Freshness SLA** | `orders` | Delay $\le 26$ Hours | `validate_quality` |
| **Freshness SLA** | `events` | Delay $\le 6$ Hours | `validate_quality` |

**Data Quality Finding:** Every rule listed in `pipeline_config.yaml` is actively implemented with precise error messaging.

---

## 6. Fault Injection Audit

Audit of all 6 supported fault injection scenarios in [`scripts/fault-injection/inject_fault.py`](file:///c:/Users/samri/OneDrive/Documents/Internship%201/scripts/fault-injection/inject_fault.py):

| Fault Name | Physical Modification | Detected By | Failure Category | Policy Gate Action | Remediation Executed | Post-Fix Verification |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`duplicate_ingestion`** | Duplicates 50 rows in `stg_orders` (total: 350) | `validate_quality` | `DUPLICATE_INGESTION` | **`AUTO_FIX`** | `RemediationExecutor.deduplicate_dataset()` | `RemediationVerifier`: 300 clean rows, 0 dupes $\rightarrow$ `PASSED` |
| **`schema_drift`** | Converts `products.price` float to string (`"$14.99"`) | `validate_schema` | `SCHEMA_DRIFT` | **`ESCALATE`** | None (Safe Halt) | Incident status `ESCALATED` $\rightarrow$ Queued for human review |
| **`null_spike`** | Injects 15% NULLs into `customer_id` | `validate_quality` | `NULL_SPIKE` | **`ESCALATE`** | None (Safe Halt) | Incident status `ESCALATED` $\rightarrow$ Queued for human review |
| **`volume_drop`** | Truncates orders to 120 rows ($< 240$ min) | `validate_quality` | `VOLUME_ANOMALY_DROP` | **`ESCALATE`** | None (Safe Halt) | Incident status `ESCALATED` $\rightarrow$ Queued for human review |
| **`referential_break`**| Replaces 10 `customer_id` with `CUST_ORPHAN_999` | `validate_quality` | `REFERENTIAL_BREAK` | **`ESCALATE`** | None (Safe Halt) | Incident status `ESCALATED` $\rightarrow$ Queued for human review |
| **`staleness`** | Sets `order_ts` to `"2026-01-01"` (30 days stale) | `validate_quality` | `STALENESS` | **`ESCALATE`** | None (Safe Halt) | Incident status `ESCALATED` $\rightarrow$ Queued for human review |

---

## 7. Backend Audit

- **Framework:** FastAPI (`apps/backend/main.py`) running on port `8000`.
- **Endpoint Inventory:**
  - `GET /health` $\rightarrow$ Returns service status, version, and data directory (`REAL`).
  - `GET /api/v1/pipeline/status` $\rightarrow$ Reads latest `data/incidents/status/status_*.json` (`REAL with fallback`).
  - `GET /api/v1/pipeline/runs` $\rightarrow$ Returns static JSON array (`HARDCODED`).
  - `GET /api/v1/pipeline/summary` $\rightarrow$ Calculates auto-fixes and escalations from incident files (`HYBRID: dynamic counts, static timestamps/run ID`).
  - `GET /api/v1/incidents` $\rightarrow$ Returns all incident JSON files on disk (`REAL`).
  - `GET /api/v1/incidents/{incident_id}` $\rightarrow$ Returns specific incident report (`REAL`).
  - `GET /api/v1/data/profiles` $\rightarrow$ Reads profiling JSON files from `data/incidents/profiles/` (`REAL`).
  - `POST /api/v1/incidents/{incident_id}/remediate` $\rightarrow$ Triggers real `RemediationTool` and `VerificationTool` (`REAL`).
  - `POST /api/v1/simulation/inject-fault` $\rightarrow$ Runs `inject_fault.py` via subprocess and calls AI adapter (`REAL`).
  - `POST /api/v1/simulation/run-scenario` $\rightarrow$ Runs complete 5-case pipeline runner (`REAL`).
  - `GET /api/v1/logs` & `GET /api/v1/logs/{filename}` $\rightarrow$ Reads and streams real logs from `logs/` directory (`REAL`).

---

## 8. Frontend Audit

- **Framework:** React 19.2.8 + Vite 8.2.0 + TypeScript 6.0.2 + Lucide Icons.
- **Build Status:** Compiles cleanly with `npm run build` in 2.29 seconds (dist size: 250 KB JS, 24 KB CSS).
- **Linter Status:** `oxlint` reports 0 errors and 3 warnings (`react(purity)` on `Math.random()`, `react(set-state-in-effect)`, `react-hooks(exhaustive-deps)`).
- **Aesthetics & UI Design:**
  - Modern, dark-mode/light-mode toggled interface with custom CSS variables.
  - Interactive SVG connection paths between DAG nodes.
  - Interactive Healing Flow Inspection cards (AI Diagnosis, Policy Gate Safety, Verification & Recovery).
  - Telemetry drawer with log console, profiling tables, and remediation audit history.
- **Frontend Disconnect Finding:**
  - The UI is currently a standalone interactive presentation mockup that manages state internally in React. It does not communicate over the network with the FastAPI backend.

---

## 9. Docker/GCP Audit

### Docker Configuration (`docker-compose.yml`):
- **Services:** `postgres:15`, `airflow-webserver`, `airflow-scheduler`, `airflow-init`.
- **Airflow Version:** `apache/airflow:2.9.3-python3.11`.
- **Executor:** `LocalExecutor` with PostgreSQL database backend.
- **Volume Mounts:** `./airflow/dags`, `./airflow/logs`, `./airflow/plugins`, `./airflow/config`, `./data`, `./incidents`.
- **Gaps:**
  - FastAPI backend and React frontend are not containerized in `docker-compose.yml`.
  - `_PIP_ADDITIONAL_REQUIREMENTS` only specifies `apache-airflow-providers-google pandas`. Missing `pyyaml`, `pydantic`, `google-generativeai`.

### GCP & BigQuery Integration:
- **Provisioning Script:** `infrastructure/gcp/setup_intern.sh` creates service account `pipeline-intern-<handle>`, dataset `pipeline_intern_<handle>`, and grants least-privilege roles `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`.
- **BigQuery Target Schemas:** Defined in `pipeline_dag_starter.py` (`dim_customers`, `dim_products`, `fct_orders`, `fct_events`).
- **Partitioning & Clustering:** `fct_orders` and `fct_events` partition on `order_ts` / `event_ts` (DAY) and cluster on `customer_id`.
- **Fallback Behavior:** If GCP service account credentials are not supplied, the pipeline gracefully logs `BLOCKED_GCP_CREDENTIALS_PENDING` and completes all local data processing without crashing.

---

## 10. Security Audit

- **API Keys & Credentials:**
  - No active Google API keys, private keys, or passwords are committed to version control.
  - `.gitignore` properly excludes `*gcp-service-account*.json`, `.env`, and virtual environment directories.
  - `.env` contains placeholder templates.
- **Subprocess Execution:**
  - Subprocess calls in `main.py` use explicit argument lists with `sys.executable` (no shell injection vulnerabilities via `shell=True`).
- **Path Traversal Protection:**
  - `main.py` uses `os.path.basename(filename)` on log file retrieval requests, preventing `../` path traversal attacks.
- **CORS Configuration:**
  - `main.py` uses `allow_origins=["*"]` with `allow_credentials=True`. Needs refinement to specific host origins.

---

## 11. Testing Results

All tests were executed directly in the project environment using `pytest -v --tb=short`.

```
============================= test session starts =============================
platform win32 -- Python 3.12.7, pytest-7.4.4, pluggy-1.0.0
rootdir: C:\Users\samri\OneDrive\Documents\Internship 1
configfile: pytest.ini
testpaths: tests
collected 36 items

tests/test_agent_diagnosis.py::test_incident_report_creation_and_save PASSED [  2%]
tests/test_agent_diagnosis.py::test_policy_gate_auto_fix_vs_escalate PASSED [  5%]
tests/test_agent_diagnosis.py::test_diagnostic_engine_reasoning PASSED   [  8%]
tests/test_agent_diagnosis.py::test_remediation_and_verifier PASSED      [ 11%]
tests/test_ai_intelligence_suite.py::test_scenario_1_duplicate_ingestion_diagnosis PASSED [ 13%]
tests/test_ai_intelligence_suite.py::test_scenario_2_schema_drift_diagnosis PASSED [ 16%]
tests/test_ai_intelligence_suite.py::test_scenario_3_null_spike_diagnosis PASSED [ 19%]
tests/test_ai_intelligence_suite.py::test_scenario_4_volume_drop_diagnosis PASSED [ 22%]
tests/test_ai_intelligence_suite.py::test_scenario_5_referential_break_diagnosis PASSED [ 25%]
tests/test_ai_intelligence_suite.py::test_policy_gate_confidence_threshold_enforcement PASSED [ 27%]
tests/test_ai_intelligence_suite.py::test_ollama_adapter_fallback PASSED [ 30%]
tests/test_ai_intelligence_suite.py::test_error_handling_malformed_evidence PASSED [ 33%]
tests/test_extended_pipeline.py::test_scenario_a_healthy_pipeline PASSED [ 36%]
tests/test_extended_pipeline.py::test_scenario_b_duplicate_ingestion PASSED [ 38%]
tests/test_extended_pipeline.py::test_scenario_c_schema_drift PASSED     [ 41%]
tests/test_extended_pipeline.py::test_scenario_d_null_spike PASSED       [ 44%]
tests/test_extended_pipeline.py::test_scenario_e_volume_drop PASSED      [ 47%]
tests/test_extended_pipeline.py::test_scenario_f_referential_break PASSED [ 50%]
tests/test_extended_pipeline.py::test_scenario_g_staleness PASSED        [ 52%]
tests/test_extended_pipeline.py::test_data_profiler PASSED               [ 55%]
tests/test_local_pipeline.py::test_valid_pipeline_run PASSED             [ 58%]
tests/test_local_pipeline.py::test_invalid_schema_missing_column PASSED  [ 61%]
tests/test_local_pipeline.py::test_invalid_schema_type_mismatch PASSED   [ 63%]
tests/test_local_pipeline.py::test_duplicate_primary_keys PASSED         [ 66%]
tests/test_local_pipeline.py::test_abnormal_volume_low_rows PASSED       [ 69%]
tests/test_local_pipeline.py::test_null_spike_violation PASSED           [ 72%]
tests/test_local_pipeline.py::test_broken_foreign_key_referential_integrity PASSED [ 75%]
tests/test_local_pipeline.py::test_staleness_sla_violation PASSED        [ 77%]
tests/test_local_pipeline.py::test_events_staleness_sla_violation PASSED [ 80%]
tests/test_local_pipeline.py::test_dag_task_structure PASSED             [ 83%]
tests/fault-injection/test_fault_injection.py::test_fault_injection_scenarios PASSED [ 86%]
tests/integration/test_backend_api.py::test_health_endpoint PASSED       [ 88%]
tests/integration/test_backend_api.py::test_pipeline_status_endpoint PASSED [ 91%]
tests/integration/test_backend_api.py::test_incidents_endpoints PASSED   [ 94%]
tests/integration/test_backend_api.py::test_remediate_endpoint PASSED    [ 97%]
tests/integration/test_demo_scenarios.py::test_manager_demo_scenarios_e2e PASSED [100%]

======================= 36 passed, 1 warning in 44.62s ========================
```

### Test Summary:
- **Total Tests:** 36
- **Passed:** 36 (100% of executed tests)
- **Failed:** 0
- **Errors:** 0
- **Skipped:** 0
- **Warnings:** 1 (StarletteDeprecationWarning on `TestClient` import)
- **Coverage Tool:** `pytest-cov` / `coverage` module is not installed in the local environment; therefore, exact statement/branch coverage percentage cannot be mathematically asserted without installing coverage packages.

---

## 12. Demo Readiness

### Can you safely demonstrate the end-to-end flow?

#### Path A: Live Python Test Harness Demonstration (100% READY)
Running `python scripts/test_pipeline_5_cases.py` demonstrates the full end-to-end self-healing flow live:
1. **Healthy Pipeline:** Stages 1–5 execute cleanly on nominal baseline.
2. **Schema Drift:** Ingestion $\rightarrow$ `validate_schema` catches string in price $\rightarrow$ Incident created $\rightarrow$ AI Diagnosis $\rightarrow$ Policy Gate: `ESCALATE` $\rightarrow$ Pipeline halts safely.
3. **Null Spike:** Ingestion $\rightarrow$ `validate_quality` catches 15% nulls $\rightarrow$ Incident created $\rightarrow$ Policy Gate: `ESCALATE` $\rightarrow$ Pipeline halts safely.
4. **Duplicate Ingestion:** Ingestion $\rightarrow$ 50 duplicates detected in `validate_quality` $\rightarrow$ Incident created $\rightarrow$ AI Diagnosis $\rightarrow$ Policy Gate: **`AUTO_FIX`** $\rightarrow$ `RemediationExecutor` deduplicates (50 removed, 300 clean rows) $\rightarrow$ `RemediationVerifier` asserts 0 duplicates remaining $\rightarrow$ Pipeline retries and **completes successfully (AUTO-HEALED)** $\rightarrow$ Transformation & Load succeed.
5. **Volume Drop:** Ingestion $\rightarrow$ Row count 120 ($< 240$) $\rightarrow$ Policy Gate: `ESCALATE` $\rightarrow$ Pipeline halts safely.

#### Path B: React Dashboard Demonstration (REQUIRES CAUTION)
- The React UI can be visually showcased with full interactivity (Dark mode, Flow diagrams, Inspector cards, Fault triggers), **BUT the presenter must know that the buttons trigger simulated React state updates rather than live FastAPI calls**.
- If asked by a manager: *"Is this UI calling your Python backend?"*, you must honestly explain that the React dashboard has client-side simulation mocks and the backend API is verified via OpenAPI Swagger at `http://localhost:8000/docs`.

---

## 13. Hardcoded / Mocked / Simulated Features

| Feature / Metric | Location in Codebase | Actual Behavior |
| :--- | :--- | :--- |
| **Frontend Network Layer** | `apps/frontend/src/App.tsx` | All API responses, DAG executions, and fault triggers are simulated with `setTimeout` and local `useState`. |
| **Frontend Metric Stats** | `apps/frontend/src/App.tsx:metrics` | `successfulRuns: 18`, `failedRuns: 2`, `storage: 14.8 MB`, `Success Rate: 98.2%` are static strings. |
| **Frontend Run Summary** | `apps/frontend/src/App.tsx:runSummary` | `runId: run-20260601-0200`, `recordsProcessed: 2060`, `duration: 135s` are static strings. |
| **Backend Pipeline Runs** | `apps/backend/main.py:get_pipeline_runs` | Returns hardcoded array of 3 runs (`run-001`, `run-002`, `run-003`). |
| **AI Confidence in Fallback** | `apps/agent/agent/diagnosis/engine.py` | Constants `0.95`, `0.92`, `0.89`, `0.88`, `0.90`, `0.91`, `0.75` used when Ollama/Gemini are unconfigured. |
| **AI Recovery Time** | `apps/agent/agent/diagnosis/engine.py` | Hardcoded `recovery_time = 3.5` seconds. |
| **Airflow Windows Imports** | `pipeline_dag_starter.py` / `check_airflow_health.py` | Mocks `fcntl` and Airflow classes if imported in Windows environments without native Airflow. |

---

## 14. Missing Features

1. **Frontend-to-Backend HTTP Client:** Missing `fetch`/`axios` integration to link React UI to FastAPI backend endpoints.
2. **Native Airflow Self-Healing Resume:** Missing Airflow TaskInstance clear / trigger call in `on_task_failure` callback to auto-resume the DAG inside the Airflow scheduler after `AUTO_FIX`.
3. **Docker Compose Backend Services:** Missing `backend` (FastAPI) and `frontend` (React) service definitions in `docker-compose.yml`.
4. **Test Coverage Package:** Missing `pytest-cov` / `coverage` in development dependencies.
5. **Raw Data Fault Injector:** Fault injector modifies staging copies rather than raw landing sources.

---

## 15. Recommended Improvements

### Must Fix Before Demo:
1. **Be 100% Prepared on UI vs. CLI Truth:** If presenting the React dashboard, clarify that the UI demonstrates the intended UX workflow while the live technical engine is verified via `scripts/test_pipeline_5_cases.py` and FastAPI Swagger docs.
2. **Fix Documentation Test Count:** Update `README.md` to state **36 passing tests** (currently states 27).
3. **Update Doc File Paths:** Correct non-existent path `apps/agent/agent/adapters/bigquery_adapter.py` in `docs/DEMO_READINESS.md` to `apps/agent/agent/tools/bq_tool.py`.
4. **Add `pyyaml` & `google-generativeai` to `apps/agent/requirements.txt`**.

### Should Fix:
5. **Connect React Frontend to FastAPI Backend:** Replace simulated `setTimeout` state handlers in `App.tsx` with standard `fetch()` calls to `http://localhost:8000/api/v1/...`.
6. **Dynamic Backend Pipeline Runs:** Replace hardcoded `run-001`, `run-002`, `run-003` in `main.py:get_pipeline_runs` with dynamic reads from `data/incidents/status/`.
7. **Refine CORS Origins:** Replace wildcard `allow_origins=["*"]` with explicit frontend origins `["http://localhost:5173", "http://127.0.0.1:5173"]`.
8. **Add Container Definitions:** Add `backend` and `frontend` containers to `docker-compose.yml` for single-command `docker compose up` orchestration.

### Nice to Have:
9. **Native Airflow Re-trigger Operator:** Add an Airflow operator that re-executes failed tasks automatically after an `AUTO_FIX` incident resolution.
10. **Install Test Coverage Tooling:** Add `pytest-cov` to measure and report exact line coverage metrics.

---

## 16. Final Score

| Area | Score (out of 10) | Evaluation Justification |
| :--- | :---: | :--- |
| **Data Engineering** | **8.5 / 10** | Solid dimensional data modeling (`dim_customers`, `dim_products`, `fct_orders`, `fct_events`), multi-source ingestion (CSV + JSONL), explicit staging, partition/cluster design, and comprehensive quality assertions. |
| **Airflow** | **8.0 / 10** | Clean DAG design with 5 TaskGroups and 8 tasks. Excellent Windows compatibility shims. Minor deduction: `on_task_failure` callback creates reports but does not automatically re-trigger failed tasks within native Airflow scheduler. |
| **AI/ML** | **8.0 / 10** | Well-architected tiered hybrid diagnostic system (Ollama $\rightarrow$ Gemini $\rightarrow$ Rule-based). Ollama request/response logging and error fallback work properly. When offline, fallback relies on static template scores. |
| **Self-Healing** | **9.0 / 10** | Outstanding safety design. The Policy Gate enforces strict risk boundaries (`AUTO_FIX` only for idempotent deduplication with $\ge 85\%$ confidence; `ESCALATE` for schema drift, null spikes, and volume drops). Verification loop actively asserts post-remediation data health. |
| **Backend** | **7.5 / 10** | FastAPI app provides comprehensive endpoints for incidents, health, logs, and simulation scenarios. Deductions for hardcoded runs in `/api/v1/pipeline/runs` and wildcard CORS credentials. |
| **Frontend** | **6.5 / 10** | Visually impressive, modern design with dark/light themes, SVG DAG flows, and inspector cards. Significant deduction because it is 100% simulated client-side and not yet wired to backend REST endpoints. |
| **Testing** | **8.5 / 10** | 36/36 tests passing across 7 test files covering unit, intelligence, extended pipeline, fault injection, and demo scenarios. Minor deduction for uninstalled coverage measurement package. |
| **Production Readiness** | **7.0 / 10** | Standalone Python execution and test harness are rock solid. Needs full Docker containerization of backend/frontend and live Airflow scheduler task resumption for true production deployment. |
| **Demo Readiness** | **8.5 / 10** | The CLI test suite (`python scripts/test_pipeline_5_cases.py`) and FastAPI OpenAPI documentation provide an unbeatable, 100% reproducible live manager demonstration. |

---

### TOP 10 THINGS TO FIX BEFORE SHOWING MY PROJECT TO MY MANAGER

```
 1. Connect React Frontend to FastAPI Backend Endpoints
    - Replace client-side setTimeout simulation in App.tsx with live fetch() calls to /api/v1/incidents,
      /api/v1/pipeline/status, /api/v1/logs, and /api/v1/simulation/inject-fault.

 2. Re-architect Fault Injection to Modify Raw Files (or Bypass Ingestion on Staged Faults)
    - Ensure running the full DAG does not immediately overwrite injected staging faults with clean raw files.

 3. Add Dynamic Run History to Backend GET /api/v1/pipeline/runs
    - Replace hardcoded [{"run_id": "run-001"...}] with real data from data/incidents/status/status_*.json.

 4. Add Missing Dependencies to apps/agent/requirements.txt
    - Add 'google-generativeai>=0.7.0' and 'pyyaml>=6.0' to prevent silent import failures when GEMINI_API_KEY is used.

 5. Sync Documentation Test Counts
    - Update README.md to state 36 passing tests instead of 27.

 6. Correct Documentation File References
    - Fix non-existent path 'apps/agent/agent/adapters/bigquery_adapter.py' in docs/DEMO_READINESS.md
      to 'apps/agent/agent/tools/bq_tool.py'.

 7. Fix FastAPI CORS Security Settings
    - Change allow_origins=["*"] with allow_credentials=True to specific localhost origins in apps/backend/main.py.

 8. Add Backend & Frontend Services to docker-compose.yml
    - Include FastAPI backend (port 8000) and React frontend (port 5173) in docker-compose.yml for single-command startup.

 9. Wire Airflow on_task_failure to Auto-Remediation Execution
    - Allow the Airflow failure callback to invoke RemediationExecutor and clear the task instance when AUTO_FIX is approved.

10. Install & Run pytest-cov to Document Verified Line Coverage
    - Install pytest-cov to generate an official test coverage report for manager review.
```
