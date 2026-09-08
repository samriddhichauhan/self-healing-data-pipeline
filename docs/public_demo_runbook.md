# Public Demonstration Runbook (Presenter Script & Guide)

This runbook is written specifically for the presenter delivering a live demonstration of the **Self-Healing Data Pipeline** to project heads, technical managers, or external audiences.

---

## PART 1 — Start Infrastructure / Local Environment

- **COMMAND:** `python scripts/verify_airflow_dag.py`
- **WHERE:** Terminal 1 (Repository Root)
- **EXPECTED:** DAG verification report showing DAG ID `self_healing_pipeline`, 8 tasks, SLA config, and clean task dependencies.
- **SAY:** *"First, let's verify our Airflow DAG definition and task lineage graph."*
- **SHOW:** Terminal output showing `Lineage Graph Summary: INGESTION -> VALIDATION -> TRANSFORM -> LOAD -> AGENT MONITORING`.
- **DO NOT CLAIM:** Do not claim Airflow webserver is required for local execution; the pipeline engine runs natively or containerized.

---

## PART 2 — Start Backend API

- **COMMAND:** `python -m uvicorn apps.backend.main:app --reload --port 8000`
- **WHERE:** Terminal 1 (Repository Root)
- **EXPECTED:** `INFO: Application startup complete. Uvicorn running on http://127.0.0.1:8000`
- **SAY:** *"Next, we initialize our FastAPI backend service, which exposes real-time pipeline metrics, incidents, and central logs."*
- **SHOW:** Terminal 1 log output.
- **DO NOT CLAIM:** Do not claim backend uses external database servers; it uses lightweight file-system telemetry.

---

## PART 3 — Start Frontend React Dashboard

- **COMMAND:** `cd apps/frontend && npm run dev`
- **WHERE:** Terminal 2 (`apps/frontend` directory)
- **EXPECTED:** `VITE v8.2.2 ready in ... Local: http://localhost:5173/`
- **SAY:** *"Now we start our React dashboard frontend, giving data engineering teams real-time visibility into lineage and incident reports."*
- **SHOW:** Terminal 2 output, then open browser to `http://localhost:5173`.
- **DO NOT CLAIM:** Do not claim dashboard data is simulated; it reads actual backend incident JSON payloads.

---

## PART 4 — Verify Airflow Health

- **COMMAND:** `python scripts/check_airflow_health.py`
- **WHERE:** Terminal 3 (Repository Root)
- **EXPECTED:** `ALL CHECKS PASSED: Airflow DAG is clean, healthy, and working properly!`
- **SAY:** *"Let's perform an automated health check across all 8 DAG tasks."*
- **SHOW:** Terminal 3 output.
- **DO NOT CLAIM:** Do not claim zero warnings exist on Windows; point out runtime warnings are handled gracefully.

---

## PART 5 — Verify Healthy Data Processing

- **COMMAND:** `python scripts/data/generate_data.py --out ./data --start-date 2026-09-07 --days 1`
- **WHERE:** Terminal 3 (Repository Root)
- **EXPECTED:** `[2026-09-07] wrote 300 orders, 1200 events`
- **SAY:** *"Here we generate a clean 1-day batch dataset containing 500 customers, 60 products, 300 orders, and 1,200 clickstream events."*
- **SHOW:** Terminal 3 file output confirmation.
- **DO NOT CLAIM:** Do not claim data is production customer PII; it is synthetic e-commerce data.

---

## PART 6 — Show Pipeline Architecture

- **COMMAND:** Open `docs/DEMO_READINESS.md` or point to Dashboard Lineage UI.
- **WHERE:** Browser / Editor
- **EXPECTED:** 5-Stage ETL Lineage diagram.
- **SAY:** *"Our pipeline processes data through 5 strict stages: Ingestion, Schema Validation, Quality Validation, Transformation, and Load with Agent Monitoring."*
- **SHOW:** Pipeline stage diagram.
- **DO NOT CLAIM:** Do not claim stages can be bypassed; lineage dependencies are strictly enforced.

---

## PART 7 — Inject Duplicate Ingestion Fault

- **COMMAND:** `python scripts/fault-injection/inject_fault.py --fault duplicate_ingestion --execution-date 2026-06-01`
- **WHERE:** Terminal 3 (Repository Root)
- **EXPECTED:** `[FAULT INJECTION SUCCESS] Injected Duplicate Ingestion: Duplicated 50 records in staged orders (total: 350 rows).`
- **SAY:** *"Now let's simulate a common production failure: duplicate ingestion caused by an upstream retry."*
- **SHOW:** Fault injector CLI output.
- **DO NOT CLAIM:** Do not claim raw source files were modified; fault injection operates on staged batch copies.

---

## PART 8 — Run Pipeline Under Fault Condition

- **COMMAND:** `python scripts/test_pipeline_5_cases.py`
- **WHERE:** Terminal 3 (Repository Root)
- **EXPECTED:** Scenario 4 execution log in Terminal 3.
- **SAY:** *"We now execute the pipeline under the duplicate fault condition."*
- **SHOW:** Terminal 3 scrolling execution logs.
- **DO NOT CLAIM:** Do not claim execution speed is fake; processing executes live pandas/SQL operations.

---

## PART 9 — Show Validation Failure

- **COMMAND:** Observe log line under Stage 3 (`validate_quality`).
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `[X] Quality Validation failed: Found 50 duplicates for column 'order_id'`
- **SAY:** *"As expected, Stage 3 quality check detects the 50 duplicate order IDs and halts pipeline execution to prevent corrupting downstream analytics."*
- **SHOW:** Red `[X] Quality Validation failed` error snippet.
- **DO NOT CLAIM:** Do not claim validation logic was hardcoded; Great Expectations quality rules evaluated the dataset.

---

## PART 10 — Show Incident Creation

- **COMMAND:** `type incidents\status\status_2026-06-01.json`
- **WHERE:** Terminal 3 / File Viewer
- **EXPECTED:** JSON incident payload with `incident_id`, `category: DUPLICATE_INGESTION`, `status: FAILED`.
- **SAY:** *"An incident payload `INC-DUP-001` is automatically created with full failure context and metric evidence."*
- **SHOW:** Incident JSON structure.
- **DO NOT CLAIM:** Do not claim incident creation requires external logging services; it is generated by the failure callback hook.

---

## PART 11 — Show AI Diagnosis

- **COMMAND:** Observe AI Diagnosis section in `test_pipeline_5_cases.py` output.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `Hypothesis: Upstream retry or file re-transmission caused duplicate rows` | `Confidence: 0.95` | `Severity: MEDIUM` | `Blast Radius: 2/10`.
- **SAY:** *"Our Hybrid AI Diagnostic Engine analyzes log evidence and correctly identifies the root cause as an upstream retry with 95% confidence."*
- **SHOW:** AI Diagnosis terminal log snippet.
- **DO NOT CLAIM:** Do not claim a cloud LLM API call was made if running offline; state that the hybrid engine used local diagnostic reasoning.

---

## PART 12 — Show Policy Gate AUTO_FIX Decision

- **COMMAND:** Observe Policy Gate log line.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `[POLICY GATE ACTION] AUTO_FIX`
- **SAY:** *"The Policy Gate evaluates confidence (0.95 >= 0.85), risk (LOW), and idempotency (True). It approves AUTO_FIX."*
- **SHOW:** `[POLICY GATE ACTION] AUTO_FIX` line.
- **DO NOT CLAIM:** Do not claim the AI Engine executed the fix directly; Policy Gate acts as an explicit gatekeeper.

---

## PART 13 — Show Remediation Execution

- **COMMAND:** Observe Remediation log snippet.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `[REMEDIATION STATUS] SUCCESS — Removed 50 duplicates (Clean count: 300)`
- **SAY:** *"The Remediation Executor applies SQL window deduplication, cleanly removing the 50 duplicate records."*
- **SHOW:** `Removed 50 duplicates` log line.
- **DO NOT CLAIM:** Do not claim records were deleted permanently without backup; staged files are updated idempotently.

---

## PART 14 — Show Remediation Verification

- **COMMAND:** Observe Verification log snippet.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `[VERIFICATION STATUS] PASSED (Verified clean row count: 300)`
- **SAY:** *"Crucially, the Remediation Verifier queries the repaired dataset to confirm 0 duplicates remain before marking the incident resolved."*
- **SHOW:** `VERIFICATION STATUS: PASSED` snippet.
- **DO NOT CLAIM:** Do not claim verification is optional; incident status remains `FAILED` until verification passes.

---

## PART 15 — Show Resolved Incident & Pipeline Recovery

- **COMMAND:** Observe pipeline recovery completion lines.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `[OK] Quality Validation passed post-auto-remediation!` | `[OK] Data Transformation completed successfully.` | `Status: RESOLVED`
- **SAY:** *"The pipeline automatically resumes execution, completes transformation and load, and updates incident status to RESOLVED."*
- **SHOW:** Final pipeline stage summary.
- **DO NOT CLAIM:** Do not claim human intervention was needed for this scenario; duplicate ingestion auto-heals 100% end-to-end.

---

## PART 16 — Run Schema Drift Safety Demonstration

- **COMMAND:** Observe Scenario 2 in `test_pipeline_5_cases.py` output.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** Scenario 2 output showing schema drift fault.
- **SAY:** *"Now let's observe what happens during a dangerous failure: Schema Drift."*
- **SHOW:** Scenario 2 execution logs.
- **DO NOT CLAIM:** Do not claim schema drift auto-fixes; it is explicitly blocked for safety.

---

## PART 17 — Show ESCALATE Safety Decision

- **COMMAND:** Observe Policy Gate output for Scenario 2.
- **WHERE:** Terminal 3 Log
- **EXPECTED:** `[POLICY GATE ACTION] ESCALATE` | `[ESCALATED] Action ESCALATED to Data Engineering team. (Human review required — execution safe-stopped)`
- **SAY:** *"Because schema modifications carry HIGH risk, our Policy Gate denies auto-remediation and ESCALATES the issue for human review, keeping production data safe."*
- **SHOW:** `[POLICY GATE ACTION] ESCALATE` snippet.
- **DO NOT CLAIM:** Do not claim the pipeline crashed dangerously; it safe-stopped execution cleanly.
