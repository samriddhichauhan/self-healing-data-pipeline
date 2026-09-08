# Self-Healing Data Pipeline — Master Command Cheatsheet

This cheatsheet lists all validated presentation commands in exact execution order, grouped by terminal and application context.

---

## TERMINAL 1 — Backend & Infrastructure Environment

Navigate to repository root:
```powershell
cd "c:\Users\samri\OneDrive\Documents\Internship 1"
```

1. **Verify Airflow DAG Structure & Lineage:**
   ```powershell
   python scripts/verify_airflow_dag.py
   ```

2. **Start FastAPI Backend REST API Server:**
   ```powershell
   python -m uvicorn apps.backend.main:app --reload --port 8000
   ```

---

## TERMINAL 2 — Frontend React Application

Navigate to frontend directory:
```powershell
cd "c:\Users\samri\OneDrive\Documents\Internship 1\apps\frontend"
```

1. **Run Frontend Production Build Check:**
   ```powershell
   npm run build
   ```

2. **Start Development Web Server:**
   ```powershell
   npm run dev
   ```

---

## TERMINAL 3 — Data Generation, Testing & Fault Execution

Navigate to repository root:
```powershell
cd "c:\Users\samri\OneDrive\Documents\Internship 1"
```

1. **Generate Synthetic Demo Data (Date: 2026-09-07):**
   ```powershell
   python scripts/data/generate_data.py --out ./data --start-date 2026-09-07 --days 1
   ```

2. **Run Airflow Healthy Pipeline Diagnostic:**
   ```powershell
   python scripts/check_airflow_health.py
   ```

3. **Inject Duplicate Ingestion Fault (Manual Standalone Fault Test):**
   ```powershell
   python scripts/fault-injection/inject_fault.py --fault duplicate_ingestion --execution-date 2026-06-01
   ```

4. **Execute Full 5-Scenario Pipeline Test Harness (Main Live Demo Command):**
   ```powershell
   python scripts/test_pipeline_5_cases.py
   ```

5. **Execute Complete Pytest Test Suite:**
   ```powershell
   pytest
   ```

---

## BROWSER TAB 1 — React Pipeline Dashboard

- **URL:** `http://localhost:5173`
- **Features to Show:**
  - Real-time pipeline lineage execution status
  - Incident Feed (`INC-DUP-001`)
  - AI Root Cause, Confidence Score (`0.95`), Severity (`MEDIUM`), Blast Radius (`2/10`)
  - Policy Gate Decision (`AUTO_FIX` vs `ESCALATE`)
  - Verification Report Status (`PASSED`)

---

## BROWSER TAB 2 — Backend OpenAPI Interactive Specs

- **URL:** `http://localhost:8000/docs`
- **Endpoints to Show:**
  - `GET /health`
  - `GET /api/v1/pipeline/status`
  - `GET /api/v1/incidents`
  - `GET /api/v1/logs`
