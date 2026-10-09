"""
FastAPI Backend for the Self-Healing Data Pipeline.

Every endpoint here reads real state: Postgres (pipeline_runs, incidents,
quality_results, aggregated_metrics) or files actually written by the
Airflow tasks (profiles, logs). Nothing here is a hardcoded placeholder —
if the database is unreachable, endpoints return a real 503, not fake data.
"""
import os
import sys
import glob
import json
import subprocess
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.database import queries as db
from src.database.connection import get_connection
from src.remediation.executor import RemediationExecutor
from src.remediation.verifier import RemediationVerifier
from src.config import get_data_dir

app = FastAPI(title="Self-Healing Pipeline API", version="2.0.0")

_cors_origins = os.environ.get("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = get_data_dir()
LOGS_DIR = os.environ.get("PIPELINE_LOGS_DIR", os.path.join(PROJECT_ROOT, "logs"))

remediation_executor = RemediationExecutor(data_dir=DATA_DIR)
remediation_verifier = RemediationVerifier(data_dir=DATA_DIR)

AIRFLOW_API_BASE = os.environ.get("AIRFLOW_API_BASE", "http://airflow-webserver:8080/api/v1")
AIRFLOW_AUTH = (os.environ.get("AIRFLOW_USER", "airflow"), os.environ.get("AIRFLOW_PASSWORD", "airflow"))


def _db_error(e: Exception):
    raise HTTPException(status_code=503, detail=f"Pipeline database unavailable: {e}")


@app.get("/health")
def health_check():
    db_ok = True
    db_detail = "connected"
    try:
        conn = get_connection()
        conn.close()
    except Exception as e:
        db_ok = False
        db_detail = str(e)
    return {
        "status": "healthy" if db_ok else "degraded",
        "service": "Self-Healing Pipeline API",
        "version": "2.0.0",
        "data_dir": DATA_DIR,
        "database": {"connected": db_ok, "detail": db_detail},
    }


@app.get("/api/v1/pipeline/status")
def get_pipeline_status():
    try:
        latest = db.get_latest_pipeline_run()
    except Exception as e:
        _db_error(e)
    if not latest:
        return {"pipeline_status": "NO_RUNS_YET", "message": "No pipeline runs recorded yet. Trigger a DAG run first."}
    return latest


@app.get("/api/v1/pipeline/runs")
def get_pipeline_runs(limit: int = 20):
    try:
        return db.list_pipeline_runs(limit=limit)
    except Exception as e:
        _db_error(e)


@app.get("/api/v1/pipeline/runs/{run_id}")
def get_pipeline_run(run_id: str):
    try:
        run = db.get_pipeline_run(run_id)
    except Exception as e:
        _db_error(e)
    if not run:
        raise HTTPException(status_code=404, detail=f"Pipeline run '{run_id}' not found")
    try:
        run["quality_results"] = db.list_quality_results(run_id)
        run["aggregated_metrics"] = db.get_aggregated_metrics(run_id)
    except Exception:
        pass
    return run


@app.get("/api/v1/pipeline/summary")
def get_pipeline_summary():
    try:
        return db.get_pipeline_summary()
    except Exception as e:
        _db_error(e)


@app.get("/api/v1/incidents")
def list_incidents(limit: int = 50):
    try:
        return db.list_incidents(limit=limit)
    except Exception as e:
        _db_error(e)


@app.get("/api/v1/incidents/{incident_id}")
def get_incident(incident_id: str):
    try:
        incidents = db.list_incidents(limit=1000)
    except Exception as e:
        _db_error(e)
    for inc in incidents:
        if inc.get("incident_id") == incident_id:
            return inc
    raise HTTPException(status_code=404, detail="Incident not found")


@app.get("/api/v1/data/profiles")
def get_data_profiles():
    profiles_dir = os.path.join(DATA_DIR, "incidents", "profiles")
    if not os.path.exists(profiles_dir):
        return []
    profiles = []
    for pf in glob.glob(os.path.join(profiles_dir, "profile_*.json")):
        try:
            with open(pf, "r") as f:
                profiles.append(json.load(f))
        except Exception:
            continue
    return sorted(profiles, key=lambda p: p.get("timestamp", ""), reverse=True)


class RemediationRequest(BaseModel):
    dataset: Optional[str] = "orders"
    primary_key: Optional[str] = "order_id"
    execution_date: Optional[str] = "2026-06-01"


@app.post("/api/v1/incidents/{incident_id}/remediate")
def remediate_incident(incident_id: str, req: RemediationRequest):
    """Manually re-runs remediation + verification for an existing incident
    (e.g. a human approving an ESCALATED incident after review)."""
    fix_res = remediation_executor.deduplicate_dataset(req.dataset, req.primary_key, req.execution_date)
    verify_res = remediation_verifier.verify(req.dataset, req.primary_key, req.execution_date)

    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE incidents SET status = %s, remediation_status = %s, verification_status = %s, updated_at = now()
                WHERE incident_id = %s
                """,
                (
                    "REMEDIATED" if verify_res.get("verified") else "ESCALATED",
                    fix_res.get("status", "UNKNOWN"),
                    "PASSED" if verify_res.get("verified") else "FAILED",
                    incident_id,
                ),
            )
        conn.close()
    except Exception as e:
        _db_error(e)

    return {"incident_id": incident_id, "remediation": fix_res, "verification": verify_res}


class FaultInjectRequest(BaseModel):
    fault_type: str
    execution_date: Optional[str] = "2026-06-01"


@app.post("/api/v1/simulation/inject-fault")
def inject_fault_endpoint(req: FaultInjectRequest):
    """Runs the real fault-injection CLI against the staged dataset."""
    cmd = [
        sys.executable,
        os.path.join(PROJECT_ROOT, "scripts", "fault-injection", "inject_fault.py"),
        "--fault", req.fault_type,
        "--data-dir", DATA_DIR,
        "--execution-date", req.execution_date or "2026-06-01",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Fault injection failed: {res.stderr}")
    return {"status": "FAULT_INJECTED", "fault_type": req.fault_type, "cli_output": res.stdout.strip()}


class TriggerRequest(BaseModel):
    execution_date: Optional[str] = "2026-06-01"


@app.post("/api/v1/pipeline/trigger")
def trigger_dag_run(req: TriggerRequest):
    """Triggers a real Airflow DAG run via the Airflow REST API.

    Airflow rejects a second DagRun at a logical_date that already has one.
    The canonical demo date is fixed and may be triggered repeatedly, so the
    *time* portion is set to the real current wall-clock time to keep each
    trigger's logical_date unique — the date portion (all task callables'
    `ds`) stays pinned to the requested demo date regardless.
    """
    import httpx
    time_of_day = datetime.utcnow().strftime("%H:%M:%S")
    logical_date = f"{req.execution_date or '2026-06-01'}T{time_of_day}Z"
    try:
        resp = httpx.post(
            f"{AIRFLOW_API_BASE}/dags/self_healing_pipeline/dagRuns",
            auth=AIRFLOW_AUTH,
            json={"logical_date": logical_date},
            timeout=15.0,
        )
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not trigger Airflow DAG run: {e}")
    return resp.json()


@app.get("/api/v1/pipeline/runs/{run_id}/airflow-state")
def get_airflow_run_state(run_id: str):
    """Proxies Airflow's own task-instance states for a given DAG run."""
    import httpx
    try:
        resp = httpx.get(
            f"{AIRFLOW_API_BASE}/dags/self_healing_pipeline/dagRuns/{run_id}/taskInstances",
            auth=AIRFLOW_AUTH, timeout=15.0,
        )
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not reach Airflow API: {e}")
    return resp.json()


@app.get("/api/v1/logs")
def list_system_logs():
    if not os.path.exists(LOGS_DIR):
        return []
    log_files = []
    for f in os.listdir(LOGS_DIR):
        fp = os.path.join(LOGS_DIR, f)
        if os.path.isfile(fp) and f.endswith(".log"):
            stat = os.stat(fp)
            log_files.append({
                "filename": f, "size_bytes": stat.st_size,
                "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
    return sorted(log_files, key=lambda x: x["modified_time"], reverse=True)


@app.get("/api/v1/logs/{filename}")
def get_log_content(filename: str, tail: int = 500):
    safe_name = os.path.basename(filename)
    filepath = os.path.join(LOGS_DIR, safe_name)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"Log file '{safe_name}' not found.")
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    tail_lines = lines[-tail:] if tail else lines
    return {"filename": safe_name, "lines": tail_lines, "total_lines": len(lines)}
