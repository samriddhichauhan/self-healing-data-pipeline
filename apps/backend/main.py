"""
FastAPI Backend Application for Self-Healing Pipeline Showcase
"""
import os
import sys
import json
import glob
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

# Add apps/agent to path for agent imports
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../agent"))
if base_dir not in sys.path:
    sys.path.append(base_dir)

from agent.diagnosis.engine import DiagnosticEngine
from agent.diagnosis.llm_adapter import LLMDiagnosticAdapter
from agent.tools.incident_tool import IncidentTool
from agent.tools.remediation_tool import RemediationTool
from agent.tools.verification_tool import VerificationTool

app = FastAPI(title="Self-Healing Pipeline API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = os.environ.get("TEST_DATA_DIR", os.path.abspath("./data"))
REPORTS_DIR = os.path.join(DATA_DIR, "incidents", "reports")

incident_tool = IncidentTool(reports_dir=REPORTS_DIR)
remediation_tool = RemediationTool(data_dir=DATA_DIR)
verification_tool = VerificationTool(data_dir=DATA_DIR)
diagnostic_engine = DiagnosticEngine(data_dir=DATA_DIR)
llm_adapter = LLMDiagnosticAdapter(fallback_engine=diagnostic_engine)


class FaultInjectRequest(BaseModel):
    fault_type: str
    execution_date: Optional[str] = "2026-06-01"


class RemediationRequest(BaseModel):
    dataset: Optional[str] = "orders"
    primary_key: Optional[str] = "order_id"
    execution_date: Optional[str] = "2026-06-01"


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "Self-Healing Pipeline API",
        "version": "1.0.0",
        "data_dir": DATA_DIR,
    }


@app.get("/api/v1/pipeline/status")
def get_pipeline_status():
    status_files = glob.glob(os.path.join(DATA_DIR, "incidents", "status", "status_*.json"))
    if status_files:
        latest_file = sorted(status_files)[-1]
        with open(latest_file, "r") as f:
            return json.load(f)

    return {
        "pipeline_status": "SUCCESS",
        "execution_date": "2026-06-01",
        "row_counts": {"customers": 500, "products": 60, "orders": 300, "events": 1200},
    }


@app.get("/api/v1/pipeline/runs")
def get_pipeline_runs():
    return [
        {"run_id": "run-001", "execution_date": "2026-06-01", "status": "SUCCESS", "duration_seconds": 12.4},
        {"run_id": "run-002", "execution_date": "2026-05-31", "status": "SUCCESS", "duration_seconds": 11.8},
        {"run_id": "run-003", "execution_date": "2026-05-30", "status": "FAILED", "duration_seconds": 4.2},
    ]


@app.get("/api/v1/incidents")
def list_incidents():
    return incident_tool.list_incidents()


@app.get("/api/v1/incidents/{incident_id}")
def get_incident(incident_id: str):
    inc = incident_tool.get_incident(incident_id)
    if not inc:
        raise HTTPException(status_code=404, detail="Incident report not found")
    return inc


@app.get("/api/v1/data/summary")
def get_data_summary():
    return {
        "raw": {"customers": "customers.csv", "products": "products.csv", "orders": "orders_*.csv", "events": "events_*.jsonl"},
        "staging": ["stg_customers.csv", "stg_products.csv", "stg_orders_*.csv", "stg_events_*.jsonl"],
        "processed": ["customers.csv", "products.csv", "orders/orders_*.csv", "events/events_*.jsonl"],
    }


@app.get("/api/v1/pipeline/summary")
def get_pipeline_summary():
    """
    Returns pipeline execution run summary statistics.
    """
    incidents = incident_tool.list_incidents()
    auto_fixes = sum(1 for i in incidents if i.get("action") == "AUTO_FIX")
    escalations = sum(1 for i in incidents if i.get("action") == "ESCALATE")
    
    return {
        "run_id": "run-20260601-0200",
        "start_time": "2026-06-01T02:00:00Z",
        "end_time": "2026-06-01T02:02:15Z",
        "duration_seconds": 135.0,
        "tasks_passed": 8,
        "tasks_failed": 0 if not incidents else 1,
        "records_processed": 2060,
        "records_rejected": 0,
        "incidents_created": len(incidents),
        "auto_fixes": auto_fixes,
        "escalations": escalations,
        "final_status": "SUCCESS" if not any(i.get("status") == "ESCALATED" for i in incidents) else "DEGRADED",
    }


@app.get("/api/v1/data/profiles")
def get_data_profiles():
    """
    Returns data profiles generated during ingestion profiling.
    """
    profiles_dir = os.path.join(DATA_DIR, "incidents", "profiles")
    if not os.path.exists(profiles_dir):
        return []

    profile_files = glob.glob(os.path.join(profiles_dir, "profile_*.json"))
    profiles = []
    for pf in profile_files:
        try:
            with open(pf, "r") as f:
                profiles.append(json.load(f))
        except Exception:
            continue

    return profiles


@app.post("/api/v1/incidents/{incident_id}/remediate")
def remediate_incident(incident_id: str, req: RemediationRequest):
    inc = incident_tool.get_incident(incident_id)
    if not inc:
        # Create virtual incident if invoking directly
        inc = {
            "incident_id": incident_id,
            "pipeline": "self_healing_pipeline",
            "dataset": req.dataset or "orders",
            "check": "validate_quality",
            "fault_category": "DUPLICATE_INGESTION",
            "action": "AUTO_FIX",
        }

    dataset = req.dataset or inc.get("dataset", "orders")
    pk = req.primary_key or ("event_id" if dataset == "events" else "order_id")
    date = req.execution_date or "2026-06-01"

    fix_res = remediation_tool.execute_fix(dataset, pk, date)
    verify_res = verification_tool.verify_remediation(dataset, pk, date)

    # Update incident record
    inc["status"] = "REMEDIATED"
    inc["remediation"] = fix_res
    inc["verification"] = verify_res

    report_file = os.path.join(REPORTS_DIR, f"inc_{incident_id}.json")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(report_file, "w") as f:
        json.dump(inc, f, indent=2)

    return {
        "incident_id": incident_id,
        "status": "REMEDIATED",
        "remediation": fix_res,
        "verification": verify_res,
    }


@app.post("/api/v1/simulation/inject-fault")
def inject_fault_endpoint(req: FaultInjectRequest):
    import subprocess
    cmd = [
        sys.executable,
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../scripts/fault-injection/inject_fault.py")),
        "--fault", req.fault_type,
        "--data-dir", DATA_DIR,
        "--execution-date", req.execution_date or "2026-06-01",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)

    # Diagnose and create incident using safe LLM/hybrid diagnostic adapter
    inc_id = f"INC-{random_int()}"
    report = llm_adapter.analyze_incident(
        incident_id=inc_id,
        pipeline_id="self_healing_pipeline",
        task_id="validate_quality" if req.fault_type != "schema_drift" else "validate_schema",
        dataset="products" if req.fault_type == "schema_drift" else "orders",
        fault_category=req.fault_type,
        observed=f"Fault injection: {req.fault_type}",
        expected="Healthy baseline contract",
        evidence={"fault_type": req.fault_type, "cli_output": res.stdout.strip()},
        execution_date=req.execution_date or "2026-06-01"
    )
    report.save(REPORTS_DIR)

    return {
        "status": "FAULT_INJECTED",
        "fault_type": req.fault_type,
        "incident_id": inc_id,
        "action": report.action,
        "report": report.to_dict(),
    }


class ScenarioRequest(BaseModel):
    scenario_id: int  # 1: Healthy, 2: Schema Drift, 3: Null Spike, 4: Duplicate (Auto-Fix), 5: Volume Drop
    execution_date: Optional[str] = "2026-06-01"


@app.post("/api/v1/simulation/run-scenario")
def run_manager_scenario(req: ScenarioRequest):
    """
    Executes a complete 5-scenario pipeline test run for live manager demonstrations.
    """
    fault_map = {
        1: None,
        2: "schema_drift",
        3: "null_spike",
        4: "duplicate_ingestion",
        5: "volume_drop",
    }
    title_map = {
        1: "Healthy Pipeline (Clean Baseline)",
        2: "Schema Drift Fault (Corrupted Data Types)",
        3: "Data Quality Fault (Null Spike in Customer ID)",
        4: "Duplicate Ingestion Fault (Auto-Remediate)",
        5: "Volume Drop Anomaly Fault (Missing Records)",
    }

    if req.scenario_id not in fault_map:
        raise HTTPException(status_code=400, detail="Invalid scenario_id. Must be between 1 and 5.")

    fault = fault_map[req.scenario_id]
    title = title_map[req.scenario_id]

    # Import test harness scenario runner
    scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../scripts"))
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from test_pipeline_5_cases import run_pipeline_scenario, reset_environment

    reset_environment(DATA_DIR)
    os.environ["TEST_DATA_DIR"] = DATA_DIR

    # Capture output safely
    run_pipeline_scenario(req.scenario_id, title, fault_type=fault)

    incidents = incident_tool.list_incidents()
    latest_incident = incidents[0] if incidents else None

    return {
        "scenario_id": req.scenario_id,
        "title": title,
        "fault_type": fault or "NONE",
        "status": "COMPLETED",
        "latest_incident": latest_incident,
    }


@app.get("/api/v1/logs")
def list_system_logs():
    """
    Lists all log files stored in the central logs/ directory.
    """
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../logs"))
    if not os.path.exists(logs_dir):
        return []

    log_files = []
    for f in os.listdir(logs_dir):
        fp = os.path.join(logs_dir, f)
        if os.path.isfile(fp) and f.endswith(".log"):
            stat = os.stat(fp)
            log_files.append({
                "filename": f,
                "size_bytes": stat.st_size,
                "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })

    return sorted(log_files, key=lambda x: x["modified_time"], reverse=True)


@app.get("/api/v1/logs/{filename}")
def get_log_content(filename: str):
    """
    Returns content of a specific log file from logs/ directory.
    """
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../logs"))
    safe_name = os.path.basename(filename)
    filepath = os.path.join(logs_dir, safe_name)

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"Log file '{safe_name}' not found.")

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    return {
        "filename": safe_name,
        "lines": content.splitlines(),
        "total_lines": len(content.splitlines()),
        "raw": content
    }


def random_int():
    import random
    return random.randint(1000, 9999)
