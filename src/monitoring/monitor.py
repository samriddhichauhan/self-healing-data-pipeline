"""
Agent monitoring: the final DAG task. Determines the real final pipeline
status by inspecting what actually happened in *this* run (not a hardcoded
"SUCCESS"):

  HEALTHY   — no incident was recorded for this run
  RECOVERED — at least one incident was auto-fixed and verified this run
  (validate_quality already fails the task/DAG run for any non-auto-fixable
  fault, so this task never reports a FAILED status for that case — Airflow
  itself marks the DAG run failed.)
"""
import os
import glob
import json
import pandas as pd
from datetime import datetime, timezone
from ..config import get_data_dir
from ..database.loader import finish_pipeline_run, record_failed_pipeline_run


def _load_run_incidents(data_dir: str, execution_date: str, run_id: str):
    report_dir = os.path.join(data_dir, "incidents", "reports")
    matched = []
    for fp in glob.glob(os.path.join(report_dir, "inc_*.json")):
        try:
            with open(fp, "r") as f:
                data = json.load(f)
        except Exception:
            continue
        evidence = data.get("evidence", {})
        same_run = run_id and evidence.get("run_id") == run_id
        same_date_fallback = (not run_id) and evidence.get("execution_date") == execution_date
        if same_run or same_date_fallback:
            matched.append(data)
    return matched


def record_failed_run(context):
    """Called from the DAG's on_failure_callback once a task has exhausted
    its retries: records this run as FAILED in Postgres with the incidents
    (and escalations) actually filed for it, so the dashboard shows the
    failure instead of the previous successful run."""
    execution_date = context.get("ds")
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual__{execution_date}"
    run_incidents = _load_run_incidents(get_data_dir(), execution_date, run_id)
    auto_fixed = [i for i in run_incidents if i.get("status") == "REMEDIATED"]
    escalated = [i for i in run_incidents if i.get("status") == "ESCALATED"]

    record_failed_pipeline_run(
        run_id=run_id,
        dag_id=dag_run.dag_id if dag_run else "self_healing_pipeline",
        logical_date=execution_date,
        started_at=getattr(dag_run, "start_date", None),
        incident_count=len(run_incidents),
        auto_fix_count=len(auto_fixed),
        escalation_count=len(escalated),
    )
    print(
        f"[AGENT MONITORING] Pipeline status: FAILED. run_id={run_id} "
        f"incidents={len(run_incidents)} (auto-fixed: {len(auto_fixed)}, escalated: {len(escalated)})."
    )


def agent_monitoring(**context):
    execution_date = context["ds"]
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual__{execution_date}"
    data_dir = get_data_dir()

    cust_proc = os.path.join(data_dir, "processed", "customers.csv")
    prod_proc = os.path.join(data_dir, "processed", "products.csv")
    orders_proc = os.path.join(data_dir, "processed", "orders", f"orders_{execution_date}.csv")
    events_proc = os.path.join(data_dir, "processed", "events", f"events_{execution_date}.jsonl")

    row_counts = {
        "customers": len(pd.read_csv(cust_proc)) if os.path.exists(cust_proc) else 0,
        "products": len(pd.read_csv(prod_proc)) if os.path.exists(prod_proc) else 0,
        "orders": len(pd.read_csv(orders_proc)) if os.path.exists(orders_proc) else 0,
        "events": len(pd.read_json(events_proc, lines=True)) if os.path.exists(events_proc) else 0,
    }

    run_incidents = _load_run_incidents(data_dir, execution_date, run_id)
    auto_fixed = [i for i in run_incidents if i.get("status") == "REMEDIATED"]
    escalated = [i for i in run_incidents if i.get("status") == "ESCALATED"]

    pipeline_status = "RECOVERED" if auto_fixed else "HEALTHY"

    monitoring_summary = {
        "execution_date": execution_date,
        "run_id": run_id,
        "pipeline_status": pipeline_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "row_counts": row_counts,
        "incident_ids": [i.get("incident_id") for i in run_incidents],
        "auto_fixed_incident_ids": [i.get("incident_id") for i in auto_fixed],
        "escalated_incident_ids": [i.get("incident_id") for i in escalated],
    }

    status_dir = os.path.join(data_dir, "incidents", "status")
    os.makedirs(status_dir, exist_ok=True)
    status_path = os.path.join(status_dir, f"status_{execution_date}.json")
    with open(status_path, "w") as f:
        json.dump(monitoring_summary, f, indent=2)

    # Authoritative final status lives in Postgres — this must succeed for
    # real (no swallowing DB errors into a fake pipeline success).
    finish_pipeline_run(
        run_id=run_id,
        status=pipeline_status,
        row_counts=row_counts,
        incident_count=len(run_incidents),
        auto_fix_count=len(auto_fixed),
        escalation_count=len(escalated),
    )

    print(
        f"[AGENT MONITORING] Pipeline status: {pipeline_status}. "
        f"Row counts: {row_counts}. Incidents this run: {len(run_incidents)} "
        f"(auto-fixed: {len(auto_fixed)}, escalated: {len(escalated)}). "
        f"Status logged to {status_path} and Postgres pipeline_runs[{run_id}]."
    )
