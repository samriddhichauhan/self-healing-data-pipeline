"""
Shapes raw upstream records into what the UI renders. Nothing here invents a
value: a field the upstream does not provide stays None and the UI shows it
as unavailable.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .config import TASK_ORDER

TEST_RUN_PREFIXES = ("test_", "__airflow_temporary_run")
TEST_INCIDENT_PREFIXES = ("INC-TEST", "INC-MANUAL", "INC-CMD", "INC-PROMPT", "TEST-", "test-")


def parse_ts(value: Any) -> Optional[datetime]:
    """Parses ISO-8601 timestamps from Postgres, Airflow and the AI log."""
    if not value or not isinstance(value, str):
        return None
    v = value.strip().replace("Z", "+00:00")
    if len(v) > 5 and v[-5] in "+-" and v[-3] != ":":  # +0000 -> +00:00
        v = f"{v[:-2]}:{v[-2:]}"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def seconds_between(start: Any, end: Any) -> Optional[float]:
    a, b = parse_ts(start), parse_ts(end)
    if a and b:
        return round((b - a).total_seconds(), 3)
    return None


def is_test_run(run_id: Optional[str]) -> bool:
    return not run_id or run_id.startswith(TEST_RUN_PREFIXES)


def is_test_incident(inc: Dict[str, Any]) -> bool:
    iid = inc.get("incident_id") or ""
    return is_test_run(inc.get("run_id")) or iid.startswith(TEST_INCIDENT_PREFIXES)


def normalize_incident(inc: Dict[str, Any]) -> Dict[str, Any]:
    payload = inc.get("payload") or {}
    ev = payload.get("evidence") or {}
    ai = ev.get("diagnosis_source") == "OLLAMA"
    detected_at = ev.get("timestamp")
    status = inc.get("status")
    return {
        "incident_id": inc.get("incident_id"),
        "run_id": inc.get("run_id"),
        "dataset": inc.get("dataset"),
        "fault_category": inc.get("fault_category"),
        "severity": inc.get("severity"),
        "status": status,
        "created_at": inc.get("created_at"),
        "updated_at": inc.get("updated_at"),
        "detected_at": detected_at,
        "execution_date": ev.get("execution_date"),
        "task_id": ev.get("task_id") or payload.get("check"),
        "diagnosis_source": "LOCAL_OLLAMA_AI" if ai else "RULE_BASED",
        "ai_mode": ev.get("ai_mode"),
        "model": ev.get("ollama_model") if ai else None,
        # Rule-based "confidence" is a fixed engine score, not an AI output,
        # so it is reported separately and never mixed into AI metrics.
        "ai_confidence": ev.get("ollama_confidence") if ai else None,
        "engine_confidence": inc.get("confidence"),
        "risk": ev.get("ollama_risk") if ai else None,
        "recommended_action": ev.get("ollama_recommended_action") if ai else inc.get("action"),
        "policy_decision": ev.get("policy_gate_action") or inc.get("action"),
        "policy_rationale": ev.get("policy_gate_rationale"),
        "root_cause": ev.get("ollama_root_cause") if ai else inc.get("hypothesis"),
        "ai_reason": ev.get("ollama_reason"),
        "hypothesis": inc.get("hypothesis"),
        "observed": payload.get("observed"),
        "expected": payload.get("expected"),
        "error_message": ev.get("error_message"),
        "possible_root_causes": payload.get("possible_root_causes") or [],
        "blast_radius": payload.get("blast_radius"),
        "remediation_status": inc.get("remediation_status"),
        "verification_status": inc.get("verification_status"),
        "remediation": payload.get("remediation") or None,
        "verification": payload.get("verification") or None,
        # Detection (evidence timestamp) -> incident row written after
        # verification. Only meaningful once remediation actually completed.
        "recovery_seconds": seconds_between(detected_at, inc.get("updated_at")) if status == "REMEDIATED" else None,
        "evidence": {k: v for k, v in ev.items() if k != "historical_healthy_baseline"},
        "is_test": is_test_incident(inc),
    }


def normalize_run(dag_run: Dict[str, Any], pipeline_run: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    run_id = dag_run.get("dag_run_id")
    pr = pipeline_run or {}
    return {
        "run_id": run_id,
        "dag_id": dag_run.get("dag_id"),
        "logical_date": dag_run.get("logical_date") or dag_run.get("execution_date"),
        "run_type": dag_run.get("run_type"),
        "state": dag_run.get("state"),
        "start_date": dag_run.get("start_date"),
        "end_date": dag_run.get("end_date"),
        "duration_s": seconds_between(dag_run.get("start_date"), dag_run.get("end_date")),
        "pipeline_status": pr.get("status"),
        "row_counts": pr.get("row_counts"),
        "incident_count": pr.get("incident_count"),
        "auto_fix_count": pr.get("auto_fix_count"),
        "escalation_count": pr.get("escalation_count"),
        "is_test": is_test_run(run_id),
    }


def normalize_tasks(task_instances: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    order = {t: i for i, t in enumerate(TASK_ORDER)}
    tasks = [{
        "task_id": ti.get("task_id"),
        "state": ti.get("state"),
        "start_date": ti.get("start_date"),
        "end_date": ti.get("end_date"),
        "duration_s": ti.get("duration"),
        "try_number": ti.get("try_number"),
        "max_tries": ti.get("max_tries"),
        "queued_when": ti.get("queued_when"),
        "operator": ti.get("operator"),
    } for ti in task_instances]
    return sorted(tasks, key=lambda t: order.get(t["task_id"], 99))
