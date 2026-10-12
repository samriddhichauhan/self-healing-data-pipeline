"""
Builds the 9-stage self-healing view of one DAG run:

  Data -> Schema -> Quality -> Incident Detection -> Ollama AI -> Policy Gate
       -> Remediation -> Verification -> Outcome

Stages 1-3 and the outcome map to real Airflow task instances. Stages 4-8 are
steps *inside* validate_quality (src/validation/quality.py); they are not
separately timed by the pipeline, so they carry the timestamps that were
actually recorded (incident evidence, AI log events, incident row) and a
duration only where one was measured (the Ollama call).
"""
from typing import Any, Dict, List, Optional

from .normalize import parse_ts, iso, seconds_between

INGEST_TASKS = ("ingest_dimensions", "ingest_orders", "ingest_events")


def _node(key, title, subtitle, status, task_name, started_at=None, ended_at=None,
          duration_s=None, details=None, note=None, highlight=False) -> Dict[str, Any]:
    return {
        "key": key, "title": title, "subtitle": subtitle, "status": status, "task_name": task_name,
        "started_at": started_at, "ended_at": ended_at, "duration_s": duration_s,
        "details": details or [], "note": note, "highlight": highlight,
    }


def _task_status(state: Optional[str]) -> str:
    return {
        "success": "success", "failed": "failed", "upstream_failed": "skipped", "skipped": "skipped",
        "running": "running", "queued": "running", "scheduled": "pending", "up_for_retry": "warning",
        "up_for_reschedule": "warning", "restarting": "running", "removed": "skipped",
    }.get(state or "", "pending")


def _aggregate(tasks: List[Dict[str, Any]]) -> str:
    states = [_task_status(t.get("state")) for t in tasks]
    if not states:
        return "pending"
    for s in ("failed", "running", "warning"):
        if s in states:
            return s
    if all(s == "success" for s in states):
        return "success"
    if all(s == "skipped" for s in states):
        return "skipped"
    return "pending"


def _task_node(key, title, subtitle, task) -> Dict[str, Any]:
    if not task:
        return _node(key, title, subtitle, "pending", "—", note="Task instance not created yet")
    return _node(key, title, subtitle, _task_status(task.get("state")), task["task_id"],
                 task.get("start_date"), task.get("end_date"), task.get("duration_s"),
                 details=[("state", task.get("state") or "none"), ("try", task.get("try_number"))])


def build_pipeline(run: Dict[str, Any], tasks: List[Dict[str, Any]], incidents: List[Dict[str, Any]],
                   diagnoses: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_id = {t["task_id"]: t for t in tasks}
    ingest = [by_id[t] for t in INGEST_TASKS if t in by_id]
    quality = by_id.get("validate_quality")
    quality_status = _task_status(quality.get("state")) if quality else "pending"
    incident = incidents[-1] if incidents else None
    diagnosis = diagnoses[-1] if diagnoses else None
    nodes: List[Dict[str, Any]] = []

    # 1. Data ingestion (three parallel Airflow tasks)
    starts = [parse_ts(t.get("start_date")) for t in ingest if t.get("start_date")]
    ends = [parse_ts(t.get("end_date")) for t in ingest if t.get("end_date")]
    start, end = (min(starts) if starts else None), (max(ends) if ends else None)
    nodes.append(_node(
        "data", "Data", "Ingestion", _aggregate(ingest), " · ".join(INGEST_TASKS),
        iso(start), iso(end), round((end - start).total_seconds(), 3) if start and end else None,
        details=[(t["task_id"], f"{t.get('state') or 'none'} · {t['duration_s']:.2f}s" if t.get("duration_s") else (t.get("state") or "none")) for t in ingest],
    ))

    # 2-3. Schema and quality validation
    nodes.append(_task_node("schema", "Schema Validation", "Contract checks", by_id.get("validate_schema")))
    nodes.append(_task_node("quality", "Quality Validation", "Rows · nulls · keys · FKs · freshness", quality))

    # 4. Incident detection (inside validate_quality)
    if incident:
        nodes.append(_node(
            "detection", "Incident Detection", incident.get("fault_category") or "Incident", "warning",
            incident.get("task_id") or "validate_quality", incident.get("detected_at"),
            details=[("incident", incident["incident_id"]), ("dataset", incident.get("dataset")),
                     ("observed", incident.get("observed")), ("expected", incident.get("expected")),
                     ("error", incident.get("error_message"))],
            note="Step inside validate_quality; not separately timed.",
        ))
    elif quality_status == "success":
        nodes.append(_node("detection", "Incident Detection", "No incident", "success", "validate_quality",
                           note="All quality checks passed; nothing to diagnose."))
    else:
        nodes.append(_node("detection", "Incident Detection", "Waiting", "failed" if quality_status == "failed" else "pending",
                           "validate_quality"))

    # 5. Ollama AI
    if incident:
        ai = incident.get("diagnosis_source") == "LOCAL_OLLAMA_AI"
        d = diagnosis or {}
        nodes.append(_node(
            "ai", "Ollama AI", d.get("model") or incident.get("model") or "llama3.2:latest",
            "success" if ai else "warning", "OllamaDiagnosticAdapter.analyze_incident",
            d.get("request_ts"), d.get("response_ts") or d.get("diagnosis_ts"), d.get("response_time_s"),
            details=[("mode", incident.get("ai_mode")), ("confidence", incident.get("ai_confidence")),
                     ("risk", incident.get("risk")), ("ai action", incident.get("recommended_action")),
                     ("root cause", incident.get("root_cause")),
                     ("fallback", d.get("fallback_reason") if d.get("fallback") else ("no" if ai else "yes"))],
            note=None if d else "No matching [OLLAMA_*] events found in the AI log for this incident.",
            highlight=True,
        ))
    else:
        nodes.append(_node("ai", "Ollama AI", "Not invoked", "skipped", "OllamaDiagnosticAdapter.analyze_incident",
                           note="Ollama is called only when validate_quality detects an incident.", highlight=True))

    # 6. Policy Gate
    if incident:
        decision = incident.get("policy_decision")
        nodes.append(_node(
            "policy", "Policy Gate", decision or "—", "success" if decision == "AUTO_FIX" else "warning",
            "PolicyGate.evaluate", (diagnosis or {}).get("diagnosis_ts"),
            details=[("decision", decision), ("rationale", incident.get("policy_rationale"))],
        ))
    else:
        nodes.append(_node("policy", "Policy Gate", "Not required", "skipped", "PolicyGate.evaluate"))

    # 7-8. Remediation and verification
    rem, ver = (incident or {}).get("remediation") or {}, (incident or {}).get("verification") or {}
    if incident and incident.get("policy_decision") == "AUTO_FIX":
        nodes.append(_node(
            "remediation", "Remediation", rem.get("action_type") or "Deduplicate",
            "success" if incident.get("remediation_status") == "SUCCESS" else "failed",
            "RemediationExecutor.deduplicate_dataset", ended_at=incident.get("updated_at"),
            details=[("status", incident.get("remediation_status")), ("initial rows", rem.get("initial_rows")),
                     ("removed", rem.get("removed_duplicates")), ("clean rows", rem.get("cleaned_rows"))],
            note="Recorded when the incident row was written; not separately timed.",
        ))
        nodes.append(_node(
            "verification", "Verification", ver.get("check") or "primary_key_deduplication",
            {"PASSED": "success", "NOT_RUN": "skipped"}.get(incident.get("verification_status"), "failed"),
            "RemediationVerifier.verify", ended_at=incident.get("updated_at"),
            details=[("status", incident.get("verification_status")), ("duplicates", ver.get("duplicate_count")),
                     ("rows", ver.get("remaining_rows")), ("notes", ver.get("verification_notes"))],
        ))
    elif incident:
        reason = "Escalated to human review by the Policy Gate"
        nodes.append(_node("remediation", "Remediation", "Escalated", "skipped", "—", note=reason,
                           details=[("method", rem.get("method")), ("rationale", rem.get("rationale"))]))
        nodes.append(_node("verification", "Verification", "Not run", "skipped", "—", note=reason))
    else:
        nodes.append(_node("remediation", "Remediation", "Not required", "skipped", "—"))
        nodes.append(_node("verification", "Verification", "Not required", "skipped", "—"))

    # 9. Outcome (downstream tasks + final pipeline status)
    tail = [by_id[t] for t in ("transform_data", "load_data", "agent_monitoring") if t in by_id]
    monitor = by_id.get("agent_monitoring")
    pipeline_status = run.get("pipeline_status")
    state = run.get("state")
    if pipeline_status == "RECOVERED":
        outcome, title = "success", "Recovered"
    elif pipeline_status == "HEALTHY" and state == "success":
        outcome, title = "success", "Healthy"
    elif state == "failed" or pipeline_status == "FAILED":
        outcome, title = "failed", "Failed"
    elif state in ("running", "queued"):
        outcome, title = "running", "In progress"
    else:
        outcome, title = _aggregate(tail) if tail else "pending", pipeline_status or (state or "Pending").title()
    nodes.append(_node(
        "outcome", title, pipeline_status or state or "—", outcome, "transform_data · load_data · agent_monitoring",
        (tail[0].get("start_date") if tail else None), (monitor or {}).get("end_date"),
        seconds_between(tail[0].get("start_date"), (monitor or {}).get("end_date")) if tail else None,
        details=[(t["task_id"], t.get("state") or "none") for t in tail]
                + [("incidents", run.get("incident_count")), ("auto-fixed", run.get("auto_fix_count")),
                   ("escalated", run.get("escalation_count"))],
    ))

    for n in nodes:
        n["details"] = [{"label": k, "value": v} for k, v in n["details"] if v not in (None, "")]
    return {"run": run, "nodes": nodes, "incident_id": (incident or {}).get("incident_id")}
