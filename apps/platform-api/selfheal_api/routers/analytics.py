"""
Chart series computed from real records only. Each series carries its source
so the UI can caption it; an empty list means "No data available".
"""
import asyncio
from collections import Counter, defaultdict

from fastapi import APIRouter, Depends

from ..clients import Upstream
from ..data import load_diagnoses, load_incidents, load_pipeline_runs, load_runs
from ..deps import get_upstream
from ..normalize import parse_ts

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


def _day(ts):
    dt = parse_ts(ts)
    return dt.date().isoformat() if dt else None


@router.get("")
async def analytics(include_test: bool = False, up: Upstream = Depends(get_upstream)):
    runs, pruns, incidents, diagnoses = await asyncio.gather(
        load_runs(up, include_test), load_pipeline_runs(up, include_test),
        load_incidents(up, include_test), load_diagnoses(up, include_test))

    by_day = defaultdict(Counter)
    for r in runs:
        day = _day(r.get("start_date")) or _day(r.get("logical_date"))
        if day:
            by_day[day][r.get("state") or "none"] += 1
    executions_by_day = [{"date": d, "success": c.get("success", 0), "failed": c.get("failed", 0),
                          "running": c.get("running", 0) + c.get("queued", 0)} for d, c in sorted(by_day.items())]

    run_states = Counter(r.get("state") for r in runs)
    outcome_counts = Counter(r.get("status") for r in pruns)

    by_type = defaultdict(Counter)
    for i in incidents:
        by_type[i.get("fault_category") or "UNKNOWN"][i.get("status") or "UNKNOWN"] += 1
    incidents_by_type = [{"fault_category": k, "remediated": c.get("REMEDIATED", 0), "escalated": c.get("ESCALATED", 0),
                          "other": sum(v for s, v in c.items() if s not in ("REMEDIATED", "ESCALATED"))}
                         for k, c in sorted(by_type.items(), key=lambda kv: -sum(kv[1].values()))]

    ai_confidence = sorted(
        [{"ts": i["created_at"], "incident_id": i["incident_id"], "confidence": i["ai_confidence"],
          "fault_category": i["fault_category"], "policy_decision": i["policy_decision"]}
         for i in incidents if i["diagnosis_source"] == "LOCAL_OLLAMA_AI" and i["ai_confidence"] is not None],
        key=lambda x: parse_ts(x["ts"]).timestamp() if parse_ts(x["ts"]) else 0)

    response_times = sorted(
        [{"ts": d["response_ts"], "incident_id": d["incident_id"], "response_time_s": d["response_time_s"],
          "model": d.get("model")} for d in diagnoses if d.get("response_time_s") is not None and parse_ts(d.get("response_ts"))],
        key=lambda x: parse_ts(x["ts"]).timestamp())

    fix_by_day = defaultdict(Counter)
    for i in incidents:
        if i["status"] in ("REMEDIATED", "ESCALATED"):
            day = _day(i.get("created_at"))
            if day:
                fix_by_day[day]["auto_fix" if i["status"] == "REMEDIATED" else "escalated"] += 1

    recovery = sorted(
        [{"ts": i["detected_at"], "incident_id": i["incident_id"], "run_id": i["run_id"], "seconds": i["recovery_seconds"],
          "diagnosis_source": i["diagnosis_source"]} for i in incidents if i["recovery_seconds"] is not None],
        key=lambda x: parse_ts(x["ts"]).timestamp() if parse_ts(x["ts"]) else 0)

    return {
        "executions_by_day": executions_by_day,
        "run_states": {k: v for k, v in run_states.items() if k},
        "pipeline_outcomes": {k: v for k, v in outcome_counts.items() if k},
        "incidents_by_type": incidents_by_type,
        "ai_confidence": ai_confidence,
        "ai_response_times": response_times,
        "autofix_vs_escalation": {
            "auto_fix": sum(1 for i in incidents if i["status"] == "REMEDIATED"),
            "escalated": sum(1 for i in incidents if i["status"] == "ESCALATED"),
            "by_day": [{"date": d, "auto_fix": c.get("auto_fix", 0), "escalated": c.get("escalated", 0)}
                       for d, c in sorted(fix_by_day.items())],
        },
        "recovery_durations": recovery,
        "sources": {
            "executions": "Airflow REST API (dagRuns)",
            "outcomes": "pipeline_runs (Postgres)",
            "incidents": "incidents (Postgres)",
            "ai_confidence": "incident evidence written by the Ollama adapter",
            "ai_response_times": "[OLLAMA_RESPONSE] events in logs/ollama_ai_agent.log",
            "recovery": "incident detection timestamp → verified incident write",
        },
    }
