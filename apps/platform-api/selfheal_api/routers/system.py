import asyncio
from collections import Counter
from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from ..clients import Upstream, UpstreamError
from ..config import settings
from ..data import load_diagnoses, load_incidents, load_pipeline_runs, load_runs
from ..deps import get_upstream
from ..health import system_health
from ..normalize import parse_ts

router = APIRouter(prefix="/api", tags=["system"])


@router.get("/config")
def get_config() -> Dict[str, Any]:
    return {
        "dag_id": settings.dag_id,
        "ollama_model": settings.ollama_model,
        "ollama_host": settings.ollama_host,
        "airflow_public_url": settings.airflow_public_url,
        "backend_url": settings.backend_url,
    }


@router.get("/system/health")
async def get_system_health(up: Upstream = Depends(get_upstream)):
    return await up.cache.get_or_set("health", 3, lambda: system_health(up))


@router.get("/dags")
async def list_dags(up: Upstream = Depends(get_upstream)):
    data = await up.airflow("/dags", params={"limit": 100})
    return [{
        "dag_id": d["dag_id"], "is_paused": d.get("is_paused"), "is_active": d.get("is_active"),
        "description": d.get("description"), "tags": [t.get("name") for t in d.get("tags") or []],
        "owners": d.get("owners"),
    } for d in data.get("dags", [])]


async def _section(coro):
    try:
        return await coro, None
    except UpstreamError as e:
        return None, f"{e.service}: {e.detail}"


@router.get("/dashboard")
async def dashboard(include_test: bool = False, up: Upstream = Depends(get_upstream)):
    (health, h_err), (runs, r_err), (pruns, p_err), (incidents, i_err), (diags, d_err) = await asyncio.gather(
        _section(up.cache.get_or_set("health", 3, lambda: system_health(up))),
        _section(load_runs(up, include_test)),
        _section(load_pipeline_runs(up, include_test)),
        _section(load_incidents(up, include_test)),
        _section(load_diagnoses(up, include_test)),
    )
    out: Dict[str, Any] = {"errors": {k: v for k, v in {
        "health": h_err, "runs": r_err, "pipeline_runs": p_err, "incidents": i_err, "ai": d_err}.items() if v}}

    out["health"] = health
    if runs is not None:
        states = Counter(r["state"] for r in runs)
        out["executions"] = {
            "total": len(runs), "success": states.get("success", 0), "failed": states.get("failed", 0),
            "running": states.get("running", 0), "queued": states.get("queued", 0),
        }
        out["recent_executions"] = runs[:8]
    if pruns is not None:
        out["latest_pipeline_run"] = pruns[0] if pruns else None
        out["pipeline_totals"] = {
            "auto_fix_count": sum(r.get("auto_fix_count") or 0 for r in pruns),
            "escalation_count": sum(r.get("escalation_count") or 0 for r in pruns),
            "recovered_runs": sum(1 for r in pruns if r.get("status") == "RECOVERED"),
        }
    if incidents is not None:
        out["incidents"] = {
            "total": len(incidents),
            "recovered": sum(1 for i in incidents if i["status"] == "REMEDIATED"),
            "escalated": sum(1 for i in incidents if i["status"] == "ESCALATED"),
        }
        out["recent_incidents"] = incidents[:6]
    if diags is not None:
        timed = [d["response_time_s"] for d in diags if d.get("response_time_s") is not None]
        out["ai"] = {
            "avg_response_time_s": round(sum(timed) / len(timed), 2) if timed else None,
            "responses_measured": len(timed),
            "last_diagnosis": diags[0] if diags else None,
        }
    return out


@router.get("/activity")
async def activity(limit: int = 40, include_test: bool = False, up: Upstream = Depends(get_upstream)):
    """A merged, newest-first feed of real events: DAG runs, incidents and AI calls."""
    (runs, _), (incidents, _), (diags, _) = await asyncio.gather(
        _section(load_runs(up, include_test)), _section(load_incidents(up, include_test)), _section(load_diagnoses(up, include_test)))
    events: List[Dict[str, Any]] = []
    for r in runs or []:
        if r.get("start_date"):
            events.append({"ts": r["start_date"], "kind": "run", "severity": "info",
                           "title": "DAG run started", "detail": r["run_id"], "ref": {"run_id": r["run_id"]}})
        if r.get("end_date") and r.get("state") in ("success", "failed"):
            ok = r["state"] == "success"
            label = r.get("pipeline_status") or r["state"].upper()
            events.append({"ts": r["end_date"], "kind": "run", "severity": "success" if ok else "error",
                           "title": f"DAG run {'completed' if ok else 'failed'} · {label}",
                           "detail": r["run_id"], "ref": {"run_id": r["run_id"]}})
    for i in incidents or []:
        if i.get("detected_at"):
            events.append({"ts": i["detected_at"], "kind": "incident", "severity": "warning",
                           "title": f"Incident detected · {i['fault_category']}",
                           "detail": f"{i['incident_id']} on {i.get('dataset')}", "ref": {"incident_id": i["incident_id"]}})
        if i["status"] in ("REMEDIATED", "ESCALATED") and i.get("updated_at"):
            fixed = i["status"] == "REMEDIATED"
            events.append({"ts": i["updated_at"], "kind": "incident", "severity": "success" if fixed else "error",
                           "title": "Auto-fixed and verified" if fixed else "Escalated to human review",
                           "detail": i["incident_id"], "ref": {"incident_id": i["incident_id"]}})
    for d in (diags or [])[:60]:
        if d["source"] == "LOCAL_OLLAMA_AI":
            conf = f" · confidence {d['confidence']:.2f}" if d.get("confidence") is not None else ""
            took = f" in {d['response_time_s']:.1f}s" if d.get("response_time_s") is not None else ""
            events.append({"ts": d.get("diagnosis_ts") or d["last_ts"], "kind": "ai", "severity": "ai",
                           "title": f"Ollama diagnosis{took}", "detail": f"{d['incident_id']} · {d.get('model')}{conf}",
                           "ref": {"incident_id": d["incident_id"]}})
        elif d.get("fallback"):
            events.append({"ts": d["last_ts"], "kind": "ai", "severity": "warning", "title": "Rule-based fallback used",
                           "detail": f"{d['incident_id']} · {d.get('fallback_reason') or ''}", "ref": {"incident_id": d["incident_id"]}})
    events = [e for e in events if parse_ts(e["ts"])]
    events.sort(key=lambda e: parse_ts(e["ts"]).timestamp(), reverse=True)
    return events[:limit]
