"""
Connected / Degraded / Offline rules for each component. Every check calls
the real upstream; an unreachable upstream is reported as offline with the
actual error, never assumed healthy.
"""
import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from .clients import Upstream, UpstreamError
from .normalize import parse_ts

CONNECTED, DEGRADED, OFFLINE = "connected", "degraded", "offline"
SCHEDULER_STALE_S = 60      # heartbeat older than this -> degraded
SCHEDULER_DEAD_S = 300      # older than this -> offline
SLOW_MS = 2000              # responses slower than this -> degraded


def _c(key, name, status, detail, latency_ms=None, meta=None) -> Dict[str, Any]:
    return {"key": key, "name": name, "status": status, "detail": detail,
            "latency_ms": latency_ms, "meta": meta or {}}


async def _timed(coro):
    t = time.monotonic()
    try:
        result = await coro
        return result, None, round((time.monotonic() - t) * 1000)
    except UpstreamError as e:
        return None, e, round((time.monotonic() - t) * 1000)


def _speed(latency_ms: int) -> str:
    return DEGRADED if latency_ms > SLOW_MS else CONNECTED


def scheduler_status(health: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    sched = (health or {}).get("scheduler") or {}
    hb = parse_ts(sched.get("latest_scheduler_heartbeat"))
    if not hb:
        return _c("scheduler", "Airflow Scheduler", OFFLINE, "No scheduler heartbeat recorded")
    age = (now - hb).total_seconds()
    meta = {"heartbeat": sched.get("latest_scheduler_heartbeat"), "heartbeat_age_s": round(age, 1),
            "executor": "LocalExecutor (tasks run inside the scheduler container)"}
    if sched.get("status") != "healthy" or age > SCHEDULER_DEAD_S:
        return _c("scheduler", "Airflow Scheduler", OFFLINE, f"Status '{sched.get('status')}', last heartbeat {age:.0f}s ago", meta=meta)
    if age > SCHEDULER_STALE_S:
        return _c("scheduler", "Airflow Scheduler", DEGRADED, f"Heartbeat is {age:.0f}s old", meta=meta)
    return _c("scheduler", "Airflow Scheduler", CONNECTED, f"Healthy · heartbeat {age:.0f}s ago", meta=meta)


def dag_status(dag: Dict[str, Any], import_errors: List[Dict[str, Any]], dag_id: str) -> Dict[str, Any]:
    meta = {"dag_id": dag_id, "is_paused": dag.get("is_paused"), "is_active": dag.get("is_active"),
            "import_errors": len(import_errors)}
    if import_errors or dag.get("has_import_errors"):
        return _c("dag", "DAG", OFFLINE, f"{len(import_errors) or 'Has'} import error(s)", meta=meta)
    if not dag.get("is_active"):
        return _c("dag", "DAG", OFFLINE, "DAG is not active (file not parsed)", meta=meta)
    if dag.get("is_paused"):
        return _c("dag", "DAG", DEGRADED, "Paused — new runs will stay queued", meta=meta)
    return _c("dag", "DAG", CONNECTED, f"{dag_id} active and unpaused", meta=meta)


def overall(components: List[Dict[str, Any]]) -> str:
    statuses = {c["status"] for c in components}
    return OFFLINE if OFFLINE in statuses else DEGRADED if DEGRADED in statuses else CONNECTED


async def system_health(up: Upstream) -> Dict[str, Any]:
    s = up.settings
    (af, af_err, af_ms), (dag, dag_err, _), (ierr, _, _), (tags, ol_err, ol_ms), (be, be_err, be_ms) = await asyncio.gather(
        _timed(up.airflow_health()),
        _timed(up.airflow(f"/dags/{s.dag_id}")),
        _timed(up.airflow("/importErrors")),
        _timed(up.ollama_tags()),
        _timed(up.backend("/health", timeout=5.0)),
    )
    now = datetime.now(timezone.utc)
    comps: List[Dict[str, Any]] = []

    if af_err:
        comps.append(_c("airflow", "Airflow Webserver", OFFLINE, af_err.detail, af_ms))
        comps.append(_c("scheduler", "Airflow Scheduler", OFFLINE, "Unknown — Airflow webserver unreachable"))
        comps.append(_c("airflow_db", "Airflow Metadata DB", OFFLINE, "Unknown — Airflow webserver unreachable"))
    else:
        comps.append(_c("airflow", "Airflow Webserver", _speed(af_ms), f"Reachable at {s.airflow_public_url}", af_ms))
        comps.append(scheduler_status(af, now))
        mdb = (af.get("metadatabase") or {}).get("status")
        comps.append(_c("airflow_db", "Airflow Metadata DB", CONNECTED if mdb == "healthy" else OFFLINE,
                        f"Metadatabase {mdb or 'unknown'}"))

    if dag_err:
        status = OFFLINE
        detail = f"DAG '{s.dag_id}' not found" if dag_err.status_code == 404 else dag_err.detail
        comps.append(_c("dag", "DAG", status, detail))
    else:
        errors = [e for e in (ierr or {}).get("import_errors", []) if s.dag_id in (e.get("filename") or "")] if ierr else []
        comps.append(dag_status(dag, errors, s.dag_id))

    if ol_err:
        comps.append(_c("ollama", "Ollama", OFFLINE, ol_err.detail, ol_ms, {"host": s.ollama_host}))
        comps.append(_c("model", "Model", OFFLINE, f"{s.ollama_model} — Ollama unreachable", meta={"model": s.ollama_model}))
    else:
        models = [m.get("name") for m in tags.get("models", [])]
        comps.append(_c("ollama", "Ollama", _speed(ol_ms), f"{len(models)} model(s) installed", ol_ms,
                        {"host": s.ollama_host, "models": models}))
        present = s.ollama_model in models
        comps.append(_c("model", "Model", CONNECTED if present else OFFLINE,
                        f"{s.ollama_model} {'installed' if present else 'not installed'}",
                        meta={"model": s.ollama_model, "installed": models}))

    if be_err:
        comps.append(_c("api", "Pipeline API", OFFLINE, be_err.detail, be_ms))
        comps.append(_c("pipeline_db", "Pipeline Database", OFFLINE, "Unknown — pipeline API unreachable"))
    else:
        db = be.get("database") or {}
        comps.append(_c("api", "Pipeline API", DEGRADED if be.get("status") != "healthy" else _speed(be_ms),
                        f"{be.get('service', 'backend')} {be.get('version', '')}".strip(), be_ms))
        comps.append(_c("pipeline_db", "Pipeline Database", CONNECTED if db.get("connected") else OFFLINE,
                        "PostgreSQL connected" if db.get("connected") else (db.get("detail") or "Not connected")))

    return {"status": overall(comps), "checked_at": now.isoformat(), "components": comps}
