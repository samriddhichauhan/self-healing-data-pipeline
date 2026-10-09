"""
Triggers the EXISTING DAG through Airflow's REST API. Optional fault
injection goes through the existing backend endpoint, which runs the existing
scripts/fault-injection/inject_fault.py. All validation happens before any
fault is injected, so a rejected trigger never leaves staging data modified.
"""
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..clients import Upstream, UpstreamError
from ..config import FAULT_TYPES
from ..deps import get_upstream
from ..normalize import parse_ts

router = APIRouter(prefix="/api/trigger", tags=["trigger"])

RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.:+-]{1,200}$")
FAULT_IDS = {f["id"] for f in FAULT_TYPES}


class TriggerRequest(BaseModel):
    dag_id: str
    run_id: str
    logical_date: str
    fault_type: Optional[str] = None


@router.get("/options")
def trigger_options():
    return {"fault_types": FAULT_TYPES, "server_time_utc": datetime.now(timezone.utc).isoformat()}


@router.post("")
async def trigger(req: TriggerRequest, up: Upstream = Depends(get_upstream)):
    if not RUN_ID_RE.match(req.run_id):
        raise HTTPException(422, "Run ID may only contain letters, digits and _ . : + - (max 200 characters).")
    logical = parse_ts(req.logical_date)
    if not logical:
        raise HTTPException(422, f"Invalid logical date '{req.logical_date}'. Use ISO-8601, e.g. 2026-09-27T06:30:00Z.")
    now = datetime.now(timezone.utc)
    if logical > now:
        raise HTTPException(422, (
            f"Logical date {logical.isoformat()} is in the future (server time {now.isoformat(timespec='seconds')}). "
            "Airflow keeps such runs queued with every task in state None until that time passes."))
    if req.fault_type and req.fault_type not in FAULT_IDS:
        raise HTTPException(422, f"Unknown fault type '{req.fault_type}'.")

    try:
        dag = await up.airflow(f"/dags/{quote(req.dag_id, safe='')}")
    except UpstreamError as e:
        if e.status_code == 404:
            raise HTTPException(404, f"DAG '{req.dag_id}' does not exist in Airflow.")
        raise
    if dag.get("is_paused"):
        raise HTTPException(409, f"DAG '{req.dag_id}' is paused; the run would stay queued. Unpause it in Airflow first.")

    try:
        await up.airflow(f"/dags/{quote(req.dag_id, safe='')}/dagRuns/{quote(req.run_id, safe='')}")
        raise HTTPException(409, f"A DAG run with ID '{req.run_id}' already exists.")
    except UpstreamError as e:
        if e.status_code != 404:
            raise
    logical_iso = logical.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    same_time = await up.airflow(f"/dags/{quote(req.dag_id, safe='')}/dagRuns",
                                 params={"execution_date_gte": logical_iso, "execution_date_lte": logical_iso})
    if same_time.get("total_entries", 0):
        existing = same_time["dag_runs"][0]["dag_run_id"]
        raise HTTPException(409, f"Run '{existing}' already uses logical date {logical_iso}. Airflow allows one run per exact timestamp.")

    fault_result = None
    if req.fault_type:
        try:
            fault_result = await up.backend("/api/v1/simulation/inject-fault", "POST", timeout=60.0,
                                            json={"fault_type": req.fault_type, "execution_date": logical.date().isoformat()})
        except UpstreamError as e:
            raise HTTPException(502, f"Fault injection failed, DAG not triggered: {e.detail}")

    try:
        run = await up.airflow(f"/dags/{quote(req.dag_id, safe='')}/dagRuns", "POST",
                               json={"dag_run_id": req.run_id, "logical_date": logical_iso})
    except UpstreamError as e:
        suffix = " The fault was already injected into staging for that date." if fault_result else ""
        raise HTTPException(e.status_code if e.status_code in (400, 409) else 502, f"Airflow rejected the trigger: {e.detail}.{suffix}")
    up.cache.invalidate("airflow:runs")
    return {"run": run, "fault_injection": fault_result}
