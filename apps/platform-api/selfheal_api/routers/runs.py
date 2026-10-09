import asyncio
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException

from ..clients import Upstream, UpstreamError
from ..data import load_diagnoses, load_incidents, load_runs
from ..deps import get_upstream
from ..normalize import normalize_run, normalize_tasks
from ..pipeline_graph import build_pipeline

router = APIRouter(prefix="/api/runs", tags=["runs"])


@router.get("")
async def list_runs(limit: int = 200, include_test: bool = False, up: Upstream = Depends(get_upstream)):
    return (await load_runs(up, include_test, limit=max(limit, 1)))[:limit]


async def _run_bundle(run_id: str, up: Upstream):
    s = up.settings
    rid = quote(run_id, safe="")
    try:
        dag_run, ti = await asyncio.gather(
            up.airflow(f"/dags/{s.dag_id}/dagRuns/{rid}"),
            up.airflow(f"/dags/{s.dag_id}/dagRuns/{rid}/taskInstances"),
        )
    except UpstreamError as e:
        if e.status_code == 404:
            raise HTTPException(404, f"DAG run '{run_id}' not found in Airflow")
        raise
    pipeline = {r["run_id"]: r for r in await up.pipeline_runs()}
    run = normalize_run(dag_run, pipeline.get(run_id))
    tasks = normalize_tasks(ti.get("task_instances", []))
    incidents = [i for i in await load_incidents(up, include_test=True) if i["run_id"] == run_id]
    incidents.reverse()  # oldest first
    return run, tasks, incidents


@router.get("/{run_id}")
async def get_run(run_id: str, up: Upstream = Depends(get_upstream)):
    run, tasks, incidents = await _run_bundle(run_id, up)
    return {"run": run, "tasks": tasks, "incidents": incidents}


@router.get("/{run_id}/pipeline")
async def get_run_pipeline(run_id: str, up: Upstream = Depends(get_upstream)):
    run, tasks, incidents = await _run_bundle(run_id, up)
    diagnoses = []
    if incidents:
        ids = {i["incident_id"] for i in incidents}
        diagnoses = [d for d in reversed(await load_diagnoses(up, include_test=True)) if d["incident_id"] in ids]
    graph = build_pipeline(run, tasks, incidents, diagnoses)
    graph["tasks"] = tasks
    return graph
