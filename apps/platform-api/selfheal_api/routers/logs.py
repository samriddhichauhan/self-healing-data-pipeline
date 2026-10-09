from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException

from ..clients import Upstream, UpstreamError
from ..deps import get_upstream
from ..log_parser import parse_lines

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("/files")
async def list_log_files(up: Upstream = Depends(get_upstream)):
    return await up.backend("/api/v1/logs")


@router.get("/files/{filename}")
async def read_log_file(filename: str, tail: int = 2000, up: Upstream = Depends(get_upstream)):
    try:
        data = await up.backend(f"/api/v1/logs/{quote(filename, safe='')}", params={"tail": max(tail, 1)}, timeout=20.0)
    except UpstreamError as e:
        if e.status_code == 404:
            raise HTTPException(404, f"Log file '{filename}' not found")
        raise
    lines = data.get("lines", [])
    first = data.get("total_lines", len(lines)) - len(lines)
    entries = parse_lines(lines)
    for e in entries:
        e["n"] += first
    return {"source": "file", "name": filename, "total_lines": data.get("total_lines"), "entries": entries}


@router.get("/task")
async def read_task_log(run_id: str, task_id: str, try_number: int = 1, up: Upstream = Depends(get_upstream)):
    s = up.settings
    path = f"/dags/{s.dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{quote(task_id, safe='')}/logs/{try_number}"
    try:
        text = await up.airflow(path, expect_json=False, headers={"Accept": "text/plain"}, params={"full_content": "true"})
    except UpstreamError as e:
        if e.status_code == 404:
            raise HTTPException(404, f"No log for {task_id} try {try_number} in run {run_id}")
        raise
    lines = text.splitlines()
    return {"source": "airflow", "name": f"{run_id} / {task_id} / try {try_number}",
            "total_lines": len(lines), "entries": parse_lines(lines)}
