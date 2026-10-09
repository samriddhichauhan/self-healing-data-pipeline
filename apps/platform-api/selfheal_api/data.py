"""Shared loaders used by several routers."""
from typing import Any, Dict, List

from .clients import Upstream
from .log_parser import build_diagnoses, parse_ai_events
from .normalize import is_test_run, normalize_incident, normalize_run, parse_ts


def _newest_first(items: List[Dict[str, Any]], *keys: str) -> List[Dict[str, Any]]:
    def key(item):
        for k in keys:
            dt = parse_ts(item.get(k))
            if dt:
                return dt.timestamp()
        return float("inf")  # not started yet (queued) sorts first
    return sorted(items, key=key, reverse=True)


async def load_incidents(up: Upstream, include_test: bool = False) -> List[Dict[str, Any]]:
    incidents = [normalize_incident(i) for i in await up.incidents()]
    if not include_test:
        incidents = [i for i in incidents if not i["is_test"]]
    return _newest_first(incidents, "created_at")


async def load_diagnoses(up: Upstream, include_test: bool = False) -> List[Dict[str, Any]]:
    """AI calls from the adapter log. A call counts as a pipeline call only if
    its incident_id belongs to a real (non-test) pipeline incident; calls from
    pytest, CLI checks or manual adapter tests are tagged is_test."""
    diagnoses = build_diagnoses(parse_ai_events(await up.ai_log_lines()))
    incidents = {i["incident_id"]: i for i in await load_incidents(up, include_test=True)}
    for d in diagnoses:
        inc = incidents.get(d["incident_id"])
        d["run_id"] = inc["run_id"] if inc else None
        d["incident_status"] = inc["status"] if inc else None
        d["is_test"] = inc is None or inc["is_test"]
    if not include_test:
        diagnoses = [d for d in diagnoses if not d["is_test"]]
    return _newest_first(diagnoses, "last_ts", "request_ts")


async def load_runs(up: Upstream, include_test: bool = False, limit: int = 500) -> List[Dict[str, Any]]:
    dag_runs = await up.dag_runs(limit)
    pipeline = {r["run_id"]: r for r in await up.pipeline_runs()}
    runs = [normalize_run(r, pipeline.get(r.get("dag_run_id"))) for r in dag_runs]
    if not include_test:
        runs = [r for r in runs if not r["is_test"]]
    return _newest_first(runs, "start_date", "logical_date")


async def load_pipeline_runs(up: Upstream, include_test: bool = False) -> List[Dict[str, Any]]:
    runs = await up.pipeline_runs()
    if not include_test:
        runs = [r for r in runs if not is_test_run(r.get("run_id"))]
    return _newest_first(runs, "started_at")
