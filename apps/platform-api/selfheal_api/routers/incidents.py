from fastapi import APIRouter, Depends, HTTPException

from ..clients import Upstream
from ..data import load_diagnoses, load_incidents
from ..deps import get_upstream

router = APIRouter(prefix="/api/incidents", tags=["incidents"])


@router.get("")
async def list_incidents(limit: int = 500, include_test: bool = False, up: Upstream = Depends(get_upstream)):
    return (await load_incidents(up, include_test))[:limit]


@router.get("/{incident_id}")
async def get_incident(incident_id: str, up: Upstream = Depends(get_upstream)):
    match = next((i for i in await load_incidents(up, include_test=True) if i["incident_id"] == incident_id), None)
    if not match:
        raise HTTPException(404, f"Incident '{incident_id}' not found")
    match = dict(match)
    match["ai_calls"] = [d for d in await load_diagnoses(up, include_test=True) if d["incident_id"] == incident_id]
    return match
