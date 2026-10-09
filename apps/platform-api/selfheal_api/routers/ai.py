import time

from fastapi import APIRouter, Depends

from ..clients import Upstream, UpstreamError
from ..data import load_diagnoses
from ..deps import get_upstream

router = APIRouter(prefix="/api/ai", tags=["ai"])


@router.get("/status")
async def ai_status(up: Upstream = Depends(get_upstream)):
    s = up.settings
    t = time.monotonic()
    try:
        tags = await up.ollama_tags()
    except UpstreamError as e:
        return {"connected": False, "host": s.ollama_host, "model": s.ollama_model, "model_installed": False,
                "models": [], "latency_ms": round((time.monotonic() - t) * 1000), "error": e.detail}
    models = [{"name": m.get("name"), "size": m.get("size"),
               "parameter_size": (m.get("details") or {}).get("parameter_size"),
               "quantization": (m.get("details") or {}).get("quantization_level"),
               "modified_at": m.get("modified_at")} for m in tags.get("models", [])]
    return {"connected": True, "host": s.ollama_host, "model": s.ollama_model,
            "model_installed": any(m["name"] == s.ollama_model for m in models),
            "models": models, "latency_ms": round((time.monotonic() - t) * 1000), "error": None}


@router.get("/diagnoses")
async def ai_diagnoses(limit: int = 100, include_test: bool = False, up: Upstream = Depends(get_upstream)):
    return (await load_diagnoses(up, include_test))[:limit]


@router.get("/summary")
async def ai_summary(include_test: bool = False, up: Upstream = Depends(get_upstream)):
    diagnoses = await load_diagnoses(up, include_test)
    timed = sorted(d["response_time_s"] for d in diagnoses if d.get("response_time_s") is not None)
    ai = [d for d in diagnoses if d["source"] == "LOCAL_OLLAMA_AI"]
    fallbacks = [d for d in diagnoses if d.get("fallback")]
    p95 = timed[min(len(timed) - 1, int(round(0.95 * (len(timed) - 1))))] if timed else None
    return {
        "total_calls": len(diagnoses), "ollama_diagnoses": len(ai), "fallbacks": len(fallbacks),
        "avg_response_time_s": round(sum(timed) / len(timed), 2) if timed else None,
        "p95_response_time_s": p95, "latest": diagnoses[0] if diagnoses else None,
        "latest_ollama": ai[0] if ai else None,
    }
