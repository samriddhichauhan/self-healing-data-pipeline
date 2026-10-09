"""
SelfHeal AI platform API.

A separate service in front of the existing system. It only calls existing
interfaces (pipeline backend REST, Airflow REST, Ollama /api/tags) and never
imports or modifies pipeline code.

    uvicorn selfheal_api.main:app --port 8001
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .clients import Upstream, UpstreamError
from .config import settings
from .routers import ai, analytics, incidents, logs, runs, system, trigger


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not hasattr(app.state, "upstream"):
        app.state.upstream = Upstream(settings)
    yield
    await app.state.upstream.close()


app = FastAPI(title="SelfHeal AI Platform API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.exception_handler(UpstreamError)
async def upstream_error(_: Request, exc: UpstreamError):
    return JSONResponse(status_code=502, content={"detail": f"{exc.service} unavailable: {exc.detail}", "service": exc.service})


@app.get("/api/ping")
def ping():
    return {"status": "ok", "service": "selfheal-platform-api"}


for r in (system, runs, incidents, ai, logs, analytics, trigger):
    app.include_router(r.router)
