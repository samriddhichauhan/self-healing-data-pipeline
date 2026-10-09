"""
HTTP clients for the three upstreams. Every call has an explicit timeout and
raises UpstreamError (never returns fabricated data) when an upstream fails.
"""
import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

import httpx

from .config import Settings


class UpstreamError(Exception):
    def __init__(self, service: str, detail: str, status_code: Optional[int] = None):
        super().__init__(f"{service}: {detail}")
        self.service = service
        self.detail = detail
        self.status_code = status_code


class TTLCache:
    """Tiny in-process cache so several widgets polling at once don't fan out
    into duplicate upstream calls."""

    def __init__(self):
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def get_or_set(self, key: str, ttl: float, fn: Callable[[], Awaitable[Any]]) -> Any:
        hit = self._store.get(key)
        if hit and hit[0] > time.monotonic():
            return hit[1]
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            hit = self._store.get(key)
            if hit and hit[0] > time.monotonic():
                return hit[1]
            value = await fn()
            self._store[key] = (time.monotonic() + ttl, value)
            return value

    def invalidate(self, prefix: str = "") -> None:
        for key in [k for k in self._store if k.startswith(prefix)]:
            del self._store[key]


class Upstream:
    def __init__(self, settings: Settings, transport: Optional[httpx.AsyncBaseTransport] = None):
        self.settings = settings
        self.http = httpx.AsyncClient(transport=transport, timeout=10.0)
        self.cache = TTLCache()

    async def close(self) -> None:
        await self.http.aclose()

    async def _request(self, service: str, method: str, url: str, *, timeout: float = 10.0,
                       expect_json: bool = True, **kwargs) -> Any:
        try:
            resp = await self.http.request(method, url, timeout=timeout, **kwargs)
        except httpx.TimeoutException:
            raise UpstreamError(service, f"timed out after {timeout:.0f}s ({url})")
        except httpx.HTTPError as e:
            raise UpstreamError(service, f"unreachable ({type(e).__name__}: {e})")
        if resp.status_code >= 400:
            detail = resp.text[:500]
            try:
                body = resp.json()
                detail = body.get("detail") or body.get("title") or detail
            except Exception:
                pass
            raise UpstreamError(service, f"HTTP {resp.status_code}: {detail}", resp.status_code)
        return resp.json() if expect_json else resp.text

    # --- existing pipeline backend (apps/backend, FastAPI) -------------------
    async def backend(self, path: str, method: str = "GET", *, timeout: float = 15.0, **kwargs) -> Any:
        return await self._request("backend", method, f"{self.settings.backend_url}{path}", timeout=timeout, **kwargs)

    # --- Airflow REST API ------------------------------------------------------
    async def airflow(self, path: str, method: str = "GET", *, timeout: float = 15.0,
                      expect_json: bool = True, **kwargs) -> Any:
        auth = (self.settings.airflow_user, self.settings.airflow_password)
        return await self._request("airflow", method, f"{self.settings.airflow_api}{path}",
                                   timeout=timeout, auth=auth, expect_json=expect_json, **kwargs)

    async def airflow_health(self) -> Dict[str, Any]:
        return await self._request("airflow", "GET", f"{self.settings.airflow_url}/health", timeout=5.0)

    # --- Ollama ----------------------------------------------------------------
    async def ollama_tags(self) -> Dict[str, Any]:
        return await self._request("ollama", "GET", f"{self.settings.ollama_host}/api/tags", timeout=4.0)

    # --- cached composites used by several endpoints ---------------------------
    async def pipeline_runs(self) -> list:
        return await self.cache.get_or_set(
            "backend:runs", 5, lambda: self.backend("/api/v1/pipeline/runs", params={"limit": 1000}))

    async def incidents(self) -> list:
        return await self.cache.get_or_set(
            "backend:incidents", 5, lambda: self.backend("/api/v1/incidents", params={"limit": 1000}))

    async def ai_log_lines(self) -> list:
        async def fetch():
            data = await self.backend(f"/api/v1/logs/{self.settings.ai_log_file}", params={"tail": 0}, timeout=20.0)
            return data.get("lines", [])
        return await self.cache.get_or_set("backend:ai_log", 10, fetch)

    async def dag_runs(self, limit: int = 500) -> list:
        async def fetch():
            runs, offset = [], 0
            while len(runs) < limit:
                page = await self.airflow(
                    f"/dags/{self.settings.dag_id}/dagRuns",
                    params={"limit": min(100, limit - len(runs)), "offset": offset, "order_by": "-start_date"},
                )
                batch = page.get("dag_runs", [])
                runs.extend(batch)
                offset += len(batch)
                if not batch or offset >= page.get("total_entries", 0):
                    break
            return runs
        return await self.cache.get_or_set(f"airflow:runs:{limit}", 5, fetch)
