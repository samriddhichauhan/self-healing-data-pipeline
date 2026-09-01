"""
Integration tests for FastAPI Backend API
"""
import os
import sys
import pytest

try:
    from fastapi.testclient import TestClient
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../apps/backend")))
    from main import app
    client = TestClient(app)
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    client = None


@pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi package not installed in local environment")
def test_health_endpoint():
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert "Self-Healing Pipeline API" in data["service"]


@pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi package not installed in local environment")
def test_pipeline_status_endpoint():
    res = client.get("/api/v1/pipeline/status")
    assert res.status_code == 200
    data = res.json()
    assert "pipeline_status" in data


@pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi package not installed in local environment")
def test_incidents_endpoints():
    res = client.get("/api/v1/incidents")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


@pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi package not installed in local environment")
def test_remediate_endpoint():
    res = client.post("/api/v1/incidents/INC-TEST-001/remediate", json={
        "dataset": "orders",
        "primary_key": "order_id",
        "execution_date": "2026-06-01"
    })
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "REMEDIATED"
    assert "verification" in data
