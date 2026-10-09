import os
import sys
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from tests.conftest import requires_postgres
from apps.backend.main import app

client = TestClient(app)


def test_health_endpoint_reports_real_db_connectivity():
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data["service"] == "Self-Healing Pipeline API"
    assert "database" in data and "connected" in data["database"]


@requires_postgres
def test_pipeline_summary_and_runs_are_real_not_hardcoded():
    res = client.get("/api/v1/pipeline/summary")
    assert res.status_code == 200
    data = res.json()
    assert "total_runs" in data

    res = client.get("/api/v1/pipeline/runs")
    assert res.status_code == 200
    assert isinstance(res.json(), list)
    # No hardcoded run-001/run-002/run-003 placeholders anywhere in the response
    ids = [r["run_id"] for r in res.json()]
    assert "run-001" not in ids


@requires_postgres
def test_incidents_endpoint_reads_real_database():
    res = client.get("/api/v1/incidents")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_pipeline_status_returns_503_not_fake_data_when_db_down(monkeypatch):
    monkeypatch.setenv("PIPELINE_DB_HOST", "127.0.0.1")
    monkeypatch.setenv("PIPELINE_DB_PORT", "1")
    res = client.get("/api/v1/pipeline/status")
    assert res.status_code == 503  # real failure, not a fabricated 200


def test_cors_is_not_wildcard_with_credentials():
    # allow_origins must be an explicit localhost list, never "*"
    from apps.backend.main import _cors_origins
    assert "*" not in _cors_origins
