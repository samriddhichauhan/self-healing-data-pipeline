"""
Full task-chain scenario tests: ingestion -> schema -> quality -> transform
-> load -> monitoring, run directly against src/ (no Docker/Airflow
required — the real Airflow execution path is exercised separately by
scripts/demo_healthy_run.py and scripts/demo_self_healing_run.py against
the live containers).
"""
import os
import uuid
import pandas as pd
import pytest
from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events
from src.validation.schema import validate_schema
from src.validation.quality import validate_quality
from src.transformation.transform import transform_data
from tests.conftest import requires_postgres


class _FakeTaskInstance:
    task_id = "validate_quality"
    try_number = 1


class _FakeDagRun:
    def __init__(self, run_id):
        self.dag_id = "self_healing_pipeline"
        self.run_id = run_id


def _full_chain_context(run_id):
    return {"ds": "2026-06-01", "task_instance": _FakeTaskInstance(), "dag_run": _FakeDagRun(run_id)}


def test_database_unavailable_fails_loudly_at_load(pipeline_env, monkeypatch):
    """Spec test 7: database unavailable -> clear failure, never a fake success."""
    monkeypatch.setenv("PIPELINE_DB_HOST", "127.0.0.1")
    monkeypatch.setenv("PIPELINE_DB_PORT", "1")  # nothing listens here
    from src.database.load_task import load_data

    ctx = {"ds": "2026-06-01", "dag_run": _FakeDagRun("test_run")}
    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)
    validate_schema(**ctx)
    validate_quality(**ctx)
    transform_data(**ctx)

    with pytest.raises(ConnectionError, match="Could not connect to pipeline Postgres"):
        load_data(**ctx)


@requires_postgres
def test_healthy_scenario_end_to_end_reports_healthy(pipeline_env):
    from src.database.load_task import load_data
    from src.monitoring.monitor import agent_monitoring
    from src.database.queries import get_pipeline_run

    run_id = f"test_healthy__{uuid.uuid4().hex[:8]}"
    ctx = _full_chain_context(run_id)

    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)
    validate_schema(**ctx)
    validate_quality(**ctx)
    transform_data(**ctx)
    load_data(**ctx)
    agent_monitoring(**ctx)

    run_row = get_pipeline_run(run_id)
    assert run_row is not None
    assert run_row["status"] == "HEALTHY"
    assert run_row["row_counts"]["orders"] == 300


@requires_postgres
def test_duplicate_ingestion_self_heals_end_to_end_reports_recovered(pipeline_env):
    from src.database.load_task import load_data
    from src.monitoring.monitor import agent_monitoring
    from src.database.queries import get_pipeline_run

    run_id = f"test_selfheal__{uuid.uuid4().hex[:8]}"
    ctx = _full_chain_context(run_id)

    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)

    stg_path = os.path.join(str(pipeline_env), "staging", "stg_orders_2026-06-01.csv")
    df = pd.read_csv(stg_path)
    pd.concat([df, df.head(50)], ignore_index=True).to_csv(stg_path, index=False)

    validate_schema(**ctx)
    validate_quality(**ctx)  # self-heals inline, does not raise
    transform_data(**ctx)
    load_data(**ctx)
    agent_monitoring(**ctx)

    run_row = get_pipeline_run(run_id)
    assert run_row["status"] == "RECOVERED"
    assert run_row["auto_fix_count"] == 1
    assert run_row["row_counts"]["orders"] == 300  # deduplicated back to 300
