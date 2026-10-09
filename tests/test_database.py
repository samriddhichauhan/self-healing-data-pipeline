"""
Tests against the real pipeline Postgres database. Skipped automatically if
Postgres isn't reachable (e.g. `docker compose up -d postgres` hasn't been
run) — never mocked, because the whole point is verifying the real DB
contract (Phase 14 of the spec: real tables, real loads, real reads).
"""
import uuid
import pandas as pd
from tests.conftest import requires_postgres


@requires_postgres
def test_schema_is_idempotent():
    from src.database.schema import ensure_schema
    ensure_schema()
    ensure_schema()  # must not raise on a second call


@requires_postgres
def test_pipeline_run_lifecycle():
    from src.database.loader import start_pipeline_run, finish_pipeline_run
    from src.database.queries import get_pipeline_run

    run_id = f"test_run__{uuid.uuid4().hex[:8]}"
    start_pipeline_run(run_id=run_id, dag_id="self_healing_pipeline", logical_date="2026-06-01")

    row = get_pipeline_run(run_id)
    assert row is not None
    assert row["status"] == "RUNNING"

    finish_pipeline_run(run_id=run_id, status="HEALTHY", row_counts={"orders": 300}, incident_count=0)
    row = get_pipeline_run(run_id)
    assert row["status"] == "HEALTHY"
    assert row["row_counts"]["orders"] == 300


@requires_postgres
def test_load_orders_and_events_upsert():
    from src.database.loader import start_pipeline_run, load_orders, load_events
    from src.database.connection import get_connection

    run_id = f"test_run__{uuid.uuid4().hex[:8]}"
    start_pipeline_run(run_id=run_id, dag_id="self_healing_pipeline", logical_date="2026-06-01")

    df_o = pd.DataFrame([{
        "order_id": f"TESTORD-{uuid.uuid4().hex[:8]}", "customer_id": "C1", "product_id": "P1",
        "order_ts": "2026-06-01 12:00:00", "quantity": 1, "order_total": 9.99, "status": "placed",
    }])
    n = load_orders(df_o, run_id=run_id, logical_date="2026-06-01")
    assert n == 1

    df_e = pd.DataFrame([{
        "event_id": f"TESTEVT-{uuid.uuid4().hex[:8]}", "customer_id": "C1", "event_type": "page_view",
        "session_id": "S1", "event_ts": "2026-06-01 12:00:00",
    }])
    n = load_events(df_e, run_id=run_id, logical_date="2026-06-01")
    assert n == 1

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (df_o.iloc[0]["order_id"],))
        assert cur.fetchone() is not None
    conn.close()


@requires_postgres
def test_record_and_query_incident():
    from src.agent.incident import IncidentReport
    from src.database.loader import record_incident
    from src.database.queries import list_incidents

    incident_id = f"INC-TEST-{uuid.uuid4().hex[:8]}"
    report = IncidentReport(
        incident_id=incident_id, timestamp="2026-06-01", pipeline="self_healing_pipeline",
        dataset="orders", check="validate_quality", severity="MEDIUM", observed=350, expected=300,
        evidence={"failure_category": "DUPLICATE_INGESTION"}, hypothesis="test hypothesis",
        confidence=0.95, blast_radius="orders", action="AUTO_FIX", status="REMEDIATED",
        remediation_status="SUCCESS", verification_status="PASSED",
    )
    record_incident(report)

    incidents = list_incidents(limit=500)
    assert any(i["incident_id"] == incident_id for i in incidents)
