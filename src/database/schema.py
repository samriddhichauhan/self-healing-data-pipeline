"""
DDL for the pipeline's Postgres business database. Idempotent — safe to run
on every task invocation (CREATE TABLE IF NOT EXISTS only).
"""
from .connection import get_connection

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id            TEXT PRIMARY KEY,
        dag_id            TEXT NOT NULL,
        logical_date      DATE NOT NULL,
        status            TEXT NOT NULL DEFAULT 'RUNNING',
        started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        completed_at      TIMESTAMPTZ,
        duration_seconds  DOUBLE PRECISION,
        row_counts        JSONB,
        incident_count    INTEGER NOT NULL DEFAULT 0,
        auto_fix_count    INTEGER NOT NULL DEFAULT 0,
        escalation_count  INTEGER NOT NULL DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS quality_results (
        id          SERIAL PRIMARY KEY,
        run_id      TEXT NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
        dataset     TEXT NOT NULL,
        check_name  TEXT NOT NULL,
        status      TEXT NOT NULL,
        details     JSONB,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS incidents (
        incident_id          TEXT PRIMARY KEY,
        -- No FK to pipeline_runs on purpose: incidents can be (and normally
        -- are) recorded by validate_quality/on_task_failure before the
        -- pipeline_runs row exists (that row is only created in load_data).
        run_id               TEXT,
        dataset              TEXT,
        fault_category       TEXT,
        severity             TEXT,
        action               TEXT,
        status               TEXT,
        confidence           DOUBLE PRECISION,
        hypothesis           TEXT,
        remediation_status   TEXT,
        verification_status  TEXT,
        payload              JSONB,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id      TEXT PRIMARY KEY,
        customer_id   TEXT,
        product_id    TEXT,
        order_ts      TIMESTAMPTZ,
        quantity      INTEGER,
        order_total   DOUBLE PRECISION,
        status        TEXT,
        run_id        TEXT,
        logical_date  DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id      TEXT PRIMARY KEY,
        customer_id   TEXT,
        event_type    TEXT,
        session_id    TEXT,
        event_ts      TIMESTAMPTZ,
        run_id        TEXT,
        logical_date  DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS aggregated_metrics (
        id            SERIAL PRIMARY KEY,
        run_id        TEXT REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
        logical_date  DATE,
        metrics       JSONB,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
]


def ensure_schema(conn=None):
    """Creates all pipeline tables if they don't already exist."""
    owns_conn = conn is None
    conn = conn or get_connection()
    try:
        with conn.cursor() as cur:
            for stmt in DDL_STATEMENTS:
                cur.execute(stmt)
    finally:
        if owns_conn:
            conn.close()
