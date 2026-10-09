"""
Write path into the pipeline's Postgres business database.
"""
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import pandas as pd
import psycopg2.extras
from .connection import get_connection
from .schema import ensure_schema


def start_pipeline_run(run_id: str, dag_id: str, logical_date: str):
    conn = get_connection()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (run_id, dag_id, logical_date, status, started_at)
                VALUES (%s, %s, %s, 'RUNNING', now())
                ON CONFLICT (run_id) DO UPDATE SET status = 'RUNNING', started_at = now()
                """,
                (run_id, dag_id, logical_date),
            )
    finally:
        conn.close()


def record_quality_result(run_id: str, dataset: str, check_name: str, status: str, details: Optional[Dict[str, Any]] = None):
    conn = get_connection()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO quality_results (run_id, dataset, check_name, status, details) VALUES (%s, %s, %s, %s, %s)",
                (run_id, dataset, check_name, status, json.dumps(details or {}, default=str)),
            )
    finally:
        conn.close()


def record_incident(diag_report) -> None:
    """Upserts an IncidentReport (src.agent.incident.IncidentReport) into Postgres."""
    conn = get_connection()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO incidents (
                    incident_id, run_id, dataset, fault_category, severity, action, status,
                    confidence, hypothesis, remediation_status, verification_status, payload, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (incident_id) DO UPDATE SET
                    action = EXCLUDED.action, status = EXCLUDED.status,
                    remediation_status = EXCLUDED.remediation_status,
                    verification_status = EXCLUDED.verification_status,
                    payload = EXCLUDED.payload, updated_at = now()
                """,
                (
                    diag_report.incident_id, getattr(diag_report, "run_id", None), diag_report.dataset,
                    diag_report.evidence.get("failure_category", diag_report.check), diag_report.severity,
                    diag_report.action, diag_report.status, diag_report.confidence, diag_report.hypothesis,
                    diag_report.remediation_status, diag_report.verification_status,
                    json.dumps(diag_report.to_dict(), default=str),
                ),
            )
    finally:
        conn.close()


def load_orders(df: pd.DataFrame, run_id: str, logical_date: str):
    if df is None or len(df) == 0:
        return 0
    conn = get_connection()
    try:
        ensure_schema(conn)
        rows = [
            (
                r["order_id"], r["customer_id"], r["product_id"], r["order_ts"],
                int(r["quantity"]), float(r["order_total"]), r["status"], run_id, logical_date,
            )
            for _, r in df.iterrows()
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO orders (order_id, customer_id, product_id, order_ts, quantity, order_total, status, run_id, logical_date)
                VALUES %s
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id, product_id = EXCLUDED.product_id,
                    order_ts = EXCLUDED.order_ts, quantity = EXCLUDED.quantity,
                    order_total = EXCLUDED.order_total, status = EXCLUDED.status,
                    run_id = EXCLUDED.run_id, logical_date = EXCLUDED.logical_date
                """,
                rows,
            )
        return len(rows)
    finally:
        conn.close()


def load_events(df: pd.DataFrame, run_id: str, logical_date: str):
    if df is None or len(df) == 0:
        return 0
    conn = get_connection()
    try:
        ensure_schema(conn)
        rows = [
            (
                r["event_id"], r["customer_id"], r["event_type"], r["session_id"],
                r["event_ts"], run_id, logical_date,
            )
            for _, r in df.iterrows()
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO events (event_id, customer_id, event_type, session_id, event_ts, run_id, logical_date)
                VALUES %s
                ON CONFLICT (event_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id, event_type = EXCLUDED.event_type,
                    session_id = EXCLUDED.session_id, event_ts = EXCLUDED.event_ts,
                    run_id = EXCLUDED.run_id, logical_date = EXCLUDED.logical_date
                """,
                rows,
            )
        return len(rows)
    finally:
        conn.close()


def load_aggregated_metrics(metrics: Dict[str, Any], run_id: str, logical_date: str):
    conn = get_connection()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO aggregated_metrics (run_id, logical_date, metrics) VALUES (%s, %s, %s)",
                (run_id, logical_date, json.dumps(metrics, default=str)),
            )
    finally:
        conn.close()


def record_failed_pipeline_run(
    run_id: str, dag_id: str, logical_date: str, started_at=None,
    incident_count: int = 0, auto_fix_count: int = 0, escalation_count: int = 0,
):
    """Records a DAG run that ended in a task failure as FAILED. load_data
    (which normally creates the row) never runs when validation fails, so
    without this an escalated run would be invisible to the dashboard.
    row_counts stays NULL: nothing was loaded."""
    conn = get_connection()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, dag_id, logical_date, status, started_at, completed_at, duration_seconds,
                    incident_count, auto_fix_count, escalation_count
                ) VALUES (%s, %s, %s, 'FAILED', COALESCE(%s, now()), now(),
                          EXTRACT(EPOCH FROM (now() - COALESCE(%s, now()))), %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'FAILED', completed_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - pipeline_runs.started_at)),
                    incident_count = EXCLUDED.incident_count, auto_fix_count = EXCLUDED.auto_fix_count,
                    escalation_count = EXCLUDED.escalation_count
                """,
                (run_id, dag_id, logical_date, started_at, started_at, incident_count, auto_fix_count, escalation_count),
            )
    finally:
        conn.close()


def finish_pipeline_run(
    run_id: str, status: str, row_counts: Dict[str, Any],
    incident_count: int = 0, auto_fix_count: int = 0, escalation_count: int = 0,
):
    """Marks a pipeline run complete with its final HEALTHY/RECOVERED/FAILED status."""
    conn = get_connection()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs SET
                    status = %s, completed_at = now(),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
                    row_counts = %s, incident_count = %s, auto_fix_count = %s, escalation_count = %s
                WHERE run_id = %s
                """,
                (status, json.dumps(row_counts, default=str), incident_count, auto_fix_count, escalation_count, run_id),
            )
    finally:
        conn.close()
