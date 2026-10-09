"""
Read path — used by the FastAPI backend to serve real dashboard data.
"""
from typing import List, Dict, Any, Optional
from .connection import get_connection, dict_cursor
from .schema import ensure_schema


def list_pipeline_runs(limit: int = 20) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        ensure_schema(conn)
        with dict_cursor(conn) as cur:
            cur.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT %s", (limit,)
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_pipeline_run(run_id: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        ensure_schema(conn)
        with dict_cursor(conn) as cur:
            cur.execute("SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_latest_pipeline_run() -> Optional[Dict[str, Any]]:
    runs = list_pipeline_runs(limit=1)
    return runs[0] if runs else None


def list_quality_results(run_id: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        ensure_schema(conn)
        with dict_cursor(conn) as cur:
            cur.execute("SELECT * FROM quality_results WHERE run_id = %s ORDER BY created_at", (run_id,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def list_incidents(limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        ensure_schema(conn)
        with dict_cursor(conn) as cur:
            cur.execute("SELECT * FROM incidents ORDER BY created_at DESC LIMIT %s", (limit,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_aggregated_metrics(run_id: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    try:
        ensure_schema(conn)
        with dict_cursor(conn) as cur:
            cur.execute("SELECT * FROM aggregated_metrics WHERE run_id = %s ORDER BY created_at DESC LIMIT 1", (run_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_pipeline_summary() -> Dict[str, Any]:
    """Aggregate counts across all runs, for the dashboard summary card."""
    conn = get_connection()
    try:
        ensure_schema(conn)
        with dict_cursor(conn) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_runs,
                    COUNT(*) FILTER (WHERE status = 'HEALTHY') AS healthy_runs,
                    COUNT(*) FILTER (WHERE status = 'RECOVERED') AS recovered_runs,
                    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_runs,
                    COALESCE(SUM(incident_count), 0) AS total_incidents,
                    COALESCE(SUM(auto_fix_count), 0) AS total_auto_fixes,
                    COALESCE(SUM(escalation_count), 0) AS total_escalations
                FROM pipeline_runs
                """
            )
            row = dict(cur.fetchone())
        latest = get_latest_pipeline_run()
        row["latest_run"] = latest
        return row
    finally:
        conn.close()
