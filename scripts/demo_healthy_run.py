#!/usr/bin/env python3
"""
Manager demo #1: healthy pipeline run.

Triggers a REAL Airflow DAG run inside the Docker container (no manual UI
action, no direct DAG import/execution on Windows), polls the real Airflow
scheduler until it finishes, independently queries Airflow's own task
states, and independently queries the real Postgres database the pipeline
loaded. Fails loudly if anything is not exactly right — never prints
SUCCESS unless every check actually passed.

Usage: python scripts/demo_healthy_run.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _pipeline_common import (
    reset_demo_state, canonical_dataset_present, trigger_dag, wait_for_dag_run,
    get_task_states, new_run_id, CANONICAL_DATE, DAG_ID,
)

EXPECTED_TASKS = [
    "ingest_dimensions", "ingest_orders", "ingest_events",
    "validate_schema", "validate_quality", "transform_data", "load_data", "agent_monitoring",
]


def main():
    print("=" * 70)
    print("  MANAGER DEMO 1: HEALTHY PIPELINE RUN (REAL AIRFLOW, DOCKER)")
    print("=" * 70)

    if not canonical_dataset_present():
        print(f"[FATAL] Canonical dataset for {CANONICAL_DATE} is missing. Run scripts/reset_demo.py first.")
        sys.exit(1)

    reset_demo_state()

    run_id = new_run_id("healthy_demo")
    print(f"\n[TRIGGER] Triggering real Airflow DAG run '{run_id}' for {DAG_ID} @ {CANONICAL_DATE}...")
    trigger_dag(run_id, CANONICAL_DATE)

    print("\n[WAIT] Polling the real Airflow scheduler until the run finishes...")
    dag_state = wait_for_dag_run(run_id, timeout_seconds=900)
    print(f"\n[RESULT] DAG run '{run_id}' finished with state: {dag_state}")

    print("\n[VERIFY] Independently querying Airflow task states for this run...")
    task_states = get_task_states(run_id)
    all_success = True
    for t in EXPECTED_TASKS:
        state = task_states.get(t, "MISSING")
        ok = state == "success"
        all_success = all_success and ok
        print(f"  [{'OK' if ok else 'FAIL'}] {t}: {state}")

    print("\n[VERIFY] Independently querying Postgres for the final pipeline_runs row...")
    try:
        from src.database.queries import get_pipeline_run
        run_row = get_pipeline_run(run_id)
    except Exception as e:
        print(f"[FATAL] Could not query Postgres: {e}")
        sys.exit(1)

    if not run_row:
        print(f"[FATAL] No pipeline_runs row found for '{run_id}' — load_data/agent_monitoring did not complete.")
        sys.exit(1)

    print(f"  pipeline_runs.status = {run_row['status']}")
    print(f"  pipeline_runs.row_counts = {run_row['row_counts']}")
    print(f"  pipeline_runs.incident_count = {run_row['incident_count']}")

    if not (all_success and dag_state == "success" and run_row["status"] == "HEALTHY"):
        print("\n[FAILED] Healthy demo did NOT pass all checks. See failures above.")
        sys.exit(1)

    print("\n" + "=" * 70)
    print(f"  RESULT: 8/8 TASKS SUCCESS — PIPELINE STATUS: HEALTHY")
    print(f"  Run ID: {run_id}")
    print("=" * 70)


if __name__ == "__main__":
    main()
