#!/usr/bin/env python3
"""
Manager demo #2: self-healing pipeline run.

GOOD DATA -> FAULT INJECTION (duplicates) -> QUALITY FAILURE -> AI DIAGNOSIS
-> POLICY APPROVAL -> REMEDIATION -> REVALIDATION -> TRANSFORM -> LOAD ->
MONITOR -> RECOVERED — all executed for real inside the Airflow container,
verified independently via Airflow's own task states and Postgres.

Usage: python scripts/demo_self_healing_run.py
"""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fault-injection"))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _pipeline_common import (
    reset_demo_state, canonical_dataset_present, trigger_dag, wait_for_dag_run,
    get_task_states, new_run_id, CANONICAL_DATE, DAG_ID, DATA_DIR,
)
from inject_fault import inject_fault

EXPECTED_TASKS = [
    "ingest_dimensions", "ingest_orders", "ingest_events",
    "validate_schema", "validate_quality", "transform_data", "load_data", "agent_monitoring",
]


def main():
    print("=" * 70)
    print("  MANAGER DEMO 2: SELF-HEALING PIPELINE RUN (REAL AIRFLOW, DOCKER)")
    print("=" * 70)

    if not canonical_dataset_present():
        print(f"[FATAL] Canonical dataset for {CANONICAL_DATE} is missing. Run scripts/reset_demo.py first.")
        sys.exit(1)

    reset_demo_state()

    raw_orders_path = os.path.join(DATA_DIR, "raw", "orders", f"orders_{CANONICAL_DATE}.csv")
    original_count = len(pd.read_csv(raw_orders_path))
    print(f"\n[BASELINE] Canonical raw orders for {CANONICAL_DATE}: {original_count} rows")

    print("\n[FAULT INJECTION] Injecting duplicate_ingestion fault into staged orders...")
    msg = inject_fault("duplicate_ingestion", DATA_DIR, CANONICAL_DATE)
    print(f"  {msg}")

    stg_path = os.path.join(DATA_DIR, "staging", f"stg_orders_{CANONICAL_DATE}.csv")
    df_staged = pd.read_csv(stg_path)
    injected_count = len(df_staged)
    dup_count = int(df_staged["order_id"].duplicated().sum())
    print(f"[VERIFY FAULT] Staged orders now have {injected_count} rows, {dup_count} duplicate order_id values.")
    if dup_count == 0:
        print("[FATAL] Fault injection did not actually create duplicates — aborting.")
        sys.exit(1)

    run_id = new_run_id("selfheal_demo")
    print(f"\n[TRIGGER] Triggering real Airflow DAG run '{run_id}' for {DAG_ID} @ {CANONICAL_DATE}...")
    trigger_dag(run_id, CANONICAL_DATE)

    print("\n[WAIT] Polling the real Airflow scheduler (this run detects the fault, diagnoses,")
    print("       remediates, re-verifies, and continues — expect this to take longer)...")
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

    print("\n[VERIFY] Independently querying Postgres for the final pipeline_runs row + incident...")
    try:
        from src.database.queries import get_pipeline_run, list_incidents
        run_row = get_pipeline_run(run_id)
        all_incidents = list_incidents(limit=200)
    except Exception as e:
        print(f"[FATAL] Could not query Postgres: {e}")
        sys.exit(1)

    if not run_row:
        print(f"[FATAL] No pipeline_runs row found for '{run_id}'.")
        sys.exit(1)

    run_incidents = [i for i in all_incidents if i.get("run_id") == run_id]
    print(f"  pipeline_runs.status = {run_row['status']}")
    print(f"  pipeline_runs.row_counts = {run_row['row_counts']}")
    print(f"  pipeline_runs.auto_fix_count = {run_row['auto_fix_count']}")
    print(f"  incidents recorded for this run: {len(run_incidents)}")

    if not run_incidents:
        print("[FAILED] No incident was recorded for this run — self-healing did not trigger as expected.")
        sys.exit(1)

    incident = run_incidents[0]
    payload = incident.get("payload", {})
    remediation = payload.get("remediation") or {}
    verification = payload.get("verification") or {}

    print("\n[INCIDENT REPORT]")
    print(f"  Incident ID: {incident['incident_id']}")
    print(f"  Diagnosis source: {payload.get('evidence', {}).get('ai_mode', 'unknown')}")
    print(f"  Hypothesis: {incident['hypothesis']}")
    print(f"  Confidence: {incident['confidence']:.2f}")
    print(f"  Policy Gate action: {incident['action']}")
    print(f"  Remediation status: {incident['remediation_status']} — removed {remediation.get('removed_duplicates', '?')} duplicates "
          f"({remediation.get('initial_rows', '?')} -> {remediation.get('cleaned_rows', '?')} rows)")
    print(f"  Verification status: {incident['verification_status']} — duplicate_count={verification.get('duplicate_count', '?')}, "
          f"remaining_rows={verification.get('remaining_rows', '?')}")

    print("\n[BEFORE / AFTER]")
    print(f"  Before remediation: {injected_count} rows ({dup_count} duplicates)")
    print(f"  After remediation:  {remediation.get('cleaned_rows', '?')} rows (0 duplicates)")

    success = (
        all_success and dag_state == "success"
        and run_row["status"] == "RECOVERED"
        and incident["status"] == "REMEDIATED"
        and verification.get("duplicate_count") == 0
    )

    if not success:
        print("\n[FAILED] Self-healing demo did NOT pass all checks. See failures above.")
        sys.exit(1)

    print("\n" + "=" * 70)
    print(f"  RESULT: 8/8 TASKS SUCCESS — PIPELINE STATUS: RECOVERED")
    print(f"  Run ID: {run_id}")
    print(f"  Incident ID: {incident['incident_id']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
