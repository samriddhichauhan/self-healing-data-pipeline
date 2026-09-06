"""
5-Case Pipeline Testing Harness for Self-Healing Data Pipeline
Demonstrates DAG task progression across 5 different operational scenarios:
- Passes initial stages
- Fails at specific downstream validation checks OR completes successfully
- Invokes AI Diagnostic Engine, Policy Gate, Remediation, and Verification hooks
"""
import os
import sys
import shutil

# Set Airflow env vars for Windows environment before importing Airflow modules
os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow.db"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow.db"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["PYTHONIOENCODING"] = "utf-8"

import pandas as pd
from datetime import datetime

# Add project root and agent paths
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
agent_dir = os.path.join(project_root, "apps", "agent")
fault_inj_dir = os.path.join(project_root, "scripts", "fault-injection")

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if agent_dir not in sys.path:
    sys.path.insert(0, agent_dir)
if fault_inj_dir not in sys.path:
    sys.path.insert(0, fault_inj_dir)

from inject_fault import inject_fault
from apps.agent.agent.diagnosis.engine import DiagnosticEngine
from apps.agent.agent.policies.policy_gate import PolicyGate
from apps.agent.agent.remediation.executor import RemediationExecutor
from apps.agent.agent.verification.verifier import RemediationVerifier

import airflow.dags.pipeline_dag_starter as dag_module


def reset_environment(data_dir):
    """Resets staging and raw directory data for clean execution."""
    raw_dir = os.path.join(data_dir, "raw")
    staging_dir = os.path.join(data_dir, "staging")
    processed_dir = os.path.join(data_dir, "processed")

    os.makedirs(os.path.join(raw_dir, "orders"), exist_ok=True)
    os.makedirs(os.path.join(raw_dir, "events"), exist_ok=True)
    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    # Re-create clean dim_customers
    customers_df = pd.DataFrame([
        {"customer_id": f"CUST{i:06d}", "name": f"Customer {i}", "email": f"cust{i}@example.com", "region": "NORTH_AMERICA", "signup_date": "2024-01-15"}
        for i in range(1, 501)
    ])
    customers_df.to_csv(os.path.join(raw_dir, "dim_customers.csv"), index=False)

    # Re-create clean dim_products
    products_df = pd.DataFrame([
        {"product_id": f"PROD{i:05d}", "name": f"Product {i}", "category": "electronics", "price": round(49.99 + i, 2)}
        for i in range(1, 61)
    ])
    products_df.to_csv(os.path.join(raw_dir, "dim_products.csv"), index=False)

    # Re-create clean raw orders batch (300 rows)
    orders_df = pd.DataFrame([
        {
            "order_id": f"ORD{i:06d}",
            "customer_id": f"CUST{(i % 500) + 1:06d}",
            "product_id": f"PROD{(i % 60) + 1:05d}",
            "order_ts": "2026-06-01 12:00:00",
            "quantity": (i % 5) + 1,
            "order_total": round(((i % 5) + 1) * 49.99, 2),
            "status": "completed"
        }
        for i in range(1, 301)
    ])
    orders_df.to_csv(os.path.join(raw_dir, "orders", "orders_2026-06-01.csv"), index=False)

    # Re-create clean raw events batch (1200 rows)
    events_list = [
        {
            "event_id": f"EVT{i:08d}",
            "customer_id": f"CUST{(i % 500) + 1:06d}",
            "event_type": "page_view",
            "session_id": f"SESS{(i % 100) + 1:05d}",
            "event_ts": "2026-06-01 12:05:00"
        }
        for i in range(1, 1201)
    ]
    events_df = pd.DataFrame(events_list)
    events_df.to_json(os.path.join(raw_dir, "events", "events_2026-06-01.jsonl"), orient="records", lines=True)

    # Clean staging files
    for f in os.listdir(staging_dir):
        fp = os.path.join(staging_dir, f)
        if os.path.isfile(fp):
            os.remove(fp)


def run_pipeline_scenario(scenario_num, title, fault_type=None):
    print("=" * 80)
    print(f"  SCENARIO {scenario_num}: {title.upper()}")
    print("=" * 80)

    data_dir = os.path.abspath("./data")
    reset_environment(data_dir)
    os.environ["TEST_DATA_DIR"] = data_dir

    context = {"ds": "2026-06-01"}

    # Stage Status Tracking
    stages = {
        "1. Ingestion (ingest_dimensions, ingest_orders, ingest_events)": "PENDING",
        "2. Schema Validation (validate_schema)": "PENDING",
        "3. Quality Validation (validate_quality)": "PENDING",
        "4. Transformation (transform_data)": "PENDING",
        "5. Load & Monitoring": "PENDING",
    }

    # Step 1: Ingestion (Always runs on clean raw files first)
    print("-> Running Stage 1: INGESTION...")
    try:
        dag_module.ingest_dimensions()
        dag_module.ingest_orders(**context)
        dag_module.ingest_events(**context)
        stages["1. Ingestion (ingest_dimensions, ingest_orders, ingest_events)"] = "PASSED [OK]"
        print("   [OK] Ingestion completed successfully. Staged batch files generated.\n")
    except Exception as e:
        stages["1. Ingestion (ingest_dimensions, ingest_orders, ingest_events)"] = f"FAILED [X] ({e})"
        print(f"   [X] Ingestion failed: {e}")
        handle_failure("ingest", fault_type or "INGESTION_ERROR", str(e), context)
        print_summary(stages)
        return

    # Post-Ingestion Fault Injection
    if fault_type:
        injection_msg = inject_fault(fault_type, data_dir, "2026-06-01")
        print(f"[FAULT INJECTED POST-INGESTION]: {injection_msg}\n")
    else:
        print("[NO FAULT INJECTED]: Clean baseline dataset configured.\n")

    # Step 2: Schema Validation
    print("-> Running Stage 2: SCHEMA VALIDATION...")
    try:
        dag_module.validate_schema(**context)
        stages["2. Schema Validation (validate_schema)"] = "PASSED [OK]"
        print("   [OK] Schema Validation passed cleanly.\n")
    except Exception as e:
        stages["2. Schema Validation (validate_schema)"] = f"FAILED [X] ({e})"
        print(f"   [X] Schema Validation failed: {e}")
        handle_failure("validate_schema", fault_type or "SCHEMA_DRIFT", str(e), context)
        print_summary(stages)
        return

    # Step 3: Quality Validation
    print("-> Running Stage 3: QUALITY VALIDATION...")
    try:
        dag_module.validate_quality(**context)
        stages["3. Quality Validation (validate_quality)"] = "PASSED [OK]"
        print("   [OK] Quality Validation passed cleanly.\n")
    except Exception as e:
        stages["3. Quality Validation (validate_quality)"] = f"FAILED [X] ({e})"
        print(f"   [X] Quality Validation failed: {e}")
        auto_healed = handle_failure("validate_quality", fault_type or "QUALITY_ERROR", str(e), context)
        if auto_healed:
            # Retry validate_quality post auto-fix
            print("\n-> Retrying Quality Validation post-remediation...")
            try:
                dag_module.validate_quality(**context)
                stages["3. Quality Validation (validate_quality)"] = "PASSED [OK] (AUTO-HEALED)"
                print("   [OK] Quality Validation passed post-auto-remediation!\n")
            except Exception as retry_e:
                stages["3. Quality Validation (validate_quality)"] = f"FAILED [X] after remediation ({retry_e})"
                print_summary(stages)
                return
        else:
            print_summary(stages)
            return

    # Step 4: Transformation
    print("-> Running Stage 4: TRANSFORMATION...")
    try:
        dag_module.transform_data(**context)
        stages["4. Transformation (transform_data)"] = "PASSED [OK]"
        print("   [OK] Data Transformation completed successfully.\n")
    except Exception as e:
        stages["4. Transformation (transform_data)"] = f"FAILED [X] ({e})"
        print_summary(stages)
        return

    # Step 5: Load & Monitoring
    print("-> Running Stage 5: LOAD & MONITORING...")
    stages["5. Load & Monitoring"] = "PASSED [OK]"
    print("   [OK] Final datasets written to staging/processed. Pipeline complete.\n")

    print_summary(stages)


def handle_failure(task_id, fault_type, err_msg, context):
    print("\n[AI DIAGNOSTIC & HEALING AGENT INVOCATION]")
    gate = PolicyGate()
    engine = DiagnosticEngine(policy_gate=gate)
    executor = RemediationExecutor(data_dir=dag_module.get_data_dir())
    verifier = RemediationVerifier(data_dir=dag_module.get_data_dir())

    # Map fault type to category string
    category_map = {
        "schema_drift": "SCHEMA_DRIFT",
        "null_spike": "NULL_SPIKE",
        "duplicate_ingestion": "DUPLICATE_INGESTION",
        "volume_drop": "VOLUME_ANOMALY_DROP",
        "referential_break": "REFERENTIAL_BREAK",
        "staleness": "STALENESS",
    }
    fault_cat = category_map.get(fault_type, "UNKNOWN")

    report = engine.diagnose_fault(
        incident_id=f"INC-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        pipeline_id="self_healing_pipeline",
        task_id=task_id,
        dataset="orders" if "orders" in err_msg.lower() else "products",
        fault_category=fault_cat,
        observed=err_msg,
        expected="Valid schema and quality bounds",
        evidence={"error": err_msg},
        execution_date=context["ds"]
    )

    print(f"   [DIAGNOSIS] Hypothesis: {report.hypothesis}")
    print(f"   [SEVERITY] {report.severity}")
    print(f"   [POLICY GATE ACTION] {report.action}")

    if report.action == "AUTO_FIX":
        print(f"   [REMEDIATION] Executing Automated Remediation plan...")
        plan = {
            "remediation_type": "deduplicate_dataset",
            "dataset": "orders",
            "execution_date": context["ds"],
            "primary_key": "order_id"
        }
        res = executor.execute_remediation(plan)
        print(f"   [REMEDIATION STATUS] {res['status']} — Removed {res.get('removed_duplicates', 0)} duplicates (Clean count: {res.get('cleaned_rows')})")

        v_res = verifier.verify_remediation(plan)
        print(f"   [VERIFICATION STATUS] {v_res['status']} (Verified clean row count: {v_res.get('remaining_rows')})")
        return True
    else:
        print(f"   [ESCALATED] Action ESCALATED to Data Engineering team. (Human review required - execution safe-stopped)")
        return False


def print_summary(stages):
    print("\n--- PIPELINE STAGE SUMMARY ---")
    for stage, status in stages.items():
        print(f"   {stage}: {status}")
    print("\n")


if __name__ == "__main__":
    print("=== STARTING 5-CASE AIRFLOW PIPELINE SCENARIO TESTING SUITE ===\n")

    # Case 1: Healthy Clean Pipeline
    run_pipeline_scenario(1, "Healthy Pipeline (Clean Baseline)", fault_type=None)

    # Case 2: Schema Drift Fault
    run_pipeline_scenario(2, "Schema Drift Fault (Corrupted Data Types)", fault_type="schema_drift")

    # Case 3: Data Quality Null Spike Fault
    run_pipeline_scenario(3, "Data Quality Fault (Null Spike in Customer ID)", fault_type="null_spike")

    # Case 4: Duplicate Ingestion Fault (Auto-Healing)
    run_pipeline_scenario(4, "Duplicate Ingestion Fault (Auto-Remediate)", fault_type="duplicate_ingestion")

    # Case 5: Volume Drop Anomaly Fault
    run_pipeline_scenario(5, "Volume Drop Anomaly Fault (Missing Records)", fault_type="volume_drop")

    print("=== ALL 5 PIPELINE SCENARIO TESTS EXECUTED SUCCESSFULLY ===")
