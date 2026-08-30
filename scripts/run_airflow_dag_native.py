"""
Script to execute the Apache Airflow DAG in native Linux (WSL) environment.
"""
import os
import sys

# Convert Windows paths to WSL paths if running in WSL
if os.path.exists("/mnt/c/Users/samri/OneDrive/Documents/Internship 1"):
    base_dir = "/mnt/c/Users/samri/OneDrive/Documents/Internship 1"
else:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

os.environ["AIRFLOW_HOME"] = os.path.join(base_dir, "airflow")
os.environ["PIPELINE_CONFIG_PATH"] = os.path.join(base_dir, "airflow", "config", "pipeline_config.yaml")
os.environ["TEST_DATA_DIR"] = os.path.join(base_dir, "data")
os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow.db"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/airflow.db"

dags_dir = os.path.join(base_dir, "airflow", "dags")
if dags_dir not in sys.path:
    sys.path.append(dags_dir)

import pipeline_dag_starter as etl

def run_dag():
    print("==================================================")
    print("      REAL AIRFLOW NATIVE LINUX DAG EXECUTION     ")
    print("==================================================")
    dag = etl.dag
    print(f"DAG ID: {dag.dag_id}")
    print(f"Description: {dag.description}")
    print(f"Registered Tasks ({len(dag.tasks)}): {[t.task_id for t in dag.tasks]}")
    
    # Run task callables sequentially following lineage
    context = {"ds": "2026-06-01"}
    print("\nExecuting Stage 1: INGESTION...")
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    
    print("\nExecuting Stage 2: VALIDATION...")
    etl.validate_schema(**context)
    etl.validate_quality(**context)
    
    print("\nExecuting Stage 3: TRANSFORM...")
    etl.transform_data(**context)
    
    print("\nExecuting Stage 4: LOAD...")
    etl.load_data(**context)
    
    print("\nExecuting Stage 5: AGENT MONITORING...")
    etl.agent_monitoring(**context)
    
    print("\n==================================================")
    print("SUCCESS: Full Airflow DAG pipeline executed cleanly!")
    print("==================================================")

if __name__ == "__main__":
    run_dag()
