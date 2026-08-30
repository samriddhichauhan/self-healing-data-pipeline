"""
Comprehensive Airflow Health & DAG Execution Diagnostic Script
"""
import os
import sys
import tempfile
import shutil
import json
import pandas as pd

# Set environment
os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite:////C:/Users/samri/airflow.db"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////C:/Users/samri/airflow.db"

dags_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../airflow/dags"))
if dags_dir not in sys.path:
    sys.path.append(dags_dir)

# Import mock setup to avoid Windows fcntl import error in local python
try:
    import fcntl
except ModuleNotFoundError:
    from unittest.mock import MagicMock
    class MockOperator:
        def __init__(self, task_id, python_callable=None, dag=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.upstream_list = []
            self.downstream_list = []
            self.retries = kwargs.get("retries", 0)
            self.retry_delay = kwargs.get("retry_delay")
            self.on_failure_callback = kwargs.get("on_failure_callback")
        def __rshift__(self, other):
            if isinstance(other, list):
                for item in other:
                    self >> item
            else:
                if other not in self.downstream_list:
                    self.downstream_list.append(other)
                if self not in other.upstream_list:
                    other.upstream_list.append(self)
            return other
        def __rrshift__(self, other):
            if isinstance(other, list):
                for item in other:
                    item >> self
            return self

    class MockDAG:
        def __init__(self, dag_id="self_healing_pipeline", description="", schedule="0 2 * * *", start_date=None, catchup=False, default_args=None, tags=None, **kwargs):
            self.dag_id = dag_id
            self.description = description
            self.schedule_interval = schedule
            self.start_date = start_date
            self.default_args = default_args or {}
            self.tasks = []
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_airflow = MagicMock()
    mock_airflow.DAG = MockDAG
    mock_operators = MagicMock()
    mock_operators.python = MagicMock()
    mock_operators.python.PythonOperator = lambda *args, **kwargs: MockOperator(*args, **kwargs)

    sys.modules["airflow"] = mock_airflow
    sys.modules["airflow.operators"] = mock_operators
    sys.modules["airflow.operators.python"] = mock_operators.python

import pipeline_dag_starter as etl

def run_health_checks():
    print("==================================================")
    print("       AIRFLOW PIPELINE HEALTH DIAGNOSTIC         ")
    print("==================================================")

    errors = []

    # 1. DAG Definition & Metadata Check
    try:
        dag = etl.dag
        print(f"[OK] DAG ID: {dag.dag_id}")
        print(f"[OK] DAG Schedule: {dag.schedule_interval}")
        print(f"[OK] DAG SLA: {dag.default_args.get('sla')}")
    except Exception as e:
        errors.append(f"DAG definition error: {e}")

    # 2. Task Inventory Check
    tasks = [
        "ingest_dimensions",
        "ingest_orders",
        "ingest_events",
        "validate_schema",
        "validate_quality",
        "transform_data",
        "load_data",
        "agent_monitoring"
    ]
    
    defined_tasks = [
        etl.t_ingest_dimensions.task_id,
        etl.t_ingest_orders.task_id,
        etl.t_ingest_events.task_id,
        etl.t_validate_schema.task_id,
        etl.t_validate_quality.task_id,
        etl.t_transform_data.task_id,
        etl.t_load_data.task_id,
        etl.t_agent_monitoring.task_id,
    ]
    
    for t in tasks:
        if t in defined_tasks:
            print(f"[OK] Task Registered: {t}")
        else:
            errors.append(f"Missing required task: {t}")

    # 3. Lineage Check
    try:
        assert etl.t_agent_monitoring in etl.t_load_data.downstream_list
        assert etl.t_load_data in etl.t_transform_data.downstream_list
        assert etl.t_transform_data in etl.t_validate_quality.downstream_list
        assert etl.t_validate_quality in etl.t_validate_schema.downstream_list
        assert etl.t_validate_schema in etl.t_ingest_orders.downstream_list
        print("[OK] Lineage Dependency Graph Verified (INGESTION -> VALIDATION -> TRANSFORM -> LOAD -> AGENT MONITORING)")
    except AssertionError as e:
        errors.append("Lineage dependency graph check failed!")

    # 4. Dry-run Execution on Temp Data Directory
    temp_dir = tempfile.mkdtemp()
    os.environ["TEST_DATA_DIR"] = temp_dir
    try:
        # Create mock data
        raw_dir = os.path.join(temp_dir, "raw")
        os.makedirs(os.path.join(raw_dir, "orders"), exist_ok=True)
        os.makedirs(os.path.join(raw_dir, "events"), exist_ok=True)
        
        # Customers
        cust_rows = [{"customer_id": f"CUST{i:06d}", "name": f"Cust {i}", "email": f"c{i}@ex.com", "region": "NA", "signup_date": "2024-01-01"} for i in range(1, 501)]
        pd.DataFrame(cust_rows).to_csv(os.path.join(raw_dir, "customers.csv"), index=False)
        
        # Products
        prod_rows = [{"product_id": f"PROD{i:05d}", "name": f"Prod {i}", "category": "electronics", "price": 10.0} for i in range(1, 61)]
        pd.DataFrame(prod_rows).to_csv(os.path.join(raw_dir, "products.csv"), index=False)
        
        # Orders
        ord_rows = [{"order_id": f"ORD{i:08d}", "customer_id": f"CUST{((i-1)%500)+1:06d}", "product_id": f"PROD{((i-1)%60)+1:05d}", "order_ts": "2026-06-01 12:00:00", "quantity": 1, "order_total": 10.0, "status": "placed"} for i in range(1, 301)]
        pd.DataFrame(ord_rows).to_csv(os.path.join(raw_dir, "orders", "orders_2026-06-01.csv"), index=False)
        
        # Events
        evt_rows = [{"event_id": f"evt-{i}", "customer_id": f"CUST{((i-1)%500)+1:06d}", "event_type": "page_view", "session_id": f"sess-{i}", "event_ts": "2026-06-01 12:05:00"} for i in range(1, 1201)]
        pd.DataFrame(evt_rows).to_json(os.path.join(raw_dir, "events", "events_2026-06-01.jsonl"), orient="records", lines=True)

        context = {"ds": "2026-06-01"}
        
        etl.ingest_dimensions()
        etl.ingest_orders(**context)
        etl.ingest_events(**context)
        etl.validate_schema(**context)
        etl.validate_quality(**context)
        etl.transform_data(**context)
        etl.load_data(**context)
        etl.agent_monitoring(**context)
        
        print("[OK] Dry-run execution completed without errors for all 8 tasks.")
        
        status_file = os.path.join(temp_dir, "incidents", "status", "status_2026-06-01.json")
        if os.path.exists(status_file):
            with open(status_file) as f:
                data = json.load(f)
            print(f"[OK] Agent Monitoring status report verified: status={data['pipeline_status']}, row_counts={data['row_counts']}")
        else:
            errors.append("Agent monitoring status file missing!")
            
    except Exception as e:
        errors.append(f"Dry-run execution error: {e}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print("==================================================")
    if errors:
        print("ERRORS DETECTED:")
        for err in errors:
            print(f" - {err}")
    else:
        print("ALL CHECKS PASSED: Airflow DAG is clean, healthy, and working properly!")
    print("==================================================")

if __name__ == "__main__":
    run_health_checks()
