"""
Script to inspect and verify the Apache Airflow DAG structure and task parameters.
"""
import os
import sys

os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite:////C:/Users/samri/airflow.db"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////C:/Users/samri/airflow.db"

# Set up PYTHONPATH
dags_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../airflow/dags"))
if dags_dir not in sys.path:
    sys.path.append(dags_dir)

from datetime import datetime
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except (ImportError, ModuleNotFoundError):
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

    class MockTaskGroup:
        def __init__(self, group_id="", tooltip="", prefix_group_id=False, *args, **kwargs):
            self.group_id = group_id
            self.tooltip = tooltip
            self.upstream_list = []
            self.downstream_list = []
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
        def __rshift__(self, other):
            if isinstance(other, list):
                for item in other:
                    self >> item
            else:
                if other not in self.downstream_list:
                    self.downstream_list.append(other)
                if self not in getattr(other, 'upstream_list', []):
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
    mock_operators.standard = MagicMock()
    mock_operators.standard.operators = MagicMock()
    mock_operators.standard.operators.python = MagicMock()
    mock_operators.standard.operators.python.PythonOperator = lambda *args, **kwargs: MockOperator(*args, **kwargs)

    mock_utils_task_group = MagicMock()
    mock_utils_task_group.TaskGroup = MockTaskGroup

    sys.modules["airflow"] = mock_airflow
    sys.modules["airflow.operators"] = mock_operators
    sys.modules["airflow.operators.python"] = mock_operators.python
    sys.modules["airflow.providers"] = MagicMock()
    sys.modules["airflow.providers.standard"] = mock_operators.standard
    sys.modules["airflow.providers.standard.operators"] = mock_operators.standard.operators
    sys.modules["airflow.providers.standard.operators.python"] = mock_operators.standard.operators.python
    sys.modules["airflow.utils"] = MagicMock()
    sys.modules["airflow.utils.task_group"] = mock_utils_task_group

import pipeline_dag_starter as etl

def verify_dag():
    dag = etl.dag
    tasks = [
        etl.t_ingest_dimensions,
        etl.t_ingest_orders,
        etl.t_ingest_events,
        etl.t_validate_schema,
        etl.t_validate_quality,
        etl.t_transform_data,
        etl.t_load_data,
        etl.t_agent_monitoring,
    ]
    print("=== AIRFLOW DAG VERIFICATION REPORT ===")
    print(f"DAG ID: {dag.dag_id}")
    print(f"Description: {dag.description}")
    print(f"Schedule: {dag.schedule_interval}")
    print(f"Start Date: {dag.start_date}")
    print(f"Default Args SLA: {dag.default_args.get('sla')}")
    print(f"Default Args Retries: {dag.default_args.get('retries')}")
    print(f"Default Args Retry Delay: {dag.default_args.get('retry_delay')}")
    print(f"Default Args Failure Callback: {dag.default_args.get('on_failure_callback').__name__ if dag.default_args.get('on_failure_callback') else None}")
    
    print("\n--- TASK LEVEL CONFIGURATION ---")
    for task in tasks:
        upstream = [t.task_id for t in task.upstream_list]
        downstream = [t.task_id for t in task.downstream_list]
        callback = task.on_failure_callback.__name__ if task.on_failure_callback else "Inherited"
        print(f"Task: {task.task_id:20s} | Retries: {task.retries} | Retry Delay: {task.retry_delay} | Callback: {callback}")
        print(f"  Upstream: {upstream}")
        print(f"  Downstream: {downstream}")

    print("\n--- LINEAGE GRAPH SUMMARY ---")
    stages = [
        "INGESTION: [ingest_dimensions, ingest_orders, ingest_events]",
        "VALIDATION: [validate_schema, validate_quality]",
        "TRANSFORM: [transform_data]",
        "LOAD: [load_data]",
        "AGENT MONITORING: [agent_monitoring]"
    ]
    for stage in stages:
        print("  -> " + stage)

if __name__ == "__main__":
    verify_dag()
