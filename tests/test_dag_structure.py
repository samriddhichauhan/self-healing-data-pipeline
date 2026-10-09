"""
Verifies the Airflow DAG definition itself: all 8 required tasks exist with
the correct explicit lineage. Airflow is mocked so this can run without a
real Airflow install (the real DAG always executes inside Docker — see
scripts/demo_healthy_run.py / demo_self_healing_run.py for that).
"""
import os
import sys
import importlib.util
from unittest.mock import MagicMock


def _load_dag_module():
    class MockOperator:
        def __init__(self, task_id, python_callable=None, dag=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.upstream_list, self.downstream_list = [], []

        def __rshift__(self, other):
            others = other if isinstance(other, list) else [other]
            for o in others:
                if o not in self.downstream_list:
                    self.downstream_list.append(o)
                if self not in getattr(o, "upstream_list", []):
                    o.upstream_list.append(self)
            return other

    class MockDAG:
        def __init__(self, dag_id="self_healing_pipeline", **kwargs):
            self.dag_id = dag_id
            self.tasks = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    mock_airflow = MagicMock()
    mock_airflow.DAG = MockDAG
    mock_operators_python = MagicMock()
    mock_operators_python.PythonOperator = MockOperator

    sys.modules["airflow"] = mock_airflow
    sys.modules["airflow.operators"] = MagicMock()
    sys.modules["airflow.operators.python"] = mock_operators_python

    dag_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "self_healing_pipeline.py"))
    spec = importlib.util.spec_from_file_location("self_healing_pipeline_dag", dag_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_has_all_8_tasks_with_correct_lineage():
    etl = _load_dag_module()

    t_ingest_dim = etl.t_ingest_dimensions
    t_ingest_ord = etl.t_ingest_orders
    t_ingest_evt = etl.t_ingest_events
    t_val_sch = etl.t_validate_schema
    t_val_qual = etl.t_validate_quality
    t_trans = etl.t_transform_data
    t_load = etl.t_load_data
    t_agent = etl.t_agent_monitoring

    tasks = [t_ingest_dim, t_ingest_ord, t_ingest_evt, t_val_sch, t_val_qual, t_trans, t_load, t_agent]
    task_ids = [t.task_id for t in tasks]

    expected = [
        "ingest_dimensions", "ingest_orders", "ingest_events",
        "validate_schema", "validate_quality", "transform_data", "load_data", "agent_monitoring",
    ]
    assert task_ids == expected, "DAG must define exactly these 8 tasks, in a stable order"

    assert t_val_sch in t_ingest_dim.downstream_list
    assert t_val_sch in t_ingest_ord.downstream_list
    assert t_val_sch in t_ingest_evt.downstream_list
    assert t_val_qual in t_val_sch.downstream_list
    assert t_trans in t_val_qual.downstream_list
    assert t_load in t_trans.downstream_list
    assert t_agent in t_load.downstream_list
