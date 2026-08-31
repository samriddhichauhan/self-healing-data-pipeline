import os
import sys
import pytest
import pandas as pd
import shutil
import yaml
from unittest.mock import MagicMock

# ------------------------------------------------------------------------------
# Mock Airflow imports before importing the DAG module
# This allows us to unit test the ETL logic without installing Airflow locally.
# ------------------------------------------------------------------------------
class MockOperator:
    def __init__(self, task_id, python_callable=None, dag=None, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.upstream_list = []
        self.downstream_list = []
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
    def __init__(self, dag_id="self_healing_pipeline", *args, **kwargs):
        self.dag_id = dag_id
        self.tasks = []
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

def mock_python_operator(*args, **kwargs):
    op = MockOperator(*args, **kwargs)
    return op

mock_airflow = MagicMock()
mock_airflow.DAG = MockDAG

mock_operators = MagicMock()
mock_operators.python = MagicMock()
mock_operators.python.PythonOperator = mock_python_operator
mock_operators.standard = MagicMock()
mock_operators.standard.operators = MagicMock()
mock_operators.standard.operators.python = MagicMock()
mock_operators.standard.operators.python.PythonOperator = mock_python_operator

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
# ------------------------------------------------------------------------------

# Add DAGs folder to python path so we can import the pipeline tasks
dags_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../airflow/dags"))
if dags_dir not in sys.path:
    sys.path.append(dags_dir)

import pipeline_dag_starter as etl


def create_valid_mock_data(tmp_path):
    raw_dir = tmp_path / "raw"
    os.makedirs(raw_dir / "orders", exist_ok=True)
    os.makedirs(raw_dir / "events", exist_ok=True)
    
    # 1. Customers (500 rows)
    cust_rows = []
    for i in range(1, 501):
        cust_rows.append({
            "customer_id": f"CUST{i:06d}",
            "name": f"Customer {i}",
            "email": f"cust{i}@example.com",
            "region": "NA",
            "signup_date": "2024-01-01"
        })
    pd.DataFrame(cust_rows).to_csv(raw_dir / "customers.csv", index=False)
    
    # 2. Products (60 rows)
    prod_rows = []
    for i in range(1, 61):
        prod_rows.append({
            "product_id": f"PROD{i:05d}",
            "name": f"Product {i}",
            "category": "electronics",
            "price": 10.0 * i
        })
    pd.DataFrame(prod_rows).to_csv(raw_dir / "products.csv", index=False)
    
    # 3. Orders (300 rows)
    order_rows = []
    for i in range(1, 301):
        order_rows.append({
            "order_id": f"ORD{i:08d}",
            "customer_id": f"CUST{((i - 1) % 500) + 1:06d}",
            "product_id": f"PROD{((i - 1) % 60) + 1:05d}",
            "order_ts": "2026-06-01 12:00:00",
            "quantity": 1,
            "order_total": 10.0,
            "status": "placed"
        })
    pd.DataFrame(order_rows).to_csv(raw_dir / "orders" / "orders_2026-06-01.csv", index=False)
    
    # 4. Events (1200 rows)
    event_rows = []
    for i in range(1, 1201):
        event_rows.append({
            "event_id": f"evt-{i}",
            "customer_id": f"CUST{((i - 1) % 500) + 1:06d}",
            "event_type": "page_view",
            "session_id": f"sess-{i}",
            "event_ts": "2026-06-01 12:05:00"
        })
    pd.DataFrame(event_rows).to_json(raw_dir / "events" / "events_2026-06-01.jsonl", orient="records", lines=True)


@pytest.fixture
def setup_test_env(tmp_path, monkeypatch):
    """Sets up standard directories and environment variables for local testing."""
    monkeypatch.setenv("TEST_DATA_DIR", str(tmp_path))
    create_valid_mock_data(tmp_path)
    yield tmp_path
    # Cleanup
    shutil.rmtree(tmp_path, ignore_errors=True)


def test_valid_pipeline_run(setup_test_env):
    """Tests that the full local pipeline runs without errors on valid data."""
    context = {"ds": "2026-06-01"}
    
    # Run pipeline tasks
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    etl.validate_quality(**context)
    etl.transform_data(**context)
    etl.load_data(**context)
    etl.agent_monitoring(**context)
    
    # Assert output files exist in processed/
    processed_dir = setup_test_env / "processed"
    assert os.path.exists(processed_dir / "customers.csv")
    assert os.path.exists(processed_dir / "products.csv")
    assert os.path.exists(processed_dir / "orders" / "orders_2026-06-01.csv")
    assert os.path.exists(processed_dir / "events" / "events_2026-06-01.jsonl")
    
    # Verify status report written by agent monitoring
    status_file = setup_test_env / "incidents" / "status" / "status_2026-06-01.json"
    assert os.path.exists(status_file)
    
    # Verify outputs are clean and have expected row sizes
    df_o = pd.read_csv(processed_dir / "orders" / "orders_2026-06-01.csv")
    assert len(df_o) == 300
    assert not df_o["order_id"].duplicated().any()


def test_invalid_schema_missing_column(setup_test_env):
    """Tests schema validation error on missing columns."""
    context = {"ds": "2026-06-01"}
    
    # Remove customer email column to simulate schema drift
    raw_customers = setup_test_env / "raw" / "customers.csv"
    df = pd.read_csv(raw_customers)
    df = df.drop(columns=["email"])
    df.to_csv(raw_customers, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    
    with pytest.raises(ValueError, match="Schema Drift: Missing required column 'email'"):
        etl.validate_schema(**context)


def test_invalid_schema_type_mismatch(setup_test_env):
    """Tests schema validation error on invalid data types (non-float value in float column)."""
    context = {"ds": "2026-06-01"}
    
    # Set products price to string values
    raw_products = setup_test_env / "raw" / "products.csv"
    df = pd.read_csv(raw_products)
    df["price"] = "Ten Dollars"
    df.to_csv(raw_products, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    
    with pytest.raises(ValueError, match="Schema Drift: Column 'price' in products contains non-float values"):
        etl.validate_schema(**context)


def test_duplicate_primary_keys(setup_test_env):
    """Tests quality check error on duplicate orders primary keys."""
    context = {"ds": "2026-06-01"}
    
    # Duplicate order row
    raw_orders = setup_test_env / "raw" / "orders" / "orders_2026-06-01.csv"
    df = pd.read_csv(raw_orders)
    df.iloc[1] = df.iloc[0]  # Copy first order to second row (duplicate primary key)
    df.to_csv(raw_orders, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    
    with pytest.raises(ValueError, match="Data Quality: Duplicate primary keys found in orders"):
        etl.validate_quality(**context)


def test_abnormal_volume_low_rows(setup_test_env):
    """Tests quality validation failure when row counts fall below expected minimums."""
    context = {"ds": "2026-06-01"}
    
    # Save orders raw with only 10 rows (minimum is 240)
    raw_orders = setup_test_env / "raw" / "orders" / "orders_2026-06-01.csv"
    df = pd.read_csv(raw_orders)
    df.head(10).to_csv(raw_orders, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    
    with pytest.raises(ValueError, match="violates bounds.*Abnormal Volume"):
        etl.validate_quality(**context)


def test_null_spike_violation(setup_test_env):
    """Tests quality check fails when Null percentages exceed thresholds."""
    context = {"ds": "2026-06-01"}
    
    # Set 50 values (16%) of order_total to null (threshold is 2% / 6 rows max)
    raw_orders = setup_test_env / "raw" / "orders" / "orders_2026-06-01.csv"
    df = pd.read_csv(raw_orders)
    df.loc[0:49, "order_total"] = None
    df.to_csv(raw_orders, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    
    with pytest.raises(ValueError, match="null rate.*exceeds threshold.*NULL Violation"):
        etl.validate_quality(**context)


def test_broken_foreign_key_referential_integrity(setup_test_env):
    """Tests validation error on broken referential integrity."""
    context = {"ds": "2026-06-01"}
    
    # Use invalid customer_id in first order
    raw_orders = setup_test_env / "raw" / "orders" / "orders_2026-06-01.csv"
    df = pd.read_csv(raw_orders)
    df.loc[0, "customer_id"] = "CUST999999"
    df.to_csv(raw_orders, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    
    with pytest.raises(ValueError, match="Referential integrity broken.*missing in customers.*Broken Foreign Key"):
        etl.validate_quality(**context)


def test_staleness_sla_violation(setup_test_env):
    """Tests validation error on orders staleness SLA violation."""
    context = {"ds": "2026-06-01"}
    
    # Set all order timestamps to a stale date (yesterday)
    raw_orders = setup_test_env / "raw" / "orders" / "orders_2026-06-01.csv"
    df = pd.read_csv(raw_orders)
    df["order_ts"] = "2026-05-31 23:59:59"
    df.to_csv(raw_orders, index=False)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    
    with pytest.raises(ValueError, match="Orders Freshness SLA breach"):
        etl.validate_quality(**context)


def test_events_staleness_sla_violation(setup_test_env):
    """Tests validation error on events staleness SLA violation."""
    context = {"ds": "2026-06-01"}
    
    # Set all event timestamps to a stale date
    raw_events = setup_test_env / "raw" / "events" / "events_2026-06-01.jsonl"
    df = pd.read_json(raw_events, lines=True)
    df["event_ts"] = "2026-05-31 12:00:00"
    df.to_json(raw_events, orient="records", lines=True)
    
    etl.ingest_dimensions()
    etl.ingest_orders(**context)
    etl.ingest_events(**context)
    etl.validate_schema(**context)
    
    with pytest.raises(ValueError, match="Events Freshness SLA breach"):
        etl.validate_quality(**context)


def test_dag_task_structure():
    """Verifies that the Airflow DAG definition has all required tasks and explicit lineage."""
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

    expected_tasks = [
        "ingest_dimensions",
        "ingest_orders",
        "ingest_events",
        "validate_schema",
        "validate_quality",
        "transform_data",
        "load_data",
        "agent_monitoring"
    ]
    
    for expected in expected_tasks:
        assert expected in task_ids, f"Task {expected} missing from DAG definition"
        
    # Verify task lineage dependencies
    assert t_agent in t_load.downstream_list
    assert t_load in t_trans.downstream_list
    assert t_trans in t_val_qual.downstream_list
    assert t_val_qual in t_val_sch.downstream_list
    assert t_val_sch in t_ingest_ord.downstream_list
    assert t_val_sch in t_ingest_dim.downstream_list
    assert t_val_sch in t_ingest_evt.downstream_list


