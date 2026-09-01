"""
Unit tests for Fault Injection CLI
"""
import os
import sys
import tempfile
import pandas as pd
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../scripts/fault-injection")))
from inject_fault import inject_fault


def test_fault_injection_scenarios():
    with tempfile.TemporaryDirectory() as tmp_dir:
        msg1 = inject_fault("duplicate_ingestion", tmp_dir, "2026-06-01")
        assert "Injected Duplicate Ingestion" in msg1

        stg_orders = os.path.join(tmp_dir, "staging", "stg_orders_2026-06-01.csv")
        assert os.path.exists(stg_orders)
        df1 = pd.read_csv(stg_orders)
        assert df1["order_id"].duplicated().any()

        msg2 = inject_fault("schema_drift", tmp_dir, "2026-06-01")
        assert "Injected Schema Drift" in msg2
        stg_prod = os.path.join(tmp_dir, "staging", "stg_products.csv")
        assert os.path.exists(stg_prod)
        df2 = pd.read_csv(stg_prod)
        assert df2["price"].dtype == object or "$" in str(df2["price"].iloc[0])
