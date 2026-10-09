import os
import sys
import tempfile
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "fault-injection")))
from inject_fault import inject_fault


@pytest.mark.parametrize("fault_type", [
    "duplicate_ingestion", "schema_drift", "volume_drop", "null_spike", "referential_break", "staleness",
])
def test_all_fault_types_produce_a_measurable_problem(fault_type):
    with tempfile.TemporaryDirectory() as tmp_dir:
        raw_dir = os.path.join(tmp_dir, "raw", "orders")
        os.makedirs(raw_dir, exist_ok=True)
        pd.DataFrame([
            {"order_id": f"ORD{i:06d}", "customer_id": f"CUST{i:06d}", "product_id": "PROD00001",
             "order_ts": "2026-06-01 12:00:00", "quantity": 1, "order_total": 50.0, "status": "placed"}
            for i in range(1, 301)
        ]).to_csv(os.path.join(raw_dir, "orders_2026-06-01.csv"), index=False)

        msg = inject_fault(fault_type, tmp_dir, "2026-06-01")
        assert msg

        if fault_type == "schema_drift":
            df = pd.read_csv(os.path.join(tmp_dir, "staging", "stg_products.csv"))
            assert "$" in str(df["price"].iloc[0])
        elif fault_type == "duplicate_ingestion":
            df = pd.read_csv(os.path.join(tmp_dir, "staging", "stg_orders_2026-06-01.csv"))
            assert len(df) == 350
            assert df["order_id"].duplicated().sum() == 50
        elif fault_type == "volume_drop":
            df = pd.read_csv(os.path.join(tmp_dir, "staging", "stg_orders_2026-06-01.csv"))
            assert len(df) == 120
        elif fault_type == "null_spike":
            df = pd.read_csv(os.path.join(tmp_dir, "staging", "stg_orders_2026-06-01.csv"))
            assert df["customer_id"].isnull().mean() > 0.10
        elif fault_type == "referential_break":
            df = pd.read_csv(os.path.join(tmp_dir, "staging", "stg_orders_2026-06-01.csv"))
            assert (df["customer_id"] == "CUST_ORPHAN_999").sum() == 11
        elif fault_type == "staleness":
            df = pd.read_csv(os.path.join(tmp_dir, "staging", "stg_orders_2026-06-01.csv"))
            assert (df["order_ts"] == "2026-01-01 00:00:00").all()


def test_unknown_fault_type_raises():
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValueError, match="Unknown fault scenario"):
            inject_fault("not_a_real_fault", tmp_dir, "2026-06-01")
