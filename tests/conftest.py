import os
import sys
import shutil
import pandas as pd
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def write_valid_dataset(base_dir):
    """Writes a canonical-shaped, fully valid dataset under base_dir/raw/."""
    raw_dir = os.path.join(base_dir, "raw")
    os.makedirs(os.path.join(raw_dir, "orders"), exist_ok=True)
    os.makedirs(os.path.join(raw_dir, "events"), exist_ok=True)

    customers = pd.DataFrame([
        {"customer_id": f"CUST{i:06d}", "name": f"Customer {i}", "email": f"cust{i}@example.com", "region": "NA", "signup_date": "2024-01-01"}
        for i in range(1, 501)
    ])
    customers.to_csv(os.path.join(raw_dir, "customers.csv"), index=False)

    products = pd.DataFrame([
        {"product_id": f"PROD{i:05d}", "name": f"Product {i}", "category": "electronics", "price": 10.0 * i}
        for i in range(1, 61)
    ])
    products.to_csv(os.path.join(raw_dir, "products.csv"), index=False)

    orders = pd.DataFrame([
        {
            "order_id": f"ORD{i:08d}", "customer_id": f"CUST{((i - 1) % 500) + 1:06d}",
            "product_id": f"PROD{((i - 1) % 60) + 1:05d}", "order_ts": "2026-06-01 12:00:00",
            "quantity": 1, "order_total": 10.0, "status": "placed",
        }
        for i in range(1, 301)
    ])
    orders.to_csv(os.path.join(raw_dir, "orders", "orders_2026-06-01.csv"), index=False)

    events = pd.DataFrame([
        {
            "event_id": f"evt-{i}", "customer_id": f"CUST{((i - 1) % 500) + 1:06d}",
            "event_type": "page_view", "session_id": f"sess-{i}", "event_ts": "2026-06-01 12:05:00",
        }
        for i in range(1, 1201)
    ])
    events.to_json(os.path.join(raw_dir, "events", "events_2026-06-01.jsonl"), orient="records", lines=True)


@pytest.fixture
def pipeline_env(tmp_path, monkeypatch):
    """A temporary, fully valid pipeline data directory with TEST_DATA_DIR set."""
    monkeypatch.setenv("TEST_DATA_DIR", str(tmp_path))
    write_valid_dataset(str(tmp_path))
    yield tmp_path
    shutil.rmtree(str(tmp_path), ignore_errors=True)


def postgres_reachable() -> bool:
    try:
        from src.database.connection import get_connection
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not postgres_reachable(),
    reason="Pipeline Postgres database not reachable (start it with `docker compose up -d postgres`)",
)
