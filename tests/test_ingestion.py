import os
import pandas as pd
import pytest
from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events


def test_ingest_dimensions_and_facts(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)

    stg = os.path.join(str(pipeline_env), "staging")
    assert os.path.exists(os.path.join(stg, "stg_customers.csv"))
    assert os.path.exists(os.path.join(stg, "stg_products.csv"))
    assert os.path.exists(os.path.join(stg, "stg_orders_2026-06-01.csv"))
    assert os.path.exists(os.path.join(stg, "stg_events_2026-06-01.jsonl"))


def test_ingest_orders_fails_loudly_on_missing_date(pipeline_env):
    with pytest.raises(FileNotFoundError, match="Ingestion failure for dataset 'orders'"):
        ingest_orders(ds="2099-01-01")


def test_ingest_events_fails_loudly_on_missing_date(pipeline_env):
    with pytest.raises(FileNotFoundError, match="Ingestion failure for dataset 'events'"):
        ingest_events(ds="2099-01-01")


def test_ingest_orders_error_lists_available_files(pipeline_env):
    with pytest.raises(FileNotFoundError) as exc_info:
        ingest_orders(ds="2099-01-01")
    msg = str(exc_info.value)
    assert "orders_2026-06-01.csv" in msg  # actually-available files are listed


def test_ingest_orders_preserves_existing_staging(pipeline_env):
    """A pre-existing staged file (e.g. from fault injection) is NOT clobbered
    by re-running ingestion — this is what lets self-healing survive a retry."""
    ctx = {"ds": "2026-06-01"}
    stg_dir = os.path.join(str(pipeline_env), "staging")
    os.makedirs(stg_dir, exist_ok=True)
    stg_path = os.path.join(stg_dir, "stg_orders_2026-06-01.csv")

    df = pd.DataFrame([{"order_id": "ORD1", "customer_id": "C1", "product_id": "P1", "order_ts": "2026-06-01 00:00:00", "quantity": 1, "order_total": 1.0, "status": "placed"}] * 3)
    df.to_csv(stg_path, index=False)

    ingest_orders(**ctx)

    reloaded = pd.read_csv(stg_path)
    assert len(reloaded) == 3  # untouched, not overwritten from raw (which has 300 rows)
