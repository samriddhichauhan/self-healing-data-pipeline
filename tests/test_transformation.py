import os
import json
import pandas as pd
from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events
from src.transformation.transform import transform_data


def test_transform_writes_processed_data_and_kpis(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)

    transform_data(**ctx)

    processed = os.path.join(str(pipeline_env), "processed")
    assert os.path.exists(os.path.join(processed, "customers.csv"))
    assert os.path.exists(os.path.join(processed, "products.csv"))
    orders_out = os.path.join(processed, "orders", "orders_2026-06-01.csv")
    assert os.path.exists(orders_out)

    df_o = pd.read_csv(orders_out)
    assert len(df_o) == 300
    assert not df_o["order_id"].duplicated().any()

    metrics_path = os.path.join(processed, "aggregated_metrics_2026-06-01.json")
    assert os.path.exists(metrics_path)
    with open(metrics_path) as f:
        metrics = json.load(f)
    assert metrics["total_orders"] == 300
    assert metrics["total_order_amount"] == 3000.0
    assert metrics["total_events"] == 1200
