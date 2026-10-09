"""
Load stage: verifies transformed/processed data is analytics-ready, then
writes it into the pipeline's Postgres database for real. This is the hard
boundary for "database unavailable -> clear failure, no fake success": if
Postgres cannot be reached, this task raises and the task/DAG run FAILS.
"""
import os
import json
import pandas as pd
from ..config import get_data_dir
from .loader import start_pipeline_run, load_orders, load_events, load_aggregated_metrics, record_quality_result


def load_data(**context):
    execution_date = context["ds"]
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual__{execution_date}"
    dag_id = dag_run.dag_id if dag_run else "self_healing_pipeline"
    data_dir = get_data_dir()

    cust_proc = os.path.join(data_dir, "processed", "customers.csv")
    prod_proc = os.path.join(data_dir, "processed", "products.csv")
    orders_proc = os.path.join(data_dir, "processed", "orders", f"orders_{execution_date}.csv")
    events_proc = os.path.join(data_dir, "processed", "events", f"events_{execution_date}.jsonl")
    metrics_path = os.path.join(data_dir, "processed", f"aggregated_metrics_{execution_date}.json")

    for path in [cust_proc, prod_proc, orders_proc, events_proc]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Load Task Failure: transformed processed dataset missing at: {path}")

    df_o = pd.read_csv(orders_proc)
    if df_o["order_id"].duplicated().any():
        raise ValueError("Load Task Failure: primary key duplicates detected in analytics orders table")

    df_e = pd.read_json(events_proc, lines=True)
    if df_e["event_id"].duplicated().any():
        raise ValueError("Load Task Failure: primary key duplicates detected in analytics events table")

    metrics = {}
    if os.path.exists(metrics_path):
        with open(metrics_path, "r") as f:
            metrics = json.load(f)

    # Real Postgres load. Raises loudly (ConnectionError) if the database is
    # unreachable — deliberately not caught here.
    start_pipeline_run(run_id=run_id, dag_id=dag_id, logical_date=execution_date)

    orders_loaded = load_orders(df_o, run_id=run_id, logical_date=execution_date)
    events_loaded = load_events(df_e, run_id=run_id, logical_date=execution_date)
    load_aggregated_metrics(metrics, run_id=run_id, logical_date=execution_date)

    record_quality_result(run_id, "orders", "primary_key_uniqueness", "PASS", {"rows": len(df_o)})
    record_quality_result(run_id, "events", "primary_key_uniqueness", "PASS", {"rows": len(df_e)})

    print(
        f"[LOAD TASK SUCCESS] execution_date={execution_date} run_id={run_id}: "
        f"loaded {orders_loaded} orders, {events_loaded} events, and aggregated metrics into Postgres."
    )
