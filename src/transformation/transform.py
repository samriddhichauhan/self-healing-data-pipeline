"""
Transformation: deduplicates (defense in depth), normalizes timestamp
formats, and derives business KPIs / aggregated metrics for the load stage.
"""
import os
import json
import pandas as pd
from ..config import get_data_dir


def transform_data(**context):
    """Writes clean processed datasets + an aggregated_metrics.json KPI summary."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    metrics = {"execution_date": execution_date}

    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    if os.path.exists(cust_stg):
        df = pd.read_csv(cust_stg).drop_duplicates(subset=["customer_id"])
        df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.strftime("%Y-%m-%d")
        cust_proc = os.path.join(data_dir, "processed", "customers.csv")
        os.makedirs(os.path.dirname(cust_proc), exist_ok=True)
        df.to_csv(cust_proc, index=False)
        metrics["total_customers"] = len(df)
        print(f"Processed customers saved to {cust_proc}")

    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    if os.path.exists(prod_stg):
        df = pd.read_csv(prod_stg).drop_duplicates(subset=["product_id"])
        prod_proc = os.path.join(data_dir, "processed", "products.csv")
        os.makedirs(os.path.dirname(prod_proc), exist_ok=True)
        df.to_csv(prod_proc, index=False)
        metrics["total_products"] = len(df)
        print(f"Processed products saved to {prod_proc}")

    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    if os.path.exists(orders_stg):
        df = pd.read_csv(orders_stg).drop_duplicates(subset=["order_id"])
        df["order_ts"] = pd.to_datetime(df["order_ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        orders_proc = os.path.join(data_dir, "processed", "orders", f"orders_{execution_date}.csv")
        os.makedirs(os.path.dirname(orders_proc), exist_ok=True)
        df.to_csv(orders_proc, index=False)
        metrics.update({
            "total_orders": int(len(df)),
            "total_order_amount": round(float(df["order_total"].sum()), 2),
            "avg_order_value": round(float(df["order_total"].mean()), 2) if len(df) else 0.0,
            "unique_customers_ordering": int(df["customer_id"].nunique()),
        })
        print(f"Processed orders saved to {orders_proc}")

    events_stg = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    if os.path.exists(events_stg):
        df = pd.read_json(events_stg, lines=True).drop_duplicates(subset=["event_id"])
        df["event_ts"] = pd.to_datetime(df["event_ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        events_proc = os.path.join(data_dir, "processed", "events", f"events_{execution_date}.jsonl")
        os.makedirs(os.path.dirname(events_proc), exist_ok=True)
        df.to_json(events_proc, orient="records", lines=True)
        metrics.update({
            "total_events": int(len(df)),
            "unique_customers_active": int(df["customer_id"].nunique()),
            "events_by_type": df["event_type"].value_counts().to_dict(),
        })
        print(f"Processed events saved to {events_proc}")

    metrics_path = os.path.join(data_dir, "processed", f"aggregated_metrics_{execution_date}.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"Aggregated KPI metrics written to {metrics_path}: {metrics}")
