"""
Controlled Fault Injection CLI Tool
Operates safely on temporary batch copies or active staging files without corrupting raw source files.
"""
import os
import sys
import argparse
import pandas as pd
import random


def inject_fault(fault_type: str, data_dir: str, execution_date: str = "2026-06-01") -> str:
    """
    Injects a specified fault scenario cleanly into staged or raw batch copies.
    Supported types: schema_drift, volume_drop, null_spike, duplicate_ingestion, referential_break, staleness
    """
    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    products_stg = os.path.join(data_dir, "staging", "stg_products.csv")

    os.makedirs(os.path.dirname(orders_stg), exist_ok=True)

    # Base orders batch
    orders_raw = os.path.join(data_dir, "raw", f"orders/orders_{execution_date}.csv")
    if os.path.exists(orders_raw):
        df_orders = pd.read_csv(orders_raw)
    else:
        df_orders = pd.DataFrame([
            {"order_id": f"ORD{i:06d}", "customer_id": f"CUST{i:06d}", "product_id": "PROD00001", "order_ts": f"{execution_date} 12:00:00", "quantity": 1, "order_total": 50.0, "status": "placed"}
            for i in range(1, 301)
        ])

    if fault_type == "schema_drift":
        # Corrupt product price data type to string in staged products
        df_prod = pd.DataFrame([
            {"product_id": f"PROD{i:05d}", "name": f"Prod {i}", "category": "electronics", "price": f"${10.0 + i}"}
            for i in range(1, 60)
        ])
        df_prod.to_csv(products_stg, index=False)
        return "Injected Schema Drift: Corrupted products.price column data type to STRING (e.g. '$14.99')."

    elif fault_type == "volume_drop":
        # Truncate orders to 120 rows (below min threshold of 240)
        df_orders = df_orders.head(120)
        df_orders.to_csv(orders_stg, index=False)
        return f"Injected Volume Drop: Truncated orders to {len(df_orders)} rows (below 240 min bound)."

    elif fault_type == "null_spike":
        # Inject NULLs into customer_id for 15% of records
        null_indices = df_orders.sample(frac=0.15).index
        df_orders.loc[null_indices, "customer_id"] = None
        df_orders.to_csv(orders_stg, index=False)
        return "Injected Null Spike: Injected 15% NULL values into non-nullable customer_id column."

    elif fault_type == "duplicate_ingestion":
        # Duplicate 50 rows
        dupes = df_orders.head(50)
        df_orders = pd.concat([df_orders, dupes], ignore_index=True)
        df_orders.to_csv(orders_stg, index=False)
        return f"Injected Duplicate Ingestion: Duplicated 50 records in staged orders (total: {len(df_orders)} rows)."

    elif fault_type == "referential_break":
        # Change customer_id to non-existent ID 'CUST_ORPHAN_999'
        df_orders.loc[:10, "customer_id"] = "CUST_ORPHAN_999"
        df_orders.to_csv(orders_stg, index=False)
        return "Injected Referential Break: Pointed 10 orders to non-existent customer_id 'CUST_ORPHAN_999'."

    elif fault_type == "staleness":
        # Set timestamps to 30 days old
        df_orders["order_ts"] = "2026-01-01 00:00:00"
        df_orders.to_csv(orders_stg, index=False)
        return "Injected Staleness: Set order_ts to 2026-01-01 (exceeds 26h freshness SLA)."

    else:
        raise ValueError(f"Unknown fault scenario: {fault_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Controlled Fault Injection CLI")
    parser.add_argument("--fault", required=True, choices=["schema_drift", "volume_drop", "null_spike", "duplicate_ingestion", "referential_break", "staleness"])
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--execution-date", default="2026-06-01")

    args = parser.parse_args()
    msg = inject_fault(args.fault, args.data_dir, args.execution_date)
    print(f"[FAULT INJECTION SUCCESS] {msg}")
