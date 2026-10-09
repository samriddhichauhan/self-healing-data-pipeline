"""
Schema validation: strict column presence + type checks against the
pipeline_config.yaml contract. Raises loudly on any drift — never silently
coerces or drops columns.
"""
import os
import pandas as pd
from ..config import config, get_data_dir


def check_dataframe_schema(df: pd.DataFrame, expected_schema: dict, dataset_name: str):
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            raise ValueError(f"Schema Drift: Missing required column '{col}' in {dataset_name} dataset")

        non_nulls = df[col].dropna()
        if non_nulls.empty:
            continue

        if expected_type == "integer":
            try:
                numeric_vals = pd.to_numeric(non_nulls)
            except Exception:
                raise ValueError(f"Schema Drift: Column '{col}' in {dataset_name} contains non-numeric values")
            if not (numeric_vals % 1 == 0).all():
                raise ValueError(f"Schema Drift: Column '{col}' in {dataset_name} contains non-integer values")
        elif expected_type == "float":
            try:
                pd.to_numeric(non_nulls)
            except Exception:
                raise ValueError(f"Schema Drift: Column '{col}' in {dataset_name} contains non-float values")
        elif expected_type in ("date", "timestamp"):
            for val in non_nulls:
                try:
                    pd.to_datetime(val)
                except Exception:
                    raise ValueError(f"Schema Drift: Column '{col}' in {dataset_name} contains invalid date/timestamp: '{val}'")


def validate_schema(**context):
    """Loads staged files and performs strict column schema verification."""
    execution_date = context["ds"]
    data_dir = get_data_dir()

    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    if os.path.exists(cust_stg):
        check_dataframe_schema(pd.read_csv(cust_stg), config["datasets"]["customers"]["schema"], "customers")

    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    if os.path.exists(prod_stg):
        check_dataframe_schema(pd.read_csv(prod_stg), config["datasets"]["products"]["schema"], "products")

    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    if os.path.exists(orders_stg):
        check_dataframe_schema(pd.read_csv(orders_stg), config["datasets"]["orders"]["schema"], "orders")

    events_stg = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    if os.path.exists(events_stg):
        check_dataframe_schema(pd.read_json(events_stg, lines=True), config["datasets"]["events"]["schema"], "events")

    print("Schema checks completed. No drifts detected.")
