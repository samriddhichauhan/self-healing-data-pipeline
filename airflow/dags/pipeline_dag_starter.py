"""
Apache Airflow DAG — self_healing_pipeline

This DAG implements a production-grade local ETL pipeline for daily order ingestion,
clickstream events ingestion, schema validation, quality checks, and clean output writing.
It has NO external BigQuery or GCP dependencies.
"""
import os
import yaml
import random
import json
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Paths configuration
CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/airflow/config/pipeline_config.yaml")

# Load pipeline configuration
try:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
except Exception as e:
    # Fallback to local path outside container for testing
    local_config = os.path.join(os.path.dirname(__file__), "../config/pipeline_config.yaml")
    if os.path.exists(local_config):
        with open(local_config, "r") as f:
            config = yaml.safe_load(f)
    else:
        raise e


def get_data_dir():
    """Helper to resolve data directory path inside container vs. local dev environment."""
    if os.environ.get("TEST_DATA_DIR"):
        return os.environ.get("TEST_DATA_DIR")
    if os.path.exists("/opt/airflow/data"):
        return "/opt/airflow/data"
    return os.path.abspath("./data")


def on_task_failure(context):
    """
    Hook called whenever any task in this DAG fails. This is where the
    agent should be invoked to diagnose + (auto-fix or escalate).
    """
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    execution_date = context.get("ds")
    
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = dag_run.dag_id if dag_run else "self_healing_pipeline"
    try_number = task_instance.try_number if task_instance else 1
    
    incident_id = f"INC-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    
    print(f"!!! Task Failure Hook Triggered !!!")
    print(f"Incident ID: {incident_id}")
    print(f"Failed Task: {task_id}")
    print(f"DAG: {dag_id}")
    print(f"Execution Date: {execution_date}")
    print(f"Try: {try_number}")
    print(f"Exception: {exception}")

    # Determine failure category from exception message
    failure_category = "UNKNOWN"
    err_msg = str(exception).upper()
    if "SCHEMA DRIFT" in err_msg:
        failure_category = "SCHEMA_DRIFT"
    elif "ROW COUNT" in err_msg or "ABNORMAL VOLUME" in err_msg:
        failure_category = "VOLUME_ANOMALY"
    elif "NULL" in err_msg:
        failure_category = "NULL_SPIKE"
    elif "DUPLICATE" in err_msg:
        failure_category = "DUPLICATE_INGESTION"
    elif "REFERENTIAL" in err_msg or "FOREIGN KEY" in err_msg:
        failure_category = "REFERENTIAL_BREAK"
    elif "FRESHNESS" in err_msg or "SLA" in err_msg:
        failure_category = "STALENESS"

    # Serialize incident details to a file for the diagnostics agent to consume
    incident_data = {
        "incident_id": incident_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "try_number": try_number,
        "failure_category": failure_category,
        "error_message": str(exception)
    }

    # Write report
    report_dir = "/opt/airflow/incidents/reports"
    if not os.path.exists(report_dir):
        # Fallback to local host directory mapped or data folder
        data_dir = get_data_dir()
        report_dir = os.path.join(data_dir, "incidents", "reports")
        
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"inc_{incident_id}.json")
    
    with open(report_path, "w") as f:
        json.dump(incident_data, f, indent=2)
        
    # Write human-readable markdown report as well
    md_report_path = os.path.join(report_dir, f"inc_{incident_id}.md")
    with open(md_report_path, "w") as f:
        f.write(f"""# Incident Report: {incident_id}

## Status: ESCALATED_PENDING_APPROVAL
* **Detection Time:** {incident_data['timestamp']}
* **Source DAG:** {dag_id}
* **Source Task:** {task_id}
* **Affected Execution Date:** {execution_date}
* **Try Number:** {try_number}
* **Failure Category:** {failure_category}

## 1. Executive Summary
Task `{task_id}` failed validation checks during execution on {execution_date}. Clean data pipeline execution was aborted to prevent downstream corruption.

## 2. Technical Details
```
{exception}
```
""")
    print(f"Written incident reports to {report_path} and {md_report_path}")


def ingest_dimensions():
    """Reads dimension datasets from raw folder and stages them locally."""
    data_dir = get_data_dir()
    
    # Ingest customers
    cust_raw = os.path.join(data_dir, "raw", config["datasets"]["customers"]["path_pattern"])
    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    if not os.path.exists(cust_raw):
        raise FileNotFoundError(f"Customers raw file not found: {cust_raw}")
    os.makedirs(os.path.dirname(cust_stg), exist_ok=True)
    pd.read_csv(cust_raw).to_csv(cust_stg, index=False)
    print(f"Staged customers to: {cust_stg}")
        
    # Ingest products
    prod_raw = os.path.join(data_dir, "raw", config["datasets"]["products"]["path_pattern"])
    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    if not os.path.exists(prod_raw):
        raise FileNotFoundError(f"Products raw file not found: {prod_raw}")
    os.makedirs(os.path.dirname(prod_stg), exist_ok=True)
    pd.read_csv(prod_raw).to_csv(prod_stg, index=False)
    print(f"Staged products to: {prod_stg}")


def ingest_orders(**context):
    """Verifies orders batch exists, reads CSV and stages to local staging folder."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    
    # Load dimensions
    ingest_dimensions()
    
    raw_pattern = config["datasets"]["orders"]["path_pattern"]
    raw_path = os.path.join(data_dir, "raw", raw_pattern.format(date=execution_date))
    
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Orders daily batch file not found: {raw_path}")
        
    print(f"Reading orders data from: {raw_path}")
    orders_df = pd.read_csv(raw_path)
    
    # Write to local staging path
    stg_path = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    os.makedirs(os.path.dirname(stg_path), exist_ok=True)
    orders_df.to_csv(stg_path, index=False)
    print(f"Successfully staged orders to: {stg_path}")


def ingest_events(**context):
    """Verifies events batch exists, reads JSONL and stages to local staging folder."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    
    raw_pattern = config["datasets"]["events"]["path_pattern"]
    raw_path = os.path.join(data_dir, "raw", raw_pattern.format(date=execution_date))
    
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Events daily batch file not found: {raw_path}")
        
    print(f"Reading events data from: {raw_path}")
    events_df = pd.read_json(raw_path, lines=True)
    
    # Write to local staging path
    stg_path = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    os.makedirs(os.path.dirname(stg_path), exist_ok=True)
    events_df.to_json(stg_path, orient="records", lines=True)
    print(f"Successfully staged events to: {stg_path}")


def check_dataframe_schema(df, expected_schema, dataset_name):
    """Checks DataFrame columns and data types against schema expectations."""
    for col, expected_type in expected_schema.items():
        # Check required columns
        if col not in df.columns:
            raise ValueError(f"Schema Drift: Missing required column '{col}' in {dataset_name} dataset")
            
        non_nulls = df[col].dropna()
        if non_nulls.empty:
            continue
            
        # Verify schema layout without silent coercion
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
                
        elif expected_type in ["date", "timestamp"]:
            for val in non_nulls:
                try:
                    pd.to_datetime(val)
                except Exception:
                    raise ValueError(f"Schema Drift: Column '{col}' in {dataset_name} contains invalid date/timestamp: '{val}'")


def validate_schema(**context):
    """Loads staged files and performs strict column schema verification."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    
    # Validate dimensions
    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    if os.path.exists(cust_stg):
        check_dataframe_schema(pd.read_csv(cust_stg), config["datasets"]["customers"]["schema"], "customers")
        
    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    if os.path.exists(prod_stg):
        check_dataframe_schema(pd.read_csv(prod_stg), config["datasets"]["products"]["schema"], "products")
        
    # Validate facts
    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    if os.path.exists(orders_stg):
        check_dataframe_schema(pd.read_csv(orders_stg), config["datasets"]["orders"]["schema"], "orders")
        
    events_stg = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    if os.path.exists(events_stg):
        check_dataframe_schema(pd.read_json(events_stg, lines=True), config["datasets"]["events"]["schema"], "events")
    print("Schema checks completed. No drifts detected.")


def validate_quality(**context):
    """Enforces row limits, null thresholds, key duplication, and referential constraints."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    
    # Load staged datasets
    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    events_stg = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    
    df_cust = pd.read_csv(cust_stg) if os.path.exists(cust_stg) else None
    df_prod = pd.read_csv(prod_stg) if os.path.exists(prod_stg) else None
    df_orders = pd.read_csv(orders_stg) if os.path.exists(orders_stg) else None
    df_events = pd.read_json(events_stg, lines=True) if os.path.exists(events_stg) else None

    # 1. Row count validations
    if df_cust is not None:
        min_c, max_c = config["datasets"]["customers"]["expected_row_count"]["min"], config["datasets"]["customers"]["expected_row_count"]["max"]
        if len(df_cust) < min_c or len(df_cust) > max_c:
            raise ValueError(f"Data Quality: Customers row count {len(df_cust)} violates bounds [{min_c}, {max_c}] (Abnormal Volume)")
            
    if df_prod is not None:
        min_p, max_p = config["datasets"]["products"]["expected_row_count"]["min"], config["datasets"]["products"]["expected_row_count"]["max"]
        if len(df_prod) < min_p or len(df_prod) > max_p:
            raise ValueError(f"Data Quality: Products row count {len(df_prod)} violates bounds [{min_p}, {max_p}] (Abnormal Volume)")

    if df_orders is not None:
        min_o, max_o = config["datasets"]["orders"]["expected_row_count_per_batch"]["min"], config["datasets"]["orders"]["expected_row_count_per_batch"]["max"]
        if len(df_orders) < min_o or len(df_orders) > max_o:
            raise ValueError(f"Data Quality: Orders row count {len(df_orders)} violates bounds [{min_o}, {max_o}] (Abnormal Volume)")
            
    if df_events is not None:
        min_e, max_e = config["datasets"]["events"]["expected_row_count_per_batch"]["min"], config["datasets"]["events"]["expected_row_count_per_batch"]["max"]
        if len(df_events) < min_e or len(df_events) > max_e:
            raise ValueError(f"Data Quality: Events row count {len(df_events)} violates bounds [{min_e}, {max_e}] (Abnormal Volume)")

    # 2. Null tolerance checks (orders only)
    if df_orders is not None:
        orders_nulls = config["datasets"]["orders"]["null_tolerance"]
        for col, tolerance in orders_nulls.items():
            null_pct = df_orders[col].isnull().mean()
            if null_pct > tolerance:
                raise ValueError(f"Data Quality: Column '{col}' null rate {null_pct:.4f} exceeds threshold {tolerance} (NULL Violation)")

    # 3. Duplicate checks
    if df_orders is not None:
        pk = config["datasets"]["orders"]["primary_key"]
        if df_orders[pk].duplicated().any():
            dup_count = df_orders[pk].duplicated().sum()
            raise ValueError(f"Data Quality: Duplicate primary keys found in orders. Found {dup_count} duplicates for column '{pk}'")
            
    if df_events is not None:
        pk = config["datasets"]["events"]["primary_key"]
        if df_events[pk].duplicated().any():
            dup_count = df_events[pk].duplicated().sum()
            raise ValueError(f"Data Quality: Duplicate primary keys found in events. Found {dup_count} duplicates for column '{pk}'")

    # 4. Referential checks
    if df_orders is not None:
        if df_cust is not None:
            orphans_cust = df_orders[~df_orders["customer_id"].isin(df_cust["customer_id"])]
            if len(orphans_cust) > 0:
                raise ValueError(f"Data Quality: Referential integrity broken. Found {len(orphans_cust)} orders referencing customer_id missing in customers (Broken Foreign Key)")
        if df_prod is not None:
            orphans_prod = df_orders[~df_orders["product_id"].isin(df_prod["product_id"])]
            if len(orphans_prod) > 0:
                raise ValueError(f"Data Quality: Referential integrity broken. Found {len(orphans_prod)} orders referencing product_id missing in products (Broken Foreign Key)")
                
    if df_events is not None and df_cust is not None:
        orphans_events = df_events[~df_events["customer_id"].isin(df_cust["customer_id"])]
        if len(orphans_events) > 0:
            raise ValueError(f"Data Quality: Referential integrity broken. Found {len(orphans_events)} events referencing customer_id missing in customers (Broken Foreign Key)")

    # 5. Freshness/SLA check
    if df_orders is not None:
        max_ts = pd.to_datetime(df_orders["order_ts"]).max()
        exec_dt = pd.to_datetime(execution_date)
        if max_ts < exec_dt:
            raise ValueError(f"Data Quality: Freshness SLA breach. Max timestamp {max_ts} is older than execution date {execution_date}")
    print("Local pipeline quality and referential integrity validations passed.")


def transform_data(**context):
    """Deduplicates data, standardizes date/timestamp formats, and writes to processed folder."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    
    # 1. Transform customers
    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    if os.path.exists(cust_stg):
        df = pd.read_csv(cust_stg)
        df = df.drop_duplicates(subset=["customer_id"])
        df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.strftime("%Y-%m-%d")
        cust_proc = os.path.join(data_dir, "processed", "customers.csv")
        os.makedirs(os.path.dirname(cust_proc), exist_ok=True)
        df.to_csv(cust_proc, index=False)
        print(f"Processed customers saved to {cust_proc}")

    # 2. Transform products
    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    if os.path.exists(prod_stg):
        df = pd.read_csv(prod_stg)
        df = df.drop_duplicates(subset=["product_id"])
        prod_proc = os.path.join(data_dir, "processed", "products.csv")
        os.makedirs(os.path.dirname(prod_proc), exist_ok=True)
        df.to_csv(prod_proc, index=False)
        print(f"Processed products saved to {prod_proc}")

    # 3. Transform orders
    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    if os.path.exists(orders_stg):
        df = pd.read_csv(orders_stg)
        df = df.drop_duplicates(subset=["order_id"])
        df["order_ts"] = pd.to_datetime(df["order_ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        orders_proc = os.path.join(data_dir, "processed", "orders", f"orders_{execution_date}.csv")
        os.makedirs(os.path.dirname(orders_proc), exist_ok=True)
        df.to_csv(orders_proc, index=False)
        print(f"Processed orders saved to {orders_proc}")

    # 4. Transform events
    events_stg = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    if os.path.exists(events_stg):
        df = pd.read_json(events_stg, lines=True)
        df = df.drop_duplicates(subset=["event_id"])
        df["event_ts"] = pd.to_datetime(df["event_ts"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        events_proc = os.path.join(data_dir, "processed", "events", f"events_{execution_date}.jsonl")
        os.makedirs(os.path.dirname(events_proc), exist_ok=True)
        df.to_json(events_proc, orient="records", lines=True)
        print(f"Processed events saved to {events_proc}")


def verify_final(**context):
    """Verifies output processed files exist, are readable, and contain no duplicates."""
    execution_date = context["ds"]
    data_dir = get_data_dir()
    
    cust_proc = os.path.join(data_dir, "processed", "customers.csv")
    prod_proc = os.path.join(data_dir, "processed", "products.csv")
    orders_proc = os.path.join(data_dir, "processed", "orders", f"orders_{execution_date}.csv")
    events_proc = os.path.join(data_dir, "processed", "events", f"events_{execution_date}.jsonl")
    
    for path in [cust_proc, prod_proc, orders_proc, events_proc]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Verification Failure: Processed output missing: {path}")
            
    # Key validation on written files
    df_o = pd.read_csv(orders_proc)
    if df_o["order_id"].duplicated().any():
        raise ValueError("Verification Failure: Duplicates found in processed orders primary key")
        
    df_e = pd.read_json(events_proc, lines=True)
    if df_e["event_id"].duplicated().any():
        raise ValueError("Verification Failure: Duplicates found in processed events primary key")
        
    print(f"Final verification succeeded. Output files written successfully for date: {execution_date}")


default_args = {
    "owner": "intern",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_task_failure,
    "sla": timedelta(hours=26),
}

with DAG(
    dag_id="self_healing_pipeline",
    description="Local daily ingestion -> validation -> transform -> processed data write",
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["intern-project", "local-pipeline"],
) as dag:

    t_ingest_orders = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
    )

    t_ingest_events = PythonOperator(
        task_id="ingest_events",
        python_callable=ingest_events,
    )

    t_validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    t_validate_quality = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
    )

    t_transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t_verify_final = PythonOperator(
        task_id="verify_final",
        python_callable=verify_final,
    )

    # Lineage dependency graph
    [t_ingest_orders, t_ingest_events] >> t_validate_schema >> t_validate_quality >> t_transform_data >> t_verify_final
