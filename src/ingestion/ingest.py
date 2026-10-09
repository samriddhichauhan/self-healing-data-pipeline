"""
Ingestion: locates the raw batch for the Airflow logical date, verifies it
exists and matches the expected date, and stages it. Fails loudly (with the
dataset, expected date, expected path, and actually-available files) rather
than silently falling back to another date or another dataset.

Staging is idempotent per logical date: if a partition is already staged
(e.g. by fault injection, or a previous partial run) this does NOT
re-overwrite it from raw. This is what lets self-healing survive a task
retry/resume without the retry itself wiping out the very fault it's meant
to detect and fix.
"""
import os
import glob
import pandas as pd
from ..config import config, get_data_dir
from ..agent.profiler import DataProfiler


def _list_available(dir_path: str) -> list:
    if not os.path.isdir(dir_path):
        return []
    return sorted(os.listdir(dir_path))


def _fail_missing(dataset: str, expected_date: str, expected_path: str, search_dir: str):
    available = _list_available(search_dir)
    raise FileNotFoundError(
        f"Ingestion failure for dataset '{dataset}': expected batch for logical date "
        f"'{expected_date}' at '{expected_path}', but that file does not exist. "
        f"Files actually available in '{search_dir}': {available or '(none)'}"
    )


def _stage_dimension(data_dir: str, dataset_name: str) -> str:
    """Stages a dimension table (customers/products). Idempotent per staging file."""
    raw_pattern = config["datasets"][dataset_name]["path_pattern"]
    raw_path = os.path.join(data_dir, "raw", raw_pattern)
    stg_path = os.path.join(data_dir, "staging", f"stg_{dataset_name}.csv")
    os.makedirs(os.path.dirname(stg_path), exist_ok=True)

    if os.path.exists(stg_path):
        print(f"Staged {dataset_name} already present — preserving existing staging: {stg_path}")
        return stg_path

    if not os.path.exists(raw_path):
        _fail_missing(dataset_name, "N/A (dimension table)", raw_path, os.path.dirname(raw_path))

    pd.read_csv(raw_path).to_csv(stg_path, index=False)
    print(f"Staged {dataset_name} to: {stg_path}")
    return stg_path


def ingest_dimensions(**_context):
    """Reads dimension datasets (customers, products) and stages them."""
    data_dir = get_data_dir()
    _stage_dimension(data_dir, "customers")
    _stage_dimension(data_dir, "products")


def ingest_orders(**context):
    """Verifies the orders batch exists for the logical date and stages it."""
    execution_date = context["ds"]
    data_dir = get_data_dir()

    stg_path = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    os.makedirs(os.path.dirname(stg_path), exist_ok=True)

    if os.path.exists(stg_path):
        print(f"Staged orders for {execution_date} already present — preserving existing staging: {stg_path}")
        orders_df = pd.read_csv(stg_path)
    else:
        raw_pattern = config["datasets"]["orders"]["path_pattern"]
        raw_path = os.path.join(data_dir, "raw", raw_pattern.format(date=execution_date))
        if not os.path.exists(raw_path):
            _fail_missing("orders", execution_date, raw_path, os.path.dirname(raw_path))
        print(f"Reading orders data from: {raw_path}")
        orders_df = pd.read_csv(raw_path)
        orders_df.to_csv(stg_path, index=False)
        print(f"Successfully staged orders to: {stg_path}")

    try:
        profiler = DataProfiler(output_dir=os.path.join(data_dir, "incidents", "profiles"))
        prof = profiler.profile_dataframe(orders_df, "orders", primary_key="order_id", date_column="order_ts")
        profiler.save_profile(prof, execution_date)
        print(f"[DATA PROFILER] Orders: {prof['row_count']} rows, {prof['null_percentage_overall']}% nulls, {prof['duplicate_percentage']}% duplicates.")
    except Exception as pe:
        print(f"[DATA PROFILER NOTICE] Profiling skipped: {pe}")


def ingest_events(**context):
    """Verifies the events batch exists for the logical date and stages it."""
    execution_date = context["ds"]
    data_dir = get_data_dir()

    stg_path = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")
    os.makedirs(os.path.dirname(stg_path), exist_ok=True)

    if os.path.exists(stg_path):
        print(f"Staged events for {execution_date} already present — preserving existing staging: {stg_path}")
        events_df = pd.read_json(stg_path, lines=True)
    else:
        raw_pattern = config["datasets"]["events"]["path_pattern"]
        raw_path = os.path.join(data_dir, "raw", raw_pattern.format(date=execution_date))
        if not os.path.exists(raw_path):
            _fail_missing("events", execution_date, raw_path, os.path.dirname(raw_path))
        print(f"Reading events data from: {raw_path}")
        events_df = pd.read_json(raw_path, lines=True)
        events_df.to_json(stg_path, orient="records", lines=True)
        print(f"Successfully staged events to: {stg_path}")

    try:
        profiler = DataProfiler(output_dir=os.path.join(data_dir, "incidents", "profiles"))
        prof = profiler.profile_dataframe(events_df, "events", primary_key="event_id", date_column="event_ts")
        profiler.save_profile(prof, execution_date)
        print(f"[DATA PROFILER] Events: {prof['row_count']} rows, {prof['null_percentage_overall']}% nulls, {prof['duplicate_percentage']}% duplicates.")
    except Exception as pe:
        print(f"[DATA PROFILER NOTICE] Profiling skipped: {pe}")
