import os
import sys
import json
import shutil
import filecmp
import subprocess
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from src.config import config
from src.validation.schema import check_dataframe_schema, validate_schema
from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events
from src.validation.quality import validate_quality
from tests.conftest import write_valid_dataset, PROJECT_ROOT

SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "generate_daily_data.py")
sys.path.insert(0, os.path.dirname(SCRIPT))
from generate_daily_data import generate_for_date  # noqa: E402

DAY = datetime(2026, 9, 28)
DS = "2026-09-28"
ORDERS_SCHEMA = config["datasets"]["orders"]["schema"]
EVENTS_SCHEMA = config["datasets"]["events"]["schema"]


def _paths(raw, ds=DS):
    return (os.path.join(raw, "orders", f"orders_{ds}.csv"),
            os.path.join(raw, "events", f"events_{ds}.jsonl"))


@pytest.fixture
def raw_dir(pipeline_env):
    return os.path.join(str(pipeline_env), "raw")


def test_cli_creates_orders_and_events_files(raw_dir):
    res = subprocess.run([sys.executable, SCRIPT, "--raw", raw_dir, "--date", DS], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    orders_path, events_path = _paths(raw_dir)
    assert os.path.isfile(orders_path)
    assert os.path.isfile(events_path)


def test_generated_schema_and_row_counts(raw_dir):
    generate_for_date(raw_dir, DAY)
    orders_path, events_path = _paths(raw_dir)
    orders = pd.read_csv(orders_path)
    events = pd.read_json(events_path, lines=True)

    assert list(orders.columns) == list(ORDERS_SCHEMA)
    assert list(events.columns) == list(EVENTS_SCHEMA)
    check_dataframe_schema(orders, ORDERS_SCHEMA, "orders")
    check_dataframe_schema(events, EVENTS_SCHEMA, "events")
    assert len(orders) == 300
    assert len(events) == 1200
    assert not orders.isnull().any().any() and not events.isnull().any().any()
    assert (pd.to_datetime(orders["order_ts"]).dt.strftime("%Y-%m-%d") == DS).all()
    assert (pd.to_datetime(events["event_ts"]).dt.strftime("%Y-%m-%d") == DS).all()

    # Order IDs continue after the highest existing ID — never collide with another date.
    existing = pd.read_csv(os.path.join(raw_dir, "orders", "orders_2026-06-01.csv"))
    assert not orders["order_id"].isin(existing["order_id"]).any()


def test_generated_batch_passes_existing_pipeline_validation(raw_dir):
    generate_for_date(raw_dir, DAY)
    ctx = {"ds": DS}
    ingest_dimensions(**ctx)
    ingest_orders(**ctx)
    ingest_events(**ctx)
    validate_schema(**ctx)
    validate_quality(**ctx)  # row bounds, nulls, PK uniqueness, FKs, freshness


def test_rerun_is_deterministic_and_leaves_existing_files_untouched(tmp_path):
    runs = []
    for name in ("a", "b"):
        base = os.path.join(str(tmp_path), name)
        write_valid_dataset(base)
        generate_for_date(os.path.join(base, "raw"), DAY)
        runs.append(os.path.join(base, "raw"))
    for a, b in zip(_paths(runs[0]), _paths(runs[1])):
        assert filecmp.cmp(a, b, shallow=False)

    before = [open(p, "rb").read() for p in _paths(runs[0])]
    result = generate_for_date(runs[0], DAY)
    assert result["orders"][1] is None and result["events"][1] is None
    assert [open(p, "rb").read() for p in _paths(runs[0])] == before
    shutil.rmtree(str(tmp_path), ignore_errors=True)


def test_existing_date_is_left_untouched_without_force_and_replaced_with_force(raw_dir):
    orders_path, events_path = _paths(raw_dir, "2026-06-01")
    before = open(orders_path, "rb").read()
    res = subprocess.run([sys.executable, SCRIPT, "--raw", raw_dir, "--date", "2026-06-01"], capture_output=True, text=True)
    assert res.returncode == 0 and "already exists - left untouched" in res.stdout
    assert open(orders_path, "rb").read() == before

    old_ids = set(pd.read_csv(orders_path)["order_id"])
    result = generate_for_date(raw_dir, datetime(2026, 6, 1), force=True)
    assert result["orders"][1] == 300 and result["events"][1] == 1200
    new = pd.read_csv(orders_path)
    assert new["order_id"].is_unique and not (set(new["order_id"]) & old_ids)


def test_real_2026_09_22_batch_is_still_valid():
    raw = os.path.join(PROJECT_ROOT, "data", "raw")
    orders_path, events_path = _paths(raw, "2026-09-22")
    if not os.path.exists(orders_path):
        pytest.skip("2026-09-22 batch not present in this checkout")
    orders = pd.read_csv(orders_path)
    events = pd.read_json(events_path, lines=True)
    customers = pd.read_csv(os.path.join(raw, "customers.csv"))
    products = pd.read_csv(os.path.join(raw, "products.csv"))

    check_dataframe_schema(orders, ORDERS_SCHEMA, "orders")
    check_dataframe_schema(events, EVENTS_SCHEMA, "events")
    assert len(orders) == 300 and len(events) == 1200
    assert orders["order_id"].is_unique and events["event_id"].is_unique
    assert orders["customer_id"].isin(customers["customer_id"]).all()
    assert orders["product_id"].isin(products["product_id"]).all()
    assert events["customer_id"].isin(customers["customer_id"]).all()


def test_failed_run_is_recorded_with_its_escalations(pipeline_env, monkeypatch):
    from src.monitoring import monitor

    report_dir = os.path.join(str(pipeline_env), "incidents", "reports")
    os.makedirs(report_dir)
    for inc_id, run_id, status in [("INC-1", "run_x", "ESCALATED"), ("INC-2", "other_run", "ESCALATED")]:
        with open(os.path.join(report_dir, f"inc_{inc_id}.json"), "w") as f:
            json.dump({"incident_id": inc_id, "status": status, "evidence": {"run_id": run_id}}, f)

    recorded = {}
    monkeypatch.setattr(monitor, "record_failed_pipeline_run", lambda **kw: recorded.update(kw))
    dag_run = SimpleNamespace(run_id="run_x", dag_id="self_healing_pipeline", start_date=None)
    monitor.record_failed_run({"ds": DS, "dag_run": dag_run})

    assert recorded["run_id"] == "run_x" and recorded["logical_date"] == DS
    assert recorded["incident_count"] == 1
    assert recorded["escalation_count"] == 1
    assert recorded["auto_fix_count"] == 0
