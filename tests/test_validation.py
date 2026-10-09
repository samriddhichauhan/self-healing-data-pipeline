import os
import pandas as pd
import pytest
from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events
from src.validation.schema import validate_schema
from src.validation.quality import validate_quality


def _ingest_all(ctx):
    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)


def test_valid_data_passes_schema_and_quality(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)
    validate_quality(**ctx)  # should not raise


def test_schema_missing_column(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_customers = os.path.join(str(pipeline_env), "raw", "customers.csv")
    df = pd.read_csv(raw_customers).drop(columns=["email"])
    df.to_csv(raw_customers, index=False)

    _ingest_all(ctx)
    with pytest.raises(ValueError, match="Schema Drift: Missing required column 'email'"):
        validate_schema(**ctx)


def test_schema_type_mismatch(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_products = os.path.join(str(pipeline_env), "raw", "products.csv")
    df = pd.read_csv(raw_products)
    df["price"] = "Ten Dollars"
    df.to_csv(raw_products, index=False)

    _ingest_all(ctx)
    with pytest.raises(ValueError, match="non-float values"):
        validate_schema(**ctx)


def test_abnormal_volume_low_rows(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    pd.read_csv(raw_orders).head(10).to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="Abnormal Volume"):
        validate_quality(**ctx)


def test_null_spike_violation(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    df = pd.read_csv(raw_orders)
    df.loc[0:49, "order_total"] = None
    df.to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="NULL Violation"):
        validate_quality(**ctx)


def test_referential_break(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    df = pd.read_csv(raw_orders)
    df.loc[0, "customer_id"] = "CUST999999"
    df.to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="Broken Foreign Key"):
        validate_quality(**ctx)


def test_staleness_sla_breach(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    df = pd.read_csv(raw_orders)
    df["order_ts"] = "2026-05-31 23:59:59"
    df.to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="Freshness SLA breach"):
        validate_quality(**ctx)


def test_duplicate_ingestion_self_heals_when_ollama_unavailable(pipeline_env, monkeypatch):
    """The DUPLICATE_INGESTION self-heal loop must work end-to-end using the
    deterministic fallback engine even with no Ollama server reachable —
    the project must never depend on an external service to function."""
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")  # nothing listens here
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)

    stg_path = os.path.join(str(pipeline_env), "staging", "stg_orders_2026-06-01.csv")
    df = pd.read_csv(stg_path)
    dupes = df.head(50)
    pd.concat([df, dupes], ignore_index=True).to_csv(stg_path, index=False)

    validate_quality(**ctx)  # must NOT raise — self-heals inline

    healed = pd.read_csv(stg_path)
    assert len(healed) == 300
    assert not healed["order_id"].duplicated().any()

    report_dir = os.path.join(str(pipeline_env), "incidents", "reports")
    reports = os.listdir(report_dir)
    assert any(f.endswith(".json") for f in reports)


def test_events_duplicate_primary_keys_is_not_auto_healed(pipeline_env):
    """Only orders duplicate ingestion is auto-fixable; events duplicates must
    still fail the task (no auto-fix path exists for events)."""
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)

    stg_path = os.path.join(str(pipeline_env), "staging", "stg_events_2026-06-01.jsonl")
    df = pd.read_json(stg_path, lines=True)
    dupes = df.head(10)
    pd.concat([df, dupes], ignore_index=True).to_json(stg_path, orient="records", lines=True)

    with pytest.raises(ValueError, match="Duplicate primary keys found in events"):
        validate_quality(**ctx)
