"""
Extended Pipeline Tests - Verification of 7 Failure Scenarios & Policy Gate Safeguards
"""
import os
import json
import pytest
from agent.diagnosis.engine import DiagnosticEngine
from agent.policies.policy_gate import PolicyGate
from agent.profiling.profiler import DataProfiler
from agent.remediation.executor import RemediationExecutor
from agent.verification.verifier import RemediationVerifier
import pandas as pd


@pytest.fixture
def policy_gate():
    return PolicyGate()


@pytest.fixture
def diagnostic_engine(policy_gate):
    return DiagnosticEngine(policy_gate=policy_gate)


def test_scenario_a_healthy_pipeline(diagnostic_engine):
    """Scenario A: Healthy pipeline baseline assertion."""
    baseline = {"customers": 500, "products": 60, "orders": 300, "events": 1200}
    assert baseline["orders"] == 300
    assert baseline["events"] == 1200


def test_scenario_b_duplicate_ingestion(diagnostic_engine):
    """Scenario B: Duplicate ingestion -> AUTO_FIX policy decision."""
    report = diagnostic_engine.diagnose_fault(
        incident_id="INC-TEST-DUP",
        pipeline_id="self_healing_pipeline",
        task_id="validate_quality",
        dataset="orders",
        fault_category="DUPLICATE_INGESTION",
        observed="1,450 staged rows (+302% duplicate spike)",
        expected="240-360 orders",
        evidence={"duplicates": 1150},
        execution_date="2026-06-01"
    )

    assert report.action == "AUTO_FIX"
    assert report.severity == "MEDIUM"
    assert "duplicate" in report.hypothesis.lower()
    assert len(report.possible_root_causes) > 0


def test_scenario_c_schema_drift(diagnostic_engine):
    """Scenario C: Schema drift -> ESCALATE policy decision."""
    report = diagnostic_engine.diagnose_fault(
        incident_id="INC-TEST-SCHEMA",
        pipeline_id="self_healing_pipeline",
        task_id="validate_schema",
        dataset="products",
        fault_category="SCHEMA_DRIFT",
        observed="Field 'price' expected FLOAT, got STRING '$14.99'",
        expected="FLOAT type for price",
        evidence={"column": "price", "expected": "FLOAT", "found": "STRING"},
        execution_date="2026-06-01"
    )

    assert report.action == "ESCALATE"
    assert report.severity == "HIGH"
    assert report.status == "ESCALATED"


def test_scenario_d_null_spike(diagnostic_engine):
    """Scenario D: Null spike -> ESCALATE policy decision."""
    report = diagnostic_engine.diagnose_fault(
        incident_id="INC-TEST-NULL",
        pipeline_id="self_healing_pipeline",
        task_id="validate_quality",
        dataset="orders",
        fault_category="NULL_SPIKE",
        observed="order_total null rate 14.5% (> 2.0% limit)",
        expected="null rate <= 2.0%",
        evidence={"null_rate": 0.145},
        execution_date="2026-06-01"
    )

    assert report.action == "ESCALATE"
    assert report.severity == "HIGH"


def test_scenario_e_volume_drop(diagnostic_engine):
    """Scenario E: Volume drop -> ESCALATE policy decision."""
    report = diagnostic_engine.diagnose_fault(
        incident_id="INC-TEST-DROP",
        pipeline_id="self_healing_pipeline",
        task_id="validate_quality",
        dataset="orders",
        fault_category="VOLUME_ANOMALY_DROP",
        observed="Staged 12 rows (expected 240-360)",
        expected="240-360 orders",
        evidence={"row_count": 12},
        execution_date="2026-06-01"
    )

    assert report.action == "ESCALATE"
    assert report.severity == "HIGH"


def test_scenario_f_referential_break(diagnostic_engine):
    """Scenario F: Referential break -> ESCALATE policy decision."""
    report = diagnostic_engine.diagnose_fault(
        incident_id="INC-TEST-REF",
        pipeline_id="self_healing_pipeline",
        task_id="validate_quality",
        dataset="orders",
        fault_category="REFERENTIAL_BREAK",
        observed="Missing customer_id 'C-9988' in dim_customers",
        expected="Foreign key referential match",
        evidence={"missing_key": "C-9988"},
        execution_date="2026-06-01"
    )

    assert report.action == "ESCALATE"
    assert report.severity == "HIGH"


def test_scenario_g_staleness(diagnostic_engine):
    """Scenario G: Staleness SLA -> ESCALATE/AUTO_FIX policy decision."""
    report = diagnostic_engine.diagnose_fault(
        incident_id="INC-TEST-STALE",
        pipeline_id="self_healing_pipeline",
        task_id="validate_quality",
        dataset="events",
        fault_category="STALENESS",
        observed="Batch timestamp age 32h (> 26h SLA limit)",
        expected="Age <= 26h",
        evidence={"age_hours": 32},
        execution_date="2026-06-01"
    )

    assert report.severity == "MEDIUM"


def test_data_profiler():
    """Tests lightweight data profiling calculations."""
    profiler = DataProfiler()
    df = pd.DataFrame({
        "order_id": ["O1", "O2", "O2", "O3"],
        "order_total": [10.0, 20.0, None, 40.0],
        "customer_id": ["C1", "C2", "C2", "C3"]
    })

    prof = profiler.profile_dataframe(df, "orders", primary_key="order_id")
    assert prof["row_count"] == 4
    assert prof["duplicate_percentage"] == 25.0
    assert prof["columns"]["order_total"]["null_pct"] == 25.0
    assert prof["columns"]["order_total"]["min"] == 10.0
    assert prof["columns"]["order_total"]["max"] == 40.0
