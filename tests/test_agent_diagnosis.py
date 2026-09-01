"""
Unit tests for AI Diagnostic Agent, Incident Model, and Policy Gate
"""
import os
import sys
import tempfile
import pytest

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../apps/agent"))
if base_dir not in sys.path:
    sys.path.append(base_dir)

from agent.models.incident import IncidentReport
from agent.policies.policy_gate import PolicyGate
from agent.diagnosis.engine import DiagnosticEngine
from agent.tools.incident_tool import IncidentTool
from agent.remediation.executor import RemediationExecutor
from agent.verification.verifier import RemediationVerifier


def test_incident_report_creation_and_save():
    with tempfile.TemporaryDirectory() as tmp_dir:
        report = IncidentReport(
            incident_id="INC-9999",
            timestamp="2026-06-01",
            pipeline="self_healing_pipeline",
            dataset="orders",
            check="validate_quality",
            severity="MEDIUM",
            observed=1450,
            expected="240-360 rows/day",
            evidence={"observed": 1450, "expected_max": 360},
            hypothesis="Duplicate file ingestion",
            confidence=0.95,
            blast_radius="staged orders dataset",
            action="AUTO_FIX",
            status="PENDING_APPROVAL"
        )

        json_path, md_path = report.save(tmp_dir)
        assert os.path.exists(json_path)
        assert os.path.exists(md_path)

        tool = IncidentTool(reports_dir=tmp_dir)
        inc = tool.get_incident("INC-9999")
        assert inc is not None
        assert inc["incident_id"] == "INC-9999"
        assert inc["action"] == "AUTO_FIX"


def test_policy_gate_auto_fix_vs_escalate():
    gate = PolicyGate(confidence_threshold=0.85)

    # Low risk, high confidence -> AUTO_FIX
    action, _ = gate.evaluate("DUPLICATE_INGESTION", 0.95, {})
    assert action == "AUTO_FIX"

    # High risk -> ESCALATE
    action, _ = gate.evaluate("SCHEMA_DRIFT", 0.95, {})
    assert action == "ESCALATE"

    # Low confidence -> ESCALATE
    action, _ = gate.evaluate("DUPLICATE_INGESTION", 0.70, {})
    assert action == "ESCALATE"


def test_diagnostic_engine_reasoning():
    engine = DiagnosticEngine()
    report = engine.diagnose_fault(
        incident_id="INC-1001",
        pipeline_id="self_healing_pipeline",
        task_id="validate_schema",
        dataset="products",
        fault_category="SCHEMA_DRIFT",
        observed="price expected FLOAT got STRING",
        expected="FLOAT",
        evidence={"drift_field": "price"},
        execution_date="2026-06-01"
    )

    assert report.incident_id == "INC-1001"
    assert report.action == "ESCALATE"
    assert "Schema drift" in report.hypothesis
    assert report.confidence >= 0.85


def test_remediation_and_verifier():
    with tempfile.TemporaryDirectory() as tmp_dir:
        stg_dir = os.path.join(tmp_dir, "staging")
        os.makedirs(stg_dir, exist_ok=True)

        orders_file = os.path.join(stg_dir, "stg_orders_2026-06-01.csv")
        with open(orders_file, "w") as f:
            f.write("order_id,customer_id,order_ts\nORD1,C1,2026-06-01\nORD1,C1,2026-06-01\nORD2,C2,2026-06-01\n")

        executor = RemediationExecutor(data_dir=tmp_dir)
        fix_res = executor.deduplicate_dataset("orders", "order_id", "2026-06-01")
        assert fix_res["status"] == "SUCCESS"
        assert fix_res["removed_duplicates"] == 1

        verifier = RemediationVerifier(data_dir=tmp_dir)
        verify_res = verifier.verify("orders", "order_id", "2026-06-01")
        assert verify_res["verified"] is True
        assert verify_res["status"] == "PASSED"
