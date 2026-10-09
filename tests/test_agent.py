import os
import tempfile
import pytest
from src.agent.incident import IncidentReport
from src.agent.policy_gate import PolicyGate
from src.agent.diagnosis_engine import DiagnosticEngine
from src.agent.ollama_adapter import OllamaDiagnosticAdapter


def test_incident_report_round_trips_to_disk():
    with tempfile.TemporaryDirectory() as tmp_dir:
        report = IncidentReport(
            incident_id="INC-9999", timestamp="2026-06-01", pipeline="self_healing_pipeline",
            dataset="orders", check="validate_quality", severity="MEDIUM", observed=1450,
            expected="240-360 rows/day", evidence={"observed": 1450, "expected_max": 360},
            hypothesis="Duplicate file ingestion", confidence=0.95, blast_radius="staged orders dataset",
            action="AUTO_FIX", status="PENDING_APPROVAL",
        )
        json_path, md_path = report.save(tmp_dir)
        assert os.path.exists(json_path)
        assert os.path.exists(md_path)

        import json
        with open(json_path) as f:
            data = json.load(f)
        reloaded = IncidentReport.from_dict(data)
        assert reloaded.incident_id == "INC-9999"
        assert reloaded.action == "AUTO_FIX"


def test_policy_gate_auto_fix_vs_escalate():
    gate = PolicyGate(confidence_threshold=0.85)
    assert gate.evaluate("DUPLICATE_INGESTION", 0.95, {})[0] == "AUTO_FIX"
    assert gate.evaluate("SCHEMA_DRIFT", 0.95, {})[0] == "ESCALATE"
    assert gate.evaluate("NULL_SPIKE", 0.99, {})[0] == "ESCALATE"
    assert gate.evaluate("REFERENTIAL_BREAK", 0.99, {})[0] == "ESCALATE"
    assert gate.evaluate("VOLUME_ANOMALY_DROP", 0.99, {})[0] == "ESCALATE"
    assert gate.evaluate("STALENESS", 0.99, {})[0] == "ESCALATE"
    # Low confidence overrides even an otherwise-safe category
    assert gate.evaluate("DUPLICATE_INGESTION", 0.70, {})[0] == "ESCALATE"


@pytest.mark.parametrize("category,expected_action", [
    ("SCHEMA_DRIFT", "ESCALATE"),
    ("NULL_SPIKE", "ESCALATE"),
    ("DUPLICATE_INGESTION", "AUTO_FIX"),
    ("REFERENTIAL_BREAK", "ESCALATE"),
    ("VOLUME_ANOMALY_DROP", "ESCALATE"),
    ("STALENESS", "ESCALATE"),
])
def test_diagnostic_engine_reasoning_per_category(category, expected_action):
    engine = DiagnosticEngine()
    report = engine.diagnose_fault(
        incident_id="INC-1001", pipeline_id="self_healing_pipeline", task_id="validate_quality",
        dataset="orders", fault_category=category, observed="observed value", expected="expected value",
        evidence={}, execution_date="2026-06-01",
    )
    assert report.action == expected_action
    assert 0.0 <= report.confidence <= 1.0
    assert report.hypothesis


def test_ollama_adapter_falls_back_when_unreachable(monkeypatch):
    """No external paid API and no Ollama running -> must still produce a
    valid, policy-gated diagnosis via the deterministic fallback."""
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")
    adapter = OllamaDiagnosticAdapter()
    report = adapter.analyze_incident(
        incident_id="INC-2002", pipeline_id="self_healing_pipeline", task_id="validate_quality",
        dataset="orders", fault_category="DUPLICATE_INGESTION", observed="350 rows (50 dup)",
        expected="300 rows", evidence={}, execution_date="2026-06-01",
    )
    assert report.action == "AUTO_FIX"
    assert "RULE_BASED_ENGINE" in report.evidence["ai_mode"]


def test_ollama_prompt_excludes_internal_engine_fields():
    # The rule-based engine runs first and tags the shared evidence dict; those
    # internal fields must not leak into the prompt sent to the LLM.
    adapter = OllamaDiagnosticAdapter(host="http://127.0.0.1:1")
    evidence = {"rows": 350, "duplicates": 50}
    adapter.fallback_engine.diagnose_fault(
        incident_id="INC-T", pipeline_id="p", task_id="validate_quality", dataset="orders",
        fault_category="DUPLICATE_INGESTION", observed="o", expected="e", evidence=evidence,
        execution_date="2026-06-01",
    )
    assert evidence["diagnosis_source"] == "RULE_BASED_ENGINE"

    prompt = adapter._build_prompt(
        "INC-T", "p", "validate_quality", "orders", "DUPLICATE_INGESTION",
        "350 rows (50 duplicate primary keys)", "300 rows (0 duplicates)", evidence, "2026-06-01",
    )
    evidence_line = next(l for l in prompt.splitlines() if l.startswith("Evidence:"))
    assert evidence_line == 'Evidence: {"rows": 350, "duplicates": 50}'
    assert "RULE_BASED_ENGINE" not in prompt
