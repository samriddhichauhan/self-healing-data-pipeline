"""
Real AI Integration Tests — Ollama Diagnostic Adapter

Tests:
  1.  Ollama available  → real AI diagnosis returned (mocked HTTP)
  2.  Ollama offline    → deterministic fallback
  3.  Ollama timeout    → deterministic fallback
  4.  Invalid JSON      → safe fallback
  5.  Missing JSON keys → safe fallback
  6.  Duplicate ingestion → AI recommends AUTO_FIX
  7.  Schema drift       → AI recommends ESCALATE
  8.  Policy Gate receives AI confidence / risk / action
  9.  Auto-fix only when Policy Gate authorizes
  10. Verification checks actual post-fix state
  11. ai_mode label is never falsified

All Ollama HTTP calls are mocked — tests run offline.
"""
import os
import sys
import json
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from io import BytesIO

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../apps/agent"))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from agent.diagnosis.ollama_adapter import OllamaDiagnosticAdapter
from agent.diagnosis.engine import DiagnosticEngine
from agent.policies.policy_gate import PolicyGate
from agent.remediation.executor import RemediationExecutor
from agent.verification.verifier import RemediationVerifier


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_ollama_tags_resp(model_name="llama3.2:latest"):
    """Fake /api/tags response."""
    body = json.dumps({"models": [{"name": model_name}]}).encode()
    resp = MagicMock()
    resp.status = 200
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def _make_ollama_generate_resp(ai_payload: dict):
    """Fake /api/generate response wrapping AI payload."""
    inner = json.dumps(ai_payload)
    body = json.dumps({"response": inner, "done": True}).encode()
    resp = MagicMock()
    resp.status = 200
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


STANDARD_KWARGS = dict(
    incident_id="INC-TEST-001",
    pipeline_id="self_healing_pipeline",
    task_id="validate_quality",
    dataset="orders",
    fault_category="DUPLICATE_INGESTION",
    observed="350 rows (50 duplicates found)",
    expected="300 rows (0 duplicates)",
    evidence={"duplicate_count": 50},
    execution_date="2026-06-01",
)

SCHEMA_KWARGS = dict(
    incident_id="INC-TEST-SCHEMA-001",
    pipeline_id="self_healing_pipeline",
    task_id="validate_schema",
    dataset="products",
    fault_category="SCHEMA_DRIFT",
    observed="Column 'price' type STRING instead of FLOAT",
    expected="FLOAT type",
    evidence={"column": "price", "expected": "FLOAT", "found": "STRING"},
    execution_date="2026-06-01",
)


# ──────────────────────────────────────────────────────────────────────────────
# Test 1: Ollama available → real AI diagnosis returned
# ──────────────────────────────────────────────────────────────────────────────

def test_ollama_available_returns_ai_diagnosis():
    """When Ollama is available and returns valid JSON, ai_mode must be LOCAL_OLLAMA_AI."""
    ai_payload = {
        "root_cause": "Duplicate ingestion from upstream retry causing 50 extra rows",
        "confidence": 0.96,
        "risk": "LOW",
        "recommended_action": "AUTO_FIX",
        "reason": "Deduplication by primary key is idempotent and safe",
        "blast_radius": "Staged orders dataset only",
        "possible_root_causes": ["Scheduler retry", "Double file upload"],
    }

    tags_resp = _make_ollama_tags_resp("llama3.2:latest")
    gen_resp = _make_ollama_generate_resp(ai_payload)

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert "LOCAL_OLLAMA_AI" in report.evidence["ai_mode"]
    assert "llama3.2" in report.evidence["ai_mode"]
    assert report.confidence == pytest.approx(0.96, abs=0.001)
    assert report.evidence["ollama_risk"] == "LOW"
    assert report.evidence["ollama_recommended_action"] == "AUTO_FIX"
    assert report.action == "AUTO_FIX"  # Policy Gate should agree
    assert "Duplicate ingestion" in report.hypothesis


# ──────────────────────────────────────────────────────────────────────────────
# Test 2: Ollama offline → deterministic fallback
# ──────────────────────────────────────────────────────────────────────────────

def test_ollama_offline_uses_rule_based_fallback():
    """When Ollama is offline, ai_mode must be RULE_BASED_ENGINE (not LOCAL_OLLAMA_AI)."""
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("Connection refused")):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert "RULE_BASED_ENGINE" in report.evidence["ai_mode"]
    assert "LOCAL_OLLAMA_AI" not in report.evidence["ai_mode"]
    assert report.incident_id == "INC-TEST-001"
    # Rule-based engine still produces a valid decision
    assert report.action in ("AUTO_FIX", "ESCALATE")


# ──────────────────────────────────────────────────────────────────────────────
# Test 3: Ollama request times out → deterministic fallback
# ──────────────────────────────────────────────────────────────────────────────

def test_ollama_timeout_uses_rule_based_fallback():
    """When Ollama generation times out, ai_mode must be RULE_BASED_ENGINE."""
    tags_resp = _make_ollama_tags_resp()

    def _timeout_on_generate(req, timeout=None):
        # Tags call succeeds, generate call times out
        if "generate" in req.full_url:
            raise TimeoutError("Read timed out")
        return tags_resp

    with patch("urllib.request.urlopen", side_effect=_timeout_on_generate):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert "RULE_BASED_ENGINE" in report.evidence["ai_mode"]
    assert report.incident_id == "INC-TEST-001"


# ──────────────────────────────────────────────────────────────────────────────
# Test 4: Invalid JSON from AI → safe fallback, never faked
# ──────────────────────────────────────────────────────────────────────────────

def test_invalid_ai_json_triggers_safe_fallback():
    """When AI returns non-JSON text, fallback must occur without faking a result."""
    bad_resp_body = json.dumps({
        "response": "Sorry, I cannot answer that in JSON format right now.",
        "done": True,
    }).encode()
    gen_resp = MagicMock()
    gen_resp.status = 200
    gen_resp.read.return_value = bad_resp_body
    gen_resp.__enter__ = lambda s: s
    gen_resp.__exit__ = MagicMock(return_value=False)

    tags_resp = _make_ollama_tags_resp()

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert "RULE_BASED_ENGINE" in report.evidence["ai_mode"]
    # Must NOT appear to be AI-generated
    assert "LOCAL_OLLAMA_AI" not in report.evidence.get("ai_mode", "")


# ──────────────────────────────────────────────────────────────────────────────
# Test 5: AI JSON missing required keys → fallback
# ──────────────────────────────────────────────────────────────────────────────

def test_ai_missing_required_keys_triggers_fallback():
    """AI JSON that lacks required keys (e.g. missing 'confidence') must trigger fallback."""
    incomplete_payload = {
        "root_cause": "Some cause",
        # Missing: confidence, risk, recommended_action, reason, blast_radius
    }
    tags_resp = _make_ollama_tags_resp()
    gen_resp = _make_ollama_generate_resp(incomplete_payload)

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert "RULE_BASED_ENGINE" in report.evidence["ai_mode"]


# ──────────────────────────────────────────────────────────────────────────────
# Test 6: Duplicate ingestion → AI recommends AUTO_FIX
# ──────────────────────────────────────────────────────────────────────────────

def test_duplicate_ingestion_ai_recommends_auto_fix():
    """For duplicate_ingestion, AI should recommend AUTO_FIX and Policy Gate agrees."""
    ai_payload = {
        "root_cause": "Upstream batch scheduler retried file upload, creating duplicate rows",
        "confidence": 0.95,
        "risk": "LOW",
        "recommended_action": "AUTO_FIX",
        "reason": "Deduplication by primary key is safe and idempotent",
        "blast_radius": "Staged orders table only",
        "possible_root_causes": ["Retry after network timeout", "Scheduler double-trigger"],
    }

    tags_resp = _make_ollama_tags_resp()
    gen_resp = _make_ollama_generate_resp(ai_payload)

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert report.action == "AUTO_FIX"
    assert report.confidence >= 0.85
    assert report.evidence["ollama_risk"] == "LOW"
    assert "LOCAL_OLLAMA_AI" in report.evidence["ai_mode"]


# ──────────────────────────────────────────────────────────────────────────────
# Test 7: Schema drift → AI recommends ESCALATE
# ──────────────────────────────────────────────────────────────────────────────

def test_schema_drift_ai_recommends_escalate():
    """For schema_drift, AI should recommend ESCALATE and Policy Gate agrees."""
    ai_payload = {
        "root_cause": "Upstream API changed 'price' field type from FLOAT to STRING without notice",
        "confidence": 0.93,
        "risk": "HIGH",
        "recommended_action": "ESCALATE",
        "reason": "Automatic schema modification risks corrupting downstream consumers",
        "blast_radius": "All downstream ETL pipelines consuming products dataset",
        "possible_root_causes": ["API version upgrade", "Upstream schema migration"],
    }

    tags_resp = _make_ollama_tags_resp()
    gen_resp = _make_ollama_generate_resp(ai_payload)

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**SCHEMA_KWARGS)

    assert report.action == "ESCALATE"
    assert report.status == "ESCALATED"
    assert report.evidence["ollama_risk"] == "HIGH"
    assert "LOCAL_OLLAMA_AI" in report.evidence["ai_mode"]


# ──────────────────────────────────────────────────────────────────────────────
# Test 8: Policy Gate receives real AI confidence / risk / action
# ──────────────────────────────────────────────────────────────────────────────

def test_policy_gate_receives_real_ai_values():
    """Policy Gate must see the AI's actual confidence and risk, not hardcoded values."""
    ai_payload = {
        "root_cause": "Partial batch failure causing volume drop",
        "confidence": 0.72,   # below threshold → should ESCALATE even if category is AUTO_FIX
        "risk": "LOW",
        "recommended_action": "AUTO_FIX",
        "reason": "Low confidence due to ambiguous evidence",
        "blast_radius": "Orders staging table",
        "possible_root_causes": ["Network failure", "Source timeout"],
    }

    tags_resp = _make_ollama_tags_resp()
    gen_resp = _make_ollama_generate_resp(ai_payload)

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    # Confidence 0.72 < 0.85 threshold → Policy Gate must ESCALATE
    assert report.action == "ESCALATE"
    assert report.confidence == pytest.approx(0.72, abs=0.001)
    assert "LOCAL_OLLAMA_AI" in report.evidence["ai_mode"]
    # The AI values must be stored in evidence
    assert report.evidence["ollama_confidence"] == pytest.approx(0.72, abs=0.001)


# ──────────────────────────────────────────────────────────────────────────────
# Test 9: Auto-fix only executes when Policy Gate authorizes it
# ──────────────────────────────────────────────────────────────────────────────

def test_auto_fix_only_when_policy_gate_authorizes():
    """AI + Policy Gate together determine AUTO_FIX; it should not bypass the gate."""
    # Case A: AI says AUTO_FIX, high confidence, low risk → should get AUTO_FIX
    ai_payload_a = {
        "root_cause": "Duplicate ingestion from retry",
        "confidence": 0.96,
        "risk": "LOW",
        "recommended_action": "AUTO_FIX",
        "reason": "Safe idempotent deduplication",
        "blast_radius": "Staging only",
        "possible_root_causes": ["Scheduler retry"],
    }

    tags_resp_a = _make_ollama_tags_resp()
    gen_resp_a = _make_ollama_generate_resp(ai_payload_a)

    with patch("urllib.request.urlopen", side_effect=[tags_resp_a, gen_resp_a]):
        adapter = OllamaDiagnosticAdapter()
        report_a = adapter.analyze_incident(**STANDARD_KWARGS)

    assert report_a.action == "AUTO_FIX"

    # Case B: AI says AUTO_FIX for SCHEMA_DRIFT (high risk category) → Policy Gate overrides to ESCALATE
    ai_payload_b = {
        "root_cause": "Schema drift detected",
        "confidence": 0.96,
        "risk": "HIGH",
        "recommended_action": "AUTO_FIX",   # AI says AUTO_FIX — but policy gate should override
        "reason": "Despite high confidence, schema changes are destructive",
        "blast_radius": "All downstream ETL",
        "possible_root_causes": ["API upgrade"],
    }

    tags_resp_b = _make_ollama_tags_resp()
    gen_resp_b = _make_ollama_generate_resp(ai_payload_b)

    with patch("urllib.request.urlopen", side_effect=[tags_resp_b, gen_resp_b]):
        adapter = OllamaDiagnosticAdapter()
        report_b = adapter.analyze_incident(**SCHEMA_KWARGS)

    # Policy Gate should override AI's AUTO_FIX for SCHEMA_DRIFT
    assert report_b.action == "ESCALATE"


# ──────────────────────────────────────────────────────────────────────────────
# Test 10: Verification checks actual post-fix state
# ──────────────────────────────────────────────────────────────────────────────

def test_verification_checks_actual_post_fix_state():
    """After deduplication, verifier must confirm zero duplicates remain (real file I/O)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stg_dir = os.path.join(tmp_dir, "staging")
        os.makedirs(stg_dir, exist_ok=True)

        orders_file = os.path.join(stg_dir, "stg_orders_2026-06-01.csv")
        # Write file with 2 duplicate rows (ORD1 appears twice)
        with open(orders_file, "w") as f:
            f.write("order_id,customer_id,order_ts\n")
            f.write("ORD1,C1,2026-06-01\n")
            f.write("ORD1,C1,2026-06-01\n")   # duplicate
            f.write("ORD2,C2,2026-06-01\n")

        executor = RemediationExecutor(data_dir=tmp_dir)
        fix_res = executor.deduplicate_dataset("orders", "order_id", "2026-06-01")
        assert fix_res["status"] == "SUCCESS"
        assert fix_res["removed_duplicates"] == 1

        verifier = RemediationVerifier(data_dir=tmp_dir)
        verify_res = verifier.verify("orders", "order_id", "2026-06-01")
        assert verify_res["verified"] is True
        assert verify_res["duplicate_count"] == 0
        assert verify_res["status"] == "PASSED"
        assert verify_res["remaining_rows"] == 2  # ORD1 + ORD2


# ──────────────────────────────────────────────────────────────────────────────
# Test 11: ai_mode label is never falsified
# ──────────────────────────────────────────────────────────────────────────────

def test_ai_mode_never_falsified():
    """The ai_mode field must accurately represent the real engine used."""
    import urllib.error

    # Offline → must say RULE_BASED_ENGINE
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("offline")):
        adapter = OllamaDiagnosticAdapter()
        report = adapter.analyze_incident(**STANDARD_KWARGS)

    assert "LOCAL_OLLAMA_AI" not in report.evidence["ai_mode"]
    assert "RULE_BASED_ENGINE" in report.evidence["ai_mode"]

    # Online + valid response → must say LOCAL_OLLAMA_AI
    ai_payload = {
        "root_cause": "Duplicate ingestion",
        "confidence": 0.95,
        "risk": "LOW",
        "recommended_action": "AUTO_FIX",
        "reason": "Safe dedup",
        "blast_radius": "Staging only",
        "possible_root_causes": [],
    }
    tags_resp = _make_ollama_tags_resp()
    gen_resp = _make_ollama_generate_resp(ai_payload)

    with patch("urllib.request.urlopen", side_effect=[tags_resp, gen_resp]):
        adapter2 = OllamaDiagnosticAdapter()
        report2 = adapter2.analyze_incident(**STANDARD_KWARGS)

    assert "LOCAL_OLLAMA_AI" in report2.evidence["ai_mode"]
    assert "RULE_BASED_ENGINE" not in report2.evidence["ai_mode"]
