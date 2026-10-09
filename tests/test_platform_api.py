"""
Tests for the SelfHeal AI platform API (apps/platform-api). Upstreams are
faked, so these tests never touch Airflow, Ollama, the backend or Postgres.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps", "platform-api")))

from selfheal_api.clients import TTLCache, UpstreamError  # noqa: E402
from selfheal_api.config import Settings  # noqa: E402
from selfheal_api.deps import get_upstream  # noqa: E402
from selfheal_api.health import CONNECTED, DEGRADED, OFFLINE, dag_status, scheduler_status  # noqa: E402
from selfheal_api.log_parser import build_diagnoses, parse_ai_events, parse_lines  # noqa: E402
from selfheal_api.main import app  # noqa: E402
from selfheal_api.normalize import normalize_incident, normalize_run  # noqa: E402
from selfheal_api.pipeline_graph import build_pipeline  # noqa: E402

AI_LOG = """\
[2026-09-27 09:01:26] [INFO] [ollama_diagnostic_adapter] - [OLLAMA_REQUEST] Ollama active at http://h:11434 using model 'llama3.2:latest' for incident INC-1
[2026-09-27 09:01:26] [INFO] [ollama_ai_agent] - [OLLAMA_REQUEST] - Timestamp: 2026-09-27T09:01:26.198826+00:00
  - incident_id: INC-1
  - host: http://host.docker.internal:11434
  - model: llama3.2:latest
  - fault_category: DUPLICATE_INGESTION
[2026-09-27 09:02:48] [INFO] [ollama_ai_agent] - [OLLAMA_RESPONSE] - Timestamp: 2026-09-27T09:02:48.373017+00:00
  - incident_id: INC-1
  - model: llama3.2:latest
  - response_time_s: 82.05
[2026-09-27 09:02:48] [INFO] [ollama_ai_agent] - [OLLAMA_DIAGNOSIS] - Timestamp: 2026-09-27T09:02:48.557783+00:00
  - incident_id: INC-1
  - ai_root_cause: Duplicate primary keys found in orders dataset
  - ai_confidence: 0.95
  - ai_risk: LOW
  - ai_recommended_action: AUTO_FIX
  - policy_gate_action: AUTO_FIX
  - policy_gate_rationale: Fault 'DUPLICATE_INGESTION' is eligible for idempotent auto-fix with confidence 0.95.
[2026-09-27 07:10:16] [INFO] [ollama_ai_agent] - [OLLAMA_FALLBACK] - Timestamp: 2026-09-27T07:10:16.120775+00:00
  - incident_id: INC-2
  - reason: Ollama server offline or unreachable at http://host.docker.internal:11434
  - active_engine: RULE_BASED_ENGINE
""".splitlines()


def test_parse_lines_marks_ai_events_and_inherits_on_continuations():
    entries = parse_lines(AI_LOG)
    by_n = {e["n"]: e for e in entries}
    assert by_n[1]["marker"] == "OLLAMA_REQUEST" and not by_n[1]["continuation"]
    assert by_n[3]["continuation"] and by_n[3]["marker"] == "OLLAMA_REQUEST"
    assert by_n[3]["ts"] == "2026-09-27 09:01:26"
    diag_lines = [e for e in entries if e["message"].strip().startswith("- policy_gate_")]
    assert diag_lines and all(e["marker"] == "POLICY_GATE" for e in diag_lines)
    assert entries[-1]["marker"] == "FALLBACK"


def test_parse_lines_unwraps_airflow_task_log_format():
    lines = [
        "[2026-09-27T08:56:37.988+0000] {python.py:237} INFO - Done. Returned value was: None",
        "[2026-09-27T09:01:26.201+0000] {logging_mixin.py:188} INFO - [2026-09-27 09:01:26] [INFO] [ollama_ai_agent] - [OLLAMA_REQUEST] - Timestamp: x",
        "[2026-09-27T09:02:49.736+0000] {logging_mixin.py:188} INFO - [SELF-HEALING SUCCESS] INC-1: DUPLICATE_INGESTION auto-fixed",
        "[2026-09-27T09:03:00.000+0000] {taskinstance.py:1} ERROR - Task failed with exception",
        "ValueError: Data Quality: Duplicate primary keys found in orders.",
    ]
    e = parse_lines(lines)
    assert e[0]["marker"] is None and e[0]["level"] == "INFO"
    assert e[1]["marker"] == "OLLAMA_REQUEST" and e[1]["source"] == "ollama_ai_agent"
    assert e[2]["marker"] == "SELF-HEALING SUCCESS"
    assert e[3]["marker"] == "ERROR" and e[4]["marker"] == "ERROR" and e[4]["continuation"]


def test_build_diagnoses_groups_request_response_and_fallback():
    diags = build_diagnoses(parse_ai_events(AI_LOG))
    assert len(diags) == 2
    ai, fb = diags
    assert ai["source"] == "LOCAL_OLLAMA_AI" and not ai["fallback"]
    assert ai["response_time_s"] == 82.05 and ai["confidence"] == 0.95 and ai["policy_gate_action"] == "AUTO_FIX"
    assert ai["request_ts"].startswith("2026-09-27T09:01:26") and ai["response_ts"].startswith("2026-09-27T09:02:48")
    assert fb["source"] == "RULE_BASED_FALLBACK" and fb["fallback"] and "offline" in fb["fallback_reason"]
    assert fb["response_time_s"] is None


def _incident(**over):
    base = {
        "incident_id": "INC-20260927-8985", "run_id": "ollama_proof__2026-09-23", "dataset": "orders",
        "fault_category": "DUPLICATE_INGESTION", "severity": "MEDIUM", "action": "AUTO_FIX", "status": "REMEDIATED",
        "confidence": 0.95, "hypothesis": "h", "remediation_status": "SUCCESS", "verification_status": "PASSED",
        "created_at": "2026-09-27T09:02:49+00:00", "updated_at": "2026-09-27T09:02:49+00:00",
        "payload": {
            "observed": "350 rows (50 duplicate primary keys)", "expected": "300 rows (0 duplicates)",
            "remediation": {"action_type": "DEDUPLICATE", "initial_rows": 350, "removed_duplicates": 50, "cleaned_rows": 300},
            "verification": {"verified": True, "duplicate_count": 0, "remaining_rows": 300},
            "evidence": {"timestamp": "2026-09-27T09:01:26+00:00", "diagnosis_source": "OLLAMA",
                         "ai_mode": "LOCAL_OLLAMA_AI (llama3.2:latest)", "ollama_model": "llama3.2:latest",
                         "ollama_confidence": 0.9, "ollama_risk": "LOW", "ollama_recommended_action": "AUTO_FIX",
                         "policy_gate_action": "AUTO_FIX", "historical_healthy_baseline": {"orders": 300}},
        },
    }
    base.update(over)
    return base


def test_normalize_incident_separates_ai_and_rule_based_confidence():
    ai = normalize_incident(_incident())
    assert ai["diagnosis_source"] == "LOCAL_OLLAMA_AI" and ai["ai_confidence"] == 0.9 and ai["risk"] == "LOW"
    assert ai["recovery_seconds"] == 83.0
    assert "historical_healthy_baseline" not in ai["evidence"] and not ai["is_test"]

    rb = _incident(status="ESCALATED")
    rb["payload"] = {"evidence": {"diagnosis_source": "RULE_BASED_ENGINE", "timestamp": "2026-09-27T09:01:26+00:00"}}
    rb = normalize_incident(rb)
    assert rb["diagnosis_source"] == "RULE_BASED" and rb["ai_confidence"] is None and rb["engine_confidence"] == 0.95
    assert rb["recovery_seconds"] is None  # not remediated

    assert normalize_incident(_incident(run_id="test_selfheal__abc"))["is_test"]
    assert normalize_incident(_incident(run_id=None))["is_test"]


def test_scheduler_and_dag_health_rules():
    now = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    beat = lambda s: {"scheduler": {"status": "healthy", "latest_scheduler_heartbeat": (now - timedelta(seconds=s)).isoformat()}}
    assert scheduler_status(beat(5), now)["status"] == CONNECTED
    assert scheduler_status(beat(120), now)["status"] == DEGRADED
    assert scheduler_status(beat(600), now)["status"] == OFFLINE
    assert scheduler_status({}, now)["status"] == OFFLINE
    assert dag_status({"is_active": True, "is_paused": False}, [], "d")["status"] == CONNECTED
    assert dag_status({"is_active": True, "is_paused": True}, [], "d")["status"] == DEGRADED
    assert dag_status({"is_active": True, "is_paused": False}, [{"filename": "d.py"}], "d")["status"] == OFFLINE


def _tasks(state="success", quality="success"):
    names = ["ingest_dimensions", "ingest_orders", "ingest_events", "validate_schema", "validate_quality",
             "transform_data", "load_data", "agent_monitoring"]
    out = []
    for n in names:
        s = quality if n == "validate_quality" else state
        out.append({"task_id": n, "state": s, "start_date": "2026-09-27T09:01:20+00:00",
                    "end_date": "2026-09-27T09:01:22+00:00", "duration_s": 2.0, "try_number": 1})
    return out


def test_pipeline_graph_recovered_healthy_and_escalated():
    run = normalize_run({"dag_run_id": "r", "state": "success"}, {"status": "RECOVERED"})
    inc = normalize_incident(_incident())
    diag = build_diagnoses(parse_ai_events(AI_LOG))[:1]
    nodes = {n["key"]: n for n in build_pipeline(run, _tasks(), [inc], diag)["nodes"]}
    assert [*nodes] == ["data", "schema", "quality", "detection", "ai", "policy", "remediation", "verification", "outcome"]
    assert nodes["ai"]["status"] == "success" and nodes["ai"]["duration_s"] == 82.05 and nodes["ai"]["highlight"]
    assert nodes["remediation"]["status"] == "success" and nodes["verification"]["status"] == "success"
    assert nodes["outcome"]["title"] == "Recovered"

    healthy = build_pipeline(normalize_run({"dag_run_id": "h", "state": "success"}, {"status": "HEALTHY"}), _tasks(), [], [])
    hn = {n["key"]: n["status"] for n in healthy["nodes"]}
    assert hn["detection"] == "success" and hn["ai"] == "skipped" and hn["remediation"] == "skipped"

    esc = normalize_incident(_incident(status="ESCALATED", action="ESCALATE", remediation_status="PENDING"))
    esc["policy_decision"] = "ESCALATE"
    en = {n["key"]: n for n in build_pipeline(normalize_run({"dag_run_id": "e", "state": "failed"}, {"status": "FAILED"}),
                                             _tasks(quality="failed"), [esc], [])["nodes"]}
    assert en["policy"]["status"] == "warning" and en["remediation"]["status"] == "skipped"
    assert en["outcome"]["status"] == "failed"


class FakeUpstream:
    """Records calls; answers like Airflow / the backend would."""

    def __init__(self, paused=False, existing_run=False, same_time=0):
        self.settings = Settings()
        self.cache = TTLCache()
        self.calls = []
        self.paused, self.existing_run, self.same_time = paused, existing_run, same_time

    async def airflow(self, path, method="GET", **kw):
        self.calls.append(("airflow", method, path))
        if method == "POST":
            return {"dag_run_id": kw["json"]["dag_run_id"], "state": "queued"}
        if path.endswith("/dagRuns"):
            return {"total_entries": self.same_time, "dag_runs": [{"dag_run_id": "other"}] * self.same_time}
        if "/dagRuns/" in path:
            if self.existing_run:
                return {"dag_run_id": "exists"}
            raise UpstreamError("airflow", "HTTP 404: not found", 404)
        return {"dag_id": "self_healing_pipeline", "is_paused": self.paused}

    async def backend(self, path, method="GET", **kw):
        self.calls.append(("backend", method, path))
        return {"status": "FAULT_INJECTED", "fault_type": kw["json"]["fault_type"]}


def _trigger(fake, **body):
    app.dependency_overrides[get_upstream] = lambda: fake
    try:
        payload = {"dag_id": "self_healing_pipeline", "run_id": "ui_run_1", "logical_date": "2026-09-23T06:00:00Z", **body}
        return TestClient(app).post("/api/trigger", json=payload)
    finally:
        app.dependency_overrides.clear()


def test_trigger_rejects_future_logical_date_without_side_effects():
    fake = FakeUpstream()
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    res = _trigger(fake, logical_date=future, fault_type="duplicate_ingestion")
    assert res.status_code == 422 and "future" in res.json()["detail"]
    assert fake.calls == []


@pytest.mark.parametrize("fake,code", [
    (FakeUpstream(paused=True), 409), (FakeUpstream(existing_run=True), 409), (FakeUpstream(same_time=1), 409),
])
def test_trigger_conflicts_never_inject_a_fault(fake, code):
    res = _trigger(fake, fault_type="null_spike")
    assert res.status_code == code
    assert not any(c[0] == "backend" for c in fake.calls)


def test_trigger_injects_fault_then_triggers_existing_dag():
    fake = FakeUpstream()
    res = _trigger(fake, fault_type="duplicate_ingestion")
    assert res.status_code == 200, res.text
    kinds = [(c[0], c[1]) for c in fake.calls]
    assert kinds.index(("backend", "POST")) < kinds.index(("airflow", "POST"))
    assert ("backend", "POST", "/api/v1/simulation/inject-fault") in fake.calls
    assert res.json()["run"]["dag_run_id"] == "ui_run_1"


def test_trigger_rejects_unknown_fault_and_bad_run_id():
    assert _trigger(FakeUpstream(), fault_type="drop_table").status_code == 422
    assert _trigger(FakeUpstream(), run_id="bad id!").status_code == 422
