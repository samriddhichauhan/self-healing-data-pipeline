"""
Parsers for the two log formats the platform displays:

  pipeline logs (src/agent/logger.py):  [2026-09-27 08:55:38] [INFO] [logger] - message
  Airflow task logs:                    [2026-09-27T08:55:38.037+0000] {file.py:188} INFO - message

Airflow task logs usually wrap pipeline-log lines (stdout is captured through
logging_mixin), so both headers can appear on one line. Lines with no header
(e.g. the "  - key: value" lines of a structured AI event, or traceback
frames) are continuations and inherit the previous entry's timestamp, level
and marker.
"""
import re
from typing import Any, Dict, List, Optional

PIPELINE_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] \[(?P<level>[A-Z]+)\] \[(?P<source>[^\]]+)\] - (?P<msg>.*)$"
)
AIRFLOW_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{4}|Z)?)\] \{(?P<source>[^}]+)\} (?P<level>[A-Z]+) - (?P<msg>.*)$"
)
AI_BLOCK_RE = re.compile(r"\[(?P<event>OLLAMA_[A-Z_]+)\] - Timestamp: (?P<ts>\S+)")
FIELD_RE = re.compile(r"^  - (?P<key>[A-Za-z_]+): ?(?P<value>.*)$")

MARKERS = (
    "OLLAMA_REQUEST", "OLLAMA_RESPONSE", "OLLAMA_DIAGNOSIS",
    "POLICY_GATE", "SELF-HEALING SUCCESS", "FALLBACK", "ERROR",
)


def classify(message: str, level: str) -> Optional[str]:
    """Maps a log message to one of MARKERS (or None)."""
    if "[OLLAMA_ERROR_FALLBACK]" in message or "[OLLAMA_FALLBACK]" in message or "RULE_BASED_ENGINE (" in message:
        return "FALLBACK"
    for m in ("OLLAMA_REQUEST", "OLLAMA_RESPONSE", "OLLAMA_DIAGNOSIS"):
        if f"[{m}]" in message:
            return m
    if "[SELF-HEALING SUCCESS]" in message:
        return "SELF-HEALING SUCCESS"
    if "Policy Gate" in message or "[ESCALATED]" in message or "policy_gate_" in message:
        return "POLICY_GATE"
    if level in ("ERROR", "CRITICAL") or "[SELF-HEALING ERROR]" in message or "Traceback (most recent call last)" in message:
        return "ERROR"
    return None


def parse_lines(lines: List[str]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    prev: Optional[Dict[str, Any]] = None
    for n, raw in enumerate(lines, 1):
        line = raw.rstrip("\r\n")
        ts = level = source = None
        msg = line
        m = AIRFLOW_RE.match(line)
        if m:
            ts, level, source, msg = m["ts"], m["level"], m["source"], m["msg"]
            inner = PIPELINE_RE.match(msg)
            if inner:  # pipeline logger output captured by Airflow
                level, source, msg = inner["level"], inner["source"], inner["msg"]
        else:
            m = PIPELINE_RE.match(line)
            if m:
                ts, level, source, msg = m["ts"], m["level"], m["source"], m["msg"]

        if ts is not None:
            entry = {"n": n, "ts": ts, "level": level, "source": source, "message": msg,
                     "text": line, "marker": classify(msg, level), "continuation": False}
        else:
            inherited = prev or {}
            marker = inherited.get("marker")
            if "policy_gate_" in line:
                marker = "POLICY_GATE"
            elif re.search(r"\b\w+(Error|Exception):", line):
                marker = "ERROR"
            entry = {"n": n, "ts": inherited.get("ts"), "level": inherited.get("level"),
                     "source": inherited.get("source"), "message": line, "text": line,
                     "marker": marker, "continuation": True}
        entries.append(entry)
        if not entry["continuation"]:
            prev = entry
    return entries


def parse_ai_events(lines: List[str]) -> List[Dict[str, Any]]:
    """Extracts the structured [OLLAMA_*] blocks written by log_ai_event()."""
    events: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    for raw in lines:
        line = raw.rstrip("\r\n")
        block = AI_BLOCK_RE.search(line)
        if block and "[ollama_ai_agent]" in line:
            current = {"event": block["event"], "ts": block["ts"], "fields": {}}
            events.append(current)
            continue
        field = FIELD_RE.match(line)
        if field and current is not None:
            current["fields"][field["key"]] = field["value"]
            continue
        current = None
    return events


def _float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_diagnoses(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Groups AI events into one record per adapter call (analyze_incident)."""
    out: List[Dict[str, Any]] = []
    open_by_id: Dict[str, Dict[str, Any]] = {}

    def new(incident_id: str) -> Dict[str, Any]:
        d = {
            "incident_id": incident_id, "model": None, "host": None, "fault_category": None,
            "request_ts": None, "response_ts": None, "diagnosis_ts": None,
            "response_time_s": None, "response_chars": None,
            "root_cause": None, "confidence": None, "risk": None, "ai_action": None,
            "policy_gate_action": None, "policy_gate_rationale": None,
            "source": "RULE_BASED_FALLBACK", "fallback": False, "fallback_reason": None, "last_ts": None,
        }
        out.append(d)
        return d

    for ev in events:
        f, kind, ts = ev["fields"], ev["event"], ev["ts"]
        iid = f.get("incident_id", "unknown")
        if kind == "OLLAMA_REQUEST":
            d = new(iid)
            open_by_id[iid] = d
            d.update(request_ts=ts, model=f.get("model"), host=f.get("host"), fault_category=f.get("fault_category"))
        elif kind == "OLLAMA_RESPONSE":
            d = open_by_id.get(iid) or new(iid)
            open_by_id[iid] = d
            d.update(response_ts=ts, response_time_s=_float(f.get("response_time_s")),
                     response_chars=_float(f.get("response_chars")), model=d["model"] or f.get("model"))
        elif kind == "OLLAMA_DIAGNOSIS":
            d = open_by_id.pop(iid, None) or new(iid)
            d.update(diagnosis_ts=ts, root_cause=f.get("ai_root_cause"), confidence=_float(f.get("ai_confidence")),
                     risk=f.get("ai_risk"), ai_action=f.get("ai_recommended_action"),
                     policy_gate_action=f.get("policy_gate_action"), policy_gate_rationale=f.get("policy_gate_rationale"),
                     model=d["model"] or f.get("model"), source="LOCAL_OLLAMA_AI")
        elif kind == "OLLAMA_ERROR_FALLBACK":
            d = open_by_id.pop(iid, None) or new(iid)
            d.update(fallback=True, fallback_reason=f.get("error") or f.get("reason"), source="RULE_BASED_FALLBACK")
        elif kind == "OLLAMA_FALLBACK":
            open_by_id.pop(iid, None)
            d = new(iid)
            d.update(fallback=True, fallback_reason=f.get("reason"), host=f.get("host"), request_ts=ts)
        else:
            continue
        d["last_ts"] = ts
    return out
