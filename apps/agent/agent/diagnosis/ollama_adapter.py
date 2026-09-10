"""
Ollama Local AI Diagnostic Adapter

Uses local Ollama models (llama3.2:latest by default) to diagnose real pipeline
failures and generate structured remediation recommendations.

Design:
  - Ollama = PRIMARY diagnosis engine
  - DiagnosticEngine = FALLBACK only (Ollama unavailable / timeout / parse error)

The adapter:
  1. Checks Ollama availability via /api/tags
  2. Sends a structured pipeline-failure prompt to /api/generate
  3. Parses and validates the returned JSON
  4. Passes AI confidence/risk/action to the existing PolicyGate
  5. Falls back to DiagnosticEngine with a clear log entry if anything fails

Logs to: logs/ollama_ai_agent.log
Never logs secrets.
"""
import os
import json
import time
import re
import urllib.request
import urllib.error
from typing import Dict, Any, Optional, Tuple
from .engine import DiagnosticEngine
from ..models.incident import IncidentReport
from ..tools.logger import get_logger, log_ai_event

logger = get_logger("ollama_diagnostic_adapter", "ollama_ai_agent.log")

# ──────────────────────────────────────────────────────────────────────────────
# Tuneable constants
# ──────────────────────────────────────────────────────────────────────────────
# How long (seconds) to wait for a single token from Ollama.
# Local 3.2B models can take 60-180 s on CPU before returning.
_GENERATION_TIMEOUT_S: float = float(
    os.environ.get("OLLAMA_GENERATION_TIMEOUT", "180")
)

# Availability check timeout — we just ping /api/tags
_AVAILABILITY_TIMEOUT_S: float = 3.0

# Allowed risk values from AI
_ALLOWED_RISK = {"LOW", "MEDIUM", "HIGH"}
# Allowed action values from AI
_ALLOWED_ACTIONS = {"AUTO_FIX", "ESCALATE"}

# Fault categories supported
SUPPORTED_FAULT_CATEGORIES = {
    "schema_drift", "volume_drop", "null_spike",
    "duplicate_ingestion", "referential_break", "staleness",
    "volume_anomaly", "volume_anomaly_drop", "volume_anomaly_spike",
}


class OllamaDiagnosticAdapter:
    """
    Primary AI diagnostic adapter backed by Ollama (local LLM).

    Falls back to DiagnosticEngine (rule-based) only when:
      - Ollama server is offline
      - Connection fails
      - Request times out
      - AI response is not valid JSON
      - Required keys are missing / values are invalid

    The IncidentReport.evidence["ai_mode"] field always indicates which engine
    produced the diagnosis:
      "LOCAL_OLLAMA_AI (llama3.2:latest)"  — real AI result
      "RULE_BASED_ENGINE (...reason...)"   — deterministic fallback
    """

    def __init__(
        self,
        fallback_engine: Optional[DiagnosticEngine] = None,
        host: str = None,
        model: str = None,
    ):
        self.fallback_engine = fallback_engine or DiagnosticEngine()
        self.host = (
            host or os.environ.get("OLLAMA_HOST") or "http://localhost:11434"
        ).rstrip("/")
        self.preferred_model = (
            model or os.environ.get("OLLAMA_MODEL") or "llama3.2:latest"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Availability check
    # ──────────────────────────────────────────────────────────────────────────
    def is_ollama_available(self) -> Tuple[bool, Optional[str]]:
        """
        Checks if Ollama server is active and returns the best matching model.
        Returns: (is_available: bool, model_name: str | None)
        """
        try:
            url = f"{self.host}/api/tags"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=_AVAILABILITY_TIMEOUT_S) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    models = [m.get("name", "") for m in data.get("models", [])]
                    if not models:
                        # Server up but no models pulled yet — use preferred anyway
                        return True, self.preferred_model

                    # Prefer exact or prefix match on preferred model
                    for m in models:
                        if m == self.preferred_model or m.startswith(
                            self.preferred_model.split(":")[0]
                        ):
                            return True, m
                    # Any available model is better than no AI
                    return True, models[0]
        except Exception as e:
            logger.debug(f"Ollama availability check failed: {e}")
        return False, None

    # ──────────────────────────────────────────────────────────────────────────
    # Prompt builder
    # ──────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _build_prompt(
        incident_id: str,
        pipeline_id: str,
        task_id: str,
        dataset: str,
        fault_category: str,
        observed: Any,
        expected: Any,
        evidence: Dict[str, Any],
        execution_date: str,
    ) -> str:
        """
        Builds a concise, tightly scoped pipeline-failure diagnosis prompt.
        Instructs Ollama to return ONLY valid JSON with the exact 7 required keys.
        """
        safe_evidence = {
            k: v for k, v in evidence.items()
            if k not in ("ai_mode", "historical_healthy_baseline")
            and not isinstance(v, (bytes, bytearray))
        }
        evidence_str = json.dumps(safe_evidence, default=str)

        return f"""You are a Data Reliability Engineer performing automated failure diagnosis.

INCIDENT:
Task: {task_id}
Dataset: {dataset}
Fault Category: {fault_category}
Observed Error: {observed}
Expected: {expected}
Evidence: {evidence_str}

Respond with ONLY a JSON object containing these 7 keys:
{{
  "root_cause": "<concise explanation of why this failure occurred>",
  "confidence": <float between 0.70 and 0.99>,
  "risk": "<LOW or MEDIUM or HIGH>",
  "recommended_action": "<AUTO_FIX or ESCALATE>",
  "reason": "<one sentence justification>",
  "blast_radius": "<impacted downstream datasets or systems>",
  "possible_root_causes": ["<cause 1>", "<cause 2>"]
}}

RULES:
- recommended_action must be AUTO_FIX only if risk is LOW and fault is idempotent (e.g. duplicate ingestion).
- recommended_action must be ESCALATE if risk is HIGH/MEDIUM, schema changes, null spikes, or uncertain.
- Output raw JSON only. No markdown fences. No other text."""

    # ──────────────────────────────────────────────────────────────────────────
    # JSON response parser
    # ──────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_ai_response(raw: str) -> Optional[Dict[str, Any]]:
        """
        Attempts to parse the AI response as structured JSON.
        Performs safe extraction if the JSON is embedded in surrounding text.
        Returns None if parsing fails.
        """
        if not raw:
            return None

        # Direct parse
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # Extract from markdown fences
        fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
        if fence_match:
            try:
                return json.loads(fence_match.group(1))
            except json.JSONDecodeError:
                pass

        # Extract first {...} block
        brace_match = re.search(r"\{.*\}", raw, re.DOTALL)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        return None

    @staticmethod
    def _validate_ai_result(parsed: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validates that the AI result has all required fields with acceptable values.
        Returns: (is_valid: bool, reason: str)
        """
        required_keys = {"root_cause", "confidence", "risk", "recommended_action", "reason", "blast_radius"}
        missing = required_keys - set(parsed.keys())
        if missing:
            return False, f"Missing required keys: {missing}"

        try:
            conf = float(parsed["confidence"])
            if not (0.0 <= conf <= 1.0):
                return False, f"Confidence {conf} out of range [0,1]"
        except (ValueError, TypeError):
            return False, f"Non-numeric confidence: {parsed['confidence']}"

        risk = str(parsed.get("risk", "")).upper().strip()
        if risk not in _ALLOWED_RISK:
            return False, f"Invalid risk value '{risk}' (must be LOW/MEDIUM/HIGH)"

        action = str(parsed.get("recommended_action", "")).upper().strip()
        if action not in _ALLOWED_ACTIONS:
            return False, f"Invalid recommended_action '{action}' (must be AUTO_FIX/ESCALATE)"

        return True, "OK"

    # ──────────────────────────────────────────────────────────────────────────
    # Core analysis method
    # ──────────────────────────────────────────────────────────────────────────
    def analyze_incident(
        self,
        incident_id: str,
        pipeline_id: str,
        task_id: str,
        dataset: str,
        fault_category: str,
        observed: Any,
        expected: Any,
        evidence: Dict[str, Any],
        execution_date: str,
    ) -> IncidentReport:
        """
        Invokes local Ollama AI to diagnose pipeline failures.
        Falls back to rule-based DiagnosticEngine only if Ollama genuinely fails.

        Flow:
          1. Check Ollama availability
          2. Get deterministic base report (used as fallback and for historical baseline)
          3. Build pipeline-specific prompt
          4. Send to Ollama /api/generate (generous timeout for local model)
          5. Parse and validate structured JSON response
          6. Map AI confidence/risk/action through existing PolicyGate
          7. Return enriched IncidentReport with ai_mode = LOCAL_OLLAMA_AI

        If any step fails, returns base_report with ai_mode = RULE_BASED_ENGINE
        """
        # ── Step 1: Obtain rule-based base report (fallback candidate) ─────────
        base_report = self.fallback_engine.diagnose_fault(
            incident_id=incident_id,
            pipeline_id=pipeline_id,
            task_id=task_id,
            dataset=dataset,
            fault_category=fault_category,
            observed=observed,
            expected=expected,
            evidence=evidence,
            execution_date=execution_date,
        )

        # ── Step 2: Check Ollama availability ─────────────────────────────────
        available, active_model = self.is_ollama_available()

        if not available:
            reason = f"Ollama server offline or unreachable at {self.host}"
            logger.warning(f"[OLLAMA_FALLBACK] {reason} — using rule-based engine for incident {incident_id}")
            log_ai_event("OLLAMA_FALLBACK", {
                "incident_id": incident_id,
                "reason": reason,
                "host": self.host,
                "active_engine": "RULE_BASED_ENGINE",
            })
            base_report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (Ollama offline at {self.host})"
            return base_report

        logger.info(
            f"[OLLAMA_REQUEST] Ollama active at {self.host} "
            f"using model '{active_model}' for incident {incident_id}"
        )

        # ── Step 3: Build prompt ───────────────────────────────────────────────
        prompt = self._build_prompt(
            incident_id=incident_id,
            pipeline_id=pipeline_id,
            task_id=task_id,
            dataset=dataset,
            fault_category=fault_category,
            observed=observed,
            expected=expected,
            evidence=evidence,
            execution_date=execution_date,
        )

        log_ai_event("OLLAMA_REQUEST", {
            "incident_id": incident_id,
            "host": self.host,
            "model": active_model,
            "fault_category": fault_category,
            "prompt_chars": len(prompt),
            "timeout_seconds": _GENERATION_TIMEOUT_S,
        })

        # ── Step 4: Call Ollama /api/generate ─────────────────────────────────
        t_start = time.monotonic()
        try:
            url = f"{self.host}/api/generate"
            payload = json.dumps({
                "model": active_model,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "num_predict": 256,
                },
            }).encode("utf-8")

            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=_GENERATION_TIMEOUT_S) as resp:
                elapsed = time.monotonic() - t_start

                if resp.status != 200:
                    raise RuntimeError(f"Ollama returned HTTP {resp.status}")

                resp_data = json.loads(resp.read().decode("utf-8"))
                raw_response = resp_data.get("response", "").strip()

                log_ai_event("OLLAMA_RESPONSE", {
                    "incident_id": incident_id,
                    "model": active_model,
                    "response_time_s": round(elapsed, 2),
                    "response_chars": len(raw_response),
                    "done": resp_data.get("done", False),
                })

                logger.info(
                    f"[OLLAMA_RESPONSE] incident={incident_id} model={active_model} "
                    f"time={elapsed:.1f}s chars={len(raw_response)}"
                )

        except urllib.error.URLError as e:
            elapsed = time.monotonic() - t_start
            reason = f"URLError after {elapsed:.1f}s: {e}"
            return self._fallback(base_report, incident_id, reason)
        except TimeoutError as e:
            elapsed = time.monotonic() - t_start
            reason = f"Request timed out after {elapsed:.1f}s (limit={_GENERATION_TIMEOUT_S}s)"
            return self._fallback(base_report, incident_id, reason)
        except Exception as e:
            elapsed = time.monotonic() - t_start
            reason = f"Unexpected error after {elapsed:.1f}s: {type(e).__name__}: {e}"
            return self._fallback(base_report, incident_id, reason)

        # ── Step 5: Parse AI response ──────────────────────────────────────────
        parsed = self._parse_ai_response(raw_response)
        if parsed is None:
            reason = f"AI response could not be parsed as JSON (raw={repr(raw_response[:200])})"
            log_ai_event("OLLAMA_ERROR_FALLBACK", {
                "incident_id": incident_id,
                "error": reason,
                "active_fallback": "RULE_BASED_ENGINE",
            })
            logger.warning(f"[OLLAMA_ERROR_FALLBACK] {reason}")
            base_report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (invalid JSON from AI: {raw_response[:80]!r})"
            return base_report

        # ── Step 6: Validate AI result ─────────────────────────────────────────
        is_valid, validation_reason = self._validate_ai_result(parsed)
        if not is_valid:
            reason = f"AI JSON failed validation: {validation_reason}"
            log_ai_event("OLLAMA_ERROR_FALLBACK", {
                "incident_id": incident_id,
                "error": reason,
                "parsed_keys": list(parsed.keys()),
                "active_fallback": "RULE_BASED_ENGINE",
            })
            logger.warning(f"[OLLAMA_ERROR_FALLBACK] {reason}")
            base_report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (AI validation failed: {validation_reason})"
            return base_report

        # ── Step 7: Map AI values ─────────────────────────────────────────────
        ai_confidence = float(parsed["confidence"])
        ai_risk = str(parsed["risk"]).upper().strip()
        ai_action = str(parsed["recommended_action"]).upper().strip()
        ai_root_cause = parsed.get("root_cause", "")
        ai_reason = parsed.get("reason", "")
        ai_blast_radius = parsed.get("blast_radius", base_report.blast_radius)
        ai_possible_causes = parsed.get("possible_root_causes", [])

        # ── Step 8: Run AI result through Policy Gate ─────────────────────────
        # The Policy Gate is the authoritative decision-maker — it may override
        # the AI's recommended_action if safety rules are violated.
        pg_action, pg_rationale = self.fallback_engine.policy_gate.evaluate(
            fault_category=fault_category,
            confidence=ai_confidence,
            evidence={**evidence, "ai_risk": ai_risk, "ai_recommended_action": ai_action},
        )

        log_ai_event("OLLAMA_DIAGNOSIS", {
            "incident_id": incident_id,
            "model": active_model,
            "ai_root_cause": ai_root_cause,
            "ai_confidence": ai_confidence,
            "ai_risk": ai_risk,
            "ai_recommended_action": ai_action,
            "policy_gate_action": pg_action,
            "policy_gate_rationale": pg_rationale,
        })

        logger.info(
            f"[OLLAMA_DIAGNOSIS] incident={incident_id} "
            f"confidence={ai_confidence:.2f} risk={ai_risk} "
            f"ai_action={ai_action} pg_action={pg_action}"
        )

        # ── Step 9: Enrich the IncidentReport with real AI values ─────────────
        base_report.hypothesis = ai_root_cause or base_report.hypothesis
        base_report.confidence = ai_confidence
        base_report.blast_radius = ai_blast_radius
        base_report.action = pg_action          # Policy Gate is authoritative
        base_report.status = "PENDING_APPROVAL" if pg_action == "AUTO_FIX" else "ESCALATED"
        if ai_possible_causes:
            base_report.possible_root_causes = (
                ai_possible_causes if isinstance(ai_possible_causes, list)
                else [ai_possible_causes]
            )

        base_report.remediation = {
            "rationale": (
                f"[Ollama AI ({active_model}): {ai_reason}] | "
                f"[Policy Gate: {pg_rationale}]"
            ),
            "method": "Deduplicate & Clean" if pg_action == "AUTO_FIX" else "Escalate to Human",
        }

        base_report.evidence.update({
            "ai_mode": f"LOCAL_OLLAMA_AI ({active_model})",
            "diagnosis_source": "OLLAMA",
            "ollama_model": active_model,
            "ollama_root_cause": ai_root_cause,
            "ollama_confidence": ai_confidence,
            "ollama_risk": ai_risk,
            "ollama_recommended_action": ai_action,
            "ollama_reason": ai_reason,
            "policy_gate_action": pg_action,
            "policy_gate_rationale": pg_rationale,
        })

        return base_report

    # ──────────────────────────────────────────────────────────────────────────
    # Internal fallback helper
    # ──────────────────────────────────────────────────────────────────────────
    def _fallback(
        self, base_report: IncidentReport, incident_id: str, reason: str
    ) -> IncidentReport:
        """Logs the fallback reason and marks the report as rule-based."""
        log_ai_event("OLLAMA_ERROR_FALLBACK", {
            "incident_id": incident_id,
            "error": reason,
            "active_fallback": "RULE_BASED_ENGINE",
        })
        logger.warning(f"[OLLAMA_ERROR_FALLBACK] incident={incident_id} — {reason}")
        base_report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (Ollama error: {reason})"
        return base_report
