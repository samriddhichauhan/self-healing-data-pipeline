"""
Ollama Local AI Diagnostic Adapter
Uses local Ollama models (e.g., llama3.2, llama3, mistral, codellama) to analyze pipeline failures and generate AI-driven remediation plans.
Logs AI prompts, raw responses, and fallback statuses to logs/ollama_ai_agent.log.
Falls back seamlessly to the deterministic DiagnosticEngine if Ollama service is unavailable.
"""
import os
import json
import urllib.request
import urllib.error
from typing import Dict, Any, Optional
from .engine import DiagnosticEngine
from ..models.incident import IncidentReport
from ..tools.logger import get_logger, log_ai_event

logger = get_logger("ollama_diagnostic_adapter", "ollama_ai_agent.log")


class OllamaDiagnosticAdapter:
    def __init__(self, fallback_engine: Optional[DiagnosticEngine] = None, host: str = None, model: str = None):
        self.fallback_engine = fallback_engine or DiagnosticEngine()
        self.host = (host or os.environ.get("OLLAMA_HOST") or "http://localhost:11434").rstrip("/")
        self.preferred_model = model or os.environ.get("OLLAMA_MODEL") or "llama3.2"

    def is_ollama_available(self) -> tuple[bool, Optional[str]]:
        """
        Checks if Ollama server is active and returns available local model.
        """
        try:
            url = f"{self.host}/api/tags"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=2.5) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    models = [m.get("name", "") for m in data.get("models", [])]
                    if not models:
                        return True, self.preferred_model

                    # Match preferred model or pick first available model
                    for m in models:
                        if self.preferred_model in m or m.startswith(self.preferred_model):
                            return True, m
                    return True, models[0]
        except Exception as e:
            logger.debug(f"Ollama server check failed: {e}")
        return False, None

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
        execution_date: str
    ) -> IncidentReport:
        """
        Invokes local Ollama AI to diagnose pipeline failures and generate remediation recommendations.
        Falls back to rule-based DiagnosticEngine if Ollama service is unreachable.
        """
        # Obtain base ground-truth report from deterministic engine
        base_report = self.fallback_engine.diagnose_fault(
            incident_id=incident_id,
            pipeline_id=pipeline_id,
            task_id=task_id,
            dataset=dataset,
            fault_category=fault_category,
            observed=observed,
            expected=expected,
            evidence=evidence,
            execution_date=execution_date
        )

        available, active_model = self.is_ollama_available()

        if not available:
            msg = f"Ollama service unconfigured or offline at {self.host}. Active fallback to deterministic DiagnosticEngine."
            logger.info(msg)
            log_ai_event("OLLAMA_FALLBACK", {
                "incident_id": incident_id,
                "reason": "Ollama host unreachable",
                "host": self.host,
                "active_engine": "Rule-Based Diagnostic Engine"
            })
            base_report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (Ollama offline at {self.host})"
            return base_report

        logger.info(f"Ollama server active at {self.host}. Using AI model: '{active_model}' for incident {incident_id}")

        prompt = f"""You are an expert AI Data Reliability Engineer fixing a failed Airflow ETL pipeline.
Analyze the following pipeline incident details:

Incident ID: {incident_id}
Pipeline ID: {pipeline_id}
Task ID: {task_id}
Target Dataset: {dataset}
Fault Category: {fault_category}
Observed Error: {observed}
Expected Behavior: {expected}
Evidence Data: {json.dumps(evidence, default=str)}

Return ONLY a valid, single JSON object with these exact keys:
{{
  "root_cause": "Detailed technical root cause statement explaining why the failure occurred",
  "hypothesis": "Concise summary hypothesis for data quality diagnosis",
  "confidence": 0.95,
  "severity": "HIGH" or "MEDIUM",
  "blast_radius": "Impact statement describing downstream ETL effects",
  "recommended_action": "AUTO_FIX" or "ESCALATE",
  "reasoning_summary": "Explanation justifying whether to auto-fix or escalate"
}}"""

        try:
            url = f"{self.host}/api/generate"
            payload = json.dumps({
                "model": active_model,
                "prompt": prompt,
                "stream": False,
                "format": "json"
            }).encode("utf-8")

            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")

            log_ai_event("OLLAMA_REQUEST", {
                "incident_id": incident_id,
                "host": self.host,
                "model": active_model,
                "fault_category": fault_category,
                "prompt_length": len(prompt)
            })

            with urllib.request.urlopen(req, timeout=15.0) as resp:
                if resp.status == 200:
                    resp_data = json.loads(resp.read().decode("utf-8"))
                    raw_response = resp_data.get("response", "").strip()

                    log_ai_event("OLLAMA_RAW_RESPONSE", {
                        "incident_id": incident_id,
                        "raw_response": raw_response
                    })

                    parsed = json.loads(raw_response)
                    base_report.hypothesis = parsed.get("hypothesis", parsed.get("root_cause", base_report.hypothesis))
                    base_report.confidence = float(parsed.get("confidence", base_report.confidence))
                    base_report.blast_radius = parsed.get("blast_radius", base_report.blast_radius)
                    base_report.evidence["ai_mode"] = f"LOCAL_OLLAMA_AI ({active_model})"
                    base_report.evidence["ollama_root_cause"] = parsed.get("root_cause")
                    base_report.evidence["ollama_model"] = active_model
                    base_report.evidence["ollama_reasoning"] = parsed.get("reasoning_summary")

                    # Re-evaluate policy decision with Ollama hypothesis
                    rec_action = parsed.get("recommended_action")
                    if rec_action in ["AUTO_FIX", "ESCALATE"]:
                        # Policy Gate still enforces safe boundaries
                        action, rationale = self.fallback_engine.policy_gate.evaluate(fault_category, base_report.confidence, base_report.evidence)
                        base_report.action = action
                        base_report.remediation["rationale"] = f"[Ollama AI: {parsed.get('reasoning_summary')}] | Policy Gate Rationale: {rationale}"

                    logger.info(f"Ollama AI Diagnosis successful for {incident_id}: Action={base_report.action}, Confidence={base_report.confidence}")
                    return base_report
        except Exception as ex:
            msg = f"Ollama generation exception: {ex}. Falling back to rule-based diagnostic engine."
            logger.warning(msg)
            log_ai_event("OLLAMA_ERROR_FALLBACK", {
                "incident_id": incident_id,
                "error": str(ex),
                "active_fallback": "Rule-Based Diagnostic Engine"
            })
            base_report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (Ollama Error: {str(ex)})"

        return base_report
