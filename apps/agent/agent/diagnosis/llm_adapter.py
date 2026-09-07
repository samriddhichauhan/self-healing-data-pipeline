"""
Safe Optional LLM & Ollama Diagnostic Adapter with Structured Reasoning
Provides generative AI root cause analysis using local Ollama models or Gemini/OpenAI APIs.
Seamlessly falls back to the deterministic DiagnosticEngine if unconfigured.
"""
import os
import json
from typing import Dict, Any, Optional
from .engine import DiagnosticEngine
from .ollama_adapter import OllamaDiagnosticAdapter
from ..models.incident import IncidentReport
from ..tools.logger import get_logger, log_ai_event

logger = get_logger("llm_diagnostic_adapter", "ollama_ai_agent.log")


class LLMDiagnosticAdapter:
    def __init__(self, fallback_engine: Optional[DiagnosticEngine] = None):
        self.fallback_engine = fallback_engine or DiagnosticEngine()
        self.ollama_adapter = OllamaDiagnosticAdapter(fallback_engine=self.fallback_engine)
        self.api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or os.environ.get("OPENAI_API_KEY")

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
        Attempts Ollama local AI diagnosis first.
        If Ollama is unavailable, attempts Gemini/OpenAI API if configured.
        Falls back to rule-based DiagnosticEngine automatically.
        """
        # 1. Try Ollama Local AI first
        is_ollama_active, active_model = self.ollama_adapter.is_ollama_available()
        if is_ollama_active:
            logger.info(f"Using Ollama local AI engine (model: {active_model}) for incident {incident_id}")
            return self.ollama_adapter.analyze_incident(
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

        # 2. Base report from deterministic engine
        report = self.fallback_engine.diagnose_fault(
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

        if not self.api_key:
            log_ai_event("AI_FALLBACK", {
                "incident_id": incident_id,
                "reason": "Ollama local AI and Cloud API keys unconfigured",
                "active_engine": "Rule-Based Diagnostic Engine"
            })
            report.evidence["ai_mode"] = "RULE_BASED_ENGINE (Ollama & Cloud AI unconfigured — fallback active)"
            return report

        # 3. Optional Gemini API call
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            model = genai.GenerativeModel("gemini-1.5-flash")

            prompt = f"""
            You are a Senior Data Reliability Engineer analyzing a pipeline incident.
            Incident ID: {incident_id}
            Pipeline ID: {pipeline_id}
            Task ID: {task_id}
            Dataset: {dataset}
            Fault Category: {fault_category}
            Observed Error: {observed}
            Expected: {expected}
            Evidence JSON: {json.dumps(evidence, default=str)}

            Return a valid JSON object with the following exact keys:
            - root_cause: concise technical root cause statement
            - reasoning_summary: brief explanation of why this fault occurred
            - confidence: float between 0.70 and 0.99
            - severity: "HIGH" or "MEDIUM"
            - blast_radius: statement describing impacted downstream systems
            - recommended_action: "AUTO_FIX" or "ESCALATE"
            """

            response = model.generate_content(prompt)
            if response and response.text:
                cleaned_text = response.text.strip()
                if "```json" in cleaned_text:
                    cleaned_text = cleaned_text.split("```json")[1].split("```")[0].strip()
                elif "```" in cleaned_text:
                    cleaned_text = cleaned_text.split("```")[1].split("```")[0].strip()

                parsed = json.loads(cleaned_text)
                report.hypothesis = parsed.get("reasoning_summary", report.hypothesis)
                report.confidence = float(parsed.get("confidence", report.confidence))
                report.blast_radius = parsed.get("blast_radius", report.blast_radius)
                report.evidence["ai_mode"] = "LLM_GEMINI_HYBRID"
                report.evidence["llm_root_cause"] = parsed.get("root_cause")
        except Exception as e:
            report.evidence["ai_mode"] = f"RULE_BASED_ENGINE (Gemini API Exception: {str(e)})"

        return report
