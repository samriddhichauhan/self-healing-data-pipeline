"""
Deterministic rule-based diagnostic engine.

This is the FALLBACK tier used only when the local Ollama LLM is offline,
times out, or returns an unparsable/invalid response (see ollama_adapter.py).
It never depends on an external paid API.
"""
import os
import glob
import json
from typing import Dict, Any, Optional, List
from .policy_gate import PolicyGate
from .incident import IncidentReport


class DiagnosticEngine:
    def __init__(self, policy_gate: Optional[PolicyGate] = None, data_dir: str = "./data"):
        self.policy_gate = policy_gate or PolicyGate()
        self.data_dir = data_dir

    def _get_historical_baseline(self, dataset: str) -> Optional[Dict[str, Any]]:
        """Fetches the most recent healthy run's row counts from status JSON logs."""
        status_dir = os.path.join(self.data_dir, "incidents", "status")
        if not os.path.exists(status_dir):
            return None

        status_files = sorted(glob.glob(os.path.join(status_dir, "status_*.json")))
        for sf in reversed(status_files):
            try:
                with open(sf, "r") as f:
                    data = json.load(f)
                    if data.get("pipeline_status") in ("HEALTHY", "SUCCESS"):
                        return data.get("row_counts", {})
            except Exception:
                continue
        return None

    def diagnose_fault(
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
        OBSERVED -> EVIDENCE -> POSSIBLE ROOT CAUSES -> CONFIDENCE -> BLAST_RADIUS -> ACTION
        """
        category = fault_category.upper().replace(" ", "_")
        baseline = self._get_historical_baseline(dataset)
        possible_root_causes: List[str] = []

        if "SCHEMA" in category:
            hypothesis = f"Schema drift: upstream contract change altered field definitions in '{dataset}'."
            possible_root_causes = [
                "Upstream source engineering updated the API schema without notifying consumers",
                "Source column type migration (e.g. float to formatted string)",
                "New unannounced fields added to the raw export payload",
            ]
            confidence, blast_radius, severity = 0.92, f"Schema drift halts all downstream ETL for {dataset}.", "HIGH"
        elif "NULL" in category:
            hypothesis = f"Upstream extraction defect produced NULLs in a non-nullable field of '{dataset}'."
            possible_root_causes = [
                "Source extraction query omitted a default fallback for pending transactions",
                "Upstream API payload truncation during network transfer",
                "Null customer_id generated during client checkout flow",
            ]
            confidence, blast_radius, severity = 0.89, f"Data quality violation in {dataset} impacts downstream aggregations.", "HIGH"
        elif "DUPLICATE" in category or category == "VOLUME_ANOMALY_SPIKE" or "SPIKE" in category:
            hypothesis = f"Upstream retry or file re-transmission caused duplicate rows in '{dataset}'."
            possible_root_causes = [
                "Upstream scheduler retry triggered a duplicate file export into staging",
                "Source CDC log replay without a deduplication window",
                "Manual re-upload of a historical batch",
            ]
            confidence, blast_radius, severity = 0.95, f"Staged {dataset} dataset and downstream analytical tables.", "MEDIUM"
        elif "REFERENTIAL" in category or "REF" in category:
            hypothesis = f"Stale dimension sync produced foreign-key orphans in '{dataset}'."
            possible_root_causes = [
                "Dimension ETL failed or ran out of sequence before fact ingestion",
                "New customer registration delayed in the stream buffer",
                "Hardcoded test customer_id used in a production order",
            ]
            confidence, blast_radius, severity = 0.88, f"Referential integrity break between {dataset} and its dimension tables.", "HIGH"
        elif "DROP" in category or "VOLUME" in category:
            hypothesis = f"Partial ingestion or incomplete source batch delivery for '{dataset}'."
            possible_root_causes = [
                "Source extract job timed out before a complete export",
                "Network interruption during transfer to staging",
                "Upstream table truncation",
            ]
            confidence, blast_radius, severity = 0.90, f"Incomplete load in {dataset} creates data loss.", "HIGH"
        elif "STALENESS" in category:
            hypothesis = f"Freshness SLA breach: '{dataset}' was not updated within the SLA threshold."
            possible_root_causes = [
                "Upstream batch generator cron failed to run on schedule",
                "Stream consumer lag accrued during peak load",
                "Timezone offset mismatch in batch filename generation",
            ]
            confidence, blast_radius, severity = 0.91, f"Stale analytics data in {dataset} dashboard reporting.", "MEDIUM"
        else:
            hypothesis = f"Unclassified task failure in '{task_id}' during batch processing."
            possible_root_causes = [
                "Uncaught exception during task execution",
                "Resource limit exceeded during local DataFrame processing",
            ]
            confidence, blast_radius, severity = 0.75, f"Pipeline execution halted at task {task_id}.", "HIGH"

        if baseline:
            evidence["historical_healthy_baseline"] = baseline
        evidence.setdefault("diagnosis_source", "RULE_BASED_ENGINE")
        evidence.setdefault("ai_mode", "RULE_BASED_ENGINE")

        action, rationale = self.policy_gate.evaluate(category, confidence, evidence)

        return IncidentReport(
            incident_id=incident_id,
            timestamp=execution_date,
            pipeline=pipeline_id,
            dataset=dataset,
            check=task_id,
            severity=severity,
            observed=observed,
            expected=expected,
            evidence=evidence,
            hypothesis=hypothesis,
            confidence=confidence,
            blast_radius=blast_radius,
            action=action,
            status="PENDING_APPROVAL" if action == "AUTO_FIX" else "ESCALATED",
            remediation={"rationale": rationale, "method": "Deduplicate & Clean" if action == "AUTO_FIX" else "Escalate to Human"},
            verification=None,
            failed_check=task_id,
            possible_root_causes=possible_root_causes,
            remediation_status="PENDING",
            verification_status="PENDING",
            recovery_time=3.5 if action == "AUTO_FIX" else None,
        )
