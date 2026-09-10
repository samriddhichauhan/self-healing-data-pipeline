"""
AI Diagnostic Reasoning Engine
"""
import os
import glob
import json
from typing import Dict, Any, Optional, List
from ..policies.policy_gate import PolicyGate
from ..models.incident import IncidentReport


class DiagnosticEngine:
    def __init__(self, policy_gate: Optional[PolicyGate] = None, data_dir: str = "./data"):
        self.policy_gate = policy_gate or PolicyGate()
        self.data_dir = data_dir

    def _get_historical_baseline(self, dataset: str) -> Optional[Dict[str, Any]]:
        """
        Fetches historical healthy pipeline run metrics from status JSON logs.
        """
        status_dir = os.path.join(self.data_dir, "incidents", "status")
        if not os.path.exists(status_dir):
            return None

        status_files = sorted(glob.glob(os.path.join(status_dir, "status_*.json")))
        if not status_files:
            return None

        for sf in reversed(status_files):
            try:
                with open(sf, "r") as f:
                    data = json.load(f)
                    if data.get("pipeline_status") == "SUCCESS":
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
        execution_date: str
    ) -> IncidentReport:
        """
        Processes an observed pipeline anomaly using structured diagnostic reasoning:
        OBSERVED -> EVIDENCE -> POSSIBLE ROOT CAUSES -> CONFIDENCE -> BLAST_RADIUS -> RECOMMENDED ACTION
        """
        category = fault_category.upper().replace(" ", "_")
        baseline = self._get_historical_baseline(dataset)

        possible_root_causes: List[str] = []

        if "SCHEMA" in category:
            hypothesis = f"Schema drift: Upstream API contract modification altered field definitions in '{dataset}'."
            possible_root_causes = [
                "Upstream source engineering updated API schema without notifying consumers",
                "Source database column type migration (e.g. float to formatted string)",
                "New unannounced fields added to raw export payload"
            ]
            confidence = 0.92
            blast_radius = f"Schema drift halts all downstream ETL transformations for {dataset}."
            severity = "HIGH"
        elif "NULL" in category:
            hypothesis = f"Database extraction failure on upstream source produced NULL values in non-nullable field of '{dataset}'."
            possible_root_causes = [
                "Source database extraction query omitted default fallback for pending transactions",
                "Upstream API payload truncation during network transfer",
                "Null customer_id generation during client checkout flow"
            ]
            confidence = 0.89
            blast_radius = f"Data quality violation in {dataset} impacts analytical aggregations."
            severity = "HIGH"
        elif "DUPLICATE" in category or category == "VOLUME_ANOMALY_SPIKE" or "SPIKE" in category:
            hypothesis = f"Upstream retry or file re-transmission caused duplicate rows in dataset '{dataset}'."
            possible_root_causes = [
                "Upstream scheduler retry triggered duplicate file export into staging",
                "Source database CDC log re-play without deduplication window",
                "Manual file upload of historical batch"
            ]
            confidence = 0.95
            blast_radius = f"Staged {dataset} dataset and downstream analytical tables."
            severity = "MEDIUM"
        elif "REFERENTIAL" in category or "REF" in category:
            hypothesis = f"Stale dimension sync resulted in foreign key orphans in '{dataset}' referencing missing primary keys."
            possible_root_causes = [
                "Dimension ETL job failed or ran out of sequence before fact ingestion",
                "New customer registration event delayed in stream buffer",
                "Hardcoded test customer_id used in production order"
            ]
            confidence = 0.88
            blast_radius = f"Referential integrity break between {dataset} and dimension tables."
            severity = "HIGH"
        elif "DROP" in category or "VOLUME" in category:
            hypothesis = f"Partial ingestion or incomplete source batch file delivery for '{dataset}'."
            possible_root_causes = [
                "Source extract job timed out prematurely before complete export",
                "Network interruption during file transfer to staging storage",
                "Source table truncation upstream"
            ]
            confidence = 0.90
            blast_radius = f"Incomplete dataset load in {dataset} creates data loss."
            severity = "HIGH"
        elif "STALENESS" in category:
            hypothesis = f"Data timestamp SLA breach: dataset '{dataset}' was not updated within SLA threshold."
            possible_root_causes = [
                "Upstream batch generator cron failed to execute on schedule",
                "Stream consumer lag accrued during peak load window",
                "Timezone offset mismatch in batch filename generation"
            ]
            confidence = 0.91
            blast_radius = f"Stale analytics data in {dataset} dashboard reporting."
            severity = "MEDIUM"
        else:
            hypothesis = f"Unclassified task failure in task '{task_id}' during batch processing."
            possible_root_causes = [
                "Uncaught exception during python_callable execution",
                "Memory limit exceeded during local DataFrame processing"
            ]
            confidence = 0.75
            blast_radius = f"Pipeline execution halted at task {task_id}."
            severity = "HIGH"

        if baseline:
            evidence["historical_healthy_baseline"] = baseline

        if "diagnosis_source" not in evidence:
            evidence["diagnosis_source"] = "RULE_BASED_ENGINE"
        if "ai_mode" not in evidence:
            evidence["ai_mode"] = "RULE_BASED_ENGINE"

        # Evaluate decision via Policy Gate
        action, rationale = self.policy_gate.evaluate(category, confidence, evidence)

        report = IncidentReport(
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
            observed_value=observed,
            expected_value=expected,
            possible_root_causes=possible_root_causes,
            remediation_status="PENDING",
            verification_status="PENDING",
            recovery_time=3.5 if action == "AUTO_FIX" else None
        )

        return report
