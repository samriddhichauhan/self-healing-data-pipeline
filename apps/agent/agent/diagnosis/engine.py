"""
AI Diagnostic Reasoning Engine
"""
import random
from typing import Dict, Any, Optional
from ..policies.policy_gate import PolicyGate
from ..models.incident import IncidentReport


class DiagnosticEngine:
    def __init__(self, policy_gate: Optional[PolicyGate] = None):
        self.policy_gate = policy_gate or PolicyGate()

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
        OBSERVED -> EVIDENCE -> HYPOTHESIS -> CONFIDENCE -> BLAST_RADIUS -> ACTION
        """
        category = fault_category.upper().replace(" ", "_")

        if "DUPLICATE" in category or "SPIKE" in category:
            hypothesis = f"Upstream retry or file re-transmission caused duplicate rows in dataset '{dataset}'."
            confidence = 0.95
            blast_radius = f"Staged {dataset} dataset and downstream target tables."
            severity = "MEDIUM"
        elif "SCHEMA" in category:
            hypothesis = f"Schema drift: Upstream API contract modification without notification altered field types in '{dataset}'."
            confidence = 0.92
            blast_radius = f"Schema drift affects all downstream ETL transformations for {dataset}."
            severity = "HIGH"
        elif "NULL" in category:
            hypothesis = f"Database extraction failure on upstream source produced NULL values in non-nullable field of '{dataset}'."
            confidence = 0.89
            blast_radius = f"Data quality violation in {dataset} impacts analytical aggregations."
            severity = "HIGH"
        elif "REFERENTIAL" in category or "REF" in category:
            hypothesis = f"Stale dimension sync resulted in foreign key orphans in '{dataset}' referencing missing primary keys."
            confidence = 0.88
            blast_radius = f"Referential integrity break between {dataset} and dimension tables."
            severity = "HIGH"
        elif "DROP" in category or "VOLUME" in category:
            hypothesis = f"Partial ingestion or incomplete source batch file delivery for '{dataset}'."
            confidence = 0.90
            blast_radius = f"Incomplete dataset load in {dataset} creates data loss."
            severity = "HIGH"
        else:
            hypothesis = f"Unclassified task failure in task '{task_id}' during batch processing."
            confidence = 0.75
            blast_radius = f"Pipeline execution halted at task {task_id}."
            severity = "HIGH"

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
            remediation={"rationale": rationale},
            verification=None
        )

        return report
