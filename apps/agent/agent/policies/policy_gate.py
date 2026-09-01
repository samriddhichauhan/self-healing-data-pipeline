"""
Policy Gate Engine: Enforces FIX vs ESCALATE decision rules.
"""
from typing import Dict, Any, Tuple


class PolicyGate:
    def __init__(self, confidence_threshold: float = 0.85):
        self.confidence_threshold = confidence_threshold
        # Explicit remediation eligibility policy mapped to fault categories
        self.policy_rules = {
            "DUPLICATE_INGESTION": {"action": "AUTO_FIX", "risk": "LOW", "idempotent": True},
            "VOLUME_ANOMALY_SPIKE": {"action": "AUTO_FIX", "risk": "LOW", "idempotent": True},
            "SCHEMA_DRIFT": {"action": "ESCALATE", "risk": "HIGH", "idempotent": False},
            "VOLUME_ANOMALY_DROP": {"action": "ESCALATE", "risk": "HIGH", "idempotent": False},
            "NULL_SPIKE": {"action": "ESCALATE", "risk": "HIGH", "idempotent": False},
            "REFERENTIAL_BREAK": {"action": "ESCALATE", "risk": "HIGH", "idempotent": False},
            "STALENESS": {"action": "ESCALATE", "risk": "MEDIUM", "idempotent": False},
        }

    def evaluate(
        self,
        fault_category: str,
        confidence: float,
        evidence: Dict[str, Any]
    ) -> Tuple[str, str]:
        """
        Evaluates diagnostic evidence against policy gate.
        Returns: (decision: 'AUTO_FIX' | 'ESCALATE', rationale: str)
        """
        category_key = fault_category.upper().replace(" ", "_")
        rule = self.policy_rules.get(category_key, {"action": "ESCALATE", "risk": "HIGH", "idempotent": False})

        if confidence < self.confidence_threshold:
            return (
                "ESCALATE",
                f"Confidence score {confidence:.2f} is below policy threshold ({self.confidence_threshold:.2f}). Escalate for manual review."
            )

        if rule["action"] == "AUTO_FIX" and rule["idempotent"] and rule["risk"] == "LOW":
            return (
                "AUTO_FIX",
                f"Fault '{fault_category}' is eligible for idempotent auto-fix with confidence {confidence:.2f}."
            )

        return (
            "ESCALATE",
            f"Fault '{fault_category}' requires manual intervention due to high risk or non-idempotent remediation."
        )
