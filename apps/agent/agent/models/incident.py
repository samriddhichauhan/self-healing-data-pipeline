"""
Standardized Incident Report Model
"""
import os
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List


@dataclass
class IncidentReport:
    incident_id: str
    timestamp: str
    pipeline: str
    dataset: str
    check: str
    severity: str  # HIGH, MEDIUM, LOW
    observed: Any
    expected: Any
    evidence: Dict[str, Any]
    hypothesis: str
    confidence: float
    blast_radius: str
    action: str  # AUTO_FIX, ESCALATE
    status: str  # PENDING_APPROVAL, REMEDIATED, ESCALATED, DECLINED
    remediation: Optional[Dict[str, Any]] = None
    verification: Optional[Dict[str, Any]] = None
    failed_check: Optional[str] = None
    observed_value: Optional[Any] = None
    expected_value: Optional[Any] = None
    possible_root_causes: List[str] = field(default_factory=list)
    remediation_status: str = "PENDING"
    verification_status: str = "PENDING"
    recovery_time: Optional[float] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self):
        if not self.failed_check:
            self.failed_check = self.check
        if self.observed_value is None:
            self.observed_value = self.observed
        if self.expected_value is None:
            self.expected_value = self.expected
        if not self.possible_root_causes and self.hypothesis:
            self.possible_root_causes = [self.hypothesis]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def save(self, report_dir: str) -> Tuple[str, str]:
        os.makedirs(report_dir, exist_ok=True)
        json_path = os.path.join(report_dir, f"inc_{self.incident_id}.json")
        md_path = os.path.join(report_dir, f"inc_{self.incident_id}.md")

        with open(json_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

        md_content = f"""# Incident Report: {self.incident_id}

## Status: {self.status}
* **Timestamp:** {self.timestamp}
* **Pipeline:** {self.pipeline}
* **Dataset:** {self.dataset}
* **Failed Check:** {self.failed_check or self.check}
* **Severity:** {self.severity}
* **Action Decision:** {self.action}
* **Confidence Score:** {self.confidence:.2f}
* **Recovery Duration:** {f"{self.recovery_time:.2f}s" if self.recovery_time else "N/A"}

## 1. Executive Summary & Observed Evidence
* **Observed Value:** `{self.observed_value or self.observed}`
* **Expected Value:** `{self.expected_value or self.expected}`
* **Blast Radius:** {self.blast_radius}

### Diagnostic Evidence Details
```json
{json.dumps(self.evidence, indent=2)}
```

## 2. Agent Structured Reasoning & Root Causes
* **Hypothesis:** {self.hypothesis}
* **Possible Root Causes:** {", ".join(self.possible_root_causes)}
* **Proposed Action:** {self.action}

## 3. Remediation & Verification Record
* **Remediation Status:** {self.remediation_status}
* **Remediation Details:** {json.dumps(self.remediation) if self.remediation else 'None / Pending'}
* **Verification Status:** {self.verification_status}
* **Verification Details:** {json.dumps(self.verification) if self.verification else 'None / Pending'}
"""
        with open(md_path, "w") as f:
            f.write(md_content)

        return json_path, md_path

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IncidentReport":
        # Filter unknown keys safely for backward compatibility
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    def to_summary_markdown(self) -> str:
        """Generates a concise Markdown summary badge for quick UI/CLI inspection."""
        status_icon = "🟢" if self.status == "REMEDIATED" else ("🔴" if self.status == "ESCALATED" else "🟡")
        return (
            f"{status_icon} **{self.incident_id}** | Dataset: `{self.dataset}` | "
            f"Action: `{self.action}` | Severity: `{self.severity}` | Confidence: `{self.confidence:.0%}`\n"
            f"> *Hypothesis*: {self.hypothesis}\n"
        )
