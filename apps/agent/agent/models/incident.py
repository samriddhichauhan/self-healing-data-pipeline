"""
Standardized Incident Report Model
"""
import os
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple


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
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

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
* **Check Name:** {self.check}
* **Severity:** {self.severity}
* **Action Decision:** {self.action}
* **Confidence Score:** {self.confidence:.2f}

## 1. Executive Summary & Observed Evidence
* **Observed:** `{self.observed}`
* **Expected:** `{self.expected}`
* **Blast Radius:** {self.blast_radius}

### Diagnostic Evidence Details
```json
{json.dumps(self.evidence, indent=2)}
```

## 2. Agent Structured Reasoning
* **Hypothesis:** {self.hypothesis}
* **Proposed Action:** {self.action}

## 3. Remediation & Verification Record
* **Remediation Details:** {json.dumps(self.remediation) if self.remediation else 'None / Pending'}
* **Verification Status:** {json.dumps(self.verification) if self.verification else 'None / Pending'}
"""
        with open(md_path, "w") as f:
            f.write(md_content)

        return json_path, md_path

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IncidentReport":
        return cls(**data)
