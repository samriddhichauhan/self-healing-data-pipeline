"""
Verification Tool Interface
"""
from typing import Dict, Any
from ..verification.verifier import RemediationVerifier


class VerificationTool:
    def __init__(self, data_dir: str = "./data"):
        self.verifier = RemediationVerifier(data_dir=data_dir)

    def verify_remediation(self, dataset: str, primary_key: str, execution_date: str) -> Dict[str, Any]:
        """Runs post-remediation verification check."""
        return self.verifier.verify(dataset, primary_key, execution_date)
