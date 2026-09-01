"""
Remediation Tool Interface
"""
from typing import Dict, Any
from ..remediation.executor import RemediationExecutor


class RemediationTool:
    def __init__(self, data_dir: str = "./data"):
        self.executor = RemediationExecutor(data_dir=data_dir)

    def execute_fix(self, dataset: str, primary_key: str, execution_date: str) -> Dict[str, Any]:
        """Triggers deterministic deduplication remediation."""
        return self.executor.deduplicate_dataset(dataset, primary_key, execution_date)
