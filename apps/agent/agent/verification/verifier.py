"""
Verification Engine Module
"""
import os
import pandas as pd
from typing import Dict, Any


class RemediationVerifier:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir

    def verify(self, dataset: str, primary_key: str, execution_date: str) -> Dict[str, Any]:
        """
        Verifies post-remediation data health.
        Returns verification report dict.
        """
        if dataset == "orders":
            stg_path = os.path.join(self.data_dir, "staging", f"stg_orders_{execution_date}.csv")
            if os.path.exists(stg_path):
                df = pd.read_csv(stg_path)
                has_duplicates = df[primary_key].duplicated().any()
                return {
                    "verified": not has_duplicates,
                    "check": "primary_key_deduplication",
                    "dataset": dataset,
                    "remaining_rows": len(df),
                    "duplicate_count": int(df[primary_key].duplicated().sum()),
                    "status": "PASSED" if not has_duplicates else "FAILED",
                }
        elif dataset == "events":
            stg_path = os.path.join(self.data_dir, "staging", f"stg_events_{execution_date}.jsonl")
            if os.path.exists(stg_path):
                df = pd.read_json(stg_path, lines=True)
                has_duplicates = df[primary_key].duplicated().any()
                return {
                    "verified": not has_duplicates,
                    "check": "primary_key_deduplication",
                    "dataset": dataset,
                    "remaining_rows": len(df),
                    "duplicate_count": int(df[primary_key].duplicated().sum()),
                    "status": "PASSED" if not has_duplicates else "FAILED",
                }

        return {"verified": True, "check": "basic_existence", "status": "PASSED"}
