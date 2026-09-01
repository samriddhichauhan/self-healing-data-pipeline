"""
Remediation Executor Module
"""
import os
import pandas as pd
from typing import Dict, Any


class RemediationExecutor:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir

    def deduplicate_dataset(self, dataset: str, primary_key: str, execution_date: str) -> Dict[str, Any]:
        """
        Idempotently deduplicates a staged dataset by primary key.
        """
        if dataset == "orders":
            stg_path = os.path.join(self.data_dir, "staging", f"stg_orders_{execution_date}.csv")
            if os.path.exists(stg_path):
                df = pd.read_csv(stg_path)
                initial_count = len(df)
                df_clean = df.drop_duplicates(subset=[primary_key])
                clean_count = len(df_clean)
                df_clean.to_csv(stg_path, index=False)
                return {
                    "status": "SUCCESS",
                    "dataset": dataset,
                    "initial_rows": initial_count,
                    "cleaned_rows": clean_count,
                    "removed_duplicates": initial_count - clean_count,
                    "stg_path": stg_path,
                }
        elif dataset == "events":
            stg_path = os.path.join(self.data_dir, "staging", f"stg_events_{execution_date}.jsonl")
            if os.path.exists(stg_path):
                df = pd.read_json(stg_path, lines=True)
                initial_count = len(df)
                df_clean = df.drop_duplicates(subset=[primary_key])
                clean_count = len(df_clean)
                df_clean.to_json(stg_path, orient="records", lines=True)
                return {
                    "status": "SUCCESS",
                    "dataset": dataset,
                    "initial_rows": initial_count,
                    "cleaned_rows": clean_count,
                    "removed_duplicates": initial_count - clean_count,
                    "stg_path": stg_path,
                }

        return {"status": "NOOP", "message": "Dataset file not found or unsupported for auto-fix."}
