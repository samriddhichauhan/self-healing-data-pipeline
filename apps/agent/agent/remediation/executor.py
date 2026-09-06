"""
Remediation Executor Module
Executes safe, deterministic, and idempotent auto-fix actions.
"""
import os
import pandas as pd
from typing import Dict, Any, Optional


class RemediationExecutor:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir

    def execute_remediation(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes remediation action based on plan contract.
        """
        rem_type = plan.get("remediation_type")
        if rem_type == "deduplicate_dataset":
            return self.deduplicate_dataset(
                dataset=plan.get("dataset", "orders"),
                primary_key=plan.get("primary_key", "order_id"),
                execution_date=plan.get("execution_date", "2026-06-01")
            )
        elif rem_type == "quarantine_invalid_records":
            return self.quarantine_invalid_records(
                dataset=plan.get("dataset", "orders"),
                condition_column=plan.get("condition_column", "customer_id"),
                execution_date=plan.get("execution_date", "2026-06-01")
            )
        return {"status": "NOOP", "message": f"Unknown remediation type '{rem_type}'"}

    def deduplicate_dataset(self, dataset: str, primary_key: str, execution_date: str) -> Dict[str, Any]:
        """
        Idempotently deduplicates a staged dataset by primary key.
        """
        stg_dir = os.path.join(self.data_dir, "staging")
        os.makedirs(stg_dir, exist_ok=True)

        if dataset == "orders":
            stg_path = os.path.join(stg_dir, f"stg_orders_{execution_date}.csv")
            if os.path.exists(stg_path):
                df = pd.read_csv(stg_path)
                initial_count = len(df)
                df_clean = df.drop_duplicates(subset=[primary_key])
                clean_count = len(df_clean)
                df_clean.to_csv(stg_path, index=False)
                return {
                    "status": "SUCCESS",
                    "action_type": "DEDUPLICATE",
                    "dataset": dataset,
                    "initial_rows": initial_count,
                    "cleaned_rows": clean_count,
                    "removed_duplicates": initial_count - clean_count,
                    "stg_path": stg_path,
                }
        elif dataset == "events":
            stg_path = os.path.join(stg_dir, f"stg_events_{execution_date}.jsonl")
            if os.path.exists(stg_path):
                df = pd.read_json(stg_path, lines=True)
                initial_count = len(df)
                df_clean = df.drop_duplicates(subset=[primary_key])
                clean_count = len(df_clean)
                df_clean.to_json(stg_path, orient="records", lines=True)
                return {
                    "status": "SUCCESS",
                    "action_type": "DEDUPLICATE",
                    "dataset": dataset,
                    "initial_rows": initial_count,
                    "cleaned_rows": clean_count,
                    "removed_duplicates": initial_count - clean_count,
                    "stg_path": stg_path,
                }

        return {"status": "NOOP", "message": f"Dataset '{dataset}' file not found or unsupported for auto-deduplication."}

    def quarantine_invalid_records(
        self,
        dataset: str,
        condition_column: str,
        execution_date: str
    ) -> Dict[str, Any]:
        """
        Quarantines invalid rows (e.g. nulls or referential orphans) to data/quarantine/.
        """
        stg_dir = os.path.join(self.data_dir, "staging")
        quarantine_dir = os.path.join(self.data_dir, "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)

        if dataset == "orders":
            stg_path = os.path.join(stg_dir, f"stg_orders_{execution_date}.csv")
            if os.path.exists(stg_path):
                df = pd.read_csv(stg_path)
                initial_count = len(df)

                bad_mask = df[condition_column].isnull()
                quarantined_df = df[bad_mask]
                clean_df = df[~bad_mask]

                if len(quarantined_df) > 0:
                    quarantine_file = os.path.join(quarantine_dir, f"quarantine_orders_{execution_date}.csv")
                    quarantined_df.to_csv(quarantine_file, index=False)
                    clean_df.to_csv(stg_path, index=False)
                    return {
                        "status": "SUCCESS",
                        "action_type": "QUARANTINE",
                        "dataset": dataset,
                        "initial_rows": initial_count,
                        "quarantined_rows": len(quarantined_df),
                        "cleaned_rows": len(clean_df),
                        "quarantine_file": quarantine_file
                    }

        return {"status": "NOOP", "message": "No invalid records matched quarantine condition."}
