"""
Remediation Verifier: closed-loop post-fix verification. A remediation is
only ever considered successful once this independently re-checks the data.
"""
import os
import pandas as pd
from typing import Dict, Any


class RemediationVerifier:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir

    def verify_remediation(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        return self.verify(
            dataset=plan.get("dataset", "orders"),
            primary_key=plan.get("primary_key", "order_id"),
            execution_date=plan.get("execution_date", "2026-06-01"),
        )

    @staticmethod
    def _not_verified(dataset: str, notes: str) -> Dict[str, Any]:
        """Nothing was actually checked, so this can never count as success."""
        return {
            "verified": False, "check": "not_verified", "dataset": dataset,
            "status": "NOT_VERIFIED", "verification_notes": notes,
        }

    def _check_primary_key(self, dataset: str, stg_path: str, read_fn, primary_key: str, clean_note: str) -> Dict[str, Any]:
        """PASSED only when the file was read, the key column exists, at least
        one row remains, and no primary key is duplicated."""
        if not os.path.exists(stg_path):
            return self._not_verified(dataset, f"Staged file not found: {stg_path}")
        try:
            df = read_fn(stg_path)
        except pd.errors.EmptyDataError:
            df = pd.DataFrame()  # empty CSV: reported below, same as empty JSONL
        except Exception as e:
            return self._not_verified(dataset, f"Staged file could not be read ({type(e).__name__}): {stg_path}")
        if df.shape == (0, 0):
            return self._not_verified(dataset, f"Staged file is empty (no columns or rows): {stg_path}")
        if primary_key not in df.columns:
            return self._not_verified(dataset, f"Primary key column '{primary_key}' missing from {stg_path}")

        dup_count = int(df[primary_key].duplicated().sum())
        remaining_rows = len(df)
        is_verified = dup_count == 0 and remaining_rows > 0
        if is_verified:
            notes = clean_note
        elif remaining_rows == 0:
            notes = "No rows remain after remediation."
        else:
            rows = "row" if dup_count == 1 else "rows"
            notes = f"Duplicate '{primary_key}' values remain after remediation ({dup_count} extra {rows})."
        return {
            "verified": is_verified, "check": "primary_key_deduplication", "dataset": dataset,
            "remaining_rows": remaining_rows, "duplicate_count": dup_count,
            "status": "PASSED" if is_verified else "REMEDIATION_FAILED_ESCALATE",
            "verification_notes": notes,
        }

    def verify(self, dataset: str, primary_key: str, execution_date: str) -> Dict[str, Any]:
        stg_dir = os.path.join(self.data_dir, "staging")

        if dataset == "orders":
            return self._check_primary_key(
                dataset, os.path.join(stg_dir, f"stg_orders_{execution_date}.csv"),
                pd.read_csv, primary_key, "Zero duplicate primary keys found.",
            )
        if dataset == "events":
            return self._check_primary_key(
                dataset, os.path.join(stg_dir, f"stg_events_{execution_date}.jsonl"),
                lambda p: pd.read_json(p, lines=True), primary_key, "Zero duplicate event IDs found.",
            )

        return self._not_verified(dataset, f"Dataset '{dataset}' is not supported by the verifier.")
