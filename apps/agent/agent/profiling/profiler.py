"""
Data Profiling Module
Calculates lightweight summary metrics and dataset health statistics.
"""
import os
import json
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timezone


class DataProfiler:
    def __init__(self, output_dir: str = "./data/incidents/profiles"):
        self.output_dir = output_dir

    def profile_dataframe(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        primary_key: Optional[str] = None,
        date_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculates data profile statistics for a DataFrame.
        """
        row_count = len(df)
        if row_count == 0:
            return {
                "dataset": dataset_name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "row_count": 0,
                "column_count": len(df.columns),
                "null_percentage_overall": 0.0,
                "duplicate_percentage": 0.0,
                "columns": {}
            }

        # Null percentage calculations
        null_counts = df.isnull().sum()
        total_cells = row_count * len(df.columns)
        overall_null_pct = round((null_counts.sum() / total_cells) * 100, 2) if total_cells > 0 else 0.0

        # Duplicate percentage calculation
        if primary_key and primary_key in df.columns:
            dup_count = df[primary_key].duplicated().sum()
            dup_pct = round((dup_count / row_count) * 100, 2)
        else:
            dup_count = df.duplicated().sum()
            dup_pct = round((dup_count / row_count) * 100, 2)

        # Per-column stats
        column_stats = {}
        for col in df.columns:
            col_null_pct = round((null_counts[col] / row_count) * 100, 2)
            n_unique = int(df[col].nunique(dropna=True))

            col_data = {
                "type": str(df[col].dtype),
                "null_pct": col_null_pct,
                "unique_values": n_unique
            }

            # Numeric min/max and distribution metrics
            if pd.api.types.is_numeric_dtype(df[col]):
                non_null_s = df[col].dropna()
                if len(non_null_s) > 0:
                    col_data["min"] = float(non_null_s.min())
                    col_data["max"] = float(non_null_s.max())
                    col_data["mean"] = round(float(non_null_s.mean()), 2)
                    col_data["std"] = round(float(non_null_s.std()), 2) if len(non_null_s) > 1 else 0.0

            column_stats[col] = col_data

        # Freshness check if date column is present
        freshness_info = None
        if date_column and date_column in df.columns:
            try:
                dates = pd.to_datetime(df[date_column], errors='coerce').dropna()
                if len(dates) > 0:
                    latest_date = dates.max()
                    freshness_info = {
                        "latest_timestamp": latest_date.isoformat(),
                        "oldest_timestamp": dates.min().isoformat()
                    }
            except Exception:
                pass

        profile = {
            "dataset": dataset_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "row_count": row_count,
            "column_count": len(df.columns),
            "null_percentage_overall": overall_null_pct,
            "duplicate_percentage": dup_pct,
            "primary_key": primary_key,
            "freshness": freshness_info,
            "columns": column_stats
        }

        return profile

    def save_profile(self, profile_data: Dict[str, Any], execution_date: str) -> str:
        """
        Persists data profile report as JSON.
        """
        os.makedirs(self.output_dir, exist_ok=True)
        dataset_name = profile_data.get("dataset", "dataset")
        filepath = os.path.join(self.output_dir, f"profile_{dataset_name}_{execution_date}.json")
        with open(filepath, "w") as f:
            json.dump(profile_data, f, indent=2)
        return filepath
