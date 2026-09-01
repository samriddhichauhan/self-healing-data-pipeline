"""
BigQuery Query & Evidence Retrieval Tool
"""
import os
from typing import Dict, Any, Optional


class BigQueryEvidenceTool:
    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID", "")
        self.credentials_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

    def inspect_dataset(self, dataset_name: str, check_type: str) -> Dict[str, Any]:
        """Queries BigQuery dataset or returns structured evidence."""
        if os.path.exists(self.credentials_path) and self.project_id and "<" not in self.project_id:
            try:
                from google.cloud import bigquery
                client = bigquery.Client.from_service_account_json(self.credentials_path)
                query = f"SELECT COUNT(*) as cnt FROM `{self.project_id}.pipeline_dataset.{dataset_name}`"
                query_job = client.query(query)
                results = list(query_job.result())
                return {
                    "status": "LIVE_GCP_SUCCESS",
                    "observed_rows": results[0]["cnt"] if results else 0,
                    "check": check_type,
                }
            except Exception as e:
                return {"status": "LIVE_GCP_ERROR", "error": str(e)}

        return {
            "status": "BLOCKED_GCP_CREDENTIALS_PENDING",
            "message": "Live BigQuery query skipped. GCP Service Account Key unconfigured.",
            "check": check_type,
            "dataset": dataset_name,
        }
