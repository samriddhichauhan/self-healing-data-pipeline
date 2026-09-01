"""
Airflow Status & DAG Metadata Inspection Tool
"""
import os
import json
from typing import Dict, Any


class AirflowStatusTool:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir

    def get_pipeline_status(self, execution_date: str) -> Dict[str, Any]:
        """Reads latest status report from incidents/status directory."""
        status_path = os.path.join(self.data_dir, "incidents", "status", f"status_{execution_date}.json")
        if os.path.exists(status_path):
            with open(status_path, "r") as f:
                return json.load(f)

        return {
            "execution_date": execution_date,
            "pipeline_status": "UNKNOWN",
            "message": "Status report file not found.",
        }
