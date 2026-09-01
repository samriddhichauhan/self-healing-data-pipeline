"""
Incident Report Management Tool
"""
import os
import glob
import json
from typing import List, Dict, Any, Optional, Tuple
from ..models.incident import IncidentReport


class IncidentTool:
    def __init__(self, reports_dir: str = "./incidents/reports"):
        self.reports_dir = reports_dir

    def list_incidents(self) -> List[Dict[str, Any]]:
        """Lists all JSON incident reports sorted by timestamp descending."""
        if not os.path.exists(self.reports_dir):
            return []

        pattern = os.path.join(self.reports_dir, "inc_*.json")
        incidents = []
        for filepath in glob.glob(pattern):
            try:
                with open(filepath, "r") as f:
                    incidents.append(json.load(f))
            except Exception:
                continue

        incidents.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return incidents

    def get_incident(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves specific incident by ID."""
        json_path = os.path.join(self.reports_dir, f"inc_{incident_id}.json")
        if os.path.exists(json_path):
            with open(json_path, "r") as f:
                return json.load(f)
        return None

    def save_report(self, report: IncidentReport) -> Tuple[str, str]:
        """Saves IncidentReport model object to disk as JSON & Markdown."""
        return report.save(self.reports_dir)
