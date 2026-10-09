"""
Settings, all from environment variables. Defaults target a host-side run
(uvicorn on Windows against the published Docker ports); the container sets
the in-network service names via docker-compose.platform.yml.
"""
import os
from dataclasses import dataclass, field


def _split(value: str) -> list:
    return [v.strip() for v in value.split(",") if v.strip()]


@dataclass(frozen=True)
class Settings:
    backend_url: str = field(default_factory=lambda: os.environ.get("BACKEND_URL", "http://localhost:8000").rstrip("/"))
    airflow_url: str = field(default_factory=lambda: os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080").rstrip("/"))
    airflow_public_url: str = field(default_factory=lambda: os.environ.get("AIRFLOW_PUBLIC_URL", "http://localhost:8080").rstrip("/"))
    airflow_user: str = field(default_factory=lambda: os.environ.get("AIRFLOW_USER", "airflow"))
    airflow_password: str = field(default_factory=lambda: os.environ.get("AIRFLOW_PASSWORD", "airflow"))
    ollama_host: str = field(default_factory=lambda: os.environ.get("OLLAMA_HOST", "http://localhost:11434").rstrip("/"))
    ollama_model: str = field(default_factory=lambda: os.environ.get("OLLAMA_MODEL", "llama3.2:latest"))
    dag_id: str = field(default_factory=lambda: os.environ.get("DAG_ID", "self_healing_pipeline"))
    ai_log_file: str = field(default_factory=lambda: os.environ.get("AI_LOG_FILE", "ollama_ai_agent.log"))
    cors_origins: list = field(default_factory=lambda: _split(os.environ.get(
        "CORS_ORIGINS", "http://localhost:5174,http://127.0.0.1:5174,http://localhost:5175,http://127.0.0.1:5175"
    )))

    @property
    def airflow_api(self) -> str:
        return f"{self.airflow_url}/api/v1"


settings = Settings()

# The fault types accepted by the existing scripts/fault-injection/inject_fault.py
# (its argparse `choices`). Only duplicate_ingestion is eligible for AUTO_FIX
# under the existing Policy Gate; everything else escalates by design.
FAULT_TYPES = [
    {"id": "duplicate_ingestion", "label": "Duplicate ingestion", "expected": "AUTO_FIX",
     "effect": "Appends 50 duplicate orders to the date's staged orders file."},
    {"id": "null_spike", "label": "Null spike", "expected": "ESCALATE",
     "effect": "Sets 15% of staged customer_id values to NULL."},
    {"id": "schema_drift", "label": "Schema drift", "expected": "ESCALATE",
     "effect": "Rewrites staged products with a string-typed price column."},
    {"id": "volume_drop", "label": "Volume drop", "expected": "ESCALATE",
     "effect": "Truncates the date's staged orders to 120 rows."},
    {"id": "referential_break", "label": "Referential break", "expected": "ESCALATE",
     "effect": "Points 11 staged orders at a non-existent customer."},
    {"id": "staleness", "label": "Staleness", "expected": "ESCALATE",
     "effect": "Sets every staged order_ts to 2026-01-01."},
]

# DAG task order, as wired in airflow/dags/self_healing_pipeline.py.
TASK_ORDER = [
    "ingest_dimensions", "ingest_orders", "ingest_events",
    "validate_schema", "validate_quality", "transform_data", "load_data", "agent_monitoring",
]
