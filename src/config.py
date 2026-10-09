"""
Shared configuration and path resolution for the self-healing pipeline.

Centralizes the two things every task/module needs:
  - the pipeline data-quality contract (pipeline_config.yaml)
  - the data directory (differs between the Airflow container and host)
"""
import os
import yaml

CONFIG_PATH = os.environ.get(
    "PIPELINE_CONFIG_PATH", "/opt/airflow/config/pipeline_config.yaml"
)

_LOCAL_CONFIG_FALLBACK = os.path.join(
    os.path.dirname(__file__), "..", "airflow", "config", "pipeline_config.yaml"
)


def load_config() -> dict:
    """Loads the pipeline data-quality contract, trying the container path first."""
    path = CONFIG_PATH if os.path.exists(CONFIG_PATH) else _LOCAL_CONFIG_FALLBACK
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"pipeline_config.yaml not found at container path '{CONFIG_PATH}' "
            f"or local fallback '{_LOCAL_CONFIG_FALLBACK}'"
        )
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_data_dir() -> str:
    """Resolves the data directory: explicit override > container mount > host repo root."""
    if os.environ.get("TEST_DATA_DIR"):
        return os.environ["TEST_DATA_DIR"]
    if os.path.exists("/opt/airflow/data"):
        return "/opt/airflow/data"
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))


config = load_config()
