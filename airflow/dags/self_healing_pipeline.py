"""
Apache Airflow DAG — self_healing_pipeline

Thin task-wiring layer only. All business logic lives in `src/` (ingestion,
validation, transformation, database, monitoring, agent). This file exists
to define the DAG graph, retries, and the on_task_failure incident hook.

Runs only inside the Airflow containers (webserver/scheduler) via
`docker compose`. The Windows-compatibility shim below exists solely so this
module can be imported directly by pytest on the host for unit-testing the
task callables in `src/` — it does not change how the DAG actually executes
inside Docker, where the real `airflow` package is always present.
"""
import os
import sys
import types
import random
from datetime import datetime, timedelta, timezone

# Make the project root importable so `import src...` works both inside the
# Airflow container (where this file lives under /opt/airflow/dags) and on
# the host (pytest).
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    import fcntl  # noqa: F401
except ModuleNotFoundError:
    mock_fcntl = types.ModuleType("fcntl")
    mock_fcntl.flock = lambda *a, **k: None
    mock_fcntl.LOCK_EX = mock_fcntl.LOCK_SH = mock_fcntl.LOCK_NB = mock_fcntl.LOCK_UN = 0
    sys.modules["fcntl"] = mock_fcntl

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except (ImportError, ModuleNotFoundError):
    class MockOperator:
        def __init__(self, task_id, python_callable=None, dag=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.upstream_list, self.downstream_list = [], []

        def __rshift__(self, other):
            others = other if isinstance(other, list) else [other]
            for o in others:
                if o not in self.downstream_list:
                    self.downstream_list.append(o)
                if self not in getattr(o, "upstream_list", []):
                    o.upstream_list.append(self)
            return other

    class MockDAG:
        def __init__(self, dag_id="self_healing_pipeline", **kwargs):
            self.dag_id = dag_id
            self.tasks = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    DAG = MockDAG
    PythonOperator = MockOperator

from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events
from src.validation.schema import validate_schema
from src.validation.quality import validate_quality
from src.transformation.transform import transform_data
from src.database.load_task import load_data
from src.monitoring.monitor import agent_monitoring
from src.config import get_data_dir


def on_task_failure(context):
    """
    True-failure path: any task that raises lands here. Creates an incident
    report and diagnoses it via the AI adapter. For DUPLICATE_INGESTION this
    is a safety net only — that path is normally healed inline inside
    validate_quality (src/validation/quality.py) before it ever raises.
    Every other fault category is ESCALATE-only by policy: this hook records
    the incident for human review but never auto-remediates, and the task
    (and DAG run) correctly stays FAILED.
    """
    from src.agent.ollama_adapter import OllamaDiagnosticAdapter
    from src.agent.logger import get_logger

    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    execution_date = context.get("ds") or "2026-06-01"

    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = dag_run.dag_id if dag_run else "self_healing_pipeline"
    run_id = dag_run.run_id if dag_run else None
    try_number = task_instance.try_number if task_instance else 1

    incident_id = f"INC-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    print(f"!!! Task Failure Hook Triggered !!! incident={incident_id} task={task_id} dag={dag_id} try={try_number}")
    print(f"Exception: {exception}")

    err_msg = str(exception).upper()
    if "SCHEMA DRIFT" in err_msg:
        failure_category = "SCHEMA_DRIFT"
    elif "ABNORMAL VOLUME" in err_msg or "ROW COUNT" in err_msg:
        failure_category = "VOLUME_ANOMALY"
    elif "NULL" in err_msg and "VIOLATION" in err_msg:
        failure_category = "NULL_SPIKE"
    elif "DUPLICATE" in err_msg:
        failure_category = "DUPLICATE_INGESTION"
    elif "REFERENTIAL" in err_msg or "FOREIGN KEY" in err_msg:
        failure_category = "REFERENTIAL_BREAK"
    elif "FRESHNESS" in err_msg or "SLA" in err_msg:
        failure_category = "STALENESS"
    else:
        failure_category = "UNKNOWN"

    err_lower = str(exception).lower()
    dataset = "orders" if "orders" in err_lower else ("events" if "events" in err_lower else "products")

    evidence = {
        "incident_id": incident_id, "timestamp": datetime.now(timezone.utc).isoformat(),
        "dag_id": dag_id, "task_id": task_id, "execution_date": execution_date,
        "try_number": try_number, "failure_category": failure_category,
        "error_message": str(exception), "dataset": dataset, "run_id": run_id,
    }

    data_dir = get_data_dir()
    report_dir = os.path.join(data_dir, "incidents", "reports")
    os.makedirs(report_dir, exist_ok=True)

    try:
        exec_logger = get_logger("pipeline_execution", "pipeline_execution.log")
        exec_logger.error(f"Task failure in '{task_id}'. Category: {failure_category}. Incident: {incident_id}")

        adapter = OllamaDiagnosticAdapter()
        diag_report = adapter.analyze_incident(
            incident_id=incident_id, pipeline_id=dag_id, task_id=task_id, dataset=dataset,
            fault_category=failure_category, observed=str(exception),
            expected="Healthy validation bounds with clean data", evidence=evidence,
            execution_date=execution_date,
        )
        diag_report.run_id = run_id
        diag_report.save(report_dir)
        print(f"[DIAGNOSIS] {incident_id}: action={diag_report.action} confidence={diag_report.confidence:.2f}")

        try:
            from src.database.loader import record_incident
            record_incident(diag_report)
        except Exception as db_err:
            print(f"[INCIDENT DB MIRROR SKIPPED] {db_err}")

        if diag_report.action == "ESCALATE":
            print(f"[ESCALATED] {incident_id}: Policy Gate requires human review for {failure_category}.")
    except Exception as ai_err:
        print(f"[AI DIAGNOSIS HOOK ERROR] {ai_err}")
        import json
        with open(os.path.join(report_dir, f"inc_{incident_id}.json"), "w") as f:
            json.dump(evidence, f, indent=2)
        print(f"[INCIDENT SAVED (MINIMAL)] {report_dir}/inc_{incident_id}.json")

    try:
        from src.monitoring.monitor import record_failed_run
        record_failed_run(context)
    except Exception as run_err:
        print(f"[FAILED RUN DB RECORD SKIPPED] {run_err}")


default_args = {
    "owner": "intern",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_task_failure,
    "sla": timedelta(hours=26),
}

with DAG(
    dag_id="self_healing_pipeline",
    description="Self-healing ETL: INGESTION -> VALIDATION -> TRANSFORM -> LOAD -> AGENT MONITORING",
    # No cron schedule on purpose: this project runs against a fixed
    # canonical demo dataset (2026-06-01), never "today's date". Runs are
    # always explicitly triggered (manager demo scripts, the dashboard's
    # trigger button, or `airflow dags trigger`) with an explicit logical
    # date, never picked up automatically off the wall clock.
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["self-healing-pipeline"],
) as dag:

    t_ingest_dimensions = PythonOperator(task_id="ingest_dimensions", python_callable=ingest_dimensions)
    t_ingest_orders = PythonOperator(task_id="ingest_orders", python_callable=ingest_orders)
    t_ingest_events = PythonOperator(task_id="ingest_events", python_callable=ingest_events)

    t_validate_schema = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    # Defense-in-depth only: validate_quality's embedded self-heal loop makes
    # a network call to the local Ollama adapter, which is now hard-bounded
    # internally (src/agent/ollama_adapter.py). This execution_timeout is a
    # second, independent safety net at the Airflow level so this task can
    # never hang the DAG run indefinitely, no matter the cause.
    t_validate_quality = PythonOperator(
        task_id="validate_quality", python_callable=validate_quality,
        execution_timeout=timedelta(minutes=5),
    )

    t_transform_data = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t_load_data = PythonOperator(task_id="load_data", python_callable=load_data)
    t_agent_monitoring = PythonOperator(task_id="agent_monitoring", python_callable=agent_monitoring)

    t_ingest_dimensions >> t_validate_schema
    t_ingest_orders >> t_validate_schema
    t_ingest_events >> t_validate_schema

    t_validate_schema >> t_validate_quality >> t_transform_data >> t_load_data >> t_agent_monitoring
