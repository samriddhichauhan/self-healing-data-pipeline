"""
Shared helpers for the demo/ops scripts: talking to the real Airflow
container via `docker exec`, resetting the canonical dataset, and reading
back real state from the data directory. No simulation anywhere here — every
function in this module either runs a real subprocess or reads a real file.
"""
import os
import sys
import json
import shutil
import subprocess
import time
from datetime import datetime, timezone

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DAG_ID = "self_healing_pipeline"
CANONICAL_DATE = "2026-06-01"


def _run(cmd, timeout=60, check=True):
    print(f"$ {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if res.stdout.strip():
        print(res.stdout.strip())
    if res.returncode != 0:
        print(res.stderr.strip(), file=sys.stderr)
        if check:
            raise RuntimeError(f"Command failed ({res.returncode}): {' '.join(cmd)}\n{res.stderr}")
    return res


def scheduler_container() -> str:
    """Finds the running airflow-scheduler container by its compose service label."""
    res = subprocess.run(
        ["docker", "ps", "--filter", "label=com.docker.compose.service=airflow-scheduler", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    names = [n for n in res.stdout.strip().splitlines() if n]
    if not names:
        raise RuntimeError(
            "No running airflow-scheduler container found. Start the stack first with:\n"
            "    docker compose up -d"
        )
    return names[0]


def airflow_cli(*args, timeout=60, check=True):
    container = scheduler_container()
    return _run(["docker", "exec", container, "airflow", *args], timeout=timeout, check=check)


def trigger_dag(run_id: str, logical_date: str = CANONICAL_DATE):
    """Triggers a real Airflow DAG run inside the container. Returns the run_id used.

    Airflow rejects a second DagRun at an execution_date that already has one,
    even with a different run_id. The canonical demo date is fixed
    (2026-06-01) and demos may be re-run repeatedly, so the *time* portion of
    the logical timestamp is set to the real current wall-clock time to keep
    each trigger's execution_date unique — the *date* portion (and therefore
    the `ds` every task callable actually uses for file paths) stays pinned
    to the canonical demo date regardless.
    """
    time_of_day = datetime.now(timezone.utc).strftime("%H:%M:%S")
    airflow_cli(
        "dags", "trigger", DAG_ID,
        "--run-id", run_id,
        "--exec-date", f"{logical_date}T{time_of_day}+00:00",
    )
    return run_id


def get_dag_run_state(run_id: str) -> str:
    # A single slow CLI call (e.g. while Ollama saturates the CPU) must not
    # abort the demo — report "unknown" and let the caller keep polling.
    try:
        res = airflow_cli("dags", "list-runs", "-d", DAG_ID, "-o", "json", timeout=120, check=False)
    except subprocess.TimeoutExpired:
        return "unknown"
    try:
        runs = json.loads(res.stdout)
    except Exception:
        return "unknown"
    for r in runs:
        if r.get("run_id") == run_id:
            return r.get("state", "unknown")
    return "not_found"


def get_task_states(run_id: str) -> dict:
    res = airflow_cli("tasks", "states-for-dag-run", DAG_ID, run_id, "-o", "json", check=False)
    try:
        rows = json.loads(res.stdout)
    except Exception:
        return {}
    return {row.get("task_id"): row.get("state") for row in rows if row.get("task_id")}


def wait_for_dag_run(run_id: str, timeout_seconds: int = 900, poll_seconds: int = 8) -> str:
    """Polls the real Airflow scheduler until the DAG run finishes or times out."""
    start = time.time()
    last_state = None
    while time.time() - start < timeout_seconds:
        state = get_dag_run_state(run_id)
        if state != last_state:
            print(f"[POLL] DAG run '{run_id}' state: {state} (t+{int(time.time() - start)}s)")
            last_state = state
        if state in ("success", "failed"):
            return state
        time.sleep(poll_seconds)
    raise TimeoutError(f"DAG run '{run_id}' did not finish within {timeout_seconds}s (last state: {last_state})")


def reset_demo_state(execution_date: str = CANONICAL_DATE):
    """Clears staging/processed/incident state so a demo run starts clean.
    Leaves data/raw/ (the canonical dataset) untouched."""
    for sub in ("staging", "processed", "quarantine"):
        d = os.path.join(DATA_DIR, sub)
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    for sub in ("incidents/reports", "incidents/profiles", "incidents/status"):
        d = os.path.join(DATA_DIR, sub)
        os.makedirs(d, exist_ok=True)
        for f in os.listdir(d):
            fp = os.path.join(d, f)
            if os.path.isfile(fp):
                os.remove(fp)

    print(f"[RESET] Cleared staging/processed/quarantine/incidents state (raw canonical dataset untouched).")


def canonical_dataset_present() -> bool:
    required = [
        os.path.join(DATA_DIR, "raw", "customers.csv"),
        os.path.join(DATA_DIR, "raw", "products.csv"),
        os.path.join(DATA_DIR, "raw", "orders", f"orders_{CANONICAL_DATE}.csv"),
        os.path.join(DATA_DIR, "raw", "events", f"events_{CANONICAL_DATE}.jsonl"),
    ]
    return all(os.path.exists(p) for p in required)


def new_run_id(prefix: str) -> str:
    return f"{prefix}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
