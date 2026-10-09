#!/usr/bin/env python3
"""
Verifies the local environment is ready for a real Docker/Airflow demo run.
Every check here runs a real command or reads a real file — nothing is
assumed. Exits non-zero (with a clear reason) if anything required is
missing, per the "fail loud" requirement for this project.
"""
import subprocess
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from _pipeline_common import canonical_dataset_present, scheduler_container, airflow_cli, DAG_ID, CANONICAL_DATE


def check(label, fn):
    try:
        ok, detail = fn()
    except Exception as e:
        ok, detail = False, str(e)
    print(f"  [{'OK' if ok else 'FAIL'}] {label}{f' — {detail}' if detail else ''}")
    return ok


def check_docker():
    res = subprocess.run(["docker", "version"], capture_output=True, text=True)
    return res.returncode == 0, None if res.returncode == 0 else res.stderr.strip()


def check_containers():
    res = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"], capture_output=True, text=True
    )
    lines = res.stdout.strip().splitlines()
    required_services = ["postgres", "airflow-webserver", "airflow-scheduler", "backend", "frontend"]
    found = {svc: any(svc in line for line in lines) for svc in required_services}
    missing = [s for s, ok in found.items() if not ok]
    if missing:
        return False, f"missing/unhealthy containers for services: {missing}. Run: docker compose up -d"
    unhealthy = [line for line in lines if "unhealthy" in line.lower() or "restarting" in line.lower()]
    if unhealthy:
        return False, f"unhealthy containers: {unhealthy}"
    return True, f"{len(lines)} containers running"


def check_dataset():
    ok = canonical_dataset_present()
    return ok, None if ok else f"canonical dataset for {CANONICAL_DATE} missing under data/raw/ — run scripts/data/generate_data.py"


def check_dag_registered():
    try:
        container = scheduler_container()
    except Exception as e:
        return False, str(e)
    res = airflow_cli("dags", "list", check=False)
    ok = DAG_ID in res.stdout
    return ok, f"container={container}" if ok else f"'{DAG_ID}' not found in `airflow dags list` output"


def main():
    print("=== ENVIRONMENT VERIFICATION ===")
    results = [
        check("Docker CLI reachable", check_docker),
        check("Required containers running (postgres, airflow-webserver, airflow-scheduler, backend, frontend)", check_containers),
        check(f"Canonical dataset present for {CANONICAL_DATE}", check_dataset),
        check("self_healing_pipeline DAG registered in Airflow", check_dag_registered),
    ]
    print()
    if all(results):
        print("Environment is ready.")
        sys.exit(0)
    else:
        print("Environment is NOT ready — fix the FAIL items above before running a demo.")
        sys.exit(1)


if __name__ == "__main__":
    main()
