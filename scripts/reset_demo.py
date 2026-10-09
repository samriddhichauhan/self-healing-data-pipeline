#!/usr/bin/env python3
"""
Resets demo state: clears staging/processed/incident state so the next DAG
run starts clean, and regenerates the canonical raw dataset if it's missing.
Never touches data/raw/ if it's already present and intact — that's the
canonical fixture, not scratch state.
"""
import os
import sys
import subprocess

sys.path.insert(0, os.path.dirname(__file__))
from _pipeline_common import reset_demo_state, canonical_dataset_present, PROJECT_ROOT


def main():
    if not canonical_dataset_present():
        print("[RESET] Canonical raw dataset missing — regenerating deterministically...")
        subprocess.run(
            [sys.executable, os.path.join(PROJECT_ROOT, "scripts", "data", "generate_data.py"),
             "--out", os.path.join(PROJECT_ROOT, "data", "raw"), "--days", "14", "--start-date", "2026-06-01"],
            check=True,
        )
    reset_demo_state()
    print("[RESET] Demo environment reset complete.")


if __name__ == "__main__":
    main()
