"""
End-to-End Live Manager Demo Scenario Automated Tests
Verifies all 5 pipeline operational scenarios execute cleanly:
- Healthy Baseline (PASS)
- Schema Drift (ESCALATE)
- Null Spike (ESCALATE)
- Duplicate Ingestion (AUTO_FIX -> AUTO-HEALED)
- Volume Drop Anomaly (ESCALATE)
"""
import os
import sys
import pytest

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
scripts_dir = os.path.join(project_root, "scripts")
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

from scripts.test_pipeline_5_cases import run_pipeline_scenario


def test_manager_demo_scenarios_e2e(tmp_path):
    """Executes full 5-scenario demo test harness."""
    os.environ["TEST_DATA_DIR"] = str(tmp_path)

    # 1. Healthy
    run_pipeline_scenario(1, "Healthy Clean Baseline", fault_type=None)

    # 2. Schema Drift
    run_pipeline_scenario(2, "Schema Drift Fault", fault_type="schema_drift")

    # 3. Null Spike
    run_pipeline_scenario(3, "Null Spike Fault", fault_type="null_spike")

    # 4. Duplicate Ingestion (Auto-Remediate)
    run_pipeline_scenario(4, "Duplicate Ingestion Fault", fault_type="duplicate_ingestion")

    # 5. Volume Drop
    run_pipeline_scenario(5, "Volume Drop Fault", fault_type="volume_drop")
