"""
Data quality validation.

Enforces row-count bounds, null tolerance, primary-key uniqueness,
referential integrity, and freshness SLAs from pipeline_config.yaml.

Self-healing is embedded directly in this task for exactly one fault type:
DUPLICATE_INGESTION. On detecting duplicate order primary keys, it invokes
the AI diagnostic adapter -> Policy Gate -> (if AUTO_FIX) Remediation
Executor -> Remediation Verifier, and only continues past this task if the
Verifier independently confirms the data is clean. Every other fault type
(schema handled upstream; null spikes, referential breaks, volume
anomalies, staleness) is NOT auto-remediated — the Policy Gate always
returns ESCALATE for those, and this task fails loudly so Airflow marks the
task/DAG run FAILED. That is the correct, safe outcome, not a bug to work
around.
"""
import os
import json
import random
import pandas as pd
from datetime import datetime, timezone
from ..config import config, get_data_dir
from ..agent.ollama_adapter import OllamaDiagnosticAdapter
from ..agent.policy_gate import PolicyGate
from ..remediation.executor import RemediationExecutor
from ..remediation.verifier import RemediationVerifier


def _new_incident_id() -> str:
    return f"INC-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(1000, 9999)}"


def _mirror_incident_to_db(diag_report):
    """Best-effort mirror of the incident into Postgres. Never fails the task."""
    try:
        from ..database.loader import record_incident
        record_incident(diag_report)
    except Exception as db_err:
        print(f"[INCIDENT DB MIRROR SKIPPED] {db_err}")


def _persist_incident(diag_report, report_dir) -> bool:
    """Saves the incident report (required) and mirrors it to Postgres (best
    effort). If the save fails, the record is marked with incident_report_error
    (so the DB row shows the report is missing even after a successful retry),
    written to the task log and still mirrored, and False is returned so the
    caller fails the task."""
    saved = True
    try:
        diag_report.save(report_dir)
    except Exception as save_err:
        saved = False
        diag_report.incident_report_error = f"{type(save_err).__name__}: {save_err}"
        print(f"[INCIDENT SAVE FAILED] {diag_report.incident_id}: {diag_report.incident_report_error}. "
              f"Full incident record follows:")
        print(json.dumps(diag_report.to_dict(), default=str))
    _mirror_incident_to_db(diag_report)
    return saved


def _rollback_failure_reason(rollback) -> str:
    """Escalation text that only claims what is known about the backup."""
    path = rollback.get("backup_path")
    backup = {
        "verified": f"original preserved at {path} (checksum verified)",
        "checksum_mismatch": f"backup at {path} failed checksum verification and cannot be trusted",
        "missing": f"backup is missing at {path}",
        "unreadable": f"backup at {path} could not be read",
    }.get(rollback.get("backup_state"), f"state of the backup at {path} is unknown")
    return f"Rollback failed ({rollback.get('error')}); {backup}."


def validate_quality(**context):
    """Enforces row limits, null thresholds, key uniqueness, referential
    integrity, and freshness SLAs. Embeds the duplicate-ingestion self-heal
    loop; raises ValueError (real Airflow task failure) for every other
    quality violation."""
    execution_date = context["ds"]
    data_dir = get_data_dir()

    cust_stg = os.path.join(data_dir, "staging", "stg_customers.csv")
    prod_stg = os.path.join(data_dir, "staging", "stg_products.csv")
    orders_stg = os.path.join(data_dir, "staging", f"stg_orders_{execution_date}.csv")
    events_stg = os.path.join(data_dir, "staging", f"stg_events_{execution_date}.jsonl")

    df_cust = pd.read_csv(cust_stg) if os.path.exists(cust_stg) else None
    df_prod = pd.read_csv(prod_stg) if os.path.exists(prod_stg) else None
    df_orders = pd.read_csv(orders_stg) if os.path.exists(orders_stg) else None
    df_events = pd.read_json(events_stg, lines=True) if os.path.exists(events_stg) else None

    # 1. Row count validations
    if df_cust is not None:
        b = config["datasets"]["customers"]["expected_row_count"]
        if not (b["min"] <= len(df_cust) <= b["max"]):
            raise ValueError(f"Data Quality: Customers row count {len(df_cust)} violates bounds [{b['min']}, {b['max']}] (Abnormal Volume)")
    if df_prod is not None:
        b = config["datasets"]["products"]["expected_row_count"]
        if not (b["min"] <= len(df_prod) <= b["max"]):
            raise ValueError(f"Data Quality: Products row count {len(df_prod)} violates bounds [{b['min']}, {b['max']}] (Abnormal Volume)")
    if df_orders is not None:
        b = config["datasets"]["orders"]["expected_row_count_per_batch"]
        if not (b["min"] <= len(df_orders) <= b["max"]):
            raise ValueError(f"Data Quality: Orders row count {len(df_orders)} violates bounds [{b['min']}, {b['max']}] (Abnormal Volume)")
    if df_events is not None:
        b = config["datasets"]["events"]["expected_row_count_per_batch"]
        if not (b["min"] <= len(df_events) <= b["max"]):
            raise ValueError(f"Data Quality: Events row count {len(df_events)} violates bounds [{b['min']}, {b['max']}] (Abnormal Volume)")

    # 2. Null tolerance checks (orders only)
    if df_orders is not None:
        for col, tolerance in config["datasets"]["orders"]["null_tolerance"].items():
            null_pct = df_orders[col].isnull().mean()
            if null_pct > tolerance:
                raise ValueError(f"Data Quality: Column '{col}' in orders null rate {null_pct:.4f} exceeds threshold {tolerance} (NULL Violation)")

    # 3. Duplicate checks — orders: self-heal loop; events: escalate (no auto-fix path for events yet)
    if df_orders is not None:
        pk = config["datasets"]["orders"]["primary_key"]
        if df_orders[pk].duplicated().any():
            dup_count = int(df_orders[pk].duplicated().sum())
            exc_msg = f"Data Quality: Duplicate primary keys found in orders. Found {dup_count} duplicates for column '{pk}'"

            auto_healed = False
            task_instance = context.get("task_instance")
            dag_run = context.get("dag_run")
            task_id = task_instance.task_id if task_instance else "validate_quality"
            dag_id = dag_run.dag_id if dag_run else "self_healing_pipeline"
            try_number = task_instance.try_number if task_instance else 1
            incident_id = _new_incident_id()

            evidence = {
                "incident_id": incident_id, "timestamp": datetime.now(timezone.utc).isoformat(),
                "dag_id": dag_id, "task_id": task_id, "execution_date": execution_date,
                "try_number": try_number, "failure_category": "DUPLICATE_INGESTION",
                "error_message": exc_msg, "dataset": "orders", "duplicate_count": dup_count,
                "run_id": dag_run.run_id if dag_run else None,
            }

            try:
                adapter = OllamaDiagnosticAdapter()
                diag_report = adapter.analyze_incident(
                    incident_id=incident_id, pipeline_id=dag_id, task_id=task_id, dataset="orders",
                    fault_category="DUPLICATE_INGESTION", observed=f"{len(df_orders)} rows ({dup_count} duplicate primary keys)",
                    expected="300 rows (0 duplicates)", evidence=evidence, execution_date=execution_date,
                )
                diag_report.run_id = evidence.get("run_id")

                gate = PolicyGate(confidence_threshold=0.85)
                gate_action, _ = gate.evaluate(fault_category="DUPLICATE_INGESTION", confidence=diag_report.confidence, evidence=evidence)

                report_dir = os.path.join(data_dir, "incidents", "reports")
                os.makedirs(report_dir, exist_ok=True)

                if gate_action == "AUTO_FIX":
                    executor = RemediationExecutor(data_dir=data_dir)
                    fix_res, verify_res, df_recheck, remediation_error = None, None, None, None
                    try:
                        fix_res = executor.deduplicate_dataset(
                            dataset="orders", primary_key=pk, execution_date=execution_date, backup_id=incident_id,
                        )
                        if fix_res.get("status") == "SUCCESS":
                            verifier = RemediationVerifier(data_dir=data_dir)
                            verify_res = verifier.verify(dataset="orders", primary_key=pk, execution_date=execution_date)
                            if verify_res.get("verified") is True and os.path.exists(orders_stg):
                                df_recheck = pd.read_csv(orders_stg)
                    except Exception as rem_err:
                        remediation_error = f"{type(rem_err).__name__}: {rem_err}"

                    # The incident is REMEDIATED only if all three agree: the
                    # executor reports SUCCESS, the verifier passes, and an
                    # independent re-read of staging shows zero duplicates.
                    fix_status = (fix_res or {}).get("status")
                    verified = verify_res is not None and verify_res.get("verified") is True
                    recheck_clean = df_recheck is not None and not df_recheck[pk].duplicated().any()
                    healed = remediation_error is None and fix_status == "SUCCESS" and verified and recheck_clean

                    remediation_record = dict(fix_res or {"status": "FAILED"})
                    if remediation_error:
                        remediation_record["error"] = remediation_error

                    if healed:
                        diag_report.remediation_status = "SUCCESS"
                        diag_report.verification_status = "PASSED"
                    elif fix_status == "CONFLICT":
                        # Staging was never modified and verification never ran.
                        diag_report.remediation_status = "CONFLICT"
                        diag_report.verification_status = "NOT_RUN"
                        remediation_record["escalation_reason"] = fix_res.get("message")
                    elif remediation_record.get("backup_path") and remediation_record.get("changed"):
                        # This remediation changed staging: put the original back.
                        # restore_backup refuses if staging no longer holds what
                        # the remediation wrote, so later changes are never lost.
                        try:
                            rollback = executor.restore_backup(remediation_record)
                        except Exception as rb_err:
                            rollback = {"restored": False, "sha256_match": False, "backup_state": None,
                                        "backup_path": remediation_record.get("backup_path"),
                                        "error": f"{type(rb_err).__name__}: {rb_err}"}
                        remediation_record["rollback"] = rollback
                        diag_report.remediation_status = "ROLLED_BACK" if rollback["restored"] else "ROLLBACK_FAILED"
                        diag_report.verification_status = "FAILED" if verify_res is not None else "NOT_RUN"
                        remediation_record["escalation_reason"] = (
                            "Remediation was not verified; original staged data restored and checksum confirmed."
                            if rollback["restored"] else _rollback_failure_reason(rollback)
                        )
                    else:
                        # Staging was not changed by this remediation; nothing to roll back.
                        diag_report.remediation_status = "FAILED"
                        diag_report.verification_status = "FAILED" if verify_res is not None else "NOT_RUN"
                        remediation_record["escalation_reason"] = remediation_error or remediation_record.get("error") or remediation_record.get("message")

                    diag_report.status = "REMEDIATED" if healed else "ESCALATED"
                    diag_report.remediation = remediation_record
                    diag_report.verification = verify_res
                    incident_saved = _persist_incident(diag_report, report_dir)

                    # The task only continues once the fix is verified AND recorded.
                    if healed and incident_saved:
                        auto_healed = True
                        df_orders = df_recheck
                        print(f"[SELF-HEALING SUCCESS] {incident_id}: DUPLICATE_INGESTION auto-fixed and verified "
                              f"({fix_res.get('removed_duplicates')} removed, {len(df_orders)} rows, 0 duplicates). Continuing downstream.")
                    elif healed:
                        print(f"[SELF-HEALING UNRECORDED] {incident_id}: fix verified but the incident report could not be "
                              f"saved; failing the task. Original kept at {fix_res.get('backup_path')}.")
                    elif diag_report.remediation_status == "ROLLBACK_FAILED":
                        print(f"[ROLLBACK FAILED] {incident_id}: {remediation_record['escalation_reason']} Manual repair required.")
                    else:
                        print(f"[SELF-HEALING FAILED] {incident_id}: remediation={diag_report.remediation_status} "
                              f"verification={diag_report.verification_status}. {remediation_record.get('escalation_reason') or ''} Escalating.")
                else:
                    diag_report.status = "ESCALATED"
                    _persist_incident(diag_report, report_dir)
            except Exception as heal_err:
                print(f"[SELF-HEALING ERROR] {heal_err}")

            if not auto_healed:
                raise ValueError(exc_msg)

    if df_events is not None:
        pk = config["datasets"]["events"]["primary_key"]
        if df_events[pk].duplicated().any():
            dup_count = df_events[pk].duplicated().sum()
            raise ValueError(f"Data Quality: Duplicate primary keys found in events. Found {dup_count} duplicates for column '{pk}'")

    # 4. Referential checks
    if df_orders is not None:
        if df_cust is not None:
            orphans_cust = df_orders[~df_orders["customer_id"].isin(df_cust["customer_id"])]
            if len(orphans_cust) > 0:
                raise ValueError(f"Data Quality: Referential integrity broken. Found {len(orphans_cust)} orders referencing customer_id missing in customers (Broken Foreign Key)")
        if df_prod is not None:
            orphans_prod = df_orders[~df_orders["product_id"].isin(df_prod["product_id"])]
            if len(orphans_prod) > 0:
                raise ValueError(f"Data Quality: Referential integrity broken. Found {len(orphans_prod)} orders referencing product_id missing in products (Broken Foreign Key)")
    if df_events is not None and df_cust is not None:
        orphans_events = df_events[~df_events["customer_id"].isin(df_cust["customer_id"])]
        if len(orphans_events) > 0:
            raise ValueError(f"Data Quality: Referential integrity broken. Found {len(orphans_events)} events referencing customer_id missing in customers (Broken Foreign Key)")

    # 5. Freshness/SLA checks
    exec_dt = pd.to_datetime(execution_date)
    if df_orders is not None:
        max_ts = pd.to_datetime(df_orders["order_ts"]).max()
        if max_ts < exec_dt:
            raise ValueError(f"Data Quality: Orders Freshness SLA breach. Max timestamp {max_ts} is older than execution date {execution_date}")
    if df_events is not None:
        max_evt_ts = pd.to_datetime(df_events["event_ts"]).max()
        if max_evt_ts < exec_dt:
            raise ValueError(f"Data Quality: Events Freshness SLA breach. Max event timestamp {max_evt_ts} is older than execution date {execution_date}")

    print("Data quality, freshness SLA, and referential integrity validations passed.")
