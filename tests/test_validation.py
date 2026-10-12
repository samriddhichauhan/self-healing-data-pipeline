import os
import json
import pandas as pd
import pytest
from src.ingestion.ingest import ingest_dimensions, ingest_orders, ingest_events
from src.validation.schema import validate_schema
from src.validation.quality import validate_quality


def _ingest_all(ctx):
    ingest_dimensions()
    ingest_orders(**ctx)
    ingest_events(**ctx)


def test_valid_data_passes_schema_and_quality(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)
    validate_quality(**ctx)  # should not raise


def test_schema_missing_column(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_customers = os.path.join(str(pipeline_env), "raw", "customers.csv")
    df = pd.read_csv(raw_customers).drop(columns=["email"])
    df.to_csv(raw_customers, index=False)

    _ingest_all(ctx)
    with pytest.raises(ValueError, match="Schema Drift: Missing required column 'email'"):
        validate_schema(**ctx)


def test_schema_type_mismatch(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_products = os.path.join(str(pipeline_env), "raw", "products.csv")
    df = pd.read_csv(raw_products)
    df["price"] = "Ten Dollars"
    df.to_csv(raw_products, index=False)

    _ingest_all(ctx)
    with pytest.raises(ValueError, match="non-float values"):
        validate_schema(**ctx)


def test_abnormal_volume_low_rows(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    pd.read_csv(raw_orders).head(10).to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="Abnormal Volume"):
        validate_quality(**ctx)


def test_null_spike_violation(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    df = pd.read_csv(raw_orders)
    df.loc[0:49, "order_total"] = None
    df.to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="NULL Violation"):
        validate_quality(**ctx)


def test_referential_break(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    df = pd.read_csv(raw_orders)
    df.loc[0, "customer_id"] = "CUST999999"
    df.to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="Broken Foreign Key"):
        validate_quality(**ctx)


def test_staleness_sla_breach(pipeline_env):
    ctx = {"ds": "2026-06-01"}
    raw_orders = os.path.join(str(pipeline_env), "raw", "orders", "orders_2026-06-01.csv")
    df = pd.read_csv(raw_orders)
    df["order_ts"] = "2026-05-31 23:59:59"
    df.to_csv(raw_orders, index=False)

    _ingest_all(ctx)
    validate_schema(**ctx)
    with pytest.raises(ValueError, match="Freshness SLA breach"):
        validate_quality(**ctx)


def test_duplicate_ingestion_self_heals_when_ollama_unavailable(pipeline_env, monkeypatch):
    """The DUPLICATE_INGESTION self-heal loop must work end-to-end using the
    deterministic fallback engine even with no Ollama server reachable —
    the project must never depend on an external service to function."""
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")  # nothing listens here
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)

    stg_path = os.path.join(str(pipeline_env), "staging", "stg_orders_2026-06-01.csv")
    df = pd.read_csv(stg_path)
    dupes = df.head(50)
    pd.concat([df, dupes], ignore_index=True).to_csv(stg_path, index=False)

    validate_quality(**ctx)  # must NOT raise — self-heals inline

    healed = pd.read_csv(stg_path)
    assert len(healed) == 300
    assert not healed["order_id"].duplicated().any()

    report_dir = os.path.join(str(pipeline_env), "incidents", "reports")
    reports = os.listdir(report_dir)
    assert any(f.endswith(".json") for f in reports)


def test_events_duplicate_primary_keys_is_not_auto_healed(pipeline_env):
    """Only orders duplicate ingestion is auto-fixable; events duplicates must
    still fail the task (no auto-fix path exists for events)."""
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)

    stg_path = os.path.join(str(pipeline_env), "staging", "stg_events_2026-06-01.jsonl")
    df = pd.read_json(stg_path, lines=True)
    dupes = df.head(10)
    pd.concat([df, dupes], ignore_index=True).to_json(stg_path, orient="records", lines=True)

    with pytest.raises(ValueError, match="Duplicate primary keys found in events"):
        validate_quality(**ctx)


def _inject_order_duplicates(pipeline_env, n=50):
    stg_path = os.path.join(str(pipeline_env), "staging", "stg_orders_2026-06-01.csv")
    df = pd.read_csv(stg_path)
    pd.concat([df, df.head(n)], ignore_index=True).to_csv(stg_path, index=False)
    return stg_path


def _saved_incidents(pipeline_env):
    report_dir = os.path.join(str(pipeline_env), "incidents", "reports")
    incidents = []
    for name in os.listdir(report_dir):
        if name.endswith(".json"):
            with open(os.path.join(report_dir, name)) as f:
                incidents.append(json.load(f))
    return incidents


def test_successful_self_heal_records_remediated_and_passed(pipeline_env, monkeypatch):
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")  # deterministic rule-based path
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)
    _inject_order_duplicates(pipeline_env)

    validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    assert incident["status"] == "REMEDIATED"
    assert incident["remediation_status"] == "SUCCESS"
    assert incident["verification_status"] == "PASSED"
    assert incident["remediation"]["initial_rows"] == 350
    assert incident["remediation"]["cleaned_rows"] == 300
    assert incident["verification"]["duplicate_count"] == 0
    assert incident["verification"]["remaining_rows"] == 300


def test_failed_verification_is_never_recorded_as_remediated(pipeline_env, monkeypatch):
    """An executor that reports SUCCESS but changes nothing must be caught by
    the verifier: the task fails and the incident is ESCALATED."""
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")
    monkeypatch.setattr(
        "src.validation.quality.RemediationExecutor.deduplicate_dataset",
        lambda self, dataset, primary_key, execution_date, backup_id=None: {"status": "SUCCESS", "removed_duplicates": 50},
    )
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)
    _inject_order_duplicates(pipeline_env)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    assert incident["status"] == "ESCALATED"
    assert incident["verification_status"] == "FAILED"
    assert incident["verification"]["status"] == "REMEDIATION_FAILED_ESCALATE"
    assert incident["verification"]["duplicate_count"] == 50


def test_not_verified_result_is_never_recorded_as_remediated(pipeline_env, monkeypatch):
    """Even if the data really is clean, an incident is only REMEDIATED when
    the verifier positively confirms it — NOT_VERIFIED must escalate."""
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")
    monkeypatch.setattr(
        "src.validation.quality.RemediationVerifier.verify",
        lambda self, dataset, primary_key, execution_date: self._not_verified(dataset, "simulated: nothing checked"),
    )
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)
    _inject_order_duplicates(pipeline_env)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    assert incident["status"] == "ESCALATED"
    assert incident["verification_status"] == "FAILED"
    assert incident["verification"]["status"] == "NOT_VERIFIED"


# --- Self-heal loop: backup, conflict refusal and rollback, end to end -----
def _setup_duplicates(pipeline_env, monkeypatch):
    """Healthy ingest + 50 exact duplicate orders (the demo fault). Returns
    (ctx, staged path, original staged bytes)."""
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")
    ctx = {"ds": "2026-06-01"}
    _ingest_all(ctx)
    validate_schema(**ctx)
    stg_path = _inject_order_duplicates(pipeline_env)
    with open(stg_path, "rb") as f:
        return ctx, stg_path, f.read()


def _read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def test_demo_350_to_300_keeps_verified_backup_and_audit_trail(pipeline_env, monkeypatch):
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)

    validate_quality(**ctx)  # must not raise

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("REMEDIATED", "SUCCESS", "PASSED")
    assert (rem["initial_rows"], rem["cleaned_rows"], rem["removed_duplicates"]) == (350, 300, 50)
    healed = pd.read_csv(stg_path)
    assert len(healed) == 300 and not healed["order_id"].duplicated().any()
    expected_dir = os.path.join(str(pipeline_env), "quarantine", "remediation_backups", incident["incident_id"])
    assert os.path.dirname(rem["backup_path"]) == expected_dir
    assert _read_bytes(rem["backup_path"]) == original
    assert len(pd.read_csv(rem["removed_rows_path"])) == 50
    assert "rollback" not in rem


def test_conflicting_order_duplicates_escalate_without_touching_staging(pipeline_env, monkeypatch):
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)
    df = pd.read_csv(stg_path)
    df.loc[len(df) - 1, "order_total"] = df.loc[len(df) - 1, "order_total"] + 1  # one copy now disagrees
    df.to_csv(stg_path, index=False)
    original = _read_bytes(stg_path)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "CONFLICT", "NOT_RUN")
    assert incident["verification"] is None
    assert rem["conflicting_key_count"] == 1
    assert "refusing to auto-deduplicate" in rem["escalation_reason"]
    assert _read_bytes(stg_path) == original
    assert not os.path.exists(os.path.join(str(pipeline_env), "quarantine", "remediation_backups"))


def test_failed_verification_rolls_back_to_original_with_checksum(pipeline_env, monkeypatch):
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)
    monkeypatch.setattr(
        "src.validation.quality.RemediationVerifier.verify",
        lambda self, dataset, primary_key, execution_date: {
            "verified": False, "status": "REMEDIATION_FAILED_ESCALATE", "verification_notes": "simulated failure"},
    )

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "ROLLED_BACK", "FAILED")
    assert rem["rollback"]["restored"] is True and rem["rollback"]["sha256_match"] is True
    assert rem["rollback"]["restored_sha256"] == rem["original_sha256"]
    assert _read_bytes(stg_path) == original  # back to the 350-row original
    assert "original staged data restored" in rem["escalation_reason"]


def test_failed_post_fix_recheck_never_overwrites_a_changed_staging_file(pipeline_env, monkeypatch):
    """Verifier says PASSED but the independent re-read finds duplicates that
    something else wrote into staging after the swap: never REMEDIATED, and
    the rollback refuses to overwrite data it didn't write."""
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)

    def verifier_that_misses_reinjected_duplicates(self, dataset, primary_key, execution_date):
        df = pd.read_csv(stg_path)
        pd.concat([df, df.head(3)], ignore_index=True).to_csv(stg_path, index=False)
        return {"verified": True, "status": "PASSED", "remaining_rows": 300, "duplicate_count": 0}
    monkeypatch.setattr("src.validation.quality.RemediationVerifier.verify", verifier_that_misses_reinjected_duplicates)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "ROLLBACK_FAILED", "FAILED")
    assert "changed since remediation" in rem["rollback"]["error"]
    assert len(pd.read_csv(stg_path)) == 303  # the later write is left in place
    assert _read_bytes(rem["backup_path"]) == original
    assert f"original preserved at {rem['backup_path']} (checksum verified)" in rem["escalation_reason"]


def test_rollback_failure_is_recorded_with_error_and_backup_kept(pipeline_env, monkeypatch):
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)

    def verifier_that_fails_and_damages_backup(self, dataset, primary_key, execution_date):
        backup_dir = os.path.join(str(pipeline_env), "quarantine", "remediation_backups")
        [incident_dir] = os.listdir(backup_dir)
        with open(os.path.join(backup_dir, incident_dir, os.path.basename(stg_path)), "ab") as f:
            f.write(b"tampered")
        return {"verified": False, "status": "REMEDIATION_FAILED_ESCALATE"}
    monkeypatch.setattr("src.validation.quality.RemediationVerifier.verify", verifier_that_fails_and_damages_backup)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"]) == ("ESCALATED", "ROLLBACK_FAILED")
    assert rem["rollback"]["restored"] is False
    assert "no longer matches the original checksum" in rem["rollback"]["error"]
    assert rem["rollback"]["backup_path"] == rem["backup_path"] and os.path.exists(rem["backup_path"])
    assert rem["rollback"]["backup_state"] == "checksum_mismatch"
    assert rem["backup_path"] in rem["escalation_reason"]
    assert "original preserved" not in rem["escalation_reason"]
    assert "failed checksum verification" in rem["escalation_reason"]
    assert _read_bytes(stg_path) != original  # nothing restored from a damaged backup


def test_exception_after_remediation_starts_still_saves_incident_and_rolls_back(pipeline_env, monkeypatch):
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)

    def verifier_crash(self, dataset, primary_key, execution_date):
        raise RuntimeError("verifier crashed (simulated)")
    monkeypatch.setattr("src.validation.quality.RemediationVerifier.verify", verifier_crash)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "ROLLED_BACK", "NOT_RUN")
    assert rem["error"] == "RuntimeError: verifier crashed (simulated)"
    assert rem["rollback"]["restored"] is True
    assert _read_bytes(stg_path) == original


def test_executor_crash_before_any_change_still_saves_incident(pipeline_env, monkeypatch):
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)

    def executor_crash(self, dataset, primary_key, execution_date, backup_id=None):
        raise RuntimeError("executor crashed (simulated)")
    monkeypatch.setattr("src.validation.quality.RemediationExecutor.deduplicate_dataset", executor_crash)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "FAILED", "NOT_RUN")
    assert incident["remediation"]["error"] == "RuntimeError: executor crashed (simulated)"
    assert _read_bytes(stg_path) == original


def test_executor_crash_after_swap_is_rolled_back_by_self_heal_loop(pipeline_env, monkeypatch):
    """The executor never raises; a failure after the swap comes back as
    FAILED + backup, and validate_quality restores the original."""
    from src.remediation import executor as executor_module
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)
    real_sha, real_replace, state = executor_module._sha256, executor_module.os.replace, {}

    def replace_and_mark(src, dst):
        real_replace(src, dst)
        if dst == stg_path:
            state["swapped"] = state.get("swapped", 0) + 1

    def sha_fails_once_after_first_swap(path):
        if state.get("swapped") == 1 and not state.get("failed"):
            state["failed"] = True
            raise OSError("checksum read failed (simulated)")
        return real_sha(path)
    monkeypatch.setattr(executor_module.os, "replace", replace_and_mark)
    monkeypatch.setattr(executor_module, "_sha256", sha_fails_once_after_first_swap)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "ROLLED_BACK", "NOT_RUN")
    assert rem["changed"] is True and "Unexpected error during deduplication" in rem["error"]
    assert rem["rollback"]["restored"] is True
    assert _read_bytes(stg_path) == original


def test_write_failure_during_self_heal_escalates_with_original_intact(pipeline_env, monkeypatch):
    """The executor's cleaned-data write fails: staging keeps the original
    350 rows, the incident is saved as ESCALATED, and the task fails."""
    from src.remediation import executor as executor_module
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)
    real_writer = executor_module._write_csv

    def fail_on_temp_file(rows, path, *args):
        if ".tmp-" in os.path.basename(path):
            raise OSError("disk full (simulated)")
        real_writer(rows, path, *args)
    monkeypatch.setattr(executor_module, "_write_csv", fail_on_temp_file)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "FAILED", "NOT_RUN")
    assert rem["status"] == "FAILED" and rem["changed"] is False
    assert "disk full (simulated)" in rem["error"]
    assert "rollback" not in rem  # staging was never changed, so nothing is rolled back
    assert _read_bytes(stg_path) == original
    assert not [f for f in os.listdir(os.path.dirname(stg_path)) if ".tmp-" in f]  # no temp file left


def test_repeated_validate_quality_after_heal_is_a_no_op(pipeline_env, monkeypatch):
    """Running the quality step again on already-healed data finds nothing to
    fix: it passes, files no new incident, and leaves staging unchanged."""
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)
    validate_quality(**ctx)
    healed = _read_bytes(stg_path)
    [first_incident] = _saved_incidents(pipeline_env)

    validate_quality(**ctx)  # second run must not raise

    assert _read_bytes(stg_path) == healed
    assert [i["incident_id"] for i in _saved_incidents(pipeline_env)] == [first_incident["incident_id"]]
    backups = os.listdir(os.path.join(str(pipeline_env), "quarantine", "remediation_backups"))
    assert backups == [first_incident["incident_id"]]  # no second backup created


# --- Incident persistence is required before the task may pass (B1) ---

def _fail_report_json(module):
    def boom(self, report_dir):
        raise OSError("incident disk full (simulated)")
    return boom


def _fail_report_markdown(module):
    real_open = open

    def flaky_open(path, mode="r", *args, **kwargs):
        if str(path).endswith(".md") and "w" in mode:
            raise OSError("incident disk full (simulated)")
        return real_open(path, mode, *args, **kwargs)
    return flaky_open


@pytest.mark.parametrize("failure", ["save_raises", "markdown_write_fails"])
def test_incident_save_failure_never_lets_the_task_pass(pipeline_env, monkeypatch, capsys, failure):
    """A verified fix whose incident can't be saved must still fail the task,
    and the full incident record must reach the task log and the DB mirror."""
    from src.agent import incident as incident_module
    from src.validation import quality as quality_module
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)
    mirrored = []
    monkeypatch.setattr(quality_module, "_mirror_incident_to_db", lambda report: mirrored.append(report.to_dict()))
    if failure == "save_raises":
        monkeypatch.setattr(incident_module.IncidentReport, "save", _fail_report_json(incident_module))
    else:
        monkeypatch.setattr(incident_module, "open", _fail_report_markdown(incident_module), raising=False)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    out = capsys.readouterr().out
    assert "[SELF-HEALING SUCCESS]" not in out
    assert "[INCIDENT SAVE FAILED]" in out and "incident disk full (simulated)" in out
    logged = json.loads(out.split("[INCIDENT SAVE FAILED]", 1)[1].splitlines()[1])  # record is the next line
    assert (logged["remediation_status"], logged["verification_status"]) == ("SUCCESS", "PASSED")
    assert logged["remediation"]["backup_path"]
    [db_record] = mirrored  # still mirrored to Postgres
    assert db_record["incident_id"] == logged["incident_id"]
    # The verified fix itself is kept; the backup holds the original.
    assert len(pd.read_csv(stg_path)) == 300


# --- Rollback never overwrites changes it didn't make (B2) ---

def test_concurrent_staging_change_is_not_overwritten_by_rollback(pipeline_env, monkeypatch):
    from src.remediation import executor as executor_module
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)
    real_writer, state = executor_module._write_csv, {}

    def write_then_concurrent_ingest(rows, path, *args):
        real_writer(rows, path, *args)
        if ".tmp-" in os.path.basename(path):
            pd.read_csv(stg_path).drop_duplicates("order_id").to_csv(stg_path, index=False)
            state["concurrent"] = _read_bytes(stg_path)
    monkeypatch.setattr(executor_module, "_write_csv", write_then_concurrent_ingest)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"], incident["verification_status"]) == ("ESCALATED", "FAILED", "NOT_RUN")
    assert rem["error"] == "Staging file changed during remediation; it was not overwritten."
    assert "rollback" not in rem
    assert _read_bytes(stg_path) == state["concurrent"]


# --- Rollback failures are always recorded (B3) ---

def test_unexpected_rollback_exception_is_recorded_and_task_fails(pipeline_env, monkeypatch):
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)
    monkeypatch.setattr(
        "src.validation.quality.RemediationVerifier.verify",
        lambda self, dataset, primary_key, execution_date: {"verified": False, "status": "REMEDIATION_FAILED_ESCALATE"},
    )

    def restore_crash(self, fix_res):
        raise RuntimeError("restore crashed (simulated)")
    monkeypatch.setattr("src.validation.quality.RemediationExecutor.restore_backup", restore_crash)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert (incident["status"], incident["remediation_status"]) == ("ESCALATED", "ROLLBACK_FAILED")
    assert rem["rollback"]["restored"] is False
    assert "RuntimeError: restore crashed (simulated)" in rem["rollback"]["error"]
    assert _read_bytes(rem["backup_path"]) == original
    assert "restore crashed (simulated)" in rem["escalation_reason"]


# --- Escalation text matches the real backup state (B4) ---

def test_missing_backup_is_reported_as_missing(pipeline_env, monkeypatch):
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)

    def verifier_that_fails_and_deletes_backup(self, dataset, primary_key, execution_date):
        backup_dir = os.path.join(str(pipeline_env), "quarantine", "remediation_backups")
        [incident_dir] = os.listdir(backup_dir)
        os.remove(os.path.join(backup_dir, incident_dir, os.path.basename(stg_path)))
        return {"verified": False, "status": "REMEDIATION_FAILED_ESCALATE"}
    monkeypatch.setattr("src.validation.quality.RemediationVerifier.verify", verifier_that_fails_and_deletes_backup)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [incident] = _saved_incidents(pipeline_env)
    rem = incident["remediation"]
    assert incident["remediation_status"] == "ROLLBACK_FAILED"
    assert rem["rollback"]["backup_state"] == "missing"
    assert "original preserved" not in rem["escalation_reason"]
    assert f"backup is missing at {rem['backup_path']}" in rem["escalation_reason"]


# --- A failed report save is recorded in the DB mirror and survives a retry (M1) ---

class _FakeIncidentsTable:
    """Stands in for Postgres behind the real record_incident(): rows are
    upserted by incident_id, exactly like its ON CONFLICT clause."""
    def __init__(self):
        self.rows = {}

    def connect(self):
        table = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params):
                table.rows[params[0]] = {"status": params[6], "remediation_status": params[9],
                                         "verification_status": params[10], "payload": json.loads(params[11])}

        class Connection:
            def cursor(self):
                return Cursor()

            def close(self):
                pass
        return Connection()


def _fake_incidents_db(monkeypatch):
    from src.database import loader
    table = _FakeIncidentsTable()
    monkeypatch.setattr(loader, "get_connection", table.connect)
    monkeypatch.setattr(loader, "ensure_schema", lambda conn: None)
    return table


def _failing_save(self, report_dir):
    raise OSError("incident disk full (simulated)")


@pytest.mark.parametrize("verification_passes, expected", [
    (True, ("REMEDIATED", "SUCCESS", "PASSED")),
    (False, ("ESCALATED", "ROLLED_BACK", "FAILED")),
], ids=["healed", "rolled_back"])
def test_report_save_failure_is_recorded_in_db_mirror_with_real_outcome(pipeline_env, monkeypatch, verification_passes, expected):
    from src.agent.incident import IncidentReport
    ctx, stg_path, original = _setup_duplicates(pipeline_env, monkeypatch)
    table = _fake_incidents_db(monkeypatch)
    monkeypatch.setattr(IncidentReport, "save", _failing_save)
    if not verification_passes:
        monkeypatch.setattr(
            "src.validation.quality.RemediationVerifier.verify",
            lambda self, dataset, primary_key, execution_date: {"verified": False, "status": "REMEDIATION_FAILED_ESCALATE"},
        )

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)

    [row] = table.rows.values()
    # The remediation/verification outcome is recorded as it really happened...
    assert (row["status"], row["remediation_status"], row["verification_status"]) == expected
    # ...and, separately, the fact that the incident report could not be saved.
    assert row["payload"]["incident_report_error"] == "OSError: incident disk full (simulated)"
    assert not os.listdir(os.path.join(str(pipeline_env), "incidents", "reports"))
    if not verification_passes:
        assert _read_bytes(stg_path) == original


def test_report_save_failure_stays_visible_after_a_successful_retry(pipeline_env, monkeypatch):
    """Attempt 1 heals the data but can't save its report and fails; the
    Airflow retry sees clean data and passes. The DB row from attempt 1 must
    still say the report is missing."""
    from src.agent.incident import IncidentReport
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)
    table = _fake_incidents_db(monkeypatch)
    real_save = IncidentReport.save
    monkeypatch.setattr(IncidentReport, "save", _failing_save)

    with pytest.raises(ValueError, match="Duplicate primary keys found in orders"):
        validate_quality(**ctx)  # attempt 1
    monkeypatch.setattr(IncidentReport, "save", real_save)
    validate_quality(**ctx)  # retry: nothing left to fix, must not raise

    [(incident_id, row)] = table.rows.items()
    assert row["payload"]["incident_report_error"] == "OSError: incident disk full (simulated)"
    assert (row["status"], row["remediation_status"], row["verification_status"]) == ("REMEDIATED", "SUCCESS", "PASSED")
    assert not os.listdir(os.path.join(str(pipeline_env), "incidents", "reports"))  # retry filed no new report


def test_successful_report_save_has_no_report_error(pipeline_env, monkeypatch):
    ctx, stg_path, _ = _setup_duplicates(pipeline_env, monkeypatch)
    table = _fake_incidents_db(monkeypatch)

    validate_quality(**ctx)

    [row] = table.rows.values()
    assert row["payload"]["incident_report_error"] is None
    [incident] = _saved_incidents(pipeline_env)
    assert incident["incident_report_error"] is None
