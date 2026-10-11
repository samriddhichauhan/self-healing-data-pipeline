import os
import hashlib
import pytest
import tempfile
import pandas as pd
from src.remediation.executor import RemediationExecutor
from src.remediation.verifier import RemediationVerifier


def test_deduplicate_orders_and_verify():
    with tempfile.TemporaryDirectory() as tmp_dir:
        stg_dir = os.path.join(tmp_dir, "staging")
        os.makedirs(stg_dir, exist_ok=True)
        with open(os.path.join(stg_dir, "stg_orders_2026-06-01.csv"), "w") as f:
            f.write("order_id,customer_id,order_ts\nORD1,C1,2026-06-01\nORD1,C1,2026-06-01\nORD2,C2,2026-06-01\n")

        executor = RemediationExecutor(data_dir=tmp_dir)
        fix_res = executor.deduplicate_dataset("orders", "order_id", "2026-06-01")
        assert fix_res["status"] == "SUCCESS"
        assert fix_res["removed_duplicates"] == 1
        assert fix_res["cleaned_rows"] == 2

        verifier = RemediationVerifier(data_dir=tmp_dir)
        verify_res = verifier.verify("orders", "order_id", "2026-06-01")
        assert verify_res["verified"] is True
        assert verify_res["status"] == "PASSED"
        assert verify_res["duplicate_count"] == 0


def test_verify_fails_when_duplicates_remain():
    """Verifier must independently re-check — it cannot be fooled by a
    remediation call that ran but did not actually change the file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        stg_dir = os.path.join(tmp_dir, "staging")
        os.makedirs(stg_dir, exist_ok=True)
        with open(os.path.join(stg_dir, "stg_orders_2026-06-01.csv"), "w") as f:
            f.write("order_id,customer_id\nORD1,C1\nORD1,C1\n")

        verifier = RemediationVerifier(data_dir=tmp_dir)
        verify_res = verifier.verify("orders", "order_id", "2026-06-01")
        assert verify_res["verified"] is False
        assert verify_res["status"] == "REMEDIATION_FAILED_ESCALATE"


def test_deduplicate_noop_on_missing_dataset():
    with tempfile.TemporaryDirectory() as tmp_dir:
        executor = RemediationExecutor(data_dir=tmp_dir)
        res = executor.deduplicate_dataset("orders", "order_id", "2026-06-01")
        assert res["status"] == "NOOP"


# --- Verifier regression matrix: every case on both read paths -------------
# (dataset, staged filename, primary key) for the CSV and JSONL paths.
CSV = ("orders", "stg_orders_2026-06-01.csv", "order_id")
JSONL = ("events", "stg_events_2026-06-01.jsonl", "event_id")


def _verify_staged(fmt, content=None):
    """Writes `content` as the staged file (None = no file) and verifies it."""
    dataset, filename, pk = fmt
    with tempfile.TemporaryDirectory() as tmp_dir:
        if content is not None:
            stg_dir = os.path.join(tmp_dir, "staging")
            os.makedirs(stg_dir, exist_ok=True)
            with open(os.path.join(stg_dir, filename), "w") as f:
                f.write(content)
        return RemediationVerifier(data_dir=tmp_dir).verify(dataset, pk, "2026-06-01")


@pytest.mark.parametrize("fmt, content, expected_note", [
    (CSV, None, "Staged file not found"),
    (JSONL, None, "Staged file not found"),
    (CSV, "", "Staged file is empty"),
    (JSONL, "", "Staged file is empty"),
    (CSV, "\n\n  \n", "Staged file is empty"),
    (JSONL, "\n\n  \n", "Staged file is empty"),
    (CSV, 'order_id,"unterminated\nO1\n', "could not be read (ParserError)"),
    (JSONL, '{"event_id": "e1"\n{not json}\n', "could not be read (ValueError)"),
    (CSV, "id,customer_id\nO1,C1\n", "Primary key column 'order_id' missing"),
    (JSONL, '{"id": "e1"}\n', "Primary key column 'event_id' missing"),
    (JSONL, "{}\n{}\n", "Primary key column 'event_id' missing"),
], ids=[
    "csv-missing-file", "jsonl-missing-file", "csv-empty", "jsonl-empty",
    "csv-whitespace", "jsonl-whitespace", "csv-corrupt", "jsonl-corrupt",
    "csv-no-pk-column", "jsonl-no-pk-column", "jsonl-empty-objects",
])
def test_verify_unusable_staged_file_is_not_verified(fmt, content, expected_note):
    """Nothing could be checked: structured NOT_VERIFIED result, never raises."""
    res = _verify_staged(fmt, content)
    assert res["verified"] is False
    assert res["status"] == "NOT_VERIFIED"
    assert res["check"] == "not_verified"
    assert res["dataset"] == fmt[0]
    assert expected_note in res["verification_notes"]


def test_verify_unsupported_dataset_is_not_verified():
    with tempfile.TemporaryDirectory() as tmp_dir:
        res = RemediationVerifier(data_dir=tmp_dir).verify("products", "product_id", "2026-06-01")
    assert res["verified"] is False
    assert res["status"] == "NOT_VERIFIED"
    assert res["verification_notes"] == "Dataset 'products' is not supported by the verifier."


def test_verify_header_only_csv_fails_with_zero_rows():
    """A header-only CSV keeps its columns, so a real check ran and found 0
    rows. (JSONL has no header: 0 rows is an empty file, covered above.)"""
    res = _verify_staged(CSV, "order_id,customer_id\n")
    assert res["verified"] is False
    assert res["status"] == "REMEDIATION_FAILED_ESCALATE"
    assert res["remaining_rows"] == 0
    assert res["duplicate_count"] == 0
    assert res["verification_notes"] == "No rows remain after remediation."


@pytest.mark.parametrize("fmt, content", [
    (CSV, "order_id,customer_id\nO1,C1\nO1,C1\nO1,C1\nO2,C2\n"),
    (JSONL, '{"event_id": "e1"}\n{"event_id": "e1"}\n{"event_id": "e1"}\n{"event_id": "e2"}\n'),
], ids=["csv", "jsonl"])
def test_verify_fails_with_accurate_count_when_duplicates_remain(fmt, content):
    res = _verify_staged(fmt, content)
    assert res["verified"] is False
    assert res["status"] == "REMEDIATION_FAILED_ESCALATE"
    assert res["remaining_rows"] == 4
    assert res["duplicate_count"] == 2
    assert res["verification_notes"] == f"Duplicate '{fmt[2]}' values remain after remediation (2 extra rows)."


@pytest.mark.parametrize("fmt, content, clean_note", [
    (CSV, "order_id,customer_id\nO1,C1\nO2,C2\n", "Zero duplicate primary keys found."),
    (JSONL, '{"event_id": "e1"}\n{"event_id": "e2"}\n', "Zero duplicate event IDs found."),
], ids=["csv", "jsonl"])
def test_verify_passes_only_for_valid_clean_data(fmt, content, clean_note):
    res = _verify_staged(fmt, content)
    assert res["verified"] is True
    assert res["status"] == "PASSED"
    assert res["check"] == "primary_key_deduplication"
    assert res["remaining_rows"] == 2
    assert res["duplicate_count"] == 0
    assert res["verification_notes"] == clean_note


# --- Safe deduplication: backup, conflict refusal, atomic write, rollback ---
from src.remediation import executor as executor_module  # noqa: E402

EXACT_DUPES = {
    "csv": "order_id,customer_id,order_total\nO1,C1,10.0\nO2,C2,20.0\nO1,C1,10.0\nO3,C3,30.0\nO2,C2,20.0\n",
    "jsonl": ('{"event_id":"e1","customer_id":"C1"}\n{"event_id":"e2","customer_id":"C2"}\n'
              '{"event_id":"e1","customer_id":"C1"}\n{"event_id":"e3","customer_id":"C3"}\n'
              '{"event_id":"e2","customer_id":"C2"}\n'),
}
CONFLICTING_DUPES = {  # same key, different value in another column
    "csv": "order_id,customer_id,order_total\nO1,C1,10.0\nO2,C2,20.0\nO1,C1,99.0\n",
    "jsonl": '{"event_id":"e1","customer_id":"C1"}\n{"event_id":"e2","customer_id":"C2"}\n{"event_id":"e1","customer_id":"C9"}\n',
}
FORMATS = {"csv": CSV, "jsonl": JSONL}


@pytest.fixture(params=["csv", "jsonl"])
def staged(request, tmp_path):
    """Returns (fmt_name, executor, staged_path, writer) for one format."""
    fmt_name = request.param
    _, filename, _ = FORMATS[fmt_name]
    stg_dir = tmp_path / "staging"
    stg_dir.mkdir()
    stg_path = stg_dir / filename

    def write(content):
        stg_path.write_text(content)
        return stg_path.read_bytes()
    return fmt_name, RemediationExecutor(data_dir=str(tmp_path)), stg_path, write


def _dedupe(staged_fixture, backup_id="INC-TEST-1"):
    fmt_name, ex, _, _ = staged_fixture
    dataset, _, pk = FORMATS[fmt_name]
    return ex.deduplicate_dataset(dataset, pk, "2026-06-01", backup_id=backup_id)


def _read(fmt_name, path):
    return pd.read_csv(path) if fmt_name == "csv" else pd.read_json(path, lines=True)


def test_exact_duplicates_are_removed_with_verified_backup_and_audit_rows(staged):
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])

    res = _dedupe(staged)

    assert res["status"] == "SUCCESS" and res["changed"] is True
    assert (res["initial_rows"], res["cleaned_rows"], res["removed_duplicates"]) == (5, 3, 2)
    assert not _read(fmt_name, stg_path)[FORMATS[fmt_name][2]].duplicated().any()
    # Backup is byte-identical to the original and lives under the incident id.
    assert os.path.dirname(res["backup_path"]) == os.path.join(ex.backup_root, "INC-TEST-1")
    with open(res["backup_path"], "rb") as f:
        assert f.read() == original
    assert res["original_sha256"] == hashlib.sha256(original).hexdigest()
    assert res["cleaned_sha256"] == hashlib.sha256(stg_path.read_bytes()).hexdigest()
    # Exactly the removed rows are kept for audit.
    assert len(_read(fmt_name, res["removed_rows_path"])) == 2
    # Existing result keys are preserved for all callers.
    for key in ("status", "action_type", "dataset", "initial_rows", "cleaned_rows", "removed_duplicates", "stg_path"):
        assert key in res


def test_conflicting_duplicates_are_refused_and_staging_untouched(staged):
    fmt_name, ex, stg_path, write = staged
    original = write(CONFLICTING_DUPES[fmt_name])

    res = _dedupe(staged)

    assert res["status"] == "CONFLICT" and res["changed"] is False
    assert res["conflicting_key_count"] == 1
    assert res["conflicting_keys"] == ["O1" if fmt_name == "csv" else "e1"]
    assert res["removed_duplicates"] == 0
    assert stg_path.read_bytes() == original
    assert not os.path.exists(ex.backup_root)  # nothing changed, so nothing to back up


def test_repeated_execution_is_idempotent(staged):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    first = _dedupe(staged)
    after_first = stg_path.read_bytes()

    second = _dedupe(staged)

    assert second["status"] == "SUCCESS" and second["changed"] is False
    assert second["removed_duplicates"] == 0 and second["cleaned_rows"] == first["cleaned_rows"]
    assert "backup_path" not in second
    assert stg_path.read_bytes() == after_first


def test_failed_write_leaves_original_intact_and_no_temp_file(staged, monkeypatch):
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])
    writer_name = "_write_csv" if fmt_name == "csv" else "_write_jsonl"
    real_writer = getattr(executor_module, writer_name)

    def partial_write_then_crash(rows, path, *args):
        if ".tmp-" in os.path.basename(path):
            with open(path, "w") as f:
                f.write("half-written")
            raise OSError("disk full (simulated)")
        real_writer(rows, path, *args)
    monkeypatch.setattr(executor_module, writer_name, partial_write_then_crash)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is False
    assert "disk full (simulated)" in res["error"]
    assert stg_path.read_bytes() == original
    assert os.listdir(stg_path.parent) == [stg_path.name]  # temp file cleaned up
    with open(res["backup_path"], "rb") as f:              # verified backup kept
        assert f.read() == original


def test_backup_checksum_mismatch_aborts_before_staging_is_touched(staged, monkeypatch):
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])

    def corrupt_copy(src, dst):
        with open(dst, "wb") as f:
            f.write(b"corrupted backup")
    monkeypatch.setattr(executor_module.shutil, "copy2", corrupt_copy)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is False
    assert "Backup checksum mismatch" in res["error"]
    assert stg_path.read_bytes() == original


def test_restore_backup_puts_original_back_and_confirms_checksum(staged):
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    assert stg_path.read_bytes() != original

    rb = ex.restore_backup(res)

    assert rb["restored"] is True and rb["sha256_match"] is True
    assert rb["restored_sha256"] == res["original_sha256"]
    assert stg_path.read_bytes() == original
    assert os.path.exists(res["backup_path"])  # backup kept after restore


def test_restore_backup_reports_failure_and_keeps_backup(staged):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    cleaned = stg_path.read_bytes()
    with open(res["backup_path"], "ab") as f:
        f.write(b"tampered")

    rb = ex.restore_backup(res)

    assert rb["restored"] is False and rb["sha256_match"] is False
    assert "no longer matches the original checksum" in rb["error"]
    assert rb["backup_path"] == res["backup_path"] and os.path.exists(res["backup_path"])
    assert stg_path.read_bytes() == cleaned  # never overwritten with a bad backup

    missing = ex.restore_backup({"stg_path": str(stg_path)})
    assert missing["restored"] is False and "No verified backup" in missing["error"]


def test_backup_id_cannot_escape_backup_root_or_overwrite_earlier_backup(staged):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    first = _dedupe(staged, backup_id="../../evil")
    write(EXACT_DUPES[fmt_name])
    second = _dedupe(staged, backup_id="../../evil")

    root = os.path.abspath(ex.backup_root)
    for res in (first, second):
        assert os.path.abspath(res["backup_path"]).startswith(root + os.sep)
    assert first["backup_path"] != second["backup_path"]
    assert os.path.exists(first["backup_path"])


def test_callers_without_backup_id_still_get_a_backup(staged):
    """apps/backend/main.py calls deduplicate_dataset(dataset, pk, date)."""
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    dataset, _, pk = FORMATS[fmt_name]

    res = ex.deduplicate_dataset(dataset, pk, "2026-06-01")

    assert res["status"] == "SUCCESS"
    assert os.path.basename(os.path.dirname(res["backup_path"])).startswith("manual-")


# --- Executor review: exact-text comparison, fidelity, crash safety ---------
@pytest.mark.parametrize("eol", ["\n", "\r\n"], ids=["lf", "crlf"])
def test_csv_kept_rows_are_written_back_byte_for_byte(tmp_path, eol):
    """Only the duplicate lines disappear; every kept line (leading zeros,
    float formatting, empty values, line endings) is unchanged."""
    rows = ["order_id,zip,total,note", "O1,00501,10.50,", "O2,02134,1,\"a, b\"", "O3,90210,3.0,NA"]
    original = (eol.join(rows + [rows[1], rows[3]]) + eol).encode()  # O1 and O3 duplicated
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_orders_2026-06-01.csv"
    stg.write_bytes(original)

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("orders", "order_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "SUCCESS" and res["removed_duplicates"] == 2
    assert stg.read_bytes() == (eol.join(rows) + eol).encode()


def test_jsonl_kept_lines_are_written_back_verbatim(tmp_path):
    lines = ['{"event_id": "e1", "n": 1.50, "meta": {"b": 2, "a": 1}}\n',
             '{"event_id":"e2","n":7}\n',
             '{"event_id": "e1", "n": 1.50, "meta": {"b": 2, "a": 1}}\n']  # byte-identical copy of line 1
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_text("".join(lines), newline="")

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "SUCCESS" and res["removed_duplicates"] == 1
    assert stg.read_text() == lines[0] + lines[1]  # kept lines are written back verbatim (1.50 stays 1.50)


def test_jsonl_same_record_with_different_line_ending_is_a_duplicate(tmp_path):
    """Only the line terminator differs, so the record text is identical."""
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_bytes(b'{"event_id":"e1","n":1.50}\r\n{"event_id":"e2"}\n{"event_id":"e1","n":1.50}\n')

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "SUCCESS" and res["removed_duplicates"] == 1
    assert stg.read_bytes() == b'{"event_id":"e1","n":1.50}\r\n{"event_id":"e2"}\n'


@pytest.mark.parametrize("content", [
    b'{"event_id":"e1"}\r\n{"event_id":"e1"}\r\r\n',   # extra CR before the CRLF terminator
    b'{"event_id":"e1"}\r\r\n{"event_id":"e1"}\r\n',   # same, other order
    b'{"event_id":"e1"}\n{"event_id":"e1"}\r',         # last record ends in a bare CR, no LF
], ids=["extra-cr-before-crlf", "extra-cr-first", "bare-cr-at-eof"])
def test_jsonl_trailing_cr_that_is_not_the_terminator_is_record_text(tmp_path, content):
    """Only one LF or CRLF terminator is removed before comparing; any other
    trailing CR belongs to the record, so these rows differ and must conflict."""
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_bytes(content)

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "CONFLICT" and res["changed"] is False
    assert res["conflicting_keys"] == ["e1"]
    assert stg.read_bytes() == content  # byte-for-byte unchanged
    assert not os.path.exists(os.path.join(str(tmp_path), "quarantine"))


@pytest.mark.parametrize("fmt_name, content, conflict_key", [
    ("csv", "order_id,zip\nO1,00501\nO1,501\nO2,123\n", "O1"),            # leading zeros
    ("csv", "order_id,total\nO1,10.50\nO1,10.5\nO2,1\n", "O1"),           # float formatting
    ("csv", "order_id,note\nO1,\nO1,NA\nO2,x\n", "O1"),                   # empty vs literal NA
    ("jsonl", '{"event_id":"e1","code":"007"}\n{"event_id":"e1","code":7}\n', "e1"),  # string vs number
    ("jsonl", '{"event_id":"e1","n":1}\n{"event_id":"e1","n":1.0}\n', "e1"),          # int vs float
    ("jsonl", '{"event_id":"e1","m":{"a":1}}\n{"event_id":"e1","m":{"a":2}}\n', "e1"),  # nested value
    # Same parsed value, different raw text: never resolved silently.
    ("jsonl", '{"event_id":"e1","amt":1.50}\n{"event_id":"e1","amt":1.5}\n', "e1"),
    ("jsonl", '{"event_id":"e1","a":1,"b":2}\n{"b": 2, "a": 1, "event_id": "e1"}\n', "e1"),
    ("jsonl", '{"event_id":"e1","x":1e2}\n{"event_id":"e1","x":100.0}\n', "e1"),
    ("jsonl", '{"event_id":"e1","n":"' + chr(92) + 'u0041"}\n{"event_id":"e1","n":"A"}\n', "e1"),
    ("jsonl", '{"event_id":"e1","x":1,"x":2}\n{"event_id":"e1","x":2}\n', "e1"),
    # Different decimals that collapse to the same float (would lose data if parsed).
    ("jsonl", '{"event_id":"e1","amt":12345678901234567890.5}\n{"event_id":"e1","amt":12345678901234567891.5}\n', "e1"),
], ids=["csv-leading-zeros", "csv-float-format", "csv-empty-vs-NA",
        "jsonl-str-vs-number", "jsonl-int-vs-float", "jsonl-nested",
        "jsonl-1.50-vs-1.5", "jsonl-key-order-and-whitespace", "jsonl-exponent-vs-decimal",
        "jsonl-unicode-escape-vs-literal", "jsonl-repeated-object-key", "jsonl-20-digit-decimals"])
def test_rows_that_differ_as_written_are_conflicts_not_duplicates(tmp_path, fmt_name, content, conflict_key):
    dataset, filename, pk = FORMATS[fmt_name]
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / filename
    stg.write_text(content, newline="")
    original = stg.read_bytes()

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset(dataset, pk, "2026-06-01", backup_id="INC-1")

    assert res["status"] == "CONFLICT" and res["changed"] is False
    assert res["conflicting_keys"] == [conflict_key]
    assert stg.read_bytes() == original
    assert not os.path.exists(os.path.join(str(tmp_path), "quarantine"))  # no backup, nothing written


def test_jsonl_nested_exact_duplicates_are_removed(tmp_path):
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_text('{"event_id":"e1","m":{"a":[1,2]}}\n{"event_id":"e1","m":{"a":[1,2]}}\n{"event_id":"e2","m":null}\n', newline="")

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "SUCCESS" and res["removed_duplicates"] == 1
    assert stg.read_text() == '{"event_id":"e1","m":{"a":[1,2]}}\n{"event_id":"e2","m":null}\n'


@pytest.mark.parametrize("content, expected_error", [
    ('{"event_id":"e1"}\n[1, 2]\n', "Line 2 is not a JSON object"),
    ('{"event_id":"e1"}\n{"id":"e2"}\n', "Primary key 'event_id' missing on line 2"),
    ('{"event_id":"e1"}\n{bad json\n', "Line 2 is not valid JSON"),
    ("\n  \n", "Staged file is empty"),
], ids=["not-an-object", "missing-key-on-a-line", "invalid-json", "blank"])
def test_unusable_jsonl_fails_explicitly_without_touching_staging(tmp_path, content, expected_error):
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_text(content, newline="")
    ex = RemediationExecutor(str(tmp_path))

    res = ex.deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "FAILED" and res["changed"] is False
    assert expected_error in res["error"]
    assert stg.read_text() == content
    assert not os.path.exists(ex.backup_root)


def test_empty_csv_fails_explicitly(tmp_path):
    (tmp_path / "staging").mkdir()
    (tmp_path / "staging" / "stg_orders_2026-06-01.csv").write_text("")
    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("orders", "order_id", "2026-06-01")
    assert res["status"] == "FAILED" and res["changed"] is False
    assert "Staged file is empty" in res["error"]


@pytest.mark.parametrize("content", [
    "order_id,v,v\nO1,a,b\nO1,a,b\nO2,c,d\n",        # repeated column, duplicate rows
    "order_id,v,v\nO1,a,b\nO2,c,d\n",                # repeated column, no duplicate rows
    "order_id,order_id\nO1,O1\nO1,O1\n",              # the key column itself repeated
], ids=["with-duplicate-rows", "without-duplicate-rows", "repeated-key-column"])
def test_csv_with_repeated_column_names_is_refused_before_pandas_renames_them(tmp_path, content):
    """pandas would silently rename the second 'v' to 'v.1' and the cleaned
    file would be written back with a changed header."""
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_orders_2026-06-01.csv"
    stg.write_text(content, newline="")
    original = stg.read_bytes()
    ex = RemediationExecutor(str(tmp_path))

    res = ex.deduplicate_dataset("orders", "order_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "FAILED" and res["changed"] is False
    assert "Duplicate column names in header" in res["error"]
    assert stg.read_bytes() == original
    assert not os.path.exists(ex.backup_root)


@pytest.mark.parametrize("content, expected_error", [
    # One row has an extra field: pandas cannot parse it consistently.
    ("order_id,v\nO1,a\nO1,a\nO2,b,EXTRA\n", "line 4 has 3 fields but the header has 2"),
    # Every row has one extra field: pandas would silently turn the first
    # column into the index and to_csv(index=False) would drop it.
    ("order_id,v\nX1,O1,a\nX1,O1,a\nX2,O2,b\n", "line 2 has 3 fields but the header has 2"),
    # A trailing delimiter on every row: same silent index inference.
    ("order_id,v\nO1,a,\nO1,a,\nO2,b,\n", "line 2 has 3 fields but the header has 2"),
    # A row is missing a field: pandas would pad it with an empty value.
    ("order_id,v,w\nO1,a,b\nO1,a,b\nO2,c\n", "line 4 has 2 fields but the header has 3"),
    # An empty column name: pandas would rename it to 'Unnamed: 1'.
    ("order_id,,v\nO1,a,b\nO1,a,b\nO2,c,d\n", "Empty column name in header (column 2)"),
], ids=["extra-field-one-row", "extra-field-every-row", "trailing-delimiter-every-row",
        "missing-field", "empty-column-name"])
def test_malformed_csv_is_refused_before_pandas_can_repair_it(tmp_path, content, expected_error):
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_orders_2026-06-01.csv"
    stg.write_text(content, newline="")
    original = stg.read_bytes()
    ex = RemediationExecutor(str(tmp_path))

    res = ex.deduplicate_dataset("orders", "order_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "FAILED" and res["changed"] is False
    assert expected_error in res["error"]
    assert "removed_rows_path" not in res and res.get("removed_duplicates", 0) == 0
    assert stg.read_bytes() == original  # byte-for-byte unchanged
    assert not os.path.exists(ex.backup_root)  # nothing backed up, nothing written


@pytest.mark.parametrize("content, kept", [
    ("\norder_id,v\nO1,a\nO1,a\nO2,b\n", "order_id,v\nO1,a\nO2,b\n"),        # leading blank line
    ("order_id,v\nO1,a\n\nO1,a\n  \nO2,b\n", "order_id,v\nO1,a\nO2,b\n"),   # blank / whitespace-only lines
    ("order_id, ,v\nO1,a,b\nO1,a,b\n", "order_id, ,v\nO1,a,b\n"),             # whitespace column name is kept
], ids=["leading-blank-line", "blank-lines-between-rows", "whitespace-column-name"])
def test_wellformed_csv_with_blank_lines_is_still_deduplicated(tmp_path, content, kept):
    """Blank lines are not rows (pandas skips them too), so they are never
    counted as malformed; a column named ' ' is not renamed by pandas."""
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_orders_2026-06-01.csv"
    stg.write_text(content, newline="")

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("orders", "order_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "SUCCESS" and res["removed_duplicates"] == 1
    assert stg.read_text() == kept


def test_crash_after_swap_returns_failed_with_backup_so_caller_can_restore(staged, monkeypatch):
    """A failure after the cleaned file is swapped in must not escape as an
    exception: the result says staging changed and carries the backup."""
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])
    real_sha = executor_module._sha256

    def sha_fails_on_cleaned_file(path):
        if res_holder.get("swapped"):
            raise OSError("checksum read failed (simulated)")
        return real_sha(path)
    real_replace = executor_module.os.replace

    def replace_and_mark(src, dst):
        real_replace(src, dst)
        res_holder["swapped"] = True
    res_holder = {}
    monkeypatch.setattr(executor_module, "_sha256", sha_fails_on_cleaned_file)
    monkeypatch.setattr(executor_module.os, "replace", replace_and_mark)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is True
    assert "Unexpected error during deduplication (OSError)" in res["error"]
    assert res["backup_path"] and res["original_sha256"] == hashlib.sha256(original).hexdigest()
    monkeypatch.undo()
    assert ex.restore_backup(res)["restored"] is True
    assert stg_path.read_bytes() == original


def test_swap_failure_reports_unknown_state_honestly(staged, monkeypatch):
    """If the swap call itself errors, the executor checks the file instead of
    assuming 'not modified'."""
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    real_replace = executor_module.os.replace

    def replace_then_error(src, dst):
        real_replace(src, dst)
        raise OSError("error after swap (simulated)")
    monkeypatch.setattr(executor_module.os, "replace", replace_then_error)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is True
    assert "staging may have been modified; restore from backup" in res["error"]
    assert res["backup_path"]


def test_staging_changed_after_backup_is_not_overwritten(staged, monkeypatch):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    late = "O9,C9,90.0\n" if fmt_name == "csv" else '{"event_id":"e9","customer_id":"C9"}\n'
    real_copy = executor_module.shutil.copy2

    def copy_then_concurrent_write(src, dst):
        out = real_copy(src, dst)
        with open(src, "a", newline="") as f:
            f.write(late)
        return out
    monkeypatch.setattr(executor_module.shutil, "copy2", copy_then_concurrent_write)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is False
    assert res["error"] == "Staging file changed during remediation; it was not overwritten."
    assert late.strip() in stg_path.read_text()
    assert os.listdir(stg_path.parent) == [stg_path.name]  # no temp file left


def test_checksum_is_taken_before_analysis(staged, monkeypatch):
    """If staging changes between the checksum and the read, the backup can't
    match and nothing is modified (the data analysed is the data backed up)."""
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    late = "O9,C9,90.0\n" if fmt_name == "csv" else '{"event_id":"e9","customer_id":"C9"}\n'
    loader_name = "_load_csv" if fmt_name == "csv" else "_load_jsonl"
    real_loader = getattr(executor_module, loader_name)

    def write_then_load(path, pk):
        with open(path, "a", newline="") as f:
            f.write(late)
        return real_loader(path, pk)
    monkeypatch.setattr(executor_module, loader_name, write_then_load)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is False
    assert "Backup checksum mismatch" in res["error"]
    assert late.strip() in stg_path.read_text()


# --- Rollback only overwrites what remediation itself wrote (B2) ---

def test_restore_refuses_when_staging_changed_since_remediation(staged):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    late = "O9,C9,90.0\n" if fmt_name == "csv" else '{"event_id":"e9","customer_id":"C9"}\n'
    with open(stg_path, "a", newline="") as f:
        f.write(late)  # another writer updates staging after the swap
    concurrent = stg_path.read_bytes()

    rb = ex.restore_backup(res)

    assert rb["restored"] is False and rb["sha256_match"] is False
    assert "changed since remediation" in rb["error"]
    assert rb["backup_state"] == "verified"
    assert stg_path.read_bytes() == concurrent  # the other writer's data survives


def test_restore_refuses_without_the_remediation_checksum(staged):
    """A result that doesn't say which bytes remediation wrote can't prove
    staging is still ours, so nothing is overwritten."""
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    cleaned = stg_path.read_bytes()
    res.pop("cleaned_sha256")

    rb = ex.restore_backup(res)

    assert rb["restored"] is False and "changed since remediation" in rb["error"]
    assert stg_path.read_bytes() == cleaned


def test_restore_when_staging_already_holds_the_original_is_a_no_op(staged):
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    stg_path.write_bytes(original)

    rb = ex.restore_backup(res)

    assert rb["restored"] is True and rb["sha256_match"] is True
    assert rb["restored_sha256"] == res["original_sha256"]
    assert stg_path.read_bytes() == original


# --- Cleanup failures never hide the original error (B3) ---

def test_restore_copy_and_cleanup_failure_reports_original_error(staged, monkeypatch):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    cleaned = stg_path.read_bytes()
    real_remove = executor_module.os.remove

    def copy_creates_tmp_then_fails(src, dst):
        open(dst, "wb").close()
        raise OSError("copy failed (simulated)")

    def remove_locked(path):
        if ".restore-" in path:
            raise PermissionError("temp file locked (simulated)")
        real_remove(path)
    monkeypatch.setattr(executor_module.shutil, "copy2", copy_creates_tmp_then_fails)
    monkeypatch.setattr(executor_module.os, "remove", remove_locked)

    rb = ex.restore_backup(res)  # must not raise

    assert rb["restored"] is False
    assert "copy failed (simulated)" in rb["error"]
    assert "temp file locked (simulated)" in rb["error"] and ".restore-" in rb["error"]
    assert rb["backup_state"] == "verified"
    assert stg_path.read_bytes() == cleaned


def test_swap_and_cleanup_failure_keep_the_swap_error(staged, monkeypatch):
    fmt_name, ex, stg_path, write = staged
    original = write(EXACT_DUPES[fmt_name])
    real_remove = executor_module.os.remove

    def replace_locked(src, dst):
        raise PermissionError("staging locked by another process (simulated)")

    def remove_locked(path):
        if ".tmp-" in path:
            raise PermissionError("temp file locked (simulated)")
        real_remove(path)
    monkeypatch.setattr(executor_module.os, "replace", replace_locked)
    monkeypatch.setattr(executor_module.os, "remove", remove_locked)

    res = _dedupe(staged)

    assert res["status"] == "FAILED" and res["changed"] is False
    assert "staging locked by another process (simulated)" in res["error"]
    assert "staging was not modified" in res["error"]
    assert "temp file locked (simulated)" in res["error"]
    assert stg_path.read_bytes() == original


# --- Backup state is reported, never assumed (B4) ---

@pytest.mark.parametrize("damage, expected_state", [
    (lambda p: open(p, "ab").write(b"tampered"), "checksum_mismatch"),
    (os.remove, "missing"),
], ids=["tampered", "deleted"])
def test_restore_reports_backup_state(staged, damage, expected_state):
    fmt_name, ex, stg_path, write = staged
    write(EXACT_DUPES[fmt_name])
    res = _dedupe(staged)
    cleaned = stg_path.read_bytes()
    damage(res["backup_path"])

    rb = ex.restore_backup(res)

    assert rb["restored"] is False and rb["backup_state"] == expected_state
    assert stg_path.read_bytes() == cleaned


# --- JSONL records split only on "\n" (B5) ---

@pytest.mark.parametrize("sep", ["\u2028", "\u2029", "\u0085"], ids=["LS", "PS", "NEL"])
def test_jsonl_unicode_line_separators_inside_strings_are_data(tmp_path, sep):
    lines = [f'{{"event_id":"e1","note":"a{sep}b"}}\n', '{"event_id":"e2","note":"c"}\r\n',
             f'{{"event_id":"e1","note":"a{sep}b"}}\n']
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_bytes("".join(lines).encode("utf-8"))

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "SUCCESS" and res["removed_duplicates"] == 1
    assert stg.read_bytes() == (lines[0] + lines[1]).encode("utf-8")
    assert RemediationVerifier(str(tmp_path)).verify("events", "event_id", "2026-06-01")["verified"] is True


@pytest.mark.parametrize("sep", ["\u2028", "\u2029", "\u0085"], ids=["LS", "PS", "NEL"])
def test_jsonl_unicode_separator_differences_are_conflicts(tmp_path, sep):
    content = f'{{"event_id":"e1","note":"a{sep}b"}}\n{{"event_id":"e1","note":"ab"}}\n'
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_bytes(content.encode("utf-8"))

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "CONFLICT" and res["conflicting_keys"] == ["e1"]
    assert stg.read_bytes() == content.encode("utf-8")


def test_jsonl_line_of_only_a_unicode_separator_is_rejected_not_dropped(tmp_path):
    content = '{"event_id":"e1"}\n\u2028\n{"event_id":"e1"}\n'
    (tmp_path / "staging").mkdir()
    stg = tmp_path / "staging" / "stg_events_2026-06-01.jsonl"
    stg.write_bytes(content.encode("utf-8"))

    res = RemediationExecutor(str(tmp_path)).deduplicate_dataset("events", "event_id", "2026-06-01", backup_id="INC-1")

    assert res["status"] == "FAILED" and "Line 2 is not valid JSON" in res["error"]
    assert stg.read_bytes() == content.encode("utf-8")
