"""
Remediation Executor: executes safe, deterministic, idempotent auto-fix
actions. Never invoked directly by the AI layer — only by the Policy Gate's
AUTO_FIX decision.

Deduplication never loses data: rows are compared as written, with no type
coercion (CSV field by field as text, JSONL by the raw text of each record),
and keys whose rows differ in any way are refused, the original staged
file is backed up (and the backup's checksum verified) before staging is
touched, removed rows are kept for audit, the cleaned file is swapped in
atomically, and restore_backup() can put the original back byte-for-byte.
"""
import os
import re
import csv
import json
import uuid
import shutil
import hashlib
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, Callable, Optional

MAX_REPORTED_CONFLICT_KEYS = 20


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _remove_quietly(path: str) -> str:
    """Best-effort temp-file cleanup that never hides the real failure.
    Returns "" or a note to append to the caller's own error."""
    try:
        if os.path.exists(path):
            os.remove(path)
        return ""
    except Exception as e:
        return f" (temp file {path} could not be removed: {type(e).__name__}: {e})"


def _backup_state(backup_path: str, original_sha: str) -> str:
    """What is actually known about a backup: verified, checksum_mismatch,
    missing or unreadable."""
    if not os.path.exists(backup_path):
        return "missing"
    try:
        return "verified" if _sha256(backup_path) == original_sha else "checksum_mismatch"
    except Exception:
        return "unreadable"


def _safe_dir_name(name: str) -> str:
    """Restricts a backup id to a single, harmless directory name."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", name).lstrip(".")
    return safe or f"backup-{uuid.uuid4().hex[:8]}"


class _UnusableFile(Exception):
    """The staged file cannot be safely deduplicated (empty, no key, ...)."""


class _Staged:
    """One staged file, loaded without type coercion. Per data row: the
    primary key, a fingerprint of the full row as written, and a display
    form of the key. write(mask, path) writes the rows where mask is True."""
    def __init__(self, keys, fingerprints, display_keys, write: Callable):
        self.keys, self.fingerprints, self.display_keys, self.write = keys, fingerprints, display_keys, write


def _write_csv(df: pd.DataFrame, path: str, lineterminator: str = "\n"):
    df.to_csv(path, index=False, lineterminator=lineterminator)


def _write_jsonl(lines, path: str):
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("".join(lines))


def _is_blank_csv_row(row) -> bool:
    """A line pandas skips: empty, or a single whitespace-only field."""
    return not row or (len(row) == 1 and not row[0].strip())


def _check_csv_shape(path: str) -> None:
    """Refuses a CSV file that pandas would silently 'repair' and then write
    back changed: repeated or empty column names (renamed to 'v.1' or
    'Unnamed: N'), and rows whose field count differs from the header (extra
    fields make pandas use the first column as the index, which
    to_csv(index=False) then drops; missing fields are padded with '')."""
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next((row for row in reader if not _is_blank_csv_row(row)), [])
        if len(header) != len(set(header)):
            raise _UnusableFile("Duplicate column names in header")
        for col, name in enumerate(header, 1):
            if name == "":
                raise _UnusableFile(f"Empty column name in header (column {col})")
        for row in reader:
            if not _is_blank_csv_row(row) and len(row) != len(header):
                raise _UnusableFile(f"Row ending on line {reader.line_num} has {len(row)} fields "
                                    f"but the header has {len(header)}")


def _load_csv(path: str, primary_key: str) -> _Staged:
    # Every value is read as the exact text in the file (no numeric/NA
    # parsing), so '00501' != '501' and kept rows keep their values. Kept
    # rows are re-serialized, so quoting and blank lines may be normalized.
    # The file's shape is checked first, so pandas never has to guess.
    _check_csv_shape(path)
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except pd.errors.EmptyDataError:
        raise _UnusableFile("Staged file is empty")
    if primary_key not in df.columns:
        raise _UnusableFile(f"Primary key column '{primary_key}' missing")
    with open(path, "rb") as f:  # keep the file's own line ending (CRLF on Windows-written files)
        eol = "\r\n" if b"\r\n" in f.readline() else "\n"
    keys = df[primary_key].tolist()
    return _Staged(keys, list(df.itertuples(index=False, name=None)), keys,
                   lambda mask, out: _write_csv(df[pd.Series(mask, index=df.index)], out, eol))


def _without_terminator(line: str) -> str:
    """Removes exactly one LF or CRLF line terminator; any other trailing CR
    is part of the record's text."""
    if line.endswith("\r\n"):
        return line[:-2]
    return line[:-1] if line.endswith("\n") else line


def _load_jsonl(path: str, primary_key: str) -> _Staged:
    # Each line is parsed with the json module only to validate it and read
    # its primary key. Rows are compared by their raw text (only the LF or
    # CRLF terminator excluded), so 1.50 vs 1.5, key order, escapes, extra
    # trailing CRs, or decimals that parse to the same float are conflicts,
    # never duplicates; kept lines are written back verbatim. Records end at
    # "\n" only:
    # str.splitlines() would also split on U+2028, U+2029 and U+0085, which
    # are valid inside JSON strings.
    with open(path, encoding="utf-8", newline="\n") as f:
        lines = f.readlines()
    record_lines, keys, fingerprints, display = [], [], [], []
    for n, line in enumerate(lines, 1):
        if not line.strip(" \t\r\n"):  # JSON whitespace only
            continue
        try:
            obj = json.loads(line)
        except ValueError as e:
            raise _UnusableFile(f"Line {n} is not valid JSON: {e}")
        if not isinstance(obj, dict):
            raise _UnusableFile(f"Line {n} is not a JSON object")
        if primary_key not in obj:
            raise _UnusableFile(f"Primary key '{primary_key}' missing on line {n}")
        record_lines.append(line)
        keys.append(json.dumps(obj[primary_key], sort_keys=True))
        fingerprints.append(_without_terminator(line))
        display.append(str(obj[primary_key]))
    if not record_lines:
        raise _UnusableFile("Staged file is empty")
    return _Staged(keys, fingerprints, display,
                   lambda mask, out: _write_jsonl([ln for ln, keep in zip(record_lines, mask) if keep], out))


class RemediationExecutor:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.backup_root = os.path.join(data_dir, "quarantine", "remediation_backups")

    def execute_remediation(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        rem_type = plan.get("remediation_type")
        if rem_type == "deduplicate_dataset":
            return self.deduplicate_dataset(
                dataset=plan.get("dataset", "orders"),
                primary_key=plan.get("primary_key", "order_id"),
                execution_date=plan.get("execution_date", "2026-06-01"),
            )
        elif rem_type == "quarantine_invalid_records":
            return self.quarantine_invalid_records(
                dataset=plan.get("dataset", "orders"),
                condition_column=plan.get("condition_column", "customer_id"),
                execution_date=plan.get("execution_date", "2026-06-01"),
            )
        return {"status": "NOOP", "message": f"Unknown remediation type '{rem_type}'"}

    def deduplicate_dataset(self, dataset: str, primary_key: str, execution_date: str,
                            backup_id: Optional[str] = None) -> Dict[str, Any]:
        """Idempotently deduplicates a staged dataset by primary key.

        Returns status SUCCESS (changed=False if there was nothing to remove),
        CONFLICT (rows share a key but differ in other columns — staging is
        not modified), FAILED, or NOOP (no file / unsupported dataset). Never
        raises. A FAILED result has changed=False unless the failure came
        after the cleaned file was swapped in; whenever a backup was made,
        backup_path and original_sha256 are present (plus cleaned_sha256 once
        the cleaned file is written) so the caller can call restore_backup().
        `backup_id` names the backup directory; the
        self-heal loop passes the incident id."""
        stg_dir = os.path.join(self.data_dir, "staging")
        os.makedirs(stg_dir, exist_ok=True)

        if dataset == "orders":
            stg_path = os.path.join(stg_dir, f"stg_orders_{execution_date}.csv")
            load_fn, ext = _load_csv, "csv"
        elif dataset == "events":
            stg_path = os.path.join(stg_dir, f"stg_events_{execution_date}.jsonl")
            load_fn, ext = _load_jsonl, "jsonl"
        else:
            stg_path = None

        if not stg_path or not os.path.exists(stg_path):
            return {"status": "NOOP", "message": f"Dataset '{dataset}' file not found or unsupported for auto-deduplication."}

        if not backup_id:
            backup_id = f"manual-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        result: Dict[str, Any] = {"action_type": "DEDUPLICATE", "dataset": dataset, "stg_path": stg_path, "changed": False}
        try:
            return self._deduplicate_file(result, stg_path, primary_key, load_fn, ext, backup_id)
        except Exception as e:
            # `result` is updated as work progresses, so a crash at any point
            # still reports whether staging changed and where the backup is.
            return {**result, "status": "FAILED", "error": f"Unexpected error during deduplication ({type(e).__name__}): {e}"}

    def _deduplicate_file(self, result: Dict[str, Any], stg_path: str, primary_key: str,
                          load_fn: Callable, ext: str, backup_id: str) -> Dict[str, Any]:
        def failed(error: str) -> Dict[str, Any]:
            return {**result, "status": "FAILED", "error": error}

        # Checksum first, so the backup is proven to match exactly the bytes
        # that were analysed — not a later version of the file.
        original_sha = _sha256(stg_path)
        try:
            staged = load_fn(stg_path, primary_key)
        except _UnusableFile as e:
            return failed(f"{e} ({stg_path})")
        except Exception as e:
            return failed(f"Staged file could not be read ({type(e).__name__}): {e}")

        # Rows are compared as written (no type coercion): rows sharing a key
        # are exact duplicates only if they are identical as written (CSV:
        # every field's text; JSONL: the record's raw text).
        first_seen: Dict[Any, Any] = {}
        dup_mask, conflict_keys = [], {}
        for key, fingerprint, display_key in zip(staged.keys, staged.fingerprints, staged.display_keys):
            if key in first_seen:
                dup_mask.append(True)
                if first_seen[key] != fingerprint:
                    conflict_keys.setdefault(key, display_key)
            else:
                first_seen[key] = fingerprint
                dup_mask.append(False)

        initial_count = len(dup_mask)
        result.update({"initial_rows": initial_count, "cleaned_rows": initial_count, "removed_duplicates": 0})
        if not any(dup_mask):
            return {**result, "status": "SUCCESS"}

        # Refuse to choose between rows that share a key but disagree on any
        # other column: that is data loss, not deduplication.
        conflict_keys = list(conflict_keys.values())
        if conflict_keys:
            return {
                **result, "status": "CONFLICT",
                "conflicting_key_count": len(conflict_keys),
                "conflicting_keys": [str(k) for k in conflict_keys[:MAX_REPORTED_CONFLICT_KEYS]],
                "message": (f"{len(conflict_keys)} '{primary_key}' values have duplicate rows that differ in other "
                            f"columns; refusing to auto-deduplicate. Staging was not modified."),
            }

        # 1. Back up the original and prove the backup is byte-identical
        #    before staging is touched.
        backup_dir = os.path.join(self.backup_root, _safe_dir_name(backup_id))
        if os.path.exists(os.path.join(backup_dir, os.path.basename(stg_path))):
            backup_dir = f"{backup_dir}-{uuid.uuid4().hex[:8]}"  # never overwrite an earlier backup
        backup_path = os.path.join(backup_dir, os.path.basename(stg_path))
        try:
            os.makedirs(backup_dir, exist_ok=True)
            shutil.copy2(stg_path, backup_path)
            if _sha256(backup_path) != original_sha:
                return failed(f"Backup checksum mismatch at {backup_path}; staging was not modified.")
        except Exception as e:
            return failed(f"Backup failed ({type(e).__name__}): {e}; staging was not modified.")
        result.update({"backup_dir": backup_dir, "backup_path": backup_path, "original_sha256": original_sha})

        # 2. Keep the removed rows for audit, then write the cleaned data to a
        #    temp file in the same directory and swap it in atomically — only
        #    if staging still holds exactly the bytes that were backed up.
        removed_rows_path = os.path.join(backup_dir, f"removed_duplicates.{ext}")
        tmp_path = f"{stg_path}.tmp-{uuid.uuid4().hex[:8]}"
        keep_mask = [not d for d in dup_mask]
        try:
            staged.write(dup_mask, removed_rows_path)
            staged.write(keep_mask, tmp_path)
            # Recorded before the swap, so a later rollback can prove staging
            # still holds exactly what this remediation wrote.
            result["cleaned_sha256"] = _sha256(tmp_path)
            if _sha256(stg_path) != original_sha:
                return failed("Staging file changed during remediation; it was not overwritten." + _remove_quietly(tmp_path))
            os.replace(tmp_path, stg_path)
        except Exception as e:
            cleanup_note = _remove_quietly(tmp_path)
            # Don't assume: check whether staging still holds the original.
            try:
                unchanged = _sha256(stg_path) == original_sha
            except Exception:
                unchanged = False
            result["changed"] = not unchanged
            state = "staging was not modified" if unchanged else "staging may have been modified; restore from backup"
            return failed(f"Writing cleaned data failed ({type(e).__name__}): {e}; {state}.{cleanup_note}")
        result["changed"] = True  # from here on, staging holds the cleaned data

        clean_count = sum(keep_mask)
        result.update({
            "cleaned_rows": clean_count, "removed_duplicates": initial_count - clean_count,
            "removed_rows_path": removed_rows_path,
        })
        if _sha256(stg_path) != result["cleaned_sha256"]:
            return failed("Staging changed right after the cleaned file was swapped in; restore from backup.")
        result["status"] = "SUCCESS"
        return result

    def restore_backup(self, fix_res: Dict[str, Any]) -> Dict[str, Any]:
        """Puts the verified backup back over staging (atomically) and
        confirms the restored file matches the original checksum. Staging is
        only overwritten while it still holds exactly what the remediation
        wrote (cleaned_sha256); any other content is someone else's change and
        is left alone. The backup itself is always kept, and backup_state
        reports what is known about it (verified, checksum_mismatch, missing,
        unreadable)."""
        backup_path = fix_res.get("backup_path")
        stg_path = fix_res.get("stg_path")
        original_sha = fix_res.get("original_sha256")
        cleaned_sha = fix_res.get("cleaned_sha256")
        out: Dict[str, Any] = {"restored": False, "sha256_match": False, "backup_path": backup_path, "backup_state": None}

        if not (backup_path and stg_path and original_sha):
            return {**out, "error": "No verified backup available to restore."}
        out["backup_state"] = _backup_state(backup_path, original_sha)
        if out["backup_state"] != "verified":
            reason = {"missing": "Backup is missing", "unreadable": "Backup could not be read",
                      "checksum_mismatch": "Backup no longer matches the original checksum"}[out["backup_state"]]
            return {**out, "error": f"{reason}; not restoring."}

        not_ours = "Staging changed since remediation (it no longer holds the remediated file); not overwriting it."
        try:
            current_sha = _sha256(stg_path) if os.path.exists(stg_path) else None
        except Exception as e:
            return {**out, "error": f"Staging could not be read ({type(e).__name__}): {e}; not restoring."}
        if current_sha == original_sha:  # already the original: nothing to put back
            return {**out, "restored": True, "sha256_match": True, "restored_sha256": current_sha}
        if not cleaned_sha or current_sha != cleaned_sha:
            return {**out, "error": not_ours}

        tmp_path = f"{stg_path}.restore-{uuid.uuid4().hex[:8]}"
        try:
            shutil.copy2(backup_path, tmp_path)
            if _sha256(stg_path) != cleaned_sha:  # re-check right before the swap
                return {**out, "error": not_ours + _remove_quietly(tmp_path)}
            os.replace(tmp_path, stg_path)
            restored_sha = _sha256(stg_path)
        except Exception as e:
            return {**out, "error": f"Restore failed ({type(e).__name__}): {e}{_remove_quietly(tmp_path)}"}
        match = restored_sha == original_sha
        return {**out, "restored": match, "sha256_match": match, "restored_sha256": restored_sha,
                **({} if match else {"error": "Restored file does not match the original checksum."})}

    def quarantine_invalid_records(self, dataset: str, condition_column: str, execution_date: str) -> Dict[str, Any]:
        """Quarantines invalid rows (e.g. nulls) to data/quarantine/."""
        stg_dir = os.path.join(self.data_dir, "staging")
        quarantine_dir = os.path.join(self.data_dir, "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)

        if dataset == "orders":
            stg_path = os.path.join(stg_dir, f"stg_orders_{execution_date}.csv")
            if os.path.exists(stg_path):
                df = pd.read_csv(stg_path)
                initial_count = len(df)
                bad_mask = df[condition_column].isnull()
                quarantined_df = df[bad_mask]
                clean_df = df[~bad_mask]
                if len(quarantined_df) > 0:
                    quarantine_file = os.path.join(quarantine_dir, f"quarantine_orders_{execution_date}.csv")
                    quarantined_df.to_csv(quarantine_file, index=False)
                    clean_df.to_csv(stg_path, index=False)
                    return {
                        "status": "SUCCESS", "action_type": "QUARANTINE", "dataset": dataset,
                        "initial_rows": initial_count, "quarantined_rows": len(quarantined_df),
                        "cleaned_rows": len(clean_df), "quarantine_file": quarantine_file,
                    }

        return {"status": "NOOP", "message": "No invalid records matched quarantine condition."}
