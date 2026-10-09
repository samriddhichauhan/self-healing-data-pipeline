import os
import tempfile
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
