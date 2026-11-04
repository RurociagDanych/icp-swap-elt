"""The ``backfill_file`` Lambda response must stay small.

The backfill Map state collects one response per object; with the full ``token_dim`` readback
and ledger lists the aggregate would exceed Step Functions' 256 KiB state-output limit
(``States.DataLimitExceeded``). The response therefore carries the verification facts only;
the full detail stays in the structured log line.
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
for name, value in {
    "RAW_BUCKET": "synthetic-raw",
    "PROCESSED_BUCKET": "synthetic-processed",
    "GLUE_DATABASE": "synthetic",
    "TABLE_NAME": "silver",
    "AWS_DEFAULT_REGION": "us-east-1",
}.items():
    os.environ.setdefault(name, value)

import boto3  # noqa: E402

with patch.object(boto3, "client", return_value=MagicMock()):
    import silver_job  # noqa: E402


def _bloated(i: int) -> dict:
    ledgers = [f"ledger-{i}-{n:03d}-aaaaa-bbbbb-ccccc-cai" for n in range(40)]
    return {
        "key": f"icp-swap-elt/canister/2026-06-23T10-25-45-642Z/{i:04d}_pool-aaaaa-aaaag-qdi3a-cai.csv",
        "rows_written": 199_000 + i,
        "rows_observed": 199_226,
        "rows_quarantined": 0,
        "issue_count": 0,
        "replayed": False,
        "quarantine_uri": None,
        "lookup": {
            "snapshot_id": 123456789,
            "files_selected": 1,
            "rows_scanned": 5,
            "existing_found": 5,
        },
        "verify": {"snapshot_id": 123456790, "ids": 199_000, "files_selected": 3, "rows_scanned": 9},
        "seam_reconciliation": {"matched": 3, "dropped": 2},
        "token_dim": {
            "snapshot_id": 987654321,
            "changed_ledgers": ledgers,
            "created_ledgers": ledgers[:5],
            "missing_attributes": {ledger: ["symbol"] for ledger in ledgers},
            "conflicts_resolved": 120,
            "conflicts": [
                {"ledger_id": ledger, "attr": "name", "values": ["a", "b"], "chosen": "b"}
                for ledger in ledgers
            ]
            * 2,
            "bytes_read": 1234,
            "bytes_staged": 4321,
            "readback": {
                ledger: {"symbol": "X", "name": "Y", "standard": "icrc1", "decimals": 8}
                for ledger in ledgers
            },
        },
    }


class BackfillResponseSizeTests(unittest.TestCase):
    def test_72_and_1500_object_aggregates_fit_the_step_functions_limit(self):
        for count in (72, 1500):
            with patch.object(silver_job, "_process_key", side_effect=_bloated_for):
                payloads = [
                    silver_job.handler({"mode": "backfill_file", "key": str(i)})
                    for i in range(count)
                ]
            # Step Functions state output / payload hard limit is 256 KiB (262,144 bytes).
            aggregate = len(json.dumps(payloads).encode())
            full = len(json.dumps([_bloated(i) for i in range(count)]).encode())
            self.assertGreater(full, 256 * 1024, "fixture must reproduce the overflow")
            if count == 72:
                self.assertLess(aggregate, 256 * 1024, f"{count} objects: {aggregate} bytes")
            # even 1500 objects stay at a bounded per-object cost
            self.assertLess(aggregate / count, 1200)

    def test_response_keeps_the_fields_verification_reads(self):
        with patch.object(silver_job, "_process_key", side_effect=_bloated_for):
            slim = silver_job.handler({"mode": "backfill_file", "key": "7"})
        for field in (
            "key",
            "rows_written",
            "rows_observed",
            "rows_quarantined",
            "issue_count",
            "replayed",
            "lookup",
            "verify",
            "seam_reconciliation",
        ):
            self.assertEqual(_bloated(7)[field], slim[field], field)
        dim = slim["token_dim"]
        self.assertEqual(987654321, dim["snapshot_id"])
        self.assertEqual(120, dim["conflicts_resolved"])
        self.assertEqual(40, dim["changed_ledgers"])  # count, not the list
        self.assertLessEqual(len(dim["conflicts"]), 3)
        self.assertNotIn("readback", dim)
        self.assertEqual(40, dim["missing_attributes"])  # ledgers with a gap: count only

    def test_response_without_optional_parts_is_passed_through(self):
        minimal = {"key": "k", "rows_written": 0, "replayed": True, "token_dim": {}}
        with patch.object(silver_job, "_process_key", return_value=dict(minimal)):
            out = silver_job.handler({"mode": "backfill_file", "key": "k"})
        self.assertEqual({}, out["token_dim"])
        issues = {"key": "k", "token_dim": {"issues": [{"code": "x"}] * 500}}
        with patch.object(silver_job, "_process_key", return_value=issues):
            noisy = silver_job.handler({"mode": "backfill_file", "key": "k"})
        self.assertEqual({"issue_count": 500}, noisy["token_dim"])
        self.assertEqual("k", out["key"])
        self.assertTrue(out["replayed"])
        self.assertNotIn("seam_reconciliation", out)


def _bloated_for(key: str, _deadline: float | None = None) -> dict:
    return _bloated(int(key))


if __name__ == "__main__":
    unittest.main()
