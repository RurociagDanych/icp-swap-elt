"""Unit tests for the raw-object routing helpers in write_planning (dependency-free).

The legacy ``dedup_key`` / ``idempotent_delete_target`` helpers (source_path-scoped deletes,
a Dataset-A composite key without the case/whitespace normalisation of the real identity)
were dead code: production dedups on ``silver_contract.silver_identity`` (``event_id``). They
were removed; their identity invariants are tested against ``silver_identity`` in
test_silver_contract.py (``test_identity_invariants_formerly_tested_on_dedup_key``).

Runnable without pytest: ``python3 test/test_write_planning.py``.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import write_planning  # noqa: E402
from write_planning import (  # noqa: E402
    incremental_prefix,
    is_backfill_object,
)


def test_dead_legacy_dedup_helpers_are_gone():
    for name in ("dedup_key", "idempotent_delete_target", "DATASET_A_KEY_FIELDS", "LEGACY_NULL_HASH"):
        assert not hasattr(write_planning, name), f"{name} is dead code and must not exist"


def test_incremental_prefix_has_no_trailing_slash():
    p = incremental_prefix("icp-swap-elt/", "2026-06-22")
    assert p == "icp-swap-elt/rest/incremental/2026-06-22", p
    assert not p.endswith("/")


def test_incremental_prefix_matches_run_timestamp_folder():
    """Regression: the daily feed lands in <date>T<time>/ folders; the date must prefix it."""
    p = incremental_prefix("icp-swap-elt/", "2026-06-22")
    key = "icp-swap-elt/rest/incremental/2026-06-22T14-46-53-217Z/transactions_incremental_x.csv"
    assert key.startswith(p), "date prefix does not match the run-timestamp folder"


def test_incremental_prefix_excludes_other_dates():
    p = incremental_prefix("icp-swap-elt/", "2026-06-22")
    other = "icp-swap-elt/rest/incremental/2026-06-23T00-00-00-000Z/x.csv"
    assert not other.startswith(p)


def test_backfill_files_are_excluded_from_daily():
    """The date folder can also hold the bulk backfill drop (a very large file). The
    daily run must skip transactions_backfill_*.csv."""
    folder = "icp-swap-elt/rest/incremental/2026-06-23T11-49-42-924Z/"
    assert is_backfill_object(folder + "transactions_backfill_1782215383463_p002201.csv")
    assert not is_backfill_object(folder + "transactions_incremental_2026-06-23T11-49-42-924Z.csv")


def test_is_backfill_object_only_matches_backfill_basename():
    # path components must not trip it; only the file's basename decides
    assert not is_backfill_object("icp-swap-elt/rest/backfill_archive/transactions_incremental_x.csv")
    assert is_backfill_object("a/b/c/transactions_backfill_0.csv")


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    sys.exit(1 if failures else 0)
