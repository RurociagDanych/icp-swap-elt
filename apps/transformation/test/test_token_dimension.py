"""Token metadata extraction and retired-operation safety tests."""

from __future__ import annotations

import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from token_dimension import MetadataError, _unique_candidates, extract_metadata

SOURCE = {
    "bucket": "synthetic",
    "key": "fixture.csv",
    "source_version": "v1",
    "content_sha256": "0" * 64,
}


def test_extracts_each_source_slot_without_inventing_missing_attributes():
    content = b"token0_id,token0_symbol,token0_standard,token0_decimals,token1_id,token1_symbol,token1_standard,token1_decimals\na,A,icrc1,0,b,B,,\n"
    observations, issues = extract_metadata(content, "A", SOURCE, max_records=10)
    assert issues == []
    assert [(item["ledger_id"], item["slot"]) for item in observations] == [
        ("a", 0),
        ("b", 1),
    ]
    assert observations[0]["decimals"] == 0
    assert observations[1]["standard"] is None and observations[1]["decimals"] is None


def test_bad_width_does_not_leak_metadata_and_syntax_keeps_valid_prefix():
    content = b'token0_id,token0_symbol,token0_name\na,A,Name\nb,B\nc,C,"broken\n'
    observations, issues = extract_metadata(content, "B", SOURCE, max_records=10)
    assert [item["ledger_id"] for item in observations] == ["a"], (observations, issues)
    assert [item["slot"] for item in observations] == [0]
    assert [
        (issue["code"], issue.get("logical_record_ordinal"), issue.get("slot"))
        for issue in issues
    ] == [
        ("missing_ledger_id", 1, 1),
        ("column_count", 2, None),
        ("malformed_csv", None, None),
    ]


def test_contradictory_alias_and_bad_decimal_fail_the_batch():
    _raises(
        MetadataError,
        "contradictory",
        extract_metadata,
        b"token0_id,token0_ledger_id,token0_symbol\na,b,A\n",
        "A",
        SOURCE,
        max_records=10,
    )
    _raises(
        MetadataError,
        "decimals",
        extract_metadata,
        b"token0_id,token0_decimals\na,1.5\n",
        "A",
        SOURCE,
        max_records=10,
    )


def test_extract_metadata_default_byte_cap_still_applies_without_explicit_max_bytes():
    """The 16 MiB literal must survive as the DEFAULT for callers
    that don't pass max_bytes - fail-closed behaviour must not silently vanish."""
    oversized = b"token0_id,token0_name\na," + b"x" * (16 * 1024 * 1024) + b"\n"
    _raises(
        MetadataError,
        "source byte cap exceeded",
        extract_metadata,
        oversized,
        "A",
        SOURCE,
        max_records=10,
    )


def test_extract_metadata_accepts_content_above_default_cap_when_max_bytes_permits():
    """A caller that passes an explicit max_bytes (mirroring the max_records keyword)
    must be able to admit content well above the 16 MiB default - real Dataset A
    objects are larger than that."""
    padding = b"x" * (20 * 1024 * 1024)
    content = b"token0_id,token0_name\na," + padding + b"\n"
    assert len(content) > 16 * 1024 * 1024
    previous_limit = csv.field_size_limit()
    csv.field_size_limit(len(content))
    try:
        observations, issues = extract_metadata(
            content, "A", SOURCE, max_records=10, max_bytes=32 * 1024 * 1024
        )
    finally:
        csv.field_size_limit(previous_limit)
    # slot 1 has no token1_id/token1_ledger_id column at all in this minimal fixture,
    # so it durably reports missing_ledger_id - the point of this test is only that
    # the oversized slot-0 observation was reached and extracted, not file rejection.
    assert [issue["code"] for issue in issues] == ["missing_ledger_id"]
    assert [item["ledger_id"] for item in observations] == ["a"]


def test_extract_metadata_still_fails_closed_above_the_passed_max_bytes():
    """Raising the effective cap must not make the path unbounded -
    it must still raise MetadataError, just at the NEW caller-supplied boundary."""
    padding = b"x" * (20 * 1024 * 1024)
    content = b"token0_id,token0_name\na," + padding + b"\n"
    _raises(
        MetadataError,
        "source byte cap exceeded",
        extract_metadata,
        content,
        "A",
        SOURCE,
        max_records=10,
        max_bytes=len(content) - 1,
    )


def _icp_observation(ledger, standard, ordinal):
    return {
        "ledger_id": ledger,
        "symbol": None,
        "name": None,
        "standard": standard,
        "decimals": None,
        "source": SOURCE,
        "logical_record_ordinal": ordinal,
        "slot": 0,
    }


def test_icp_standard_conflict_resolves_to_icp_regardless_of_observation_order():
    """The native ICP ledger canister (ryjl3-tyaaa-aaaaa-aaaba-cai) is recorded with
    standard="ICP" in some rows and "ICRC2" in others within the SAME Dataset-A file (it
    genuinely exposes both a legacy ICP-ledger interface and an ICRC-2 interface). "ICP"
    must win, in either observation order."""
    icp_ledger = "ryjl3-tyaaa-aaaaa-aaaba-cai"
    icp_then_icrc2 = [
        _icp_observation(icp_ledger, "ICP", 1),
        _icp_observation(icp_ledger, "ICRC2", 2),
    ]
    icrc2_then_icp = [
        _icp_observation(icp_ledger, "ICRC2", 1),
        _icp_observation(icp_ledger, "ICP", 2),
    ]
    for observations in (icp_then_icrc2, icrc2_then_icp):
        candidates = _unique_candidates(observations)
        assert candidates[icp_ledger]["standard"] == {"ICP"}, candidates


def test_unrelated_standard_conflict_resolves_deterministically_instead_of_raising():
    """A conflict that does not involve "ICP" (ly36x-wiaaa-aaaai-aqj7q-cai:
    {"ICRC2", "ICRC1"} in the same file) never stops the load. The two observations
    carry no timestamp, so they tie and the greater value wins, in either observation
    order."""
    unrelated_ledger = "ly36x-wiaaa-aaaai-aqj7q-cai"
    for first, second in (("ICRC2", "ICRC1"), ("ICRC1", "ICRC2")):
        observations = [
            _icp_observation(unrelated_ledger, first, 1),
            _icp_observation(unrelated_ledger, second, 2),
        ]
        candidates = _unique_candidates(observations)
        assert candidates[unrelated_ledger]["standard"] == {"ICRC2"}, candidates


def test_unrelated_symbol_conflict_resolves_deterministically_instead_of_raising():
    """Undated contradicting symbols tie and the
    lexicographically greater one wins, whatever the observation order."""
    ledger = "cngnf-vqaaa-aaaar-qag4q-cai"
    for first, second in (("ckUSDT", "DIFFERENT"), ("DIFFERENT", "ckUSDT")):
        observations = [
            {**_icp_observation(ledger, None, 1), "symbol": first},
            {**_icp_observation(ledger, None, 2), "symbol": second},
        ]
        candidates = _unique_candidates(observations)
        assert candidates[ledger]["symbol"] == {"ckUSDT"}, candidates


def _raises(error_type, match, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except error_type as exc:
        assert match in str(exc)
    else:
        raise AssertionError(f"expected {error_type.__name__}")


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
