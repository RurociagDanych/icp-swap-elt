"""Offline flat-Silver contract tests at the real CSV text boundary."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from silver_csv import normalize_csv
from silver_contract import normalize_silver, silver_identity

LINEAGE = {
    "lineage_ref": "fixture:batch:occurrence",
    "ingested_at": "2026-10-30T12:00:00Z",
}
B_HEADER = (
    "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
    "token0_ledger_id,token1_ledger_id,token0_amount_in,token1_amount_in,"
    "token0_amount_out,token1_amount_out"
)
A_HEADER = (
    "hash,timestamp,action,pool_id,from,to,token0_id,token1_id,amount_token0,"
    "amount_token1,token0_change,token1_change,amount_usd"
)


def parse(text: str, dataset: str):
    return normalize_csv(text, dataset, lambda _line, _row: LINEAGE)


def b_line(
    tx_hash: str,
    action: str,
    in0: str = "0",
    in1: str = "0",
    out0: str = "0",
    out1: str = "0",
) -> str:
    return ",".join(
        (
            tx_hash,
            "1793363500000",
            action,
            "pool",
            "",
            "",
            "ledger0",
            "ledger1",
            in0,
            in1,
            out0,
            out1,
        )
    )


class FlatSilverCsvTests(unittest.TestCase):
    def test_public_api_opposite_swaps_keep_source_ledger_pairing_and_exact_magnitudes(
        self,
    ):
        text = "\n".join(
            (
                B_HEADER,
                "public-hash-1,1793363500000,Swap,mohjv-bqaaa-aaaag-qjyia-cai,,,"
                "ryjl3-tyaaa-aaaaa-aaaba-cai,xevnm-gaaaa-aaaar-qafnq-cai,1.125,0,0,2.782385",
                "public-hash-2,1793363222000,Swap,mohjv-bqaaa-aaaag-qjyia-cai,,,"
                "xevnm-gaaaa-aaaar-qafnq-cai,ryjl3-tyaaa-aaaaa-aaaba-cai,56.789648,0,0,22.82478437",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual([], issues)
        self.assertEqual(2, len(records))
        self.assertEqual("ryjl3-tyaaa-aaaaa-aaaba-cai", records[0]["token0_ledger_id"])
        self.assertEqual(Decimal("1.125"), records[0]["token0_amount"])
        self.assertEqual(Decimal("2.782385"), records[0]["token1_amount"])
        self.assertEqual("xevnm-gaaaa-aaaar-qafnq-cai", records[1]["token0_ledger_id"])
        self.assertEqual(Decimal("56.789648"), records[1]["token0_amount"])
        self.assertEqual(Decimal("22.82478437"), records[1]["token1_amount"])
        self.assertNotEqual(
            records[0]["token0_ledger_id"], records[1]["token0_ledger_id"]
        )
        self.assertEqual(
            set(
                (
                    "event_id",
                    "event_timestamp",
                    "action",
                    "pool_id",
                    "token0_ledger_id",
                    "token1_ledger_id",
                    "token0_amount",
                    "token1_amount",
                    "amount_usd",
                    "from_principal",
                    "to_principal",
                    "contract_version",
                    "lineage_ref",
                    "quality_status",
                    "ingested_at",
                )
            ),
            set(records[0]),
        )
        self.assertEqual(
            datetime.fromtimestamp(1793363500, timezone.utc),
            records[0]["event_timestamp"],
        )
        self.assertEqual(
            datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc),
            records[0]["ingested_at"],
        )

    def test_real_claim_add_and_decrease_keep_zero_legs_without_becoming_swaps(self):
        text = "\n".join(
            (
                B_HEADER,
                "claim-hash,1793355948000,Claim,pool,,,ryjl3-tyaaa-aaaaa-aaaba-cai,"
                "xevnm-gaaaa-aaaar-qafnq-cai,0,0,0.01119689,0.031753",
                "add-hash,1793330107000,AddLiquidity,pool,,,ryjl3-tyaaa-aaaaa-aaaba-cai,"
                "xevnm-gaaaa-aaaar-qafnq-cai,0,0.343803,0,0",
                "decrease-hash,1793319908000,DecreaseLiquidity,pool,,,ryjl3-tyaaa-aaaaa-aaaba-cai,"
                "xevnm-gaaaa-aaaar-qafnq-cai,0,0,0,7680.105446",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual([], issues)
        self.assertEqual(
            ["claim", "add_liquidity", "decrease_liquidity"],
            [r["action"] for r in records],
        )
        self.assertEqual(
            (Decimal("0.01119689"), Decimal("0.031753")),
            (records[0]["token0_amount"], records[0]["token1_amount"]),
        )
        self.assertEqual(
            (Decimal("0"), Decimal("0.343803")),
            (records[1]["token0_amount"], records[1]["token1_amount"]),
        )
        self.assertEqual(
            (Decimal("0"), Decimal("7680.105446")),
            (records[2]["token0_amount"], records[2]["token1_amount"]),
        )

    def test_real_a_swap_sample_uses_exact_equal_amount_change_without_rescaling(self):
        # Sanitized Dataset-A forward-swap sample (retained raw sample row 2): both legs are
        # equal-magnitude amounts straight from the source row, with no rescaling applied.
        text = "\n".join(
            (
                A_HEADER,
                "hash,1713229323,Swap,pool-xmiu5,from-a,to-a,mxzaz-hqaaa-aaaar-qaada-cai,"
                "ryjl3-tyaaa-aaaaa-aaaba-cai,0.0010857,9.90461527,0.001085700,9.904615270,",
            )
        )
        records, issues = parse(text, "A")
        self.assertEqual([], issues)
        self.assertEqual(1, len(records))
        self.assertEqual(Decimal("0.0010857"), records[0]["token0_amount"])
        self.assertEqual(Decimal("9.90461527"), records[0]["token1_amount"])
        self.assertEqual("mxzaz-hqaaa-aaaar-qaada-cai", records[0]["token0_ledger_id"])
        self.assertEqual("ryjl3-tyaaa-aaaaa-aaaba-cai", records[0]["token1_ledger_id"])

    def test_synthetic_mixed_flow_and_a_disagreement_are_unresolved_without_abs_or_fallback(
        self,
    ):
        b_text = "\n".join(
            (
                B_HEADER,
                "mixed,1793363500000,AddLiquidity,pool,,,ledger0,ledger1,100,0,99,0",
            )
        )
        b_records, b_issues = parse(b_text, "B")
        self.assertEqual(1, len(b_records))
        self.assertIsNone(b_records[0]["token0_amount"])
        self.assertIn("mixed_in_out", {issue["code"] for issue in b_issues})

        a_text = "\n".join(
            (
                A_HEADER,
                "hash,1713229323,Swap,pool,from-a,to-a,ledger0,ledger1,4,2,3,2,",
            )
        )
        a_records, a_issues = parse(a_text, "A")
        self.assertEqual(1, len(a_records))
        self.assertIsNone(a_records[0]["token0_amount"])
        self.assertIn("amount_change_conflict", {issue["code"] for issue in a_issues})

    def test_synthetic_unknown_action_is_retained_with_issue(self):
        text = "\n".join(
            (
                B_HEADER,
                "unknown-hash,1793363500000,FutureAction,pool,,,ledger0,ledger1,0,0,0,0",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual(1, len(records))
        self.assertEqual("unknown", records[0]["action"])
        self.assertEqual("unresolved", records[0]["quality_status"])
        self.assertIn("unknown_action", {issue["code"] for issue in issues})

    def test_synthetic_exact_decimal_range_and_identity_namespace(self):
        row = {
            "hash": "hash",
            "pool_id": "p",
            "timestamp": "1",
            "from": "a",
            "to": "b",
            "action": "swap",
            "amount_token0": "1.0000000000000000001",
            "amount_token1": "2",
            "token0_change": "1.0000000000000000001",
            "token1_change": "2",
            "amount_usd": "3",
        }
        self.assertEqual(
            silver_identity(row, "A"),
            silver_identity(row | {"amount_token0": "1.000000000000000000100"}, "A"),
        )
        self.assertNotEqual(
            silver_identity({"tx_hash": "same"}, "A"),
            silver_identity({"tx_hash": "same"}, "B"),
        )

    def test_identity_invariants_formerly_tested_on_dedup_key(self):
        # Ported from the removed write_planning.dedup_key tests (L20): the SAME invariants,
        # asserted on the identity production actually uses.
        def a_row(**over):
            row = {
                "hash": "hash", "pool_id": "p1", "timestamp": "1776301323", "from": "alice",
                "to": "pool", "action": "swap", "amount_token0": "5", "amount_token1": "2",
                "token0_change": "5", "token1_change": "2", "amount_usd": "15",
            }
            row.update(over)
            return row

        ident = lambda row, dataset="A": silver_identity(row, dataset)  # noqa: E731
        # B: genuine tx_hash
        self.assertEqual(ident({"tx_hash": "xy-1"}, "B"), ident({"tx_hash": "xy-1"}, "B"))
        self.assertNotEqual(ident({"tx_hash": "xy-1"}, "B"), ident({"tx_hash": "xy-2"}, "B"))
        # A: the constant sentinel "hash" falls back to the composite key
        self.assertNotEqual(ident(a_row()), ident(a_row(timestamp="1776330549")))
        self.assertEqual(ident(a_row()), ident(a_row()))
        # A: a real hash is used when present, distinct real hashes stay distinct
        self.assertNotEqual(ident(a_row(hash="abc-cai7930")), ident(a_row(hash="abc-cai7931")))
        self.assertEqual(
            ident(a_row(hash="abc-cai7930", amount_token0="5")),
            ident(a_row(hash="abc-cai7930", amount_token0="9.9")),
        )
        self.assertNotEqual(ident(a_row(hash="abc-cai1")), ident(a_row()))
        # the sentinel is recognised case/whitespace-insensitively (the normalisation the
        # removed helper lacked)
        self.assertEqual(ident(a_row()), ident(a_row(hash=" HASH ")))
        # every composite field participates (tokenN_change only when amount_tokenN is absent)
        for field, alt in (
            ("pool_id", "p2"), ("from", "bob"), ("to", "other"), ("action", "claim"),
            ("amount_token0", "6"), ("amount_token1", "3"), ("amount_usd", "99"),
        ):
            self.assertNotEqual(ident(a_row()), ident(a_row(**{field: alt})), field)

    def test_csv_adapter_rejects_bad_shape_and_nonfinite_values(self):
        text = "\n".join(
            (
                B_HEADER,
                "bad-shape,1793363500000,Swap,pool,,,ledger0",
                "nonfinite,1793363500000,Swap,pool,,,ledger0,ledger1,NaN,0,0,2",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual(1, len(records))
        self.assertIsNone(records[0]["token0_amount"])
        self.assertEqual("unresolved", records[0]["quality_status"])
        self.assertGreaterEqual(len(issues), 2)
        self.assertTrue(
            {"column_count", "invalid_decimal"} & {issue["code"] for issue in issues}
        )

    def test_synthetic_identity_failures_quarantine_instead_of_using_occurrence_id(
        self,
    ):
        record, issues = normalize_silver({"action_type": "Swap"}, "B", LINEAGE)
        self.assertIsNone(record)
        self.assertEqual("identity_unresolved", issues[0]["code"])
        record, issues = normalize_silver({"hash": "hash"}, "A", LINEAGE)
        self.assertIsNone(record)
        self.assertEqual("identity_unresolved", issues[0]["code"])
        record, issues = normalize_silver(
            {"tx_hash": "valid"}, "B", {"lineage_ref": "x", "ingested_at": "local-time"}
        )
        self.assertIsNone(record)
        self.assertEqual("lineage_unresolved", issues[0]["code"])

    def test_synthetic_decimal_range_overflow_is_rejected_and_sub_quantum_scale_rounds(self):
        # Integral overflow (> 20 integral digits) is still rejected to NULL. A value whose
        # only nonzero digit is beyond 18 fractional digits rounds HALF_EVEN to zero (flagged
        # ``amount_rounded``), which then fails the swap shape like any zero input would.
        text = "\n".join(
            (
                B_HEADER,
                "scale,1793363500000,Swap,pool,,,ledger0,ledger1,0.0000000000000000001,0,0,1",
                "range,1793363500000,Swap,pool,,,ledger0,ledger1,100000000000000000000,0,0,1",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual(2, len(records))
        self.assertTrue(all(record["token0_amount"] is None for record in records))
        self.assertTrue(
            all(record["quality_status"] == "unresolved" for record in records)
        )
        codes = {issue["code"] for issue in issues}
        self.assertTrue({"invalid_amount", "invalid_swap_shape", "amount_rounded"} <= codes)

    def test_float64_amounts_beyond_18_fractional_digits_are_rounded_not_nulled(self):
        # H3: the canister feed serialises float64 amounts; small magnitudes print with more
        # than 18 fractional digits. They must be quantized to DECIMAL(38,18) with
        # ROUND_HALF_EVEN and flagged, never dropped to NULL (which made a swap lose BOTH
        # amounts and its USD value).
        a_row = {
            "hash": "real-hash-1",
            "timestamp": "1780814246",
            "action": "swap",
            "pool_id": "pool",
            "from": "a",
            "to": "b",
            "token0_id": "ledger0",
            "token1_id": "ledger1",
            # 19 and 20 fractional digits (float64 repr of small values)
            "amount_token0": "0.0001115933237495279",
            "token0_change": "0.0001115933237495279",
            "amount_token1": "0.00010937604351238843",
            "token1_change": "0.00010937604351238843",
            "amount_usd": "1.2345678901234567e-7",  # 23 fractional digits
        }
        record, issues = normalize_silver(a_row, "A", LINEAGE)
        self.assertEqual(Decimal("0.000111593323749528"), record["token0_amount"])
        # ...388|43 -> ...388 (the dropped digits are below half)
        self.assertEqual(Decimal("0.000109376043512388"), record["token1_amount"])
        self.assertEqual(Decimal("0.000000123456789012"), record["amount_usd"])
        self.assertEqual("ok", record["quality_status"])
        rounded = [issue for issue in issues if issue["code"] == "amount_rounded"]
        self.assertEqual(
            {"token0_amount", "token1_amount", "amount_usd"},
            {issue["field"] for issue in rounded},
        )
        self.assertFalse(
            [issue for issue in issues if issue["code"] != "amount_rounded"], issues
        )
        # the event identity is computed from the RAW lexeme, so rounding never merges
        # two distinct source rows
        other = dict(a_row, hash="hash", amount_token0="0.0001115933237495280",
                     token0_change="0.0001115933237495280")
        legacy = dict(a_row, hash="hash")
        self.assertNotEqual(silver_identity(legacy, "A"), silver_identity(other, "A"))

    def test_rounding_is_half_even_with_an_explicit_context(self):
        from decimal import localcontext

        from silver_contract import _bounded_amount

        issues: list = []
        # exact ties at the 19th digit: HALF_EVEN goes to the even neighbour
        self.assertEqual(
            Decimal("0.000000000000000002"),
            _bounded_amount("0.0000000000000000025", issues, "f"),
        )
        self.assertEqual(
            Decimal("0.000000000000000004"),
            _bounded_amount("0.0000000000000000035", issues, "f"),
        )
        # an ambient low-precision / other-rounding context must not leak in
        with localcontext() as ctx:
            ctx.prec = 5
            from decimal import ROUND_DOWN

            ctx.rounding = ROUND_DOWN
            self.assertEqual(
                Decimal("12345678901234567890.123456789012345679"),
                _bounded_amount("12345678901234567890.1234567890123456789", issues, "f"),
            )
        self.assertEqual(3, len(issues))
        # rounding that carries into a 21st integral digit is still out of range
        with self.assertRaises(ValueError):
            _bounded_amount("99999999999999999999.9999999999999999999", [], "f")

    def test_b_price_fallback_usd_is_rounded_not_nulled(self):
        header = B_HEADER + ",token0_price"
        text = "\n".join(
            (
                header,
                # 1.125 * 0.123456789012345678 has 21 fractional digits
                "usd,1793363500000,Swap,pool,,,ledger0,ledger1,1.125,0,0,2,"
                "0.123456789012345678",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual(Decimal("0.138888887638888888"), records[0]["amount_usd"])
        self.assertEqual("ok", records[0]["quality_status"])
        self.assertIn("amount_rounded", {issue["code"] for issue in issues})

    def test_csv_duplicate_identity_header_rejects_ambiguous_file(self):
        header = B_HEADER + ",tx_hash"
        text = "\n".join(
            (header, b_line("first-hash", "Swap", "1", "0", "0", "2") + ",second-hash")
        )
        records, issues = parse(text, "B")
        self.assertEqual([], records)
        self.assertIn("duplicate_header", {issue["code"] for issue in issues})

    def test_csv_duplicate_amount_header_rejects_ambiguous_file(self):
        header = B_HEADER + ",token0_amount_in"
        text = "\n".join(
            (
                header,
                b_line("duplicate-amount", "AddLiquidity", "1", "0", "0", "0") + ",999",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual([], records)
        self.assertIn("duplicate_header", {issue["code"] for issue in issues})

    def test_csv_headers_that_collide_after_strip_or_become_blank_reject_file(self):
        cases = (
            (B_HEADER + ", token0_amount_in", "duplicate_header"),
            (B_HEADER + ",   ", "blank_header"),
        )
        for header, expected_issue in cases:
            with self.subTest(header=header[-24:], expected_issue=expected_issue):
                text = "\n".join((header, b_line("bad-header", "AddLiquidity") + ",1"))
                records, issues = parse(text, "B")
                self.assertEqual([], records)
                self.assertIn(expected_issue, {issue["code"] for issue in issues})

    def test_b_action_direction_is_checked_per_slot_despite_invalid_neighbor(self):
        actions = (
            ("AddLiquidity", "out", "in"),
            ("IncreaseLiquidity", "out", "in"),
            ("DecreaseLiquidity", "in", "out"),
            ("Claim", "in", "out"),
        )
        invalid_neighbors = (
            ("", "missing_quantity"),
            ("NaN", "invalid_decimal"),
            ("-1", "invalid_amount"),
            ("not-a-number", "invalid_decimal"),
        )
        for action, forbidden_direction, neighbor_direction in actions:
            for wrong_slot in (0, 1):
                neighbor_slot = 1 - wrong_slot
                for bad_neighbor, neighbor_issue in invalid_neighbors:
                    with self.subTest(
                        action=action, wrong_slot=wrong_slot, neighbor=bad_neighbor
                    ):
                        inputs, outputs = ["0", "0"], ["0", "0"]
                        target = outputs if forbidden_direction == "out" else inputs
                        neighbor = inputs if neighbor_direction == "in" else outputs
                        target[wrong_slot] = "7"
                        neighbor[neighbor_slot] = bad_neighbor
                        text = "\n".join(
                            (
                                B_HEADER,
                                b_line(
                                    f"{action}-{wrong_slot}-{bad_neighbor}",
                                    action,
                                    inputs[0],
                                    inputs[1],
                                    outputs[0],
                                    outputs[1],
                                ),
                            )
                        )
                        records, issues = parse(text, "B")
                        self.assertEqual(1, len(records))
                        self.assertIsNone(records[0][f"token{wrong_slot}_amount"])
                        self.assertIn(
                            "action_shape", {issue["code"] for issue in issues}
                        )
                        self.assertIn(
                            neighbor_issue, {issue["code"] for issue in issues}
                        )

    def test_malformed_quoted_csv_headers_return_structured_issue(self):
        for header in ('"unterminated', '"quoted,header\nrest'):
            with self.subTest(header=header):
                records, issues = parse(header, "B")
                self.assertEqual([], records)
                self.assertIn("malformed_csv", {issue["code"] for issue in issues})

    def test_multiline_bad_rows_keep_logical_occurrence_ordinals_and_physical_diagnostics(
        self,
    ):
        seen = []
        text = "\n".join(
            (
                B_HEADER,
                '"multiline-hash",1793363500000,Swap,pool,"alice\nprincipal",pool,ledger0,ledger1,1,0,0,2',
                "malformed,1793363500000,Swap,pool,,,ledger0",
                b_line("last-hash", "Claim"),
            )
        )

        def lineage(ordinal, row):
            seen.append((ordinal, row["tx_hash"]))
            return LINEAGE | {"lineage_ref": f"fixture:ordinal:{ordinal}"}

        records, issues = normalize_csv(text, "B", lineage)
        self.assertEqual([(1, "multiline-hash"), (3, "last-hash")], seen)
        self.assertEqual(
            ["fixture:ordinal:1", "fixture:ordinal:3"],
            [record["lineage_ref"] for record in records],
        )
        self.assertIn({"line": 4, "code": "column_count"}, issues)

    def test_ingested_at_string_utc_overflow_quarantines_only_that_csv_row(self):
        text = "\n".join(
            (B_HEADER, b_line("bad-ingest", "Claim"), b_line("good-ingest", "Claim"))
        )

        def lineage(ordinal, _row):
            value = (
                "0001-01-01T00:00:00+01:00" if ordinal == 1 else LINEAGE["ingested_at"]
            )
            return {"lineage_ref": f"fixture:{ordinal}", "ingested_at": value}

        records, issues = normalize_csv(text, "B", lineage)
        self.assertEqual(1, len(records))
        self.assertEqual("fixture:2", records[0]["lineage_ref"])
        self.assertIn("lineage_unresolved", {issue["code"] for issue in issues})

    def test_amount_usd_dataset_a_uses_raw_column_directly_and_takes_magnitude(self):
        text = "\n".join(
            (
                A_HEADER,
                "amount-usd-hash-1,1713229323,Swap,pool,from-a,to-a,ledger0,ledger1,"
                "0.0010857,9.90461527,0.0010857,9.90461527,-123.45",
            )
        )
        records, issues = parse(text, "A")
        self.assertEqual([], issues)
        self.assertEqual(1, len(records))
        self.assertEqual(Decimal("123.45"), records[0]["amount_usd"])

    def test_amount_usd_dataset_a_unparsable_raw_value_is_an_issue_not_a_crash(self):
        text = "\n".join(
            (
                A_HEADER,
                "amount-usd-hash-2,1713229323,Swap,pool,from-a,to-a,ledger0,ledger1,"
                "0.0010857,9.90461527,0.0010857,9.90461527,not-a-number",
            )
        )
        records, issues = parse(text, "A")
        self.assertEqual(1, len(records))
        self.assertIsNone(records[0]["amount_usd"])
        self.assertIn(
            "invalid_decimal",
            {issue["code"] for issue in issues if issue["field"] == "amount_usd"},
        )

    def test_amount_usd_derived_for_b_from_tx_value(self):
        header = B_HEADER + ",token0_tx_value,token0_price"
        text = "\n".join(
            (
                header,
                b_line("swap-hash", "Swap", "10", "0", "0", "5") + ",-75.25,1.5",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual([], issues)
        self.assertEqual(1, len(records))
        # tx_value is preferred over the price*amount fallback when both are present.
        self.assertEqual(Decimal("75.25"), records[0]["amount_usd"])

    def test_amount_usd_falls_back_to_price_times_amount_for_b_without_tx_value(self):
        header = B_HEADER + ",token0_price"
        text = "\n".join(
            (
                header,
                b_line("swap-hash", "Swap", "10", "0", "0", "5") + ",2.5",
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual([], issues)
        self.assertEqual(1, len(records))
        self.assertEqual(Decimal("10"), records[0]["token0_amount"])
        self.assertEqual(Decimal("25.0"), records[0]["amount_usd"])

    def test_amount_usd_left_null_without_issue_when_b_has_neither_input(self):
        text = "\n".join(
            (
                B_HEADER,
                b_line("swap-hash", "Swap", "10", "0", "0", "5"),
            )
        )
        records, issues = parse(text, "B")
        self.assertEqual([], issues)
        self.assertEqual(1, len(records))
        self.assertIsNone(records[0]["amount_usd"])

    def test_ingested_at_datetime_utc_overflow_quarantines_only_that_csv_row(self):
        from datetime import datetime, timedelta

        text = "\n".join(
            (
                B_HEADER,
                b_line("bad-datetime", "Claim"),
                b_line("good-datetime", "Claim"),
            )
        )

        def lineage(ordinal, _row):
            value = (
                datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=-1)))
                if ordinal == 1
                else datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)
            )
            return {"lineage_ref": f"fixture:{ordinal}", "ingested_at": value}

        records, issues = normalize_csv(text, "B", lineage)
        self.assertEqual(1, len(records))
        self.assertEqual("fixture:2", records[0]["lineage_ref"])
        self.assertIn("lineage_unresolved", {issue["code"] for issue in issues})


if __name__ == "__main__":
    unittest.main(verbosity=2)
