"""token_dim single-``as_of`` semantics.

Real PyIceberg SQLite catalog + file:// warehouse (no mocks of the table layer).

One nullable ``as_of`` column per ledger row = the timestamp of the latest observation applied.
Within an object, per attribute the LATEST observation that supplies it wins; against the
stored row a strictly newer object replaces every attribute it supplies, an equal/older one
only fills NULLs (equal timestamp + different value: the greater value wins, decimals
numerically); ``ICP`` always wins ``standard``; NO attribute conflict raises; every resolved
conflict is reported. The fixtures mirror real contradictions seen in Dataset A (ICP/ICRC2,
ICRC1/ICRC2, DIP20/ICRC2, differing decimals, a renamed symbol).
"""

from __future__ import annotations

import itertools
import os
import sys
import threading
import unittest
import unittest.mock
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import test_token_dim_latest_wins as lw  # noqa: E402
from token_dimension import (  # noqa: E402
    MetadataError,
    _unique_candidates,
    apply_metadata,
    prepare_dim_target,
    read_metadata,
)

IDENT = lw.IDENT
MX = "mxzaz-hqaaa-aaaar-qaada-cai"  # standard ICRC1 vs ICRC2
XE = "xevnm-gaaaa-aaaar-qafnq-cai"  # standard ICRC1 vs ICRC2
PH = "ph45x-riaaa-aaaak-afabq-cai"  # standard DIP20 vs ICRC2
RY = lw.OTHER  # ryjl3-tyaaa-aaaaa-aaaba-cai: standard ICP vs ICRC2
PA = "pa53d-4qaaa-aaaak-afaba-cai"  # decimals 18 vs 12
S = "osxsl-naaaa-aaaah-adzna-cai"  # same-timestamp contradictions
U = "uztta-ziaaa-aaaar-qaczq-cai"
TS = 1702827259  # a real 0001 timestamp


def conf_object(*rows):
    """rows: (ts_seconds, ledger, symbol, standard, decimals) -> A-style object (token0)."""
    lines = [lw.A_HEADER] + [
        f"{ts},{ledger},{symbol},{standard},{decimals},,,," for ts, ledger, symbol, standard, decimals in rows
    ]
    return ("\n".join(lines) + "\n").encode()


def dim_rows(table):
    table.refresh()
    return {r["ledger_id"]: r for r in table.scan().to_arrow().to_pylist()}


def view(table, ledger):
    row = dim_rows(table)[ledger]
    return (row["symbol"], row["name"], row["standard"], row["decimals"], row["as_of"])


class Base(lw.Base):
    def apply(self, table, content, dataset="A"):
        return apply_metadata(table, lw.observe(content, dataset))

    def conflict(self, report, ledger, attr):
        found = [c for c in report["conflicts"] if c["ledger_id"] == ledger and c["attr"] == attr]
        self.assertEqual(1, len(found), report["conflicts"])
        return found[0]


# --------------------------------------------------------------------------- schema (no migration)


class SchemaTests(Base):
    """token_dim is created fresh with exactly six columns; there is NO in-place
    evolution (tables with the 5-column or the 7-column per-attribute-as_of schema are
    refused and must be dropped)."""

    SIX = ["ledger_id", "symbol", "name", "standard", "decimals", "as_of"]

    def test_fresh_table_has_exactly_the_six_columns_with_the_documented_types(self):
        schema = self.fresh_dim().schema().as_arrow()
        self.assertEqual(self.SIX, schema.names)
        self.assertEqual(
            ["string", "string", "string", "string", "int64", "timestamp[us, tz=UTC]"],
            [str(schema.field(n).type).replace("large_string", "string") for n in self.SIX],
        )
        self.assertFalse(schema.field("ledger_id").nullable)
        for name in self.SIX[1:]:
            self.assertTrue(schema.field(name).nullable, name)

    def test_second_prepare_loads_the_existing_table_untouched(self):
        first = self.fresh_dim()
        self.apply(first, conf_object((5, "L6", "SIX", "ICRC1", 8)))
        location = self.catalog.load_table(IDENT).metadata_location
        second = prepare_dim_target(self.catalog, IDENT)
        self.assertEqual(location, second.metadata_location)
        self.assertEqual(self.SIX, second.schema().as_arrow().names)

    def test_concurrent_creation_converges_on_one_table(self):
        barrier = threading.Barrier(4)
        errors, uuids = [], []

        def worker():
            try:
                catalog = self.new_catalog()
                barrier.wait()
                uuids.append(prepare_dim_target(catalog, IDENT).metadata.table_uuid)
            except Exception as exc:  # noqa: BLE001 - report every failure
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual([], errors)
        self.assertEqual(1, len(set(uuids)), uuids)
        self.assertEqual(self.SIX, self.catalog.load_table(IDENT).schema().as_arrow().names)

    def test_read_metadata_reads_the_six_column_table(self):
        table = self.fresh_dim()
        self.apply(table, conf_object((5, "L6", "SIX", "ICRC1", 8)))
        token = read_metadata(table, ["L6", "absent"])["tokens"]
        self.assertEqual(("SIX", "ICRC1", 8), (token[0]["symbol"], token[0]["standard"], token[0]["decimals"]))
        self.assertEqual(["symbol", "name", "standard", "decimals"], token[1]["missing_attributes"])

    def test_every_other_schema_is_refused_and_left_untouched(self):
        base = [
            pa.field("ledger_id", pa.string(), nullable=False),
            pa.field("symbol", pa.string()),
            pa.field("name", pa.string()),
            pa.field("standard", pa.string()),
            pa.field("decimals", pa.int64()),
        ]
        ts, naive = pa.timestamp("us", tz="UTC"), pa.timestamp("us")
        cases = {
            "five_column": base,
            "seven_column": base + [pa.field("symbol_as_of", ts), pa.field("name_as_of", ts)],
            "extra_column": base + [pa.field("as_of", ts), pa.field("extra", pa.string())],
            "only_symbol_as_of": base + [pa.field("symbol_as_of", ts)],
            "as_of_wrong_type": base + [pa.field("as_of", pa.string())],
            "as_of_naive_ts": base + [pa.field("as_of", naive)],
            "decimals_wrong_type": base[:4] + [pa.field("decimals", pa.string()), pa.field("as_of", ts)],
            "missing_column": base[:4] + [pa.field("as_of", ts)],
        }
        for label, fields in cases.items():
            ident = f"synthetic.drift_{label}"
            table = self.catalog.create_table(ident, schema=pa.schema(fields))
            snapshots = [s.snapshot_id for s in table.snapshots()]
            location = table.metadata_location
            for call in (
                lambda: prepare_dim_target(self.catalog, ident),
                lambda: apply_metadata(self.catalog.load_table(ident), lw.observe(lw.a_object((1, lw.LUV, "X")))),
                lambda: read_metadata(self.catalog.load_table(ident), [lw.LUV]),
            ):
                with self.assertRaisesRegex(MetadataError, "incompatible token_dim", msg=label) as raised:
                    call()
            if label in {"five_column", "seven_column", "extra_column", "only_symbol_as_of", "missing_column"}:
                self.assertIn("ledger_id, symbol, name, standard, decimals, as_of", str(raised.exception), label)
            reloaded = self.catalog.load_table(ident)
            self.assertEqual(location, reloaded.metadata_location, label)  # no evolution
            self.assertEqual(snapshots, [s.snapshot_id for s in reloaded.snapshots()], label)


# --------------------------------------------------------------------------- in-object (real 0001)


class RealInObjectConflictTests(Base):
    def test_mxzaz_icrc1_vs_icrc2_latest_observation_wins_and_is_reported(self):
        table = self.fresh_dim()
        report = self.apply(
            table, conf_object((1701487132, MX, "MXZ", "ICRC1", 8), (1702827259, MX, "MXZ", "ICRC2", 8))
        )
        self.assertEqual("ICRC2", view(table, MX)[2])
        self.assertEqual(1, report["conflicts_resolved"])
        self.assertEqual(
            {"ledger_id": MX, "attr": "standard", "values": ["ICRC1", "ICRC2"], "chosen": "ICRC2"},
            self.conflict(report, MX, "standard"),
        )

    def test_there_is_no_standard_precedence_table_only_the_icp_exception(self):
        for ledger, older, newer in ((XE, "ICRC2", "ICRC1"), (PH, "ICRC2", "DIP20")):
            table = self.fresh_dim(f"synthetic.t_{ledger[:5]}")
            self.apply(table, conf_object((10, ledger, "X", older, ""), (20, ledger, "X", newer, "")))
            self.assertEqual(newer, view(table, ledger)[2], ledger)

    def test_ph45x_dip20_vs_icrc2(self):
        table = self.fresh_dim()
        self.apply(table, conf_object((1702716039, PH, "PH", "DIP20", 8), (1702823692, PH, "PH", "ICRC2", 8)))
        self.assertEqual("ICRC2", view(table, PH)[2])

    def test_ryjl3_icp_wins_whichever_observation_is_later(self):
        for n, rows in enumerate(
            [
                [(1702827074, RY, "ICP", "ICRC2", 8), (1702827283, RY, "ICP", "ICP", 8)],
                [(1702827074, RY, "ICP", "ICP", 8), (1702827283, RY, "ICP", "ICRC2", 8)],
            ]
        ):
            table = self.fresh_dim(f"synthetic.ry_{n}")
            report = self.apply(table, conf_object(*rows))
            self.assertEqual("ICP", view(table, RY)[2])
            self.assertEqual("ICP", self.conflict(report, RY, "standard")["chosen"])

    def test_pa53d_decimals_latest_observation_wins_not_the_larger_value(self):
        table = self.fresh_dim()
        rows = [(1699607630, PA, "PA", "ICRC2", 18)] * 3 + [(1702082045, PA, "PA", "ICRC2", 12)]
        report = self.apply(table, conf_object(*rows))
        self.assertEqual(12, view(table, PA)[3])
        self.assertEqual(
            {"ledger_id": PA, "attr": "decimals", "values": [12, 18], "chosen": 12},
            self.conflict(report, PA, "decimals"),
        )

    def test_same_timestamp_symbols_greater_string_wins_in_any_order(self):
        for n, rows in enumerate(itertools.permutations([(500, S, "OhFuck", "", ""), (500, S, "OhFuck2", "", "")])):
            table = self.fresh_dim(f"synthetic.sym_{n}")
            report = self.apply(table, conf_object(*rows))
            self.assertEqual(("OhFuck2", lw.utc(500)), (view(table, S)[0], view(table, S)[4]))
            self.assertEqual(
                {"ledger_id": S, "attr": "symbol", "values": ["OhFuck", "OhFuck2"], "chosen": "OhFuck2"},
                self.conflict(report, S, "symbol"),
            )

    def test_same_timestamp_decimals_compare_numerically(self):
        table = self.fresh_dim()
        self.apply(table, conf_object((7, PA, "", "", 9), (7, PA, "", "", 18)))
        self.assertEqual(18, view(table, PA)[3])  # "9" > "18" as strings, but 18 > 9

    def test_same_timestamp_names_greater_string_wins(self):
        table = self.fresh_dim()
        report = self.apply(table, lw.b_object((500_000, S, "SYM", "Alpha"), (500_000, S, "SYM", "Beta")), "B")
        self.assertEqual("Beta", view(table, S)[1])
        self.assertEqual("Beta", self.conflict(report, S, "name")["chosen"])

    def test_real_renames_6ohjj_and_2qrsq_latest_symbol_wins(self):
        table = self.fresh_dim()
        content = conf_object(
            (lw.LUV_OLD_TS, lw.LUV, lw.LUV_OLD, "ICRC1", 8),
            (lw.CARROT_OLD_TS, lw.CARROT, lw.CARROT_OLD, "ICRC1", 8),
            (lw.LUV_NEW_TS, lw.LUV, lw.LUV_NEW, "ICRC1", 8),
            (lw.CARROT_NEW_TS, lw.CARROT, lw.CARROT_NEW, "ICRC1", 8),
        )
        report = self.apply(table, content)
        self.assertEqual((lw.LUV_NEW, lw.utc(lw.LUV_NEW_TS)), (view(table, lw.LUV)[0], view(table, lw.LUV)[4]))
        self.assertEqual(lw.CARROT_NEW, view(table, lw.CARROT)[0])
        self.assertEqual(2, report["conflicts_resolved"])

    def test_candidate_as_of_is_the_max_timestamp_even_if_it_supplies_no_value(self):
        table = self.fresh_dim()
        self.apply(table, conf_object((10, S, "old", "ICRC1", 8), (30, S, "new", "", ""), (40, S, "", "ICRC2", "")))
        self.assertEqual(("new", None, "ICRC2", 8, lw.utc(40)), view(table, S))

    def test_clean_object_reports_zero_conflicts(self):
        report = self.apply(self.fresh_dim(), conf_object((10, MX, "X", "ICRC2", 8)))
        self.assertEqual(0, report["conflicts_resolved"])
        self.assertEqual([], report["conflicts"])

    def test_undated_values_are_oldest_and_ties_pick_the_greater(self):
        undated = lambda sym: b"token0_id,token0_symbol\n" + S.encode() + b"," + sym + b"\n"  # noqa: E731
        table = self.fresh_dim()
        report = self.apply(table, undated(b"aaa") + undated(b"bbb")[len(b"token0_id,token0_symbol\n"):])
        self.assertEqual(("bbb", None), (view(table, S)[0], view(table, S)[4]))
        self.assertEqual(1, report["conflicts_resolved"])
        self.apply(table, undated(b"aaa"))  # undated vs stored undated: greater wins, no raise
        self.assertEqual("bbb", view(table, S)[0])
        self.apply(table, lw.a_object((100, S, "dated")))  # a dated value outranks undated
        self.apply(table, undated(b"zzz"))  # undated vs stored DATED = older: fills only
        self.assertEqual(("dated", lw.utc(100)), (view(table, S)[0], view(table, S)[4]))

    def test_unique_candidates_keeps_its_shape_and_resolves_instead_of_raising(self):
        candidates = _unique_candidates(lw.observe(conf_object((1, U, "x", "DIP20", 6), (9, U, "x", "ICRC2", 8))))
        self.assertEqual({"ICRC2"}, candidates[U]["standard"])
        self.assertEqual({8}, candidates[U]["decimals"])

    def test_report_list_is_capped_at_50_but_the_count_is_exact(self):
        ledgers = [f"ledger-{i:03d}" for i in range(60)]
        rows = [r for ledger in ledgers for r in ((1, ledger, "", "ICRC1", ""), (2, ledger, "", "ICRC2", ""))]
        report = self.apply(self.fresh_dim(), conf_object(*rows))
        self.assertEqual(60, report["conflicts_resolved"])
        self.assertEqual(ledgers[:50], [c["ledger_id"] for c in report["conflicts"]])

    def test_other_invalid_input_still_fails_closed(self):
        table = self.fresh_dim()
        bad = lw.observe(conf_object((1, MX, "", "ICRC1", "")))
        bad[0]["decimals"] = -1
        with self.assertRaises(MetadataError):
            apply_metadata(table, bad)
        self.assertIsNone(table.current_snapshot())


# --------------------------------------------------------------------------- stored vs incoming


class StoredVersusIncomingTests(Base):
    def seeded(self, ident=IDENT, **values):
        table = self.fresh_dim(ident)
        row = {"ledger_id": MX, "symbol": None, "name": None, "standard": None, "decimals": None, "as_of": None}
        row.update(values)
        table.append(pa.Table.from_pylist([row], schema=table.schema().as_arrow()))
        return table

    def test_newer_object_replaces_what_it_supplies_and_keeps_the_rest(self):
        table = self.seeded(symbol="OLD", name="Name", standard="ICRC1", decimals=6, as_of=lw.utc(100))
        report = self.apply(table, conf_object((200, MX, "NEW", "", 8)))
        self.assertEqual(("NEW", "Name", "ICRC1", 8, lw.utc(200)), view(table, MX))
        self.assertEqual([MX], report["changed_ledgers"])

    def test_older_object_fills_nulls_only_and_never_overwrites(self):
        table = self.seeded(symbol="CUR", standard=None, decimals=8, as_of=lw.utc(200))
        report = self.apply(table, conf_object((100, MX, "OLD", "ICRC2", 6)))
        self.assertEqual(("CUR", None, "ICRC2", 8, lw.utc(200)), view(table, MX))
        self.assertEqual([MX], report["changed_ledgers"])
        snapshot = table.current_snapshot().snapshot_id
        again = self.apply(table, conf_object((100, MX, "OLD", "ICRC2", 6)))
        self.assertEqual([], again["changed_ledgers"])
        self.assertEqual(snapshot, table.current_snapshot().snapshot_id)

    def test_equal_timestamp_greater_value_wins_and_as_of_stays(self):
        table = self.seeded(symbol="aaa", decimals=9, as_of=lw.utc(500))
        self.apply(table, conf_object((500, MX, "bbb", "", 18)))
        self.assertEqual(("bbb", None, None, 18, lw.utc(500)), view(table, MX))
        snapshot = table.current_snapshot().snapshot_id
        loser = self.apply(table, conf_object((500, MX, "aaa", "", 9)))
        self.assertEqual([], loser["changed_ledgers"])
        self.assertEqual(snapshot, table.current_snapshot().snapshot_id)
        self.assertEqual("bbb", self.conflict(loser, MX, "symbol")["chosen"])

    def test_null_stored_as_of_is_oldest(self):
        table = self.seeded(symbol="LEGACY")
        self.apply(table, conf_object((10, MX, "DATED", "", "")))
        self.assertEqual(("DATED", lw.utc(10)), (view(table, MX)[0], view(table, MX)[4]))

    def test_icp_always_wins_standard_regardless_of_timestamps(self):
        stored_newer = self.seeded("synthetic.i1", standard="ICRC2", as_of=lw.utc(900))
        self.apply(stored_newer, conf_object((100, MX, "", "ICP", "")))  # OLDER object, still ICP
        self.assertEqual(("ICP", lw.utc(900)), (view(stored_newer, MX)[2], view(stored_newer, MX)[4]))
        sticky = self.seeded("synthetic.i2", standard="ICP", as_of=lw.utc(100))
        self.apply(sticky, conf_object((900, MX, "NEW", "ICRC2", "")))  # NEWER object, ICP stays
        self.assertEqual(("NEW", "ICP", lw.utc(900)), (view(sticky, MX)[0], view(sticky, MX)[2], view(sticky, MX)[4]))
        self.assertEqual("ICP", self.conflict(self.apply(sticky, conf_object((900, MX, "", "ICRC1", ""))), MX, "standard")["chosen"])

    def test_other_value_types_and_caps_still_fail_closed_against_a_stored_row(self):
        table = self.seeded(decimals=8, as_of=lw.utc(1))
        bad = lw.observe(conf_object((5, MX, "", "", 6)))
        bad[0]["decimals"] = 2**70
        with self.assertRaises(MetadataError):
            apply_metadata(table, bad)


# --------------------------------------------------------------------------- order independence


OBJECTS = {
    "o1": conf_object(
        (1000, MX, "MXA", "ICRC1", 8),
        (1100, MX, "MXA", "ICRC2", 8),
        (1000, PA, "PA", "ICRC2", 18),
        (1050, PA, "PA", "ICRC2", 12),
        (500, S, "tie-a", "ICRC1", 8),
        (500, S, "tie-b", "ICRC1", 8),
        (1100, RY, "ICP", "ICP", 8),
        (1099, RY, "ICP", "ICRC2", 8),
        (lw.CARROT_NEW_TS, lw.CARROT, lw.CARROT_NEW, "ICRC1", 8),
    ),
    "o2": conf_object(
        (2000, PH, "PHA", "DIP20", 8),
        (2100, PH, "PHA", "ICRC2", 8),
        (2000, RY, "ICP", "ICRC2", 8),
        (2000, MX, "MXB", "ICRC1", 8),
        (500, S, "tie-c", "ICRC1", 8),
        (lw.LUV_OLD_TS, lw.LUV, lw.LUV_OLD, "ICRC1", 8),
    ),
    "o3": conf_object(
        (3000, PA, "PA", "ICRC2", 18),
        (1500, PH, "PHOLD", "DIP20", 8),
        (3000, U, "UA", "ZZZ", 6),
        (3000, MX, "MXC", "ICRC2", 8),
        (lw.LUV_NEW_TS, lw.LUV, lw.LUV_NEW, "ICRC1", 8),
    ),
    "o4": conf_object(
        (4000, U, "UB", "AAA", 6),
        (4000, U, "UC", "BBB", 7),
        (400, S, "older", "ICRC1", 8),
        (lw.CARROT_OLD_TS, lw.CARROT, lw.CARROT_OLD, "ICRC1", 8),
        (1500, XE, "XE", "ICRC1", 8),
        (1400, XE, "XE", "ICRC2", 8),
    ),
}
EXPECTED = {
    MX: ("MXC", None, "ICRC2", 8, lw.utc(3000)),
    PH: ("PHA", None, "ICRC2", 8, lw.utc(2100)),
    RY: ("ICP", None, "ICP", 8, lw.utc(2000)),
    PA: ("PA", None, "ICRC2", 18, lw.utc(3000)),
    S: ("tie-c", None, "ICRC1", 8, lw.utc(500)),
    U: ("UC", None, "BBB", 7, lw.utc(4000)),
    lw.LUV: (lw.LUV_NEW, None, "ICRC1", 8, lw.utc(lw.LUV_NEW_TS)),
    lw.CARROT: (lw.CARROT_NEW, None, "ICRC1", 8, lw.utc(lw.CARROT_NEW_TS)),
    XE: ("XE", None, "ICRC1", 8, lw.utc(1500)),
}


class PureDatasetAOrderIndependenceTests(Base):
    def test_every_permutation_of_four_a_objects_gives_the_identical_dim(self):
        finals = {}
        for n, order in enumerate(itertools.permutations(OBJECTS)):
            table = self.fresh_dim(f"synthetic.perm_{n}")
            for name in order:
                self.apply(table, OBJECTS[name])
            finals[order] = {ledger: view(table, ledger) for ledger in dim_rows(table)}
        self.assertEqual(24, len(finals))
        distinct = {tuple(sorted(f.items())) for f in finals.values()}
        self.assertEqual(1, len(distinct))
        self.assertEqual(EXPECTED, next(iter(finals.values())))

    def test_replaying_every_object_afterwards_changes_nothing_and_keeps_the_snapshot(self):
        table = self.fresh_dim()
        for content in OBJECTS.values():
            self.apply(table, content)
        snapshot = table.current_snapshot().snapshot_id
        final = dim_rows(table)
        for name, content in OBJECTS.items():
            report = self.apply(table, content)
            self.assertEqual([], report["changed_ledgers"], name)
            self.assertEqual(snapshot, report["snapshot_id"], name)
            self.assertEqual(snapshot, table.current_snapshot().snapshot_id, name)
        self.assertEqual(final, dim_rows(table))


class DocumentedLimitationTests(Base):
    def test_planned_load_order_all_a_then_b_leaves_the_first_older_b_name(self):
        """Accepted limitation of ONE as_of: B names older than the newest A observation
        can only fill, so with two older B names the FIRST one applied sticks."""
        table = self.fresh_dim()
        self.apply(table, conf_object((100, MX, "A-SYM", "ICRC2", 8)), "A")
        self.apply(table, lw.b_object((50_000, MX, "B-SYM1", "OldName")), "B")
        self.apply(table, lw.b_object((60_000, MX, "B-SYM2", "NewerName")), "B")
        self.assertEqual(("A-SYM", "OldName", "ICRC2", 8, lw.utc(100)), view(table, MX))

    def test_partial_attribute_objects_can_be_order_dependent(self):
        """Same root cause: when the newest object does not supply an attribute, two older
        objects that do supply it fill in arrival order. Real A rows supply all attributes."""
        newest = conf_object((30, MX, "S", "", ""))
        a = conf_object((10, MX, "", "", 8))
        b = conf_object((20, MX, "", "", 6))
        outcomes = {}
        for order in ((newest, a, b), (newest, b, a)):
            table = self.fresh_dim(f"synthetic.lim_{id(order)}")
            for content in order:
                self.apply(table, content)
            outcomes[order[1] is a] = view(table, MX)[3]
        self.assertEqual({True: 8, False: 6}, outcomes)


class PerAttributeTimestampTests(Base):
    """L15: the merge decision is taken per ATTRIBUTE, with the timestamp of the observation
    that supplied it, not with the object's maximum timestamp."""

    def test_older_attribute_in_a_newer_object_never_overwrites_a_newer_value(self):
        table = self.fresh_dim()
        self.apply(table, conf_object((10, MX, "NEW", "ICRC1", 8)))
        # One object: symbol observed at t=5 (older than the stored t=10 symbol), standard
        # observed at t=20. The object's max timestamp is 20; the symbol is still from t=5.
        self.apply(table, conf_object((5, MX, "OLD", "", ""), (20, MX, "", "ICRC2", "")))
        self.assertEqual(("NEW", None, "ICRC2", 8, lw.utc(20)), view(table, MX))

    def test_newer_attribute_still_replaces_and_null_attributes_are_still_filled(self):
        table = self.fresh_dim()
        self.apply(table, conf_object((10, MX, "NEW", "", 8)))
        self.apply(table, conf_object((5, MX, "", "ICRC1", ""), (20, MX, "NEWER", "", "")))
        # standard was NULL -> filled by the older observation; symbol replaced by t=20
        self.assertEqual(("NEWER", None, "ICRC1", 8, lw.utc(20)), view(table, MX))


class CommitFenceTests(Base):
    """L16: the snapshot fence comes from PyIceberg's public overwrite path, not from
    appending to the private ``Transaction._requirements``."""

    def test_no_private_transaction_requirements_access(self):
        source = (
            Path(__file__).resolve().parents[1] / "src" / "token_dimension.py"
        ).read_text()
        self.assertNotIn("._requirements", source)

    def test_commit_is_fenced_on_the_planned_snapshot(self):
        from pyiceberg.table.update import AssertRefSnapshotId

        table = self.fresh_dim()
        self.apply(table, conf_object((10, MX, "A", "ICRC1", 8)))
        planned = self.catalog.load_table(IDENT).current_snapshot().snapshot_id
        captured = []
        original = self.catalog.commit_table

        def capture(tbl, requirements, updates):
            captured.append(requirements)
            return original(tbl, requirements, updates)

        self.catalog.commit_table = capture
        try:
            self.apply(self.catalog.load_table(IDENT), conf_object((20, MX, "B", "ICRC1", 8)))
        finally:
            self.catalog.commit_table = original
        self.assertEqual(1, len(captured))
        fences = [r for r in captured[0] if isinstance(r, AssertRefSnapshotId)]
        self.assertEqual([planned], [r.snapshot_id for r in fences])


class RetryBackoffTests(Base):
    """M6: commit conflicts on token_dim back off exponentially with jitter, bounded."""

    def test_aborted_dimension_commit_leaves_no_orphaned_parquet(self):
        """L19 (token_dim side): a staging abort (here the staged-byte cap) removes the data
        files the aborted transaction wrote."""
        table = self.fresh_dim()
        self.apply(table, conf_object((10, MX, "A", "ICRC1", 8)))
        with self.assertRaisesRegex(MetadataError, "staged byte cap"):
            apply_metadata(
                self.catalog.load_table(IDENT),
                lw.observe(conf_object((20, MX, "B", "ICRC1", 8))),
                limits={"max_staged_bytes": 1},
            )
        current = self.catalog.load_table(IDENT)
        referenced = {
            Path(task.file.file_path.removeprefix("file://")).resolve()
            for task in current.scan().plan_files()
        }
        on_disk = {p.resolve() for p in (self.base / "warehouse").rglob("*.parquet")}
        self.assertEqual(referenced, on_disk)

    def test_conflicts_back_off_between_attempts(self):
        from pyiceberg.exceptions import CommitFailedException

        import token_dimension

        table = self.fresh_dim()
        self.apply(table, conf_object((10, MX, "A", "ICRC1", 8)))
        original = self.catalog.commit_table
        rejected = []
        sleeps = []

        def reject_twice(tbl, requirements, updates):
            if len(rejected) < 2:
                rejected.append(1)
                raise CommitFailedException("injected concurrent writer")
            return original(tbl, requirements, updates)

        self.catalog.commit_table = reject_twice
        try:
            with (
                unittest.mock.patch.object(token_dimension, "_sleep", sleeps.append),
                unittest.mock.patch.object(token_dimension, "_jitter", lambda: 1.0),
            ):
                self.apply(self.catalog.load_table(IDENT), conf_object((20, MX, "B", "ICRC1", 8)))
        finally:
            self.catalog.commit_table = original
        base = token_dimension._RETRY_BASE_SECONDS
        self.assertEqual([base, 2 * base], sleeps)
        self.assertEqual("B", view(table, MX)[0])
        self.assertGreaterEqual(token_dimension.DEFAULT_LIMITS["max_attempts"], 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
