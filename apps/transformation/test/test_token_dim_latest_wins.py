"""token_dim "current value" semantics: latest observed wins (single ``as_of``).

Real PyIceberg SQLite catalog + file:// warehouse (no mocks of the table layer). Covers a
Dataset A object carrying two symbols for one renamed ledger, replay safety, deterministic
(never raising) tie-breaks and the bounded file count. The conflict rules and the schema
checks have their own module, ``test_token_dim_as_of.py``.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from token_dimension import (  # noqa: E402
    DEFAULT_LIMITS,
    MetadataError,
    _unique_candidates,
    apply_metadata,
    extract_metadata,
    prepare_dim_target,
    read_metadata,
)

SOURCE = {
    "bucket": "synthetic",
    "key": "fixture.csv",
    "source_version": "v1",
    "content_sha256": "0" * 64,
}
IDENT = "synthetic.token_dim"

# Two real renamed ledgers, with their real timestamps.
LUV = "6ohjj-4qaaa-aaaai-aapua-cai"
LUV_OLD, LUV_OLD_TS = "01", 1683886069  # 2023-05-12
LUV_NEW, LUV_NEW_TS = "LUV", 1695313142  # 2023-09-21
CARROT = "2qrsq-uiaaa-aaaai-aa3zq-cai"
CARROT_OLD, CARROT_OLD_TS = "\U0001f955", 1683832425  # 2023-05-11
CARROT_NEW, CARROT_NEW_TS = "\U0001f430\U0001f955", 1695887064  # 2023-09-28
OTHER = "ryjl3-tyaaa-aaaaa-aaaba-cai"

A_HEADER = (
    "timestamp,token0_id,token0_symbol,token0_standard,token0_decimals,"
    "token1_id,token1_symbol,token1_standard,token1_decimals"
)
B_HEADER = (
    "tx_time,token0_ledger_id,token0_symbol,token0_name,"
    "token1_ledger_id,token1_symbol,token1_name"
)


def utc(seconds):
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def a_object(*rows):
    """rows: (ts_seconds, ledger, symbol) -> an A-style object (token0 only)."""
    lines = [A_HEADER] + [f"{ts},{ledger},{sym},,,,,," for ts, ledger, sym in rows]
    return ("\n".join(lines) + "\n").encode()


def b_object(*rows):
    """rows: (ts_ms, ledger, symbol, name) -> a B-style object (token0 only)."""
    lines = [B_HEADER] + [f"{ts},{ledger},{sym},{name},,," for ts, ledger, sym, name in rows]
    return ("\n".join(lines) + "\n").encode()


def observe(content, dataset="A"):
    observations, _issues = extract_metadata(content, dataset, SOURCE, max_records=1000)
    return observations


def labels(table):
    """Final dim as a comparable, order-free structure:
    (ledger_id, symbol, name, standard, decimals, as_of)."""
    table.refresh()
    rows = table.scan().to_arrow().to_pylist()
    return sorted(
        (
            (r["ledger_id"], r["symbol"], r["name"], r["standard"], r["decimals"], r.get("as_of"))
            for r in rows
        ),
        key=lambda t: t[0],
    )


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-token-dim-")
        self.base = Path(self.tmp.name)
        self.catalog = self.new_catalog()
        self.catalog.create_namespace("synthetic")

    def tearDown(self):
        self.tmp.cleanup()

    def new_catalog(self, name="token_dim"):
        return SqlCatalog(
            name,
            uri=f"sqlite:///{self.base / 'catalog.sqlite'}",
            warehouse=(self.base / "warehouse").as_uri(),
        )

    def fresh_dim(self, ident=IDENT):
        return prepare_dim_target(self.catalog, ident)

    def legacy_dim(self, rows=None, ident=IDENT):
        """The ORIGINAL 5-column token_dim (deployed before any as-of column existed)."""
        schema = pa.schema(
            [
                pa.field("ledger_id", pa.string(), nullable=False),
                pa.field("symbol", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
                pa.field("standard", pa.string(), nullable=True),
                pa.field("decimals", pa.int64(), nullable=True),
            ]
        )
        table = self.catalog.create_table(ident, schema=schema)
        rows = (
            rows
            if rows is not None
            else [
                {"ledger_id": "ryjl3-tyaaa-aaaaa-aaaba-cai", "symbol": "ICP"},
                {"ledger_id": "maptf-ledger", "symbol": "MAPTF"},
            ]
        )
        full = [
            {"ledger_id": None, "symbol": None, "name": None, "standard": None,
             "decimals": None, **row}
            for row in rows
        ]
        table.append(pa.Table.from_pylist(full, schema=schema))
        return table


class ExtractTimestampTests(unittest.TestCase):
    def test_a_seconds_and_b_milliseconds_parse_like_silver_contract(self):
        a = observe(a_object((LUV_OLD_TS, LUV, LUV_OLD)), "A")
        b = observe(b_object((LUV_OLD_TS * 1000 + 123, LUV, LUV_OLD, "Name")), "B")
        self.assertEqual(utc(LUV_OLD_TS), a[0]["timestamp"])
        self.assertEqual(
            utc(LUV_OLD_TS).replace(microsecond=123000), b[0]["timestamp"]
        )

    def test_missing_or_invalid_timestamp_is_none_not_an_error(self):
        content = (
            "token0_id,token0_symbol\nL1,X\n"  # no timestamp column
        ).encode()
        self.assertIsNone(observe(content)[0]["timestamp"])
        bad = a_object(("not-a-number", LUV, "X"), (-5, LUV, "X"), ("", LUV, "X"))
        self.assertEqual([None, None, None], [o["timestamp"] for o in observe(bad)])

    def test_standard_and_decimals_only_observations_are_dated_too(self):
        """One as_of orders every attribute, so a row that supplies only standard
        or decimals carries its event timestamp as well."""
        content = b"timestamp,token0_id,token0_standard,token0_decimals\n77,L1,ICRC2,8\n"
        self.assertEqual(utc(77), observe(content)[0]["timestamp"])


class LatestWinsTests(Base):
    def test_real_renamed_ledgers_in_one_a_object_latest_wins(self):
        """Two symbols for one ledger inside ONE Dataset A object: the latest one wins."""
        dim = self.fresh_dim()
        content = a_object(
            (LUV_OLD_TS, LUV, LUV_OLD),
            (CARROT_OLD_TS, CARROT, CARROT_OLD),
            (LUV_OLD_TS + 5, LUV, LUV_OLD),
            (LUV_NEW_TS, LUV, LUV_NEW),
            (CARROT_NEW_TS, CARROT, CARROT_NEW),
            (CARROT_NEW_TS + 9, CARROT, CARROT_NEW),
        )
        result = apply_metadata(dim, observe(content))
        self.assertEqual(sorted([LUV, CARROT]), sorted(result["changed_ledgers"]))
        got = {row[0]: row for row in labels(dim)}
        self.assertEqual(LUV_NEW, got[LUV][1])
        self.assertEqual(utc(LUV_NEW_TS), got[LUV][5])
        self.assertEqual(CARROT_NEW, got[CARROT][1])
        self.assertEqual(utc(CARROT_NEW_TS + 9), got[CARROT][5])
        self.assertIsNone(got[LUV][2])  # A rows carry no name

    def test_synthetic_file_with_the_real_symbol_conflict_pattern_loads(self):
        """The scan finds exactly the symbol conflicts in the file, and the object loads
        with the latest symbol per ledger."""
        path = self.base / "0000_pool.csv"
        rows = []
        for i in range(30):
            rows.append((LUV_OLD_TS + i, LUV, LUV_OLD))
            rows.append((CARROT_OLD_TS + i, CARROT, CARROT_OLD))
        for i in range(30):
            rows.append((LUV_NEW_TS + i, LUV, LUV_NEW))
            rows.append((CARROT_NEW_TS + i, CARROT, CARROT_NEW))
        rows.append((1700000000, OTHER, "ICP"))
        path.write_bytes(a_object(*rows))
        conflicts = _scan_conflicts(path)
        self.assertEqual(
            {(LUV, "symbol"), (CARROT, "symbol")}, {(l, a) for l, a, _ in conflicts}
        )
        dim = self.fresh_dim()
        result = apply_metadata(dim, observe(path.read_bytes()))
        self.assertEqual(3, len(result["changed_ledgers"]))
        got = {row[0]: row[1] for row in labels(dim)}
        self.assertEqual({LUV: LUV_NEW, CARROT: CARROT_NEW, OTHER: "ICP"}, got)

    def test_random_splits_and_orders_match_the_latest_observation_oracle(self):
        """Seeded property test: random Dataset A
        rows that supply symbol+standard+decimals, randomly split into objects and applied in
        a random order with random replays, always end at the oracle: per ledger the values
        of the latest observation (equal timestamp: greater value, decimals numerically),
        ``standard`` ICP overriding, with as_of = the latest timestamp."""
        import random

        for seed in range(12):
            rng = random.Random(seed)
            ledgers = [f"ledger-{i}" for i in range(4)]
            events = []  # (ts, ledger, symbol, standard, decimals)
            for _ in range(40):
                ts = rng.randrange(1_600_000_000, 1_600_000_030)  # small range: real ties
                events.append(
                    (ts, rng.choice(ledgers), f"S{rng.randrange(5)}",
                     rng.choice(["ICRC1", "ICRC2", "DIP20", "ICP", "ICRC2"]),
                     rng.randrange(6, 19))
                )
            oracle = {}
            for ledger in ledgers:
                mine = [e for e in events if e[1] == ledger]
                if not mine:
                    continue
                top = max(e[0] for e in mine)
                tied = [e for e in mine if e[0] == top]
                standard = "ICP" if any(e[3] == "ICP" for e in mine) else max(e[3] for e in tied)
                oracle[ledger] = (max(e[2] for e in tied), standard, max(e[4] for e in tied), utc(top))
            groups = [[] for _ in range(4)]
            for event in events:
                rng.choice(groups).append(event)
            objects = []
            for group in groups:
                if not group:
                    continue
                lines = [A_HEADER] + [f"{ts},{l},{s},{st},{d},,,," for ts, l, s, st, d in group]
                objects.append(("\n".join(lines) + "\n").encode())
            order = list(objects)
            rng.shuffle(order)
            order += rng.sample(objects, k=min(3, len(objects)))  # replays
            dim = self.fresh_dim(f"synthetic.prop_{seed}")
            for content in order:
                apply_metadata(dim, observe(content))
            got = {r[0]: r for r in labels(dim)}
            self.assertEqual(sorted(oracle), sorted(got), seed)
            for ledger, (symbol, standard, decimals, as_of) in oracle.items():
                self.assertEqual(
                    (symbol, None, standard, decimals, as_of), got[ledger][1:], (seed, ledger)
                )

    def test_replaying_an_old_object_is_a_reported_noop(self):
        dim = self.fresh_dim()
        old = observe(a_object((LUV_OLD_TS, LUV, LUV_OLD)))
        new = observe(a_object((LUV_NEW_TS, LUV, LUV_NEW)))
        apply_metadata(dim, old)
        apply_metadata(dim, new)
        before = labels(dim)
        snapshot = dim.current_snapshot().snapshot_id
        for replay in (old, new, old):
            report = apply_metadata(dim, replay)
            self.assertEqual([], report["changed_ledgers"])
            self.assertEqual(snapshot, dim.current_snapshot().snapshot_id)
        self.assertEqual(before, labels(dim))
        self.assertEqual(LUV_NEW, before[0][1])

    def test_same_value_at_later_timestamp_advances_as_of_deterministically(self):
        dim = self.fresh_dim()
        apply_metadata(dim, observe(a_object((100, LUV, "X"))))
        report = apply_metadata(dim, observe(a_object((900, LUV, "X"))))
        self.assertEqual([LUV], report["changed_ledgers"])
        self.assertEqual(utc(900), labels(dim)[0][5])
        self.assertEqual([], apply_metadata(dim, observe(a_object((100, LUV, "X"))))["changed_ledgers"])

    # Equal-timestamp contradictions are resolved deterministically (greater value wins), never raised.
    def test_equal_timestamp_contradiction_within_object_resolves_to_the_greater_value(self):
        dim = self.fresh_dim()
        content = a_object((500, LUV, "X"), (500, LUV, "Y"), (100, LUV, "OLD"))
        report = apply_metadata(dim, observe(content))
        self.assertEqual("Y", labels(dim)[0][1])
        self.assertEqual(1, report["conflicts_resolved"])

    def test_equal_timestamp_contradiction_against_existing_row_resolves_without_raising(self):
        dim = self.fresh_dim()
        apply_metadata(dim, observe(a_object((500, LUV, "X"))))
        apply_metadata(dim, observe(a_object((500, LUV, "Y"))))
        self.assertEqual("Y", labels(dim)[0][1])
        snapshot = dim.current_snapshot().snapshot_id
        apply_metadata(dim, observe(a_object((500, LUV, "X"))))  # the loser changes nothing
        self.assertEqual(snapshot, dim.current_snapshot().snapshot_id)
        self.assertEqual("Y", labels(dim)[0][1])

    def test_equal_timestamp_contradiction_applies_to_name_too(self):
        dim = self.fresh_dim()
        content = b_object((5000, LUV, "S", "N1"), (5000, LUV, "S", "N2"))
        apply_metadata(dim, observe(content, "B"))
        self.assertEqual("N2", labels(dim)[0][2])

    def test_decimals_and_standard_conflicts_resolve_latest_wins(self):
        """Decimals/standard conflicts never raise ``MetadataError``; they are resolved
        (latest observation wins, ICP exception)."""
        dim = self.fresh_dim()
        dec = ("timestamp,token0_id,token0_decimals\n10,L1,6\n20,L1,8\n").encode()
        apply_metadata(dim, observe(dec))
        std = ("timestamp,token0_id,token0_standard\n10,L2,ICRC1\n20,L2,ICRC2\n").encode()
        apply_metadata(dim, observe(std))
        got = {row[0]: row for row in labels(dim)}
        self.assertEqual(8, got["L1"][4])
        self.assertEqual("ICRC2", got["L2"][3])
        # vs the existing row: a newer object replaces, an older one only fills
        apply_metadata(
            dim, observe(b"timestamp,token0_id,token0_decimals,token0_standard\n10,L3,6,ICRC1\n")
        )
        apply_metadata(dim, observe(b"timestamp,token0_id,token0_decimals\n99,L3,8\n"))
        apply_metadata(dim, observe(b"timestamp,token0_id,token0_standard\n99,L3,ICRC2\n"))
        got = {row[0]: row for row in labels(dim)}
        self.assertEqual((8, "ICRC2", utc(99)), (got["L3"][4], got["L3"][3], got["L3"][5]))

    def test_icp_standard_precedence_is_unchanged_with_timestamps(self):
        dim = self.fresh_dim()
        content = (
            "timestamp,token0_id,token0_standard\n10,%s,ICRC2\n20,%s,ICP\n30,%s,ICRC2\n"
            % (OTHER, OTHER, OTHER)
        ).encode()
        apply_metadata(dim, observe(content))
        self.assertEqual("ICP", labels(dim)[0][3])
        apply_metadata(
            dim, observe(("timestamp,token0_id,token0_standard\n40,%s,ICRC2\n" % OTHER).encode())
        )
        self.assertEqual("ICP", labels(dim)[0][3])

    def test_undated_observations_are_lowest_priority(self):
        undated = b"token0_id,token0_symbol\n" + LUV.encode() + b",UNDATED\n"
        dated = a_object((100, LUV, "DATED"))
        # fills an empty ledger
        dim = self.fresh_dim("synthetic.u1")
        apply_metadata(dim, observe(undated))
        row = labels(dim)[0]
        self.assertEqual(("UNDATED", None), (row[1], row[5]))
        # a dated observation replaces it, in either order
        apply_metadata(dim, observe(dated))
        self.assertEqual(("DATED", utc(100)), (labels(dim)[0][1], labels(dim)[0][5]))
        dim = self.fresh_dim("synthetic.u2")
        apply_metadata(dim, observe(dated))
        report = apply_metadata(dim, observe(undated))
        self.assertEqual([], report["changed_ledgers"])
        self.assertEqual("DATED", labels(dim)[0][1])
        # undated and dated in the SAME object: dated wins
        dim = self.fresh_dim("synthetic.u3")
        mixed = observe(undated) + observe(dated)
        apply_metadata(dim, mixed)
        self.assertEqual("DATED", labels(dim)[0][1])
        # two contradicting undated labels tie: the greater one wins
        dim = self.fresh_dim("synthetic.u4")
        apply_metadata(dim, observe(undated))
        apply_metadata(dim, observe(b"token0_id,token0_symbol\n" + LUV.encode() + b",OTHER\n"))
        self.assertEqual("UNDATED", labels(dim)[0][1])  # "UNDATED" > "OTHER"
        candidates = _unique_candidates(
            observe(undated) + observe(b"token0_id,token0_symbol\n" + LUV.encode() + b",OTHER\n")
        )
        self.assertEqual({"UNDATED"}, candidates[LUV]["symbol"])

    def test_unique_candidates_keeps_its_shape_for_callers(self):
        candidates = _unique_candidates(
            observe(a_object((1, LUV, "X"), (9, LUV, "Y")))
        )
        self.assertEqual({"Y"}, candidates[LUV]["symbol"])

    def test_read_metadata_returns_current_label(self):
        dim = self.fresh_dim()
        apply_metadata(dim, observe(a_object((1, LUV, "X"), (9, LUV, "Y"))))
        token = read_metadata(dim, [LUV])["tokens"][0]
        self.assertEqual("Y", token["symbol"])
        self.assertEqual(["name", "standard", "decimals"], token["missing_attributes"])


def data_files(table):
    table.refresh()
    return len(list(table.scan().plan_files()))


def append_files(table, count, *, wide=False):
    """Give the dim ``count`` extra single-row data files without going through
    apply_metadata. ``wide`` adds an ``aaa-``/``zzz-`` row to every file so its min/max
    statistics span all ``filler-`` ids: ``plan_files()`` cannot prune such files, like
    real (random-id) ledgers."""
    schema = table.schema().as_arrow()
    for i in range(count):
        ids = [f"filler-{i:04d}"]
        if wide:
            ids = [f"aaa-{i:04d}", *ids, f"zzz-{i:04d}"]
        table.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": ledger,
                        "symbol": "F",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                        "as_of": None,
                    }
                    for ledger in ids
                ],
                schema=schema,
            )
        )


def wide_object(n):
    """An A object whose three ledgers are new and span the whole id range, so every
    earlier dim file must be selected by ``In(ledger_id, ...)`` (no stats pruning)."""
    return a_object(
        *[(1_700_000_000 + n, f"{p}-{n:03d}", f"S{n}") for p in ("aaa", "mmm", "zzz")]
    )


class FileCountBoundTests(Base):
    """Every dim commit adds a data file and Iceberg never merges them, so without
    compaction the table would hit ``max_files`` (16) after ~17 commits and every later
    object would fail closed. The file count must stay bounded however many commits
    happen; rows/bytes caps stay."""

    def test_many_sequential_objects_with_disjoint_ledgers_stay_bounded(self):
        dim = self.fresh_dim()
        bound = DEFAULT_LIMITS["compact_files"] + 1
        for n in range(60):
            apply_metadata(dim, observe(wide_object(n)))
            self.assertLessEqual(data_files(dim), bound, f"after object {n}")
        self.assertEqual(180, len(labels(dim)))
        self.assertEqual(
            {f"S{n}" for n in range(60)}, {row[1] for row in labels(dim)}
        )

    def test_ascending_timestamp_worst_case_250_objects_stays_bounded(self):
        """Worst case: every object advances the as-of of ~90 of 300 ledgers."""
        self._ascending(universe=300, per_object=90, objects=250)

    def test_ascending_timestamp_sparse_mix_stays_within_the_file_cap(self):
        """U=1000, k=90 ascending: without compaction the 18th object would hit the file cap."""
        self._ascending(universe=1000, per_object=90, objects=120)

    def _ascending(self, *, universe, per_object, objects):
        import random

        rng = random.Random(19)
        universe = [f"ledger-{i:04d}" for i in range(universe)]
        newest = {}
        dim = self.fresh_dim()
        bound = DEFAULT_LIMITS["compact_files"] + 1
        peak = 0
        for n in range(objects):
            ts = 1_700_000_000 + n * 1000
            chosen = rng.sample(universe, per_object)
            for ledger in chosen:
                newest[ledger] = ts
            report = apply_metadata(
                dim, observe(a_object(*[(ts, ledger, "SYM") for ledger in chosen]))
            )
            self.assertEqual(sorted(chosen), sorted(report["changed_ledgers"]))
            peak = max(peak, data_files(dim))
        self.assertLessEqual(peak, bound)
        final = {row[0]: row for row in labels(dim)}
        self.assertEqual(sorted(newest), sorted(final))
        for ledger, ts in newest.items():
            self.assertEqual(ts, int(final[ledger][5].timestamp()), ledger)
            self.assertEqual("SYM", final[ledger][1])

    def test_replay_after_compaction_creates_no_new_snapshot(self):
        dim = self.fresh_dim()
        objects = [wide_object(n) for n in range(25)]
        for content in objects:
            apply_metadata(dim, observe(content))
        dim.refresh()
        snapshot, rows = dim.current_snapshot().snapshot_id, labels(dim)
        for content in reversed(objects):
            report = apply_metadata(dim, observe(content))
            self.assertEqual([], report["changed_ledgers"])
            dim.refresh()
            self.assertEqual(snapshot, dim.current_snapshot().snapshot_id)
        self.assertEqual(rows, labels(dim))

    def test_replay_on_a_table_above_the_trigger_reads_whole_table_but_writes_nothing(
        self,
    ):
        dim = self.fresh_dim()
        append_files(dim, DEFAULT_LIMITS["compact_files"] + 3)
        dim.refresh()
        snapshot = dim.current_snapshot().snapshot_id
        before = labels(dim)
        # an undated observation of the stored value changes nothing
        report = apply_metadata(dim, observe(a_object(("", "filler-0001", "F"))))
        self.assertEqual([], report["changed_ledgers"])
        dim.refresh()
        self.assertEqual(snapshot, dim.current_snapshot().snapshot_id)
        self.assertEqual(before, labels(dim))

    def test_commit_over_the_trigger_compacts_to_one_file_and_keeps_other_rows(self):
        dim = self.fresh_dim()
        append_files(dim, DEFAULT_LIMITS["max_files"] + 6, wide=True)  # past the file cap
        self.assertGreater(data_files(dim), DEFAULT_LIMITS["max_files"])
        before = {row[0]: row for row in labels(dim)}
        report = apply_metadata(dim, observe(a_object((5, "brand-new", "NEW"))))
        self.assertEqual(["brand-new"], report["changed_ledgers"])
        self.assertEqual(1, data_files(dim))
        after = {row[0]: row for row in labels(dim)}
        self.assertEqual(set(before) | {"brand-new"}, set(after))
        for ledger, row in before.items():
            self.assertEqual(row, after[ledger])
        self.assertEqual("NEW", after["brand-new"][1])

    def test_read_metadata_accepts_many_files_within_row_and_byte_caps(self):
        dim = self.fresh_dim()
        append_files(dim, DEFAULT_LIMITS["max_files"] + 4, wide=True)
        result = read_metadata(dim, ["filler-0000", "filler-0019", "absent"])
        by_id = {t["ledger_id"]: t for t in result["tokens"]}
        self.assertEqual("F", by_id["filler-0000"]["symbol"])
        self.assertEqual("F", by_id["filler-0019"]["symbol"])
        self.assertEqual(
            ["symbol", "name", "standard", "decimals"],
            by_id["absent"]["missing_attributes"],
        )

    def test_file_ceiling_is_still_fail_closed_for_apply_and_read(self):
        dim = self.fresh_dim()
        append_files(dim, 6, wide=True)
        snapshot = dim.current_snapshot().snapshot_id
        obs = observe(a_object((5, "brand-new", "NEW")))
        with self.assertRaisesRegex(MetadataError, "file/row/byte cap"):
            apply_metadata(
                dim, obs, limits={"compact_files": 0, "max_compact_files": 5}
            )
        with self.assertRaisesRegex(MetadataError, "file/row/byte cap"):
            read_metadata(dim, ["filler-0000"], limits={"max_compact_files": 5})
        dim.refresh()
        self.assertEqual(snapshot, dim.current_snapshot().snapshot_id)

    def test_rows_and_bytes_caps_still_fail_closed_on_the_compaction_path(self):
        dim = self.fresh_dim()
        append_files(dim, DEFAULT_LIMITS["compact_files"] + 2, wide=True)  # 30 rows
        snapshot, rows = dim.current_snapshot().snapshot_id, labels(dim)
        obs = observe(a_object((5, "brand-new", "NEW")))
        for limits in ({"max_rows": 3}, {"max_bytes": 1}):
            with self.subTest(limits=limits):
                with self.assertRaisesRegex(MetadataError, "file/row/byte cap"):
                    apply_metadata(dim, obs, limits=limits)
                with self.assertRaisesRegex(MetadataError, "file/row/byte cap"):
                    read_metadata(dim, ["filler-0000"], limits=limits)
        # the whole-table output is bounded as well: 30 existing + 1 new > 30
        with self.assertRaisesRegex(MetadataError, "output row cap"):
            apply_metadata(dim, obs, limits={"max_rows": 30})
        dim.refresh()
        self.assertEqual(snapshot, dim.current_snapshot().snapshot_id)
        self.assertEqual(rows, labels(dim))

    def test_duplicate_ledger_rows_fail_closed_on_the_compaction_path(self):
        dim = self.fresh_dim()
        append_files(dim, 3)
        append_files(dim, 1)  # filler-0000 a second time, in another file
        with self.assertRaisesRegex(MetadataError, "duplicate"):
            apply_metadata(
                dim,
                observe(a_object((5, "brand-new", "NEW"))),
                limits={"compact_files": 0},
            )

    def test_concurrent_writer_during_a_compaction_commit_converges(self):
        from unittest.mock import patch

        dim = self.fresh_dim()
        append_files(dim, DEFAULT_LIMITS["compact_files"] + 2)
        rival = prepare_dim_target(self.new_catalog(), IDENT)
        calls = []
        original = self.catalog.commit_table

        def racing_commit(table, requirements, updates):
            calls.append(len(calls))
            if len(calls) == 1:
                apply_metadata(rival, observe(a_object((7, "rival-ledger", "RIVAL"))))
            return original(table, requirements, updates)

        with patch.object(self.catalog, "commit_table", racing_commit):
            report = apply_metadata(dim, observe(a_object((9, "mine", "MINE"))))
        self.assertGreaterEqual(len(calls), 2)  # first commit lost, retried on a refresh
        self.assertEqual(["mine"], report["changed_ledgers"])
        final = {row[0]: row for row in labels(dim)}
        self.assertEqual("RIVAL", final["rival-ledger"][1])
        self.assertEqual("MINE", final["mine"][1])
        self.assertEqual(DEFAULT_LIMITS["compact_files"] + 2 + 2, len(final))
        self.assertLessEqual(data_files(dim), 2)


def _scan_conflicts(path):
    """Scan a Dataset A file for ledgers that carry more than one symbol (aliasing)."""
    import csv
    from collections import defaultdict

    seen = defaultdict(lambda: defaultdict(set))
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        names = [n.strip() for n in next(reader)]
        for cells in reader:
            if len(cells) != len(names):
                continue
            row = {n: v.strip() for n, v in zip(names, cells)}
            for slot in (0, 1):
                prefix = f"token{slot}_"
                ledger = row.get(prefix + "id") or row.get(prefix + "ledger_id") or ""
                if not ledger:
                    continue
                for attr in ("symbol", "name", "standard", "decimals"):
                    value = row.get(prefix + attr, "")
                    if value != "":
                        seen[ledger][attr].add(value)
    return [
        (ledger, attr, sorted(values))
        for ledger, attrs in sorted(seen.items())
        for attr, values in attrs.items()
        if len(values) > 1
    ]


if __name__ == "__main__":
    unittest.main(verbosity=2)
