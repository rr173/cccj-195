"""Tests for ``profile`` / ``verify-profile``.

Coverage (acceptance matrix):

* exact bucket-boundary arrivals, empty windows vs unknowable windows;
* negative keys and both int64 endpoints; touching / overlapping / separated
  closed intervals; empty range results; tombstones; aborted transactions;
  unfinished scans; log/operation gaps; runtime input drift; tampered
  profiles; one huge sparse int64 interval (cost in end points, not width);
* cross-check against the brute-force per-key enumeration oracle in
  ``tests/profile_oracle.py`` (the production code never enumerates keys).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapshot_audit.model import INT64_MAX, INT64_MIN, load_history  # noqa: E402
from snapshot_audit.profile import (  # noqa: E402
    PROFILE_FORMAT, ProfileError, bucket_index, run, validate_top_n,
    validate_window_width,
)
from snapshot_audit.store import Store  # noqa: E402
from tests.helpers import (  # noqa: E402
    HistoryBuilder, del_op, put_op, range_op, read_op,
)
from tests.profile_oracle import oracle_sections  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cli(args, cwd=ROOT):
    return subprocess.run([sys.executable, "-m", "snapshot_audit"] + args,
                          capture_output=True, text=True, cwd=cwd)


def profile_doc(doc: Dict[str, Any], width: float, top_n: int = 20,
                tmp: Optional[str] = None) -> Dict[str, Any]:
    """Run the production profiler in-process over a raw document."""
    if tmp is None:
        tmp = tempfile.mkdtemp(prefix="prof-")
    path = os.path.join(tmp, "h.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    from snapshot_audit.hashing import hash_input
    info = hash_input(path)
    store = Store()
    try:
        header, commits, txns = load_history(path, None)
        store.ingest(header, commits, txns)
        return run(store, info, float(width), top_n)
    finally:
        store.close()


def _write(tmp: str, name: str, obj: Any) -> str:
    path = os.path.join(tmp, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh)
    return path


def _txn(tid, sp, start, end, ops, status="committed", writes=None):
    t = {"txn_id": tid, "status": status, "snapshot_pos": sp,
         "start_time": start, "end_time": end, "operations": ops}
    if writes is not None:
        t["writes"] = writes
    return t


def _doc(txns, commits=None, initial=None, complete=True):
    log = {"first_commit_pos": None, "last_commit_pos": None,
           "complete": complete, "commits": commits or []}
    if commits:
        poss = [c["commit_pos"] for c in commits]
        log["first_commit_pos"], log["last_commit_pos"] = min(poss), max(poss)
    return {"format": "snapshot-history/1",
            "initial_state": initial or [],
            "commit_log": log, "transactions": txns}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = self.tmp.name


# ---------------------------------------------------------------------------
# windowing
# ---------------------------------------------------------------------------

class WindowingTest(Base):
    def _three_bucket_doc(self):
        # anchor = end_time 10 (T1); width 10 -> boundaries at 20, 30, 40
        txns = [
            _txn("T1", 0, 0, 10, [read_op(0, 1, found=False)]),
            _txn("T2", 0, 15, 20, [put_op(0, 1, 7)]),       # exact boundary -> bucket 1
            _txn("T3", 0, 25, 30, [read_op(0, 1, 7, "v1")]),  # boundary -> bucket 2
            _txn("T4", 0, 31, 39, [del_op(0, 1)]),           # bucket 2
        ]
        commits = [{"txn_id": "T2", "commit_pos": 1,
                    "writes": [{"key": 1, "version_id": "v1",
                                "predecessor": None, "value": 7}]}]
        return _doc(txns, commits)

    def test_anchor_and_exact_boundary_buckets(self):
        prof = profile_doc(self._three_bucket_doc(), 10, tmp=self.d)
        self.assertEqual(prof["anchor_end_time"], 10.0)
        self.assertEqual(prof["windowing"]["window_count"], 3)
        idx = {w["window_index"]: w for w in prof["windows"]}
        self.assertEqual(idx[0]["transactions"], ["T1"])
        self.assertEqual(idx[1]["transactions"], ["T2"])
        self.assertEqual(sorted(idx[2]["transactions"]), ["T3", "T4"])
        # explicit boundary rule
        self.assertEqual(bucket_index(20.0, 10.0, 10.0), 1)
        self.assertEqual(bucket_index(30.0, 10.0, 10.0), 2)
        self.assertEqual(bucket_index(10.0, 10.0, 10.0), 0)

    def test_empty_window_is_complete_zero_not_unknown(self):
        # T1 ends at 10, T2 ends at 35 -> bucket 0 and bucket 2; bucket 1 empty
        txns = [
            _txn("T1", 0, 0, 10, [read_op(0, 1, found=False)]),
            _txn("T2", 0, 30, 35, [read_op(0, 1, found=False)]),
        ]
        prof = profile_doc(_doc(txns), 10, tmp=self.d)
        self.assertEqual(prof["status"], "complete")
        empty = prof["windows"][1]
        self.assertEqual(empty["status"], "complete")
        self.assertEqual(empty["transaction_count"], 0)
        self.assertEqual(empty["operation_total"], 0)
        self.assertEqual(empty["transactions"], [])
        self.assertEqual(empty["gaps"], [])
        # and it must be distinguishable from an unknown window
        self.assertIsNone(empty["duration"]["sum_ms"])
        self.assertEqual(empty["duration"]["status"], "complete")

    def test_cross_window_classification_counts(self):
        prof = profile_doc(self._three_bucket_doc(), 10, tmp=self.d)
        idx = {w["window_index"]: w for w in prof["windows"]}
        self.assertEqual((idx[0]["point_reads"], idx[0]["range_reads"],
                          idx[0]["writes"], idx[0]["operation_total"]),
                         (1, 0, 0, 1))
        self.assertEqual((idx[1]["point_reads"], idx[1]["writes"]), (0, 1))
        self.assertEqual(idx[2]["operation_total"], 2)
        self.assertEqual(idx[2]["point_reads"], 1)
        self.assertEqual(idx[2]["writes"], 1)
        self.assertEqual(idx[2]["range_reads"], 0)

    def test_duration_summary_explainable_and_sorted_sum(self):
        txns = [
            _txn("T1", 0, 0, 10, [read_op(0, 1, found=False)]),       # d=10
            _txn("T2", 0, 12, 19, [read_op(0, 1, found=False)]),      # d=7
        ]
        prof = profile_doc(_doc(txns), 10, tmp=self.d)
        self.assertEqual(len(prof["windows"]), 1)
        w0 = prof["windows"][0]
        self.assertEqual(w0["duration"]["status"], "complete")
        self.assertEqual(w0["duration"]["explained_transactions"], 2)
        self.assertEqual(w0["duration"]["min_ms"], 7.0)
        self.assertEqual(w0["duration"]["max_ms"], 10.0)
        self.assertEqual(w0["duration"]["sum_ms"], 17.0)
        self.assertEqual(w0["duration"]["mean_ms"], 8.5)

    def test_float_width_and_float_times(self):
        txns = [
            _txn("T1", 0, 0.0, 0.5, [read_op(0, 1, found=False)]),
            _txn("T2", 0, 0.6, 1.0, [read_op(0, 1, found=False)]),
        ]
        prof = profile_doc(_doc(txns), 0.5, tmp=self.d)
        # T1 at 0.5 exactly on the boundary -> bucket 1; T2 at 1.0 -> bucket 2
        self.assertEqual([w["transactions"] for w in prof["windows"]],
                         [["T1"], ["T2"]])

    def test_deterministic_reruns_byte_identical(self):
        doc = self._three_bucket_doc()
        a = profile_doc(doc, 10, tmp=self.d)
        b = profile_doc(doc, 10, tmp=self.d)
        a.pop("runtime_stats"), b.pop("runtime_stats")
        self.assertEqual(
            json.dumps(a, sort_keys=True, ensure_ascii=False),
            json.dumps(b, sort_keys=True, ensure_ascii=False))

    def test_invalid_width_and_topn_rejected(self):
        for bad in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ProfileError):
                validate_window_width(bad)
        for bad in (0, -3, 1.5, True):
            with self.assertRaises(ProfileError):
                validate_top_n(bad)


# ---------------------------------------------------------------------------
# per-key heat semantics
# ---------------------------------------------------------------------------

class KeyHeatTest(Base):
    def test_event_semantics_point_write_and_range(self):
        txns = [
            _txn("T1", 0, 0, 10, [
                read_op(0, 1, 5, "i1"),       # point key 1
                read_op(1, 2, found=False),   # point key 2 still counts
                put_op(2, 1, 9),              # write key 1
                del_op(3, 2),                 # write key 2
                range_op(4, 1, 3, [(1, 9, "v1"), (3, 0, "i3")]),
            ]),
        ]
        commits = [{"txn_id": "T1", "commit_pos": 1, "writes": [
            {"key": 1, "version_id": "v1", "predecessor": "i1", "value": 9},
            {"key": 2, "version_id": "d2", "predecessor": None,
             "tombstone": True},
        ]}]
        prof = profile_doc(_doc(txns, commits), 10, tmp=self.d)
        heat = {e["key"]: e for e in prof["key_heat"]["ranked_keys"]}
        self.assertEqual(heat[1]["point_read_events"], 1)
        self.assertEqual(heat[1]["write_events"], 1)
        self.assertEqual(heat[1]["range_returned_events"], 1)
        self.assertEqual(heat[1]["total_events"], 3)
        # key 2: point read (found=false still an event) + delete write;
        # not returned by the range (only live keys 1 and 3 returned)
        self.assertEqual(heat[2]["point_read_events"], 1)
        self.assertEqual(heat[2]["write_events"], 1)
        self.assertEqual(heat[2]["range_returned_events"], 0)
        # key 3 appears only via the range result
        self.assertEqual(heat[3]["range_returned_events"], 1)
        self.assertEqual(prof["key_heat"]["status"], "complete")

    def test_tombstone_entries_excluded_from_range_heat(self):
        # range result carrying an explicit tombstone entry must not count it
        op = range_op(0, -5, 5, [])
        op["entries"] = [{"key": -1, "value": None, "tombstone": True},
                         {"key": 2, "value": 7, "version_id": "v2"}]
        txns = [_txn("T1", 0, 0, 10, [op])]
        prof = profile_doc(_doc(txns), 10, tmp=self.d)
        keys = {e["key"] for e in prof["key_heat"]["ranked_keys"]}
        self.assertNotIn(-1, keys)
        self.assertIn(2, keys)

    def test_empty_range_result_still_covers_but_no_key_heat(self):
        txns = [_txn("T1", 0, 0, 10,
                     [range_op(0, 100, 200, [], complete=True)])]
        prof = profile_doc(_doc(txns), 10, tmp=self.d)
        self.assertEqual(prof["key_heat"]["distinct_keys"], 0)
        self.assertEqual(prof["scan_coverage"]["completed_scans"], 1)
        self.assertEqual(prof["scan_coverage"]["segments"],
                         [{"lo": 100, "hi": 200, "coverage": 1}])

    def test_topn_tie_order_is_by_key_ascending(self):
        txns = [_txn("T1", 0, 0, 10, [
            read_op(0, 30, found=False), read_op(1, 10, found=False),
            read_op(2, 20, found=False), read_op(3, 10, found=False),
        ])]
        prof = profile_doc(_doc(txns), 10, top_n=2, tmp=self.d)
        ranked = prof["key_heat"]["ranked_keys"]
        self.assertEqual([e["key"] for e in ranked], [10, 20])
        self.assertEqual(prof["key_heat"]["top_n"], 2)
        self.assertEqual(prof["key_heat"]["distinct_keys"], 3)

    def test_heat_is_not_renamed_audit_metric(self):
        # the profile must carry workload counters, never anomaly/node counts
        prof = profile_doc(
            _doc([_txn("T1", 0, 0, 10, [read_op(0, 1, found=False)])]),
            10, tmp=self.d)
        text = json.dumps(prof)
        for forbidden in ("anomaly", "lineage_node", "error_count",
                          "node_count"):
            self.assertNotIn(forbidden, text)


# ---------------------------------------------------------------------------
# scan coverage: oracle cross-checks and closed-end-point semantics
# ---------------------------------------------------------------------------

class CoverageOracleTest(Base):
    def _run_crosscheck(self, intervals, domain=None):
        ops = [range_op(i, lo, hi, [], complete=True)
               for i, (lo, hi) in enumerate(intervals)]
        txns = [_txn("T1", 0, 0, 10, ops)]
        doc = _doc(txns)
        prof = profile_doc(doc, 10, tmp=self.d)
        if domain is None:
            keys = [k for iv in intervals for k in iv]
            domain = (min(keys), max(keys))
        oracle = oracle_sections(doc, 10, 20, domain)
        self.assertEqual(
            prof["scan_coverage"]["segments"],
            oracle["scan_coverage"]["segments"])
        self.assertEqual(prof["scan_coverage"]["completed_scans"],
                         oracle["scan_coverage"]["completed_scans"])
        return prof

    def test_touching_intervals_share_endpoint(self):
        # [0,10] and [11,20] are adjacent -> merged into coverage 1 segment
        prof = self._run_crosscheck([(0, 10), (11, 20)])
        self.assertEqual(prof["scan_coverage"]["segments"],
                         [{"lo": 0, "hi": 20, "coverage": 1}])

    def test_closed_endpoint_touch_overlap_at_one_key(self):
        # [0,10] and [10,20]: key 10 covered twice (closed endpoints)
        prof = self._run_crosscheck([(0, 10), (10, 20)])
        self.assertEqual(prof["scan_coverage"]["segments"], [
            {"lo": 0, "hi": 9, "coverage": 1},
            {"lo": 10, "hi": 10, "coverage": 2},
            {"lo": 11, "hi": 20, "coverage": 1},
        ])

    def test_separated_intervals_stay_disjoint(self):
        prof = self._run_crosscheck([(0, 4), (10, 14)])
        self.assertEqual(prof["scan_coverage"]["segments"], [
            {"lo": 0, "hi": 4, "coverage": 1},
            {"lo": 10, "hi": 14, "coverage": 1},
        ])

    def test_nested_and_overlapping(self):
        self._run_crosscheck([(0, 100), (20, 80), (30, 120), (60, 60)])

    def test_negative_keys(self):
        self._run_crosscheck([(-100, -1), (-5, 5), (-1, 0)])

    def test_int64_endpoints_against_oracle(self):
        # the oracle may only enumerate small finite sub-domains; probe just
        # the neighborhoods of both int64 endpoints
        sub_domains = [(INT64_MIN, INT64_MIN + 8),
                       (INT64_MAX - 8, INT64_MAX)]
        prof = self._run_crosscheck(
            [(INT64_MIN, INT64_MIN + 5), (INT64_MAX - 5, INT64_MAX),
             (INT64_MIN, INT64_MIN)],
            domain=sub_domains)
        segs = prof["scan_coverage"]["segments"]
        self.assertEqual(segs[0]["lo"], INT64_MIN)
        self.assertEqual(segs[-1]["hi"], INT64_MAX)
        # INT64_MIN is covered twice (two closed intervals start there)
        self.assertEqual(segs[0],
                         {"lo": INT64_MIN, "hi": INT64_MIN, "coverage": 2})

    def test_random_interval_sets_match_oracle(self):
        import random
        rng = random.Random(20261120)
        for trial in range(40):
            intervals = []
            for _ in range(rng.randint(1, 12)):
                lo = rng.randint(-200, 200)
                hi = lo + rng.randint(0, 300)
                intervals.append((lo, hi))
            self._run_crosscheck(intervals, domain=(-250, 550))

    def test_single_point_intervals(self):
        self._run_crosscheck([(5, 5), (5, 5), (7, 7)])


class SparseHugeRangeTest(Base):
    def test_one_int64_wide_interval_costs_two_endpoints(self):
        op = range_op(0, INT64_MIN, INT64_MAX, [], complete=True)
        doc = _doc([_txn("T1", 0, 0, 10, [op])])
        t0 = time.monotonic()
        prof = profile_doc(doc, 10, tmp=self.d)
        elapsed = time.monotonic() - t0
        cov = prof["scan_coverage"]
        # exactly one segment over the whole int64 domain, built from 2 events
        self.assertEqual(cov["endpoint_events"], 2)
        self.assertEqual(cov["completed_scans"], 1)
        self.assertEqual(cov["segments"],
                         [{"lo": INT64_MIN, "hi": INT64_MAX,
                           "coverage": 1}])
        self.assertEqual(prof["stats"]["endpoint_events_processed"], 2)
        # no per-key expansion: runtime must be independent of domain width
        self.assertLess(elapsed, 5.0)

    def test_many_sparse_wide_intervals_scale_with_endpoints(self):
        n = 500
        ops = [range_op(i, INT64_MIN + i, INT64_MAX - i, [], complete=True)
               for i in range(n)]
        doc = _doc([_txn("T1", 0, 0, 10, ops)])
        prof = profile_doc(doc, 10, tmp=self.d)
        self.assertEqual(prof["stats"]["endpoint_events_processed"], 2 * n)
        # segments grow with endpoint count, never with domain width
        self.assertLessEqual(
            len(prof["scan_coverage"]["segments"]), 2 * n)


# ---------------------------------------------------------------------------
# unknown propagation (unknown must never become zero)
# ---------------------------------------------------------------------------

class UnknownPropagationTest(Base):
    def test_unfinished_scan_marks_heat_unknown_but_counts_ops(self):
        txns = [_txn("T1", 0, 0, 10, [
            read_op(0, 1, 5, "i1"),
            range_op(1, 0, 10, [(1, 5, "i1")], complete=False),
        ])]
        prof = profile_doc(_doc(txns), 10, tmp=self.d)
        self.assertEqual(prof["status"], "unknown")
        # the op itself is still classified and counted in the window
        w0 = prof["windows"][0]
        self.assertEqual(w0["range_reads"], 1)
        self.assertEqual(w0["operation_total"], 2)
        self.assertIn("heat_unproven", w0["gaps"])
        # heat sections are unknown lower bounds, and the unfinished scan
        # contributes neither returned-key heat nor interval coverage
        self.assertEqual(prof["key_heat"]["status"], "unknown")
        self.assertEqual(prof["scan_coverage"]["status"], "unknown")
        self.assertEqual(prof["scan_coverage"]["completed_scans"], 0)
        self.assertEqual(prof["scan_coverage"]["segments"], [])
        kinds = {g["kind"] for g in prof["gaps"]}
        self.assertIn("incomplete_scan", kinds)

    def test_completed_scan_missing_entries_keeps_coverage(self):
        op = {"seq": 0, "type": "range_read", "lo": 0, "hi": 10,
              "complete": True}  # no entries field at all
        doc = _doc([_txn("T1", 0, 0, 10, [op])])
        prof = profile_doc(doc, 10, tmp=self.d)
        self.assertEqual(prof["scan_coverage"]["status"], "unknown")
        self.assertEqual(prof["scan_coverage"]["completed_scans"], 1)
        self.assertEqual(prof["scan_coverage"]["segments"],
                         [{"lo": 0, "hi": 10, "coverage": 1}])
        self.assertEqual(prof["key_heat"]["status"], "unknown")

    def test_missing_operations_pollutes_windows_and_heat(self):
        t = _txn("T1", 0, 0, 10, None)
        prof = profile_doc(_doc([t]), 10, tmp=self.d)
        w0 = prof["windows"][0]
        self.assertEqual(w0["status"], "unknown")
        self.assertIn("window_membership_unproven", w0["gaps"])
        self.assertIn("operation_classification_unproven", w0["gaps"])
        self.assertIn("heat_unproven", w0["gaps"])
        # observable count is zero but the window is explicitly not trusted
        self.assertEqual(w0["operation_total"], 0)

    def test_missing_end_time_is_unbinned_and_unknown(self):
        t1 = _txn("T1", 0, 0, 10, [read_op(0, 1, found=False)])
        t2 = _txn("T2", 0, 5, None, [read_op(0, 1, found=False)])
        prof = profile_doc(_doc([t1, t2]), 10, tmp=self.d)
        self.assertEqual(
            [u["txn_id"] for u in prof["unbinned_transactions"]], ["T2"])
        self.assertEqual(prof["status"], "unknown")
        # the bounded txn still lands in its window
        self.assertEqual(prof["windows"][0]["transactions"], ["T1"])
        kinds = {g["kind"] for g in prof["gaps"]}
        self.assertIn("missing_times", kinds)

    def test_no_end_times_at_all_leaves_window_section_unknown(self):
        t1 = _txn("T1", 0, None, None, [read_op(0, 1, found=False)])
        prof = profile_doc(_doc([t1]), 10, tmp=self.d)
        self.assertEqual(prof["windowing"]["window_count"], 0)
        self.assertEqual(prof["windows"], [])
        self.assertEqual(
            [u["txn_id"] for u in prof["unbinned_transactions"]], ["T1"])
        self.assertEqual(prof["status"], "unknown")
        # heat evidence still available without any time anchor
        self.assertEqual(prof["key_heat"]["distinct_keys"], 1)

    def test_incomplete_log_marks_every_window_membership_unproven(self):
        txns = [
            _txn("T1", 0, 0, 10, [read_op(0, 1, found=False)]),
            _txn("T2", 0, 20, 25, [read_op(0, 1, found=False)]),
        ]
        prof = profile_doc(_doc(txns, complete=False), 10, tmp=self.d)
        for w in prof["windows"]:
            self.assertEqual(w["status"], "unknown")
            self.assertIn("window_membership_unproven", w["gaps"])

    def test_end_before_start_unknown_duration_but_known_counts(self):
        t = _txn("T1", 0, 30, 10, [read_op(0, 1, found=False)])
        prof = profile_doc(_doc([t]), 10, tmp=self.d)
        w0 = prof["windows"][0]
        self.assertEqual(w0["transaction_count"], 1)
        self.assertEqual(w0["operation_total"], 1)
        self.assertEqual(w0["duration"]["status"], "unknown")
        self.assertEqual(w0["duration"]["unexplained_transactions"], ["T1"])
        self.assertIsNone(w0["duration"]["sum_ms"])

    def test_inverted_interval_does_not_corrupt_coverage(self):
        # hi < lo is a contract gap; the scan must not produce inverted events
        op = {"seq": 0, "type": "range_read", "lo": 10, "hi": 1,
              "entries": [], "complete": True}
        good = range_op(1, 20, 22, [])
        prof = profile_doc(_doc([_txn("T1", 0, 0, 10, [op, good])]),
                           10, tmp=self.d)
        self.assertEqual(prof["status"], "unknown")
        # only the well-formed interval contributes coverage
        self.assertEqual(prof["scan_coverage"]["segments"],
                         [{"lo": 20, "hi": 22, "coverage": 1}])
        kinds = {g["kind"] for g in prof["gaps"]}
        self.assertIn("bad_range", kinds)


# ---------------------------------------------------------------------------
# aborted transactions are excluded entirely
# ---------------------------------------------------------------------------

class AbortedExclusionTest(Base):
    def test_aborted_txn_neither_window_nor_heat(self):
        good = _txn("T1", 0, 0, 10, [read_op(0, 1, found=False)])
        bad = _txn("A1", 0, 5, 8, [put_op(0, 9, 99)], status="aborted",
                   writes=[{"key": 9, "version_id": "bad1", "value": 99}])
        prof = profile_doc(_doc([good, bad]), 10, tmp=self.d)
        self.assertEqual(prof["stats"]["aborted_transactions_excluded"], 1)
        self.assertEqual(prof["windows"][0]["transactions"], ["T1"])
        keys = {e["key"] for e in prof["key_heat"]["ranked_keys"]}
        self.assertNotIn(9, keys)

    def test_only_aborted_history_is_known_empty_not_unknown(self):
        bad = _txn("A1", 0, 5, 8, [put_op(0, 9, 99)], status="aborted",
                   writes=[{"key": 9, "version_id": "bad1", "value": 99}])
        prof = profile_doc(_doc([bad]), 10, tmp=self.d)
        self.assertEqual(prof["status"], "complete")
        self.assertEqual(prof["windows"], [])
        self.assertEqual(prof["windowing"]["window_count"], 0)
        self.assertEqual(prof["stats"]["committed_transactions"], 0)
        self.assertEqual(prof["stats"]["aborted_transactions_excluded"], 1)
        self.assertEqual(prof["key_heat"]["ranked_keys"], [])
        self.assertEqual(prof["scan_coverage"]["segments"], [])


# ---------------------------------------------------------------------------
# full oracle cross-check on a mixed history
# ---------------------------------------------------------------------------

class OracleCrossCheckTest(Base):
    def test_mixed_history_matches_oracle_section_by_section(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(3, 0, "i3")
        b.commit("T1", 1, [(1, "v1", "i1", 7)])
        b.commit("T2", 2, [(3, "v3", "i3", 8)])
        b.txn("T1", 0, 0, 10, [
            read_op(0, 1, 0, "i1"), put_op(1, 1, 7),
            range_op(2, 1, 5, [(1, 7, "v1"), (3, 0, "i3")]),
        ])
        b.txn("T2", 1, 12, 20, [
            range_op(0, 3, 9, [(3, 8, "v3")]), put_op(1, 3, 8),
        ])
        b.txn("R3", 2, 25, 35, [read_op(0, 3, 8, "v3")])
        doc = b.doc()
        prof = profile_doc(doc, 10, top_n=10, tmp=self.d)
        oracle = oracle_sections(doc, 10, 10)
        self.assertEqual(prof["status"], oracle["status"])
        self.assertEqual(prof["anchor_end_time"], oracle["anchor_end_time"])
        self.assertEqual(prof["windowing"]["window_count"],
                         oracle["window_count"])
        for pw, ow in zip(prof["windows"], oracle["windows"]):
            self.assertEqual(pw["window_index"], ow["window_index"])
            self.assertEqual(pw["transactions"], ow["transactions"])
            self.assertEqual(pw["transaction_count"],
                             ow["transaction_count"])
            self.assertEqual(pw["operation_total"], ow["operation_total"])
            self.assertEqual(pw["point_reads"], ow["point_reads"])
            self.assertEqual(pw["range_reads"], ow["range_reads"])
            self.assertEqual(pw["writes"], ow["writes"])
            self.assertEqual(pw["duration"]["status"],
                             ow["duration"]["status"])
            self.assertEqual(pw["duration"]["sum_ms"],
                             ow["duration"]["sum_ms"])
        self.assertEqual(
            [(e["key"], e["total_events"])
             for e in prof["key_heat"]["ranked_keys"]],
            [(e["key"], e["total_events"])
             for e in oracle["key_heat"]["ranked_keys"]])
        self.assertEqual(prof["scan_coverage"]["segments"],
                         oracle["scan_coverage"]["segments"])

    def test_oracle_gap_status_parity_incomplete_log(self):
        doc = _doc(
            [_txn("T1", 0, 0, 10,
                  [range_op(0, -5, 5, []), read_op(1, 2, found=False)])],
            complete=False)
        prof = profile_doc(doc, 10, tmp=self.d)
        oracle = oracle_sections(doc, 10, 10)
        self.assertEqual(prof["status"], "unknown")
        self.assertEqual(prof["status"], oracle["status"])
        self.assertEqual(prof["windows"][0]["gaps"],
                         oracle["windows"][0]["gaps"])
        # heat itself is still exact: the log gap does not invalidate the
        # recorded operation evidence, only window membership completeness
        self.assertEqual(prof["key_heat"]["status"],
                         oracle["key_heat"]["status"])
        self.assertEqual(prof["scan_coverage"]["segments"],
                         oracle["scan_coverage"]["segments"])

    def test_oracle_gap_parity_unfinished_scan_and_tombstone(self):
        op = range_op(0, -10, 10, [(1, 1, "v1")], complete=False)
        doc = _doc([_txn("T1", 0, 0, 10, [op, read_op(1, 7, found=False)])])
        prof = profile_doc(doc, 10, tmp=self.d)
        oracle = oracle_sections(doc, 10, 10, domain=(-10, 10))
        self.assertEqual(prof["status"], oracle["status"])
        self.assertEqual(prof["key_heat"]["status"],
                         oracle["key_heat"]["status"])
        self.assertEqual(prof["scan_coverage"]["status"],
                         oracle["scan_coverage"]["status"])
        self.assertEqual(prof["scan_coverage"]["segments"],
                         oracle["scan_coverage"]["segments"])
        # the point read still gives provable key heat despite the scan gap
        ranked = {e["key"]: e for e in prof["key_heat"]["ranked_keys"]}
        self.assertEqual(ranked[7]["point_read_events"], 1)


# ---------------------------------------------------------------------------
# CLI, binding, verification and tamper detection
# ---------------------------------------------------------------------------

class CliAndVerifyTest(Base):
    def _doc(self):
        return _doc([
            _txn("T1", 0, 0, 10, [read_op(0, 1, found=False),
                                  put_op(1, 1, 7)]),
            _txn("T2", 0, 20, 25, [range_op(0, 1, 4, [])]),
        ], commits=[{"txn_id": "T1", "commit_pos": 1, "writes": [
            {"key": 1, "version_id": "v1", "predecessor": None,
             "value": 7}]}])

    def _profile_cli(self, doc, *extra, expect=0):
        hist = _write(self.d, "h.json", doc)
        out = os.path.join(self.d, "p.json")
        r = _cli(["profile", "--input", hist, "--out", out] + list(extra))
        self.assertEqual(r.returncode, expect, r.stderr)
        with open(out) as fh:
            return json.load(fh), out, hist

    def test_cli_structure_binding_and_signature(self):
        from snapshot_audit.hashing import canonical_json, sha256_text
        from snapshot_audit.profile import _unsigned
        prof, out, hist = self._profile_cli(
            self._doc(), "--window-width", "10", "--top-n", "5")
        self.assertEqual(prof["format"], PROFILE_FORMAT)
        self.assertEqual(prof["parameters"],
                         {"window_width": 10.0, "top_n": 5})
        self.assertEqual(prof["input"]["path"], os.path.realpath(hist))
        self.assertEqual(len(prof["input"]["sha256"]), 64)
        self.assertEqual(
            prof["profile_sha256"],
            sha256_text(canonical_json(_unsigned(prof))))
        for field_ in ("input_records", "endpoint_events_processed",
                       "segments_emitted", "heat_events_processed"):
            self.assertIn(field_, prof["stats"])
        self.assertIn("rss_bytes_exact", prof["runtime_stats"])

    def test_verify_ok_and_jsonl_directory_input(self):
        doc = self._doc()
        out_dir = os.path.join(self.d, "hist")
        os.makedirs(out_dir, exist_ok=True)
        log = doc["commit_log"]
        with open(os.path.join(out_dir, "header.json"), "w") as fh:
            json.dump({"format": doc["format"],
                       "initial_state": doc["initial_state"],
                       "commit_log": {k: log[k] for k in
                                      ("first_commit_pos", "last_commit_pos",
                                       "complete")}}, fh)
        with open(os.path.join(out_dir, "commits.jsonl"), "w") as fh:
            for c in log["commits"]:
                fh.write(json.dumps(c) + "\n")
        with open(os.path.join(out_dir, "transactions.jsonl"), "w") as fh:
            for t in doc["transactions"]:
                fh.write(json.dumps(t) + "\n")
        out = os.path.join(self.d, "p.json")
        r = _cli(["profile", "--input-dir", out_dir, "--window-width", "10",
                  "--out", out])
        self.assertEqual(r.returncode, 0, r.stderr)
        r = _cli(["verify-profile", "--profile", out])
        self.assertEqual(r.returncode, 0, r.stderr)
        report = json.loads(r.stdout)
        self.assertTrue(report["verified"])

    def test_unknown_exit_code(self):
        t = _txn("T1", 0, 0, 10, [
            range_op(0, 0, 9, [], complete=False)])
        # unknown -> exit 2
        self._profile_cli(_doc([t]), "--window-width", "10", expect=2)

    def test_bad_usage_exit_code(self):
        hist = _write(self.d, "h.json", self._doc())
        r = _cli(["profile", "--input", hist, "--window-width", "0"])
        self.assertEqual(r.returncode, 3)

    def _verify(self, prof_path, hist=None, expect=0):
        args = ["verify-profile", "--profile", prof_path]
        if hist:
            args += ["--input", hist]
        r = _cli(args)
        self.assertEqual(r.returncode, expect, r.stdout + r.stderr)
        return json.loads(r.stdout)

    def test_tamper_count_is_detected_with_field_path(self):
        prof, path, hist = self._profile_cli(
            self._doc(), "--window-width", "10")
        prof["windows"][0]["transaction_count"] += 1
        with open(path, "w") as fh:
            json.dump(prof, fh)
        report = self._verify(path, hist, expect=1)
        self.assertFalse(report["verified"])
        joined = " ".join(report["problems"])
        self.assertIn("windows[0].transaction_count", joined)
        self.assertIn("profile_sha256", joined)

    def test_move_txn_between_buckets_is_detected(self):
        # add a third window so a bucket move is possible
        doc = self._doc()
        doc["transactions"].append(
            _txn("T3", 0, 40, 45, [read_op(0, 1, found=False)]))
        prof, path, hist = self._profile_cli(doc, "--window-width", "10")
        # claim T3 belongs to window 0
        prof["windows"][0]["transactions"].append("T3")
        prof["windows"][0]["transaction_count"] += 1
        with open(path, "w") as fh:
            json.dump(prof, fh)
        report = self._verify(path, hist, expect=1)
        self.assertFalse(report["verified"])
        self.assertIn("windows[0]", " ".join(report["problems"]))

    def test_delete_coverage_segment_is_detected(self):
        prof, path, hist = self._profile_cli(
            self._doc(), "--window-width", "10")
        self.assertTrue(prof["scan_coverage"]["segments"])
        prof["scan_coverage"]["segments"].pop()
        with open(path, "w") as fh:
            json.dump(prof, fh)
        report = self._verify(path, hist, expect=1)
        self.assertIn("scan_coverage", " ".join(report["problems"]))

    def test_unknown_flip_to_complete_is_detected(self):
        t = _txn("T1", 0, 0, 10,
                 [range_op(0, 0, 9, [], complete=False)])
        prof, path, hist = self._profile_cli(
            _doc([t]), "--window-width", "10", expect=2)
        self.assertEqual(prof["status"], "unknown")
        prof["status"] = "complete"
        prof["gaps"] = []
        with open(path, "w") as fh:
            json.dump(prof, fh)
        report = self._verify(path, hist, expect=1)
        self.assertFalse(report["verified"])

    def test_source_file_change_fails_verification(self):
        prof, path, hist = self._profile_cli(
            self._doc(), "--window-width", "10")
        with open(hist) as fh:
            doc = json.load(fh)
        doc["transactions"][0]["end_time"] = 11
        with open(hist, "w") as fh:
            json.dump(doc, fh)
        report = self._verify(path, hist, expect=1)
        self.assertIn("input.sha256", " ".join(report["problems"]))

    def test_signature_only_change_fails(self):
        prof, path, hist = self._profile_cli(
            self._doc(), "--window-width", "10")
        prof["profile_sha256"] = "0" * 64
        with open(path, "w") as fh:
            json.dump(prof, fh)
        report = self._verify(path, hist, expect=1)
        self.assertIn("profile_sha256", " ".join(report["problems"]))

    def test_verify_with_relocated_input_override(self):
        prof, path, hist = self._profile_cli(
            self._doc(), "--window-width", "10")
        # copy the raw history elsewhere and point the verifier at it via an
        # explicit override; content identity (SHA-256) is what binds
        moved = os.path.join(self.d, "moved.json")
        with open(hist) as src, open(moved, "w") as dst:
            dst.write(src.read())
        r = _cli(["verify-profile", "--profile", path,
                  "--input", moved])
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_runtime_stats_tolerance(self):
        prof, path, hist = self._profile_cli(
            self._doc(), "--window-width", "10")
        # an out-of-tolerance RSS is flagged; a different wall time is not
        prof["runtime_stats"]["rss_bytes_exact"] = 10
        prof["runtime_stats"]["wall_time_ms_exact"] = 9999
        with open(path, "w") as fh:
            json.dump(prof, fh)
        report = self._verify(path, hist, expect=1)
        joined = " ".join(report["problems"])
        self.assertIn("rss_bytes_exact", joined)
        self.assertNotIn("wall_time", joined)

    def test_input_drift_during_profile_is_rejected(self):
        # model "the file changed mid-run": the pre/post hash digests differ,
        # which is precisely what a mutation between the two hashes produces
        from snapshot_audit import cli, hashing
        hist = _write(self.d, "h.json", self._doc())
        orig = hashing.hash_input
        calls = {"n": 0}

        def drifted(path):
            info = dict(orig(path))
            calls["n"] += 1
            if calls["n"] == 2:
                info["sha256"] = "f" * 64
            return info

        argv = ["profile", "--input", hist, "--window-width", "10"]
        old_argv = sys.argv
        try:
            hashing.hash_input = drifted
            rc = cli.main(argv)
        finally:
            hashing.hash_input = orig
            sys.argv = old_argv
        self.assertEqual(rc, 3)

    def test_input_drift_real_file_mutation_subprocess(self):
        # end-to-end: a wrapper mutates the history when the post-run hash is
        # taken, so the two real digests genuinely differ
        hist = _write(self.d, "h.json", self._doc())
        wrapper = os.path.join(self.d, "drift_real.py")
        with open(wrapper, "w") as fh:
            fh.write(
                "import sys, json\n"
                f"sys.path.insert(0, {ROOT!r})\n"
                "import snapshot_audit.hashing as h\n"
                "import snapshot_audit.cli  # noqa: F401 (binds h module)\n"
                "_hi = h.hash_input\n"
                "state = {'n': 0}\n"
                "def wrapped(path):\n"
                "    info = _hi(path)\n"
                "    state['n'] += 1\n"
                "    if state['n'] == 2:\n"
                "        with open(path) as f: d = json.load(f)\n"
                "        d['transactions'][0]['end_time'] = 99\n"
                "        with open(path, 'w') as f: json.dump(d, f)\n"
                "        info = _hi(path)\n"  # re-hash after mutation
                "    return info\n"
                "h.hash_input = wrapped\n"
                "from snapshot_audit.cli import main\n"
                f"sys.exit(main(['profile', '--input', {hist!r}, "
                "'--window-width', '10']))\n")
        r = subprocess.run([sys.executable, wrapper], capture_output=True,
                           text=True, cwd=ROOT)
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertIn("input changed during execution", r.stderr)


# ---------------------------------------------------------------------------
# bounded memory on a larger generated history
# ---------------------------------------------------------------------------

class BoundedResourceTest(Base):
    def test_heat_spill_keeps_memory_bounded_and_counts_exact(self):
        # force the in-memory heat counter cap to spill to SQLite; the
        # aggregated result must remain exact and never expand intervals
        from snapshot_audit import profile as profile_mod
        old_cap = profile_mod.HEAT_DICT_CAP
        profile_mod.HEAT_DICT_CAP = 500
        try:
            n_distinct = 4000
            ops = [read_op(i, 10_000 + i, found=False)
                   for i in range(n_distinct)]
            # one repeated key to exercise counter merging across the spill
            ops.append(put_op(n_distinct, 10_000 + 7, 3))
            doc = _doc([_txn("T1", 0, 0, 10, ops)])
            prof = profile_doc(doc, 10, top_n=n_distinct + 5, tmp=self.d)
            ranked = {e["key"]: e for e in prof["key_heat"]["ranked_keys"]}
            self.assertEqual(len(ranked), n_distinct)
            # key 10007 had one point read + one write = 2 events
            self.assertEqual(ranked[10_007]["total_events"], 2)
            self.assertEqual(ranked[10_007]["point_read_events"], 1)
            self.assertEqual(ranked[10_007]["write_events"], 1)
            # a key seen once
            self.assertEqual(ranked[10_000]["total_events"], 1)
            self.assertEqual(prof["key_heat"]["status"], "complete")
        finally:
            profile_mod.HEAT_DICT_CAP = old_cap

    def test_scale_history_resources_and_stats(self):
        gen = os.path.join(ROOT, "bench", "gen_scale.py")
        hist = os.path.join(self.d, "hist")
        r = subprocess.run(
            [sys.executable, gen, "--out", hist, "--txns", "300",
             "--keys", "80", "--reads", "3", "--ranges", "2",
             "--range-width", "20", "--writes", "1", "--seed", "11"],
            capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(r.returncode, 0, r.stderr)
        out = os.path.join(self.d, "p.json")
        r = _cli(["profile", "--input-dir", hist, "--window-width", "10",
                  "--top-n", "15", "--out", out])
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(out) as fh:
            prof = json.load(fh)
        self.assertEqual(prof["stats"]["committed_transactions"], 300)
        self.assertGreater(prof["stats"]["input_records"], 0)
        self.assertGreater(prof["stats"]["endpoint_events_processed"], 0)
        self.assertGreater(prof["stats"]["segments_emitted"], 0)
        # peak RSS stays modest (everything growing lives in SQLite)
        self.assertLess(prof["runtime_stats"]["rss_bytes_exact"],
                        512 * 1024 * 1024)
        # verification recomputes from the raw JSONL data
        r = _cli(["verify-profile", "--profile", out])
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
