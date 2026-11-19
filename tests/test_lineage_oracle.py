"""Cross-checks (对拍) of the lineage engine against the independent
reference implementation in tests/lineage_oracle.py.

Every scenario runs the SQLite-backed engine and the document-based oracle
and compares the full projected lineage: results, nodes, edges (with
evidence), cycles, unknown propagation, truncation cursors and status.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapshot_audit.lineage import run  # noqa: E402
from snapshot_audit.model import load_history  # noqa: E402
from snapshot_audit.store import Store  # noqa: E402
from tests.helpers import (  # noqa: E402
    HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)
from tests.lineage_oracle import oracle_lineage  # noqa: E402

DUMMY_INPUT = {"kind": "file", "path": "/nonexistent", "sha256": "0" * 64,
               "bytes": 0, "files": []}


def engine_core(doc, query, max_depth=64, max_nodes=8192):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(doc, fh)
        path = fh.name
    try:
        store = Store()
        try:
            header, commits, txns = load_history(path, None)
            store.ingest(header, commits, txns)
            res = run(store, DUMMY_INPUT, query, max_depth=max_depth,
                      max_nodes=max_nodes)
            return {k: res[k] for k in (
                "status", "results", "results_complete", "results_gaps",
                "nodes", "edges", "cycles", "unknowns", "truncation")}
        finally:
            store.close()
    finally:
        os.unlink(path)


def _gap_kinds(gs):
    return sorted(g["kind"] for g in gs)


def project(core, keep_locs=True):
    """Normalize an engine/oracle core to a comparable structure."""
    nodes = []
    for n in core["nodes"]:
        obs = n["observation_set"]
        node = {
            "id": n["id"], "key": n["key"], "version_id": n["version_id"],
            "commit_pos": n["commit_pos"], "writer_txn": n["writer_txn"],
            "write_op_seq": n["write_op_seq"],
            "semantics": n["version_semantics"],
            "depth": n["depth"], "expanded": n["expanded"],
            "obs": None if obs is None else (
                obs["scope"], obs["complete"], obs["reads_before_write"]),
            "status": n["status"], "gaps": _gap_kinds(n["gaps"]),
        }
        if "value" in n:
            node["value"] = n["value"]
        if keep_locs:
            node["loc"] = n["loc"]
        nodes.append(node)
    edges = []
    for e in core["edges"]:
        edge = {k: e[k] for k in (
            "src", "kind", "reader_txn", "read_seq", "reader_snapshot_pos",
            "key", "lo", "hi", "observed", "target", "target_version_id",
            "version_recorded", "intra_txn", "deferred", "status")}
        edge["gaps"] = _gap_kinds(e["gaps"])
        ev = dict(e["evidence"])
        if not keep_locs:
            ev = {k: v for k, v in ev.items() if not k.endswith("loc")}
        edge["evidence"] = ev
        edges.append(edge)
    results = []
    for r in core["results"]:
        results.append({k: r[k] for k in r if k != "gaps"}
                       | {"gaps": _gap_kinds(r["gaps"])})
    return {
        "status": core["status"],
        "results": results,
        "results_complete": core["results_complete"],
        "results_gaps": _gap_kinds(core["results_gaps"]),
        "nodes": nodes,
        "edges": edges,
        "cycles": core["cycles"],
        "unknowns": core["unknowns"],
        "truncation": core["truncation"],
    }


class OracleCase(unittest.TestCase):
    def check(self, doc, query, max_depth=64, max_nodes=8192):
        eng = project(engine_core(doc, query, max_depth, max_nodes))
        ora = project(oracle_lineage(doc, query, max_depth, max_nodes))
        if eng != ora:
            msg = ["engine/oracle mismatch:"]
            for key in eng:
                if eng[key] != ora[key]:
                    msg.append(f"== {key} ==\nengine: "
                               f"{json.dumps(eng[key], indent=1, default=str)[:3000]}"
                               f"\noracle: "
                               f"{json.dumps(ora[key], indent=1, default=str)[:3000]}")
            self.fail("\n".join(msg))
        return eng


class PointQueryTest(OracleCase):
    def doc(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(2, 0, "i2")
        b.commit("T1", 1, [(1, "a1", "i1", 10)])
        b.commit("T2", 2, [(2, "b2", "i2", 20)])
        b.txn("T1", 0, 1, 2, [read_op(0, 2, 0, "i2"), put_op(1, 1, 10)])
        b.txn("T2", 1, 3, 4, [read_op(0, 1, 10, "a1"), put_op(1, 2, 20)])
        return b.doc()

    def test_point_query_chain(self):
        core = self.check(self.doc(), {"commit_pos": 2, "key": 2})
        self.assertEqual(core["status"], "complete")
        self.assertEqual([n["id"] for n in core["nodes"]],
                         ["v:a1", "v:b2", "v:i2"])
        # v:b2 (T2) observed v:a1; v:a1 (T1) observed v:i2
        edges = [(e["src"], e["target"], e["kind"]) for e in core["edges"]]
        self.assertEqual(edges, [("v:a1", "v:i2", "point_read"),
                                 ("v:b2", "v:a1", "point_read")])
        depths = {n["id"]: n["depth"] for n in core["nodes"]}
        self.assertEqual(depths, {"v:b2": 0, "v:a1": 1, "v:i2": 2})

    def test_point_query_at_earlier_position(self):
        core = self.check(self.doc(), {"commit_pos": 1, "key": 2})
        self.assertEqual(core["results"][0]["version_id"], "i2")
        self.assertEqual(core["results"][0]["found"], True)

    def test_point_query_absent_key(self):
        core = self.check(self.doc(), {"commit_pos": 2, "key": 99})
        r = core["results"][0]
        self.assertEqual((r["found"], r["reason"], r["node"]),
                         (False, "no_version", None))
        self.assertEqual(core["status"], "complete")
        self.assertEqual(core["nodes"], [])


class RangeQueryTest(OracleCase):
    def test_range_excludes_tombstones(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(2, 0, "i2").init(3, 0, "i3")
        b.commit("T1", 1, [(2, "d2", "i2", TOMBSTONE)])
        b.txn("T1", 0, 1, 2, [read_op(0, 1, 0, "i1"), del_op(1, 2)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "lo": 1, "hi": 3})
        self.assertEqual([r["key"] for r in core["results"]], [1, 3])
        self.assertEqual(core["status"], "complete")
        # no value node for the deleted key 2
        self.assertNotIn("v:d2", [n["id"] for n in core["nodes"]])

    def test_range_predicate_for_absent_key(self):
        # key 7 never exists inside [1,10]; the writer of key 5 did a
        # complete range read over [1,10]: the predicate observation is
        # kept, but no value node/edge fabricates a value for key 7
        b = HistoryBuilder()
        b.init(3, 0, "i3")
        b.commit("T1", 1, [(5, "a5", None, 50)])
        b.txn("T1", 0, 1, 2, [range_op(0, 1, 10, [(3, 0, "i3")]),
                              put_op(1, 5, 50)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "lo": 1, "hi": 10})
        self.assertEqual([r["key"] for r in core["results"]], [3, 5])
        keys = {n["key"] for n in core["nodes"]}
        self.assertNotIn(7, keys)
        pred = [e for e in core["edges"] if e["kind"] == "range_predicate"
                and e["observed"] == "predicate"]
        self.assertEqual(len(pred), 1)
        self.assertEqual(pred[0]["evidence"]["returned_keys"], [3])
        self.assertEqual((pred[0]["lo"], pred[0]["hi"]), (1, 10))
        # nothing claims a value was read for key 7
        for e in core["edges"]:
            self.assertNotEqual(e["key"], 7)


class TombstoneTest(OracleCase):
    def test_tombstone_reconstruction(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(2, 0, "i2")
        b.commit("T1", 1, [(1, "a1", "i1", 10)])
        b.commit("T2", 2, [(1, "d1", "a1", TOMBSTONE)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 10)])
        b.txn("T2", 1, 3, 4, [read_op(0, 1, 10, "a1"),
                              read_op(1, 2, 0, "i2"), del_op(2, 1)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 2, "key": 1})
        r = core["results"][0]
        self.assertEqual((r["found"], r["reason"]), (False, "tombstone"))
        self.assertEqual(r["node"], "v:d1")
        tomb = [n for n in core["nodes"] if n["id"] == "v:d1"][0]
        self.assertEqual(tomb["semantics"], "tombstone")
        self.assertEqual(tomb["writer_txn"], "T2")
        self.assertEqual(tomb["write_op_seq"], 2)
        self.assertNotIn("value", tomb)
        # the deleter's reads before the delete are observable inputs
        edges = {(e["src"], e["target"], e["kind"]) for e in core["edges"]}
        self.assertIn(("v:d1", "v:a1", "point_read"), edges)
        self.assertIn(("v:d1", "v:i2", "point_read"), edges)
        self.assertEqual(core["status"], "complete")

    def test_range_read_tombstone_absence_edge(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(2, 0, "i2")
        b.commit("T1", 1, [(2, "d2", "i2", TOMBSTONE)])
        b.commit("T2", 2, [(3, "c3", None, 30)])
        b.txn("T1", 0, 1, 2, [del_op(0, 2)])
        b.txn("T2", 1, 3, 4, [range_op(0, 1, 3, [(1, 0, "i1")]),
                              put_op(1, 3, 30)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 2, "key": 3})
        kinds = [(e["kind"], e["observed"], e["key"], e["target"])
                 for e in core["edges"] if e["src"] == "v:c3"]
        self.assertIn(("range_read_item", "version", 1, "v:i1"), kinds)
        self.assertIn(("range_predicate", "absent_tombstone", 2, "v:d2"),
                      kinds)
        self.assertIn(("range_predicate", "predicate", None, None), kinds)
        self.assertEqual(core["status"], "complete")

    def test_point_read_absent_tombstone(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(1, "d1", "i1", TOMBSTONE)])
        b.commit("T2", 2, [(2, "b2", None, 20)])
        b.txn("T1", 0, 1, 2, [del_op(0, 1)])
        b.txn("T2", 1, 3, 4, [read_op(0, 1, found=False), put_op(1, 2, 20)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 2, "key": 2})
        edges = [(e["kind"], e["observed"], e["target"])
                 for e in core["edges"] if e["src"] == "v:b2"]
        self.assertIn(("point_read", "absent_tombstone", "v:d1"), edges)
        self.assertEqual(core["status"], "complete")

    def test_point_read_absent_no_version(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(2, "b2", None, 20)])
        b.txn("T1", 0, 1, 2, [read_op(0, 99, found=False), put_op(1, 2, 20)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 2})
        edges = [e for e in core["edges"] if e["src"] == "v:b2"]
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["observed"], "absent_no_version")
        self.assertIsNone(edges[0]["target"])
        self.assertEqual(core["status"], "complete")


class MultiReadTest(OracleCase):
    def test_same_version_observed_by_multiple_ops(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(2, 0, "i2")
        b.commit("T1", 1, [(3, "c3", None, 30)])
        b.txn("T1", 0, 1, 2, [read_op(0, 1, 0, "i1"),
                              read_op(1, 1, 0, "i1"),   # same version again
                              read_op(2, 2, 0, "i2"),
                              put_op(3, 3, 30)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 3})
        edges = [e for e in core["edges"] if e["target"] == "v:i1"]
        # each observing operation keeps its own evidence
        self.assertEqual([e["read_seq"] for e in edges], [0, 1])
        self.assertEqual(len({e["evidence"]["read_loc"] for e in edges}), 2)
        self.assertEqual(len(core["edges"]), 3)
        self.assertEqual(core["status"], "complete")


class OwnWriteTest(OracleCase):
    def test_write_then_read_intra_txn(self):
        b = HistoryBuilder()
        b.init(2, 0, "i2")
        b.commit("T1", 1, [(1, "a1", None, 10), (2, "b2", "i2", 20)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 10),
                              read_op(1, 1, 10),          # own write-then-read
                              read_op(2, 2, 0, "i2"),
                              put_op(3, 2, 20)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 2})
        own = [e for e in core["edges"] if e["kind"] == "own_write"]
        self.assertEqual(len(own), 1)
        self.assertEqual((own[0]["src"], own[0]["target"]),
                         ("v:b2", "v:a1"))
        self.assertTrue(own[0]["intra_txn"])
        self.assertEqual(own[0]["observed"], "own_value")
        # no transaction-level self-loop, no cycle
        self.assertEqual(core["cycles"], [])
        self.assertEqual(core["status"], "complete")
        # v:a1's write is seq 0: no reads before it
        a1 = [n for n in core["nodes"] if n["id"] == "v:a1"][0]
        self.assertEqual(a1["obs"], ("transaction_level", True, 0))

    def test_own_delete_then_range_scan(self):
        b = HistoryBuilder()
        b.init(3, 0, "i3")
        b.commit("T1", 1, [(3, "d3", "i3", TOMBSTONE), (4, "c4", None, 40)])
        b.txn("T1", 0, 1, 2, [del_op(0, 3),
                              range_op(1, 1, 5, []),   # own delete hides key 3
                              put_op(2, 4, 40)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 4})
        own = [e for e in core["edges"] if e["kind"] == "own_write"]
        self.assertEqual(len(own), 1)
        self.assertEqual(own[0]["observed"], "own_absent")
        self.assertEqual((own[0]["key"], own[0]["target"]), (3, "v:d3"))
        self.assertEqual(core["status"], "complete")


class ReorderTest(OracleCase):
    def test_version_and_txn_reordering(self):
        def build(swap):
            b = HistoryBuilder()
            b.init(1, 0, "i1").init(2, 0, "i2")
            b.commit("T1", 1, [(1, "a1", "i1", 10)])
            b.commit("T2", 2, [(2, "b2", "i2", 20)])
            t1 = {"txn_id": "T1", "status": "committed", "snapshot_pos": 0,
                  "start_time": 1, "end_time": 2,
                  "operations": [read_op(0, 2, 0, "i2"), put_op(1, 1, 10)]}
            t2 = {"txn_id": "T2", "status": "committed", "snapshot_pos": 1,
                  "start_time": 3, "end_time": 4,
                  "operations": [read_op(0, 1, 10, "a1"), put_op(1, 2, 20)]}
            b.txns = [t2, t1] if swap else [t1, t2]
            return b.doc()

        q = {"commit_pos": 2, "key": 2}
        for doc in (build(False), build(True)):
            self.check(doc, q)
        # reordering transactions only changes evidence locations
        e1 = project(engine_core(build(False), q), keep_locs=False)
        e2 = project(engine_core(build(True), q), keep_locs=False)
        self.assertEqual(e1, e2)


class TruncationTest(OracleCase):
    def doc(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        for i in range(1, 6):
            b.commit(f"T{i}", i, [(i + 1, f"v{i+1}", None, i * 10)])
        for i in range(1, 6):
            b.txn(f"T{i}", i - 1, i, i + 0.5,
                  [read_op(0, i, (i - 1) * 10,
                           f"v{i}" if i > 1 else "i1"),
                   put_op(1, i + 1, i * 10)])
        return b.doc()

    def test_max_depth(self):
        doc = self.doc()
        core = self.check(doc, {"commit_pos": 5, "key": 6}, max_depth=2)
        self.assertEqual(core["status"], "truncated")
        depths = {n["id"]: n["depth"] for n in core["nodes"]}
        self.assertEqual(depths, {"v:v6": 0, "v:v5": 1, "v:v4": 2})
        cursor = core["truncation"]["cursor"]
        self.assertEqual([(c["version_id"], c["reason"]) for c in cursor],
                         [("v4", "max_depth")])
        self.assertEqual(core["truncation"]["reasons"], ["max_depth"])

    def test_max_nodes(self):
        doc = self.doc()
        core = self.check(doc, {"commit_pos": 5, "key": 6}, max_nodes=3)
        self.assertEqual(core["status"], "truncated")
        self.assertEqual(len(core["nodes"]), 3)
        self.assertEqual(core["truncation"]["reasons"], ["max_nodes"])
        deferred = [e for e in core["edges"] if e["deferred"]]
        self.assertTrue(deferred)
        self.assertIsNone(deferred[0]["target"])
        self.assertEqual(deferred[0]["target_version_id"], "v3")

    def test_no_truncation_when_limits_suffice(self):
        doc = self.doc()
        core = self.check(doc, {"commit_pos": 5, "key": 6})
        self.assertEqual(core["status"], "complete")
        self.assertIsNone(core["truncation"])


class UnknownGapTest(OracleCase):
    def test_incomplete_log_marks_result_but_not_recorded_edges(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(2, "b2", None, 20)])
        b.txn("T1", 0, 1, 2, [read_op(0, 1, 0, "i1"), put_op(1, 2, 20)])
        b.log_complete = False
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 2})
        self.assertEqual(core["status"], "unknown")
        self.assertEqual(core["results"][0]["status"], "unknown")
        self.assertIn("log_range_incomplete", core["results"][0]["gaps"])
        # the recorded read edge is still a provable fact
        edge = [e for e in core["edges"] if e["src"] == "v:b2"][0]
        self.assertEqual(edge["status"], "complete")
        self.assertEqual(edge["target"], "v:i1")

    def test_incomplete_scan_marks_predicate_only(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(2, "b2", None, 20)])
        b.txn("T1", 0, 1, 2, [range_op(0, 1, 5, [(1, 0, "i1")], complete=False),
                              put_op(1, 2, 20)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 2})
        self.assertEqual(core["status"], "unknown")
        pred = [e for e in core["edges"] if e["kind"] == "range_predicate"][0]
        self.assertEqual(pred["status"], "unknown")
        self.assertIn("incomplete_scan", pred["gaps"])
        item = [e for e in core["edges"] if e["kind"] == "range_read_item"][0]
        self.assertEqual(item["status"], "complete")

    def test_broken_chain_on_queried_key(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(1, "a1", "no-such-version", 10)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 10)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 1})
        self.assertEqual(core["status"], "unknown")
        self.assertIn("version_chain_broken", core["results"][0]["gaps"])

    def test_missing_operations_marks_node(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(1, "a1", "i1", 10)])
        b.txn("T1", 0, 1, 2, None)  # no operation record
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 1})
        self.assertEqual(core["status"], "unknown")
        node = [n for n in core["nodes"] if n["id"] == "v:a1"][0]
        self.assertEqual(node["status"], "unknown")
        self.assertIn("missing_operations", node["gaps"])
        self.assertIn("write_op_unattributed", node["gaps"])

    def test_unrelated_branch_gap_does_not_pollute(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(1, "a1", "i1", 10)])
        b.commit("T9", 2, [(99, "z99", "no-such-version", 1)])  # broken chain
        b.txn("T1", 0, 1, 2, [read_op(0, 1, 0, "i1"), put_op(1, 1, 10)])
        b.txn("T9", 1, 3, 4, [put_op(0, 99, 1)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 2, "key": 1})
        self.assertEqual(core["status"], "complete")
        self.assertEqual(core["unknowns"], [])

    def test_aborted_and_unknown_version_observed(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commit("T1", 1, [(2, "b2", None, 20)])
        b.txn("T1", 0, 1, 2, [read_op(0, 1, 9, "bad1"),
                              read_op(1, 3, 9, "ghost"),
                              put_op(2, 2, 20)])
        b.txn("TX", None, None, None, None, status="aborted",
              writes=[{"key": 1, "version_id": "bad1", "value": 9}])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 2})
        self.assertEqual(core["status"], "unknown")
        by_key = {e["key"]: e for e in core["edges"]}
        self.assertEqual(by_key[1]["status"], "unknown")
        self.assertIn("aborted_version_observed", by_key[1]["gaps"])
        self.assertIsNone(by_key[1]["target"])
        self.assertEqual(by_key[1]["evidence"]["aborted_txn"], "TX")
        self.assertEqual(by_key[3]["status"], "unknown")
        self.assertIn("unknown_version_observed", by_key[3]["gaps"])
        # the node itself stays complete: only the edges are unprovable
        node = [n for n in core["nodes"] if n["id"] == "v:b2"][0]
        self.assertEqual(node["status"], "complete")


class CycleConflictTest(OracleCase):
    def test_cycle_from_anomalous_reads(self):
        # T1 reads T2's future version of key 2; T2 reads T1's future
        # version of key 1: a version-level observation cycle
        b = HistoryBuilder()
        b.init(1, 0, "i1").init(2, 0, "i2")
        b.commit("T1", 1, [(1, "a1", "i1", 10)])
        b.commit("T2", 2, [(2, "b2", "i2", 20)])
        b.txn("T1", 0, 1, 2, [read_op(0, 2, 20, "b2"), put_op(1, 1, 10)])
        b.txn("T2", 0, 1, 2, [read_op(0, 1, 10, "a1"), put_op(1, 2, 20)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 2, "key": 1})
        self.assertEqual(core["cycles"], [["v:a1", "v:b2"]])
        # evidence is complete (recorded facts); the cycle is reported as-is
        self.assertEqual(core["status"], "complete")
        # finite recursion: each version expanded exactly once (the two
        # anomalous reads reference the committed versions, not the initial
        # ones, so the graph has exactly the two cycle nodes)
        self.assertEqual(len(core["nodes"]), 2)

    def test_conflicting_writes_in_one_commit(self):
        # one commit writes the same key twice with different versions:
        # conflicting evidence about which version is current at pos 1
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        b.commits.append({"txn_id": "T1", "commit_pos": 1, "writes": [
            {"key": 1, "version_id": "a1", "predecessor": "i1", "value": 10},
            {"key": 1, "version_id": "a1b", "predecessor": "i1", "value": 11},
        ]})
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 10)])
        doc = b.doc()
        core = self.check(doc, {"commit_pos": 1, "key": 1})
        self.assertEqual(core["status"], "unknown")
        self.assertIn("key_written_twice_in_commit",
                      core["results"][0]["gaps"])
        # the conflict is reported, not silently resolved into a
        # complete lineage
        self.assertFalse(core["results_complete"])


class LargeSparseTest(OracleCase):
    def test_large_sparse_reachable_subgraph(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        # a short dependency chain on keys 1..9
        for i in range(1, 9):
            b.commit(f"C{i}", i, [(i + 1, f"c{i+1}", None, i * 10)])
        for i in range(1, 9):
            b.txn(f"C{i}", i - 1, i, i + 0.5,
                  [read_op(0, i, (i - 1) * 10, f"c{i}" if i > 1 else "i1"),
                   put_op(1, i + 1, i * 10)])
        # 1500 unrelated transactions, never reachable from the query
        for j in range(1500):
            pos = 9 + j
            b.commit(f"N{j}", pos, [(10000 + j, f"n{j}", None, j)])
            b.txn(f"N{j}", pos - 1, pos, pos + 0.5,
                  [read_op(0, 1, 0, "i1"), put_op(1, 10000 + j, j)])
        doc = b.doc()
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as fh:
            json.dump(doc, fh)
            path = fh.name
        try:
            store = Store()
            try:
                header, commits, txns = load_history(path, None)
                store.ingest(header, commits, txns)
                res = run(store, DUMMY_INPUT, {"commit_pos": 8, "key": 9})
            finally:
                store.close()
        finally:
            os.unlink(path)
        stats = res["stats"]
        # only the reachable subgraph was expanded: 9 versions, not 1509
        self.assertEqual(stats["nodes_created"], 9)
        self.assertEqual(stats["read_ops_examined"], 8)
        self.assertLess(stats["version_lookups"], 40)
        self.assertEqual(res["status"], "complete")
        # and it still matches the oracle exactly
        core = project({k: res[k] for k in (
            "status", "results", "results_complete", "results_gaps", "nodes",
            "edges", "cycles", "unknowns", "truncation")})
        ora = project(oracle_lineage(doc, {"commit_pos": 8, "key": 9}))
        self.assertEqual(core, ora)


if __name__ == "__main__":
    unittest.main()
