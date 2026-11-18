"""Hand-written write skew: T1 reads y=0 writes x=1, T2 reads x=0 writes y=1,
both on the same old snapshot -> bidirectional anti-dependency cycle."""

import json
import os
import unittest

from tests.helpers import AuditCase, HistoryBuilder, TOMBSTONE, put_op, read_op

SAMPLE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "samples", "write_skew.json")


class WriteSkewTest(AuditCase):
    def test_sample_file_write_skew(self):
        import json as j
        with open(SAMPLE) as fh:
            doc = j.load(fh)
        result = self.audit(doc, want_bruteforce=True)
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["invalid_reason"], "dependency_cycle")
        cycle = result["graph"]["cycle"]
        self.assertTrue(cycle["independently_verified"])
        self.assertTrue(cycle["minimal"])
        edges = cycle["edges"]
        self.assertEqual(len(edges), 2)
        kinds = {e["kind"] for e in edges}
        self.assertEqual(kinds, {"rw"})
        # T1 -> T2 on key 2 (y), T2 -> T1 on key 1 (x)
        m = {(e["src"], e["dst"]): e for e in edges}
        self.assertEqual(m[("T1", "T2")]["key"], 2)
        self.assertEqual(m[("T1", "T2")]["version_id"], "iy")
        self.assertEqual(m[("T1", "T2")]["succ_version_id"], "y1")
        self.assertEqual(m[("T2", "T1")]["key"], 1)
        self.assertEqual(m[("T2", "T1")]["succ_version_id"], "x1")
        # every edge carries operation and original-location evidence
        for e in edges:
            self.assertIn("loc", e["evidence"]["read"])
            self.assertIn("succ_loc", e["evidence"])
        # brute force agrees
        self.assertFalse(result["bruteforce"]["verdict"])
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])

    def test_skew_broken_by_realtime_is_valid(self):
        # T2 starts after T1 ended AND its snapshot includes T1's commit:
        # read-from, anti-dependency and real-time all align T1 -> T2.
        b = HistoryBuilder()
        b.init(1, 0, "ix").init(2, 0, "iy")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(2, "y1", "iy", 1)])
        b.txn("T1", 0, 10, 12, [read_op(0, 2, 0, "iy"), put_op(1, 1, 1)])
        b.txn("T2", 1, 20, 22, [read_op(0, 1, 1, "x1"), put_op(1, 2, 1)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")
        cert = result["certificate"]
        self.assertEqual(cert["order"][0], "@initial")
        self.assertLess(cert["order"].index("T1"), cert["order"].index("T2"))
        self.assertTrue(result["bruteforce"]["verdict"])
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])

    def test_stale_snapshot_despite_realtime_is_invalid(self):
        # T2 starts after T1 ended, yet its snapshot predates T1's commit:
        # real-time T1->T2 and anti-dependency T2->T1 form a cycle.
        b = HistoryBuilder()
        b.init(1, 0, "ix").init(2, 0, "iy")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(2, "y1", "iy", 1)])
        b.txn("T1", 0, 10, 12, [read_op(0, 2, 0, "iy"), put_op(1, 1, 1)])
        b.txn("T2", 0, 20, 22, [read_op(0, 1, 0, "ix"), put_op(1, 2, 1)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "invalid")
        kinds = [e["kind"] for e in result["graph"]["cycle"]["edges"]]
        self.assertIn("realtime", kinds)
        self.assertFalse(result["bruteforce"]["verdict"])

    def test_realtime_edge_can_close_cycle(self):
        # T1 -> T2 anti-dependency; T2 ended before T3 started and T3 -> T1
        # anti-dependency: three-node cycle through a real-time edge
        b = HistoryBuilder()
        b.init(1, 0, "ix").init(2, 0, "iy").init(3, 0, "iz")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(2, "y1", "iy", 1)])
        b.commit("T3", 3, [(3, "z1", "iz", 1)])
        # T1 reads y (succ T2)  => T1->T2
        b.txn("T1", 0, 10, 20, [read_op(0, 2, 0, "iy"), put_op(1, 1, 1)])
        # T2 reads nothing relevant; ends at 12
        b.txn("T2", 0, 5, 12, [put_op(0, 2, 1)])
        # T3 starts at 13 (after T2 ended): T2->T3 realtime; T3 reads x (succ T1)
        b.txn("T3", 0, 13, 30, [read_op(0, 1, 0, "ix"), put_op(1, 3, 1)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "invalid")
        kinds = [e["kind"] for e in result["graph"]["cycle"]["edges"]]
        self.assertIn("realtime", kinds)
        self.assertIn("rw", kinds)
        self.assertFalse(result["bruteforce"]["verdict"])
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])


if __name__ == "__main__":
    unittest.main()
