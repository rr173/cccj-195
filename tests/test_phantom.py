"""Phantom edges: an empty range read followed by an insert into that range,
and phantoms closing a cycle.  Phantom edges must be derived from actual key
changes in the interval, not only from keys the scan returned."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)


class PhantomTest(AuditCase):
    def test_empty_range_then_insert_phantom_edge(self):
        # T1 scans [10,20] (empty at snapshot 0); T2 inserts key 15 later.
        # A phantom edge T1->T2 must exist even though 15 was never returned.
        b = HistoryBuilder()
        b.init(1, 100, "ia")
        b.commit("T2", 1, [(15, "v15", None, 7)])
        b.txn("T1", 0, 10, 20, [range_op(0, 10, 20, [])])
        b.txn("T2", 0, 11, 21, [put_op(0, 15, 7)])
        result = self.audit(b.doc())
        # T1 -> T2 phantom, no way back: valid, and T1 before T2
        self.assertVerdict(result, "valid")
        order = result["certificate"]["order"]
        self.assertLess(order.index("T1"), order.index("T2"))
        # check the phantom edge was actually generated (via a cyclic variant)
        # -- here we assert on the edge table through a fresh audit
        from tests.helpers import audit_doc
        res2, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        rows = store.db.execute(
            "SELECT src, dst, kind, key, lo, hi FROM edges "
            "WHERE kind='rw'").fetchall()
        self.assertIn(("T1", "T2", "rw", 15, 10, 20), rows)

    def test_phantom_cycle_invalid(self):
        # T1 scans [10,20] empty and writes a=2; T2 reads a=1 and inserts 15.
        # T1->T2 phantom on key 15, T2->T1 anti-dependency on key 1.
        b = HistoryBuilder()
        b.init(1, 1, "ia")
        b.commit("T2", 1, [(15, "v15", None, 7)])
        b.commit("T1", 2, [(1, "a2", "ia", 2)])
        b.txn("T1", 0, 10, 20,
              [range_op(0, 10, 20, []), put_op(1, 1, 2)])
        b.txn("T2", 0, 11, 21,
              [read_op(0, 1, 1, "ia"), put_op(1, 15, 7)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["invalid_reason"], "dependency_cycle")
        cycle = result["graph"]["cycle"]["edges"]
        self.assertEqual(len(cycle), 2)
        m = {(e["src"], e["dst"]): e for e in cycle}
        self.assertEqual(m[("T1", "T2")]["kind"], "rw")
        self.assertEqual(m[("T1", "T2")]["key"], 15)
        self.assertEqual((m[("T1", "T2")]["lo"], m[("T1", "T2")]["hi"]), (10, 20))
        self.assertIsNone(m[("T1", "T2")]["version_id"])  # key did not exist
        self.assertEqual(m[("T1", "T2")]["succ_version_id"], "v15")
        self.assertEqual(m[("T2", "T1")]["key"], 1)
        self.assertFalse(result["bruteforce"]["verdict"])
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])

    def test_phantom_prevented_by_realtime(self):
        # same insert, but the scanner commits before the inserter starts and
        # the inserter's snapshot includes the scan nothing... real time
        # aligns with the phantom edge: valid.
        b = HistoryBuilder()
        b.init(1, 100, "ia")
        b.commit("T2", 1, [(15, "v15", None, 7)])
        b.txn("T1", 0, 10, 12, [range_op(0, 10, 20, [])])
        b.txn("T2", 0, 20, 22, [put_op(0, 15, 7)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])

    def test_phantom_on_previously_deleted_key(self):
        # key 15 existed, was deleted before T1's snapshot; T1 scans it as
        # absent; T3 re-inserts it afterwards: phantom edge T1->T3.
        b = HistoryBuilder()
        b.commit("T0", 1, [(15, "v15a", None, 5)])
        b.commit("T0b", 2, [(15, "v15b", "v15a", TOMBSTONE)])
        b.commit("T3", 3, [(15, "v15c", "v15b", 9)])
        b.txn("T0", 0, 1, 2, [put_op(0, 15, 5)])
        b.txn("T0b", 1, 3, 4, [del_op(0, 15)])
        b.txn("T1", 2, 10, 20, [range_op(0, 10, 20, [])])
        b.txn("T3", 2, 11, 21, [put_op(0, 15, 9)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "valid")
        order = result["certificate"]["order"]
        self.assertLess(order.index("T1"), order.index("T3"))
        self.assertLess(order.index("T0b"), order.index("T1"))

    def test_insert_outside_interval_no_phantom(self):
        # insert at key 21 is outside [10,20]: no phantom, order free
        b = HistoryBuilder()
        b.commit("T2", 1, [(21, "v21", None, 7)])
        b.txn("T1", 0, 10, 20, [range_op(0, 10, 20, [])])
        b.txn("T2", 0, 11, 21, [put_op(0, 21, 7)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "valid")
        from tests.helpers import audit_doc
        res2, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        rows = store.db.execute(
            "SELECT src, dst FROM edges WHERE kind='rw'").fetchall()
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
