"""Own-write-then-read must be valid and must not create self loops."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)
from tests.helpers import audit_doc


class OwnWriteReadTest(AuditCase):
    def test_put_then_point_read_own_key(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 42)])
        b.txn("T1", 0, 10, 20, [put_op(0, 1, 42), read_op(1, 1, 42)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])

    def test_put_then_read_wrong_value_is_anomaly(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 42)])
        b.txn("T1", 0, 10, 20, [put_op(0, 1, 42), read_op(1, 1, 41)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        a = result["anomalies"][0]
        self.assertEqual(a["kind"], "value_mismatch")
        self.assertEqual(a["expected"]["source"], "own_write")
        self.assertEqual(a["op_seq"], 1)

    def test_delete_then_read_own_key_absent(self):
        b = HistoryBuilder()
        b.init(1, 7, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", TOMBSTONE)])
        b.txn("T1", 0, 10, 20, [del_op(0, 1), read_op(1, 1, found=False)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_delete_then_read_own_key_present_is_anomaly(self):
        b = HistoryBuilder()
        b.init(1, 7, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", TOMBSTONE)])
        b.txn("T1", 0, 10, 20, [del_op(0, 1), read_op(1, 1, 7, "ix")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "value_mismatch")

    def test_own_write_inside_range(self):
        # T1 inserts key 15 then scans [10,20]: the scan must observe 15,
        # and no phantom self-edge may appear.
        b = HistoryBuilder()
        b.commit("T1", 1, [(15, "v15", None, 5)])
        b.txn("T1", 0, 10, 20,
              [put_op(0, 15, 5), range_op(1, 10, 20, [(15, 5, None)])])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")
        res2, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        self.assertEqual(store.db.execute(
            "SELECT COUNT(*) FROM edges WHERE src=dst").fetchone()[0], 0)
        self.assertEqual(store.db.execute(
            "SELECT COUNT(*) FROM edges WHERE kind='rw'").fetchone()[0], 0)

    def test_own_delete_hides_key_from_own_range(self):
        b = HistoryBuilder()
        b.init(15, 5, "i15")
        b.commit("T1", 1, [(15, "v15", "i15", TOMBSTONE)])
        b.txn("T1", 0, 10, 20, [del_op(0, 15), range_op(1, 10, 20, [])])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_scan_before_own_insert_still_phantom_free_self(self):
        # scan first, then insert into the scanned range: the insert is the
        # txn's own successor version - must not become a self anti-edge.
        b = HistoryBuilder()
        b.commit("T1", 1, [(15, "v15", None, 5)])
        b.txn("T1", 0, 10, 20,
              [range_op(0, 10, 20, []), put_op(1, 15, 5)])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")
        res2, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        self.assertEqual(store.db.execute(
            "SELECT COUNT(*) FROM edges WHERE src=dst").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
