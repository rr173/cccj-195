"""Tombstones: delete then recreate; readers at different snapshots;
same value carried by different versions."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)
from tests.helpers import audit_doc


class TombstoneTest(AuditCase):
    def _recreate_builder(self):
        # key 1: v1=put A (pos1), v2=tombstone (pos2), v3=put B (pos3)
        b = HistoryBuilder()
        b.commit("T1", 1, [(1, "v1", None, "A")])
        b.commit("T2", 2, [(1, "v2", "v1", TOMBSTONE)])
        b.commit("T3", 3, [(1, "v3", "v2", "B")])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, "A")])
        b.txn("T2", 1, 3, 4, [del_op(0, 1)])
        b.txn("T3", 2, 5, 6, [put_op(0, 1, "B")])
        return b

    def test_readers_around_tombstone_and_recreate(self):
        b = self._recreate_builder()
        # R1 at snapshot 1 sees A/v1; R2 at snapshot 2 sees absent;
        # R3 at snapshot 3 sees B/v3.  Reader windows overlap the writers
        # they conflict with, so no real-time edge contradicts the reads.
        b.txn("R1", 1, 3.5, 10, [read_op(0, 1, "A", "v1")])
        b.txn("R2", 2, 5.5, 20, [read_op(0, 1, found=False)])
        b.txn("R3", 3, 6.5, 30, [read_op(0, 1, "B", "v3")])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")
        self.assertTrue(result["bruteforce"]["agrees_with_graph"])

    def test_range_excludes_tombstone_includes_recreated(self):
        b = self._recreate_builder()
        b.txn("R2", 2, 5.5, 20, [range_op(0, 0, 10, [])])
        b.txn("R3", 3, 6.5, 30, [range_op(0, 0, 10, [(1, "B", "v3")])])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_read_of_tombstone_version_as_present_is_anomaly(self):
        b = self._recreate_builder()
        # claims to see the tombstone version v2 as a present value
        b.txn("R2", 2, 12, 13, [read_op(0, 1, "A", "v2")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "stale_version_read")

    def test_read_deleted_key_as_old_version_is_stale(self):
        b = self._recreate_builder()
        # at snapshot 2 the key is deleted; observing old v1 is stale
        b.txn("R2", 2, 12, 13, [read_op(0, 1, "A", "v1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "stale_version_read")

    def test_missing_key_after_delete_reported(self):
        b = self._recreate_builder()
        # at snapshot 3 the key is visible as B/v3; a range that omits it
        # is a confirmed missing_key
        b.txn("R3", 3, 14, 15, [range_op(0, 0, 10, [])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        a = result["anomalies"][0]
        self.assertEqual(a["kind"], "missing_key")
        self.assertEqual(a["key"], 1)
        self.assertEqual(a["expected"]["version_id"], "v3")

    def test_absence_read_orders_deleter_before_reader(self):
        # reader sees the key absent at snapshot 2; replayer must place the
        # deleter T2 before the reader (wr edge on the tombstone).  The
        # reader sits between T2 and T3 in real time, which aligns with the
        # dependency edges.
        b = self._recreate_builder()
        b.txn("R2", 2, 4.5, 4.8, [read_op(0, 1, found=False)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "valid")
        res2, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        rows = store.db.execute(
            "SELECT src, dst, kind, key FROM edges WHERE kind='wr'").fetchall()
        self.assertIn(("T2", "R2", "wr", 1), rows)


class SameValueVersionsTest(AuditCase):
    def test_same_value_different_versions(self):
        # v1 and v2 carry the same value 5; the version identity matters
        b = HistoryBuilder()
        b.commit("T1", 1, [(1, "v1", None, 5)])
        b.commit("T2", 2, [(1, "v2", "v1", 5)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 5)])
        b.txn("T2", 1, 3, 4, [put_op(0, 1, 5)])
        # reader at snapshot 2 must observe v2
        b.txn("R", 2, 10, 11, [read_op(0, 1, 5, "v2")])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_same_value_wrong_version_is_stale(self):
        b = HistoryBuilder()
        b.commit("T1", 1, [(1, "v1", None, 5)])
        b.commit("T2", 2, [(1, "v2", "v1", 5)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 5)])
        b.txn("T2", 1, 3, 4, [put_op(0, 1, 5)])
        # same value 5 but claims the superseded version v1 at snapshot 2
        b.txn("R", 2, 10, 11, [read_op(0, 1, 5, "v1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        a = result["anomalies"][0]
        self.assertEqual(a["kind"], "stale_version_read")
        self.assertEqual(a["expected"]["version_id"], "v2")
        self.assertEqual(a["observed"]["version_id"], "v1")

    def test_same_value_range_result_wrong_version(self):
        b = HistoryBuilder()
        b.commit("T1", 1, [(1, "v1", None, 5)])
        b.commit("T2", 2, [(1, "v2", "v1", 5)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 5)])
        b.txn("T2", 1, 3, 4, [put_op(0, 1, 5)])
        b.txn("R", 2, 10, 11, [range_op(0, 0, 9, [(1, 5, "v1")])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "stale_version_read")


if __name__ == "__main__":
    unittest.main()
