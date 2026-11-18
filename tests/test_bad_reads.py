"""Dirty / future / unknown-version reads must be reported as their own
classes, never conflated with generic value differences; and some classes
are confirmable even when the log is incomplete."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, put_op, range_op, read_op,
)


class DirtyFutureReadTest(AuditCase):
    def test_future_version_read_point(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        # R's snapshot is 0, but it claims the version committed at pos 1
        b.txn("R", 0, 10, 20, [read_op(0, 1, 1, "x1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        a = result["anomalies"][0]
        self.assertEqual(a["kind"], "future_version_read")
        self.assertEqual(a["observed"]["version_id"], "x1")
        self.assertEqual(a["expected"]["version_id"], "ix")

    def test_future_version_read_inside_range(self):
        b = HistoryBuilder()
        b.init(5, 0, "i5")
        b.commit("T1", 1, [(5, "v5", "i5", 9)])
        b.txn("T1", 0, 1, 2, [put_op(0, 5, 9)])
        b.txn("R", 0, 10, 20, [range_op(0, 0, 10, [(5, 9, "v5")])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        kinds = {a["kind"] for a in result["anomalies"]}
        self.assertIn("future_version_read", kinds)

    def test_aborted_version_read(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        # Tx aborted after writing key 1 = 99; its version must never be read
        b.txn("Tx", 0, 1, 2, [], status="aborted",
              writes=[{"key": 1, "version_id": "bad1", "value": 99}])
        b.txn("R", 0, 10, 20, [read_op(0, 1, 99, "bad1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        a = result["anomalies"][0]
        self.assertEqual(a["kind"], "aborted_version_read")
        self.assertIn("Tx", a["detail"])

    def test_aborted_txn_writes_are_not_versions(self):
        # aborted writes must not appear in the commit graph / replay state
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.txn("Tx", 0, 1, 2, [], status="aborted",
              writes=[{"key": 1, "version_id": "bad1", "value": 99}])
        b.txn("R", 0, 10, 20, [read_op(0, 1, 0, "ix")])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_unknown_version_read_complete_log(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.txn("R", 0, 10, 20, [read_op(0, 1, 5, "ghost")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "unknown_version_read")

    def test_unknown_version_read_incomplete_log_is_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.log_complete = False
        b.txn("R", 0, 10, 20, [read_op(0, 1, 5, "ghost")])
        result = self.audit(b.doc())
        # log incomplete: the ghost version might live in the missing part
        self.assertVerdict(result, "unknown")
        self.assertEqual(result["anomalies_total"], 0)
        gap_kinds = {g["kind"] for g in result["gaps"]}
        self.assertIn("log_range_incomplete", gap_kinds)
        self.assertIn("unverifiable_read", gap_kinds)

    def test_future_read_still_invalid_with_incomplete_log(self):
        # a version known to be committed after the snapshot is unreadable
        # no matter what else is missing from the log
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.log_complete = False
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("R", 0, 10, 20, [read_op(0, 1, 1, "x1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "future_version_read")

    def test_aborted_read_still_invalid_with_incomplete_log(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.log_complete = False
        b.txn("Tx", 0, 1, 2, [], status="aborted",
              writes=[{"key": 1, "version_id": "bad1", "value": 99}])
        b.txn("R", 0, 10, 20, [read_op(0, 1, 99, "bad1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "aborted_version_read")

    def test_cross_key_version_read(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.init(2, 0, "iy")
        # version "iy" belongs to key 2, observed for key 1
        b.txn("R", 0, 10, 20, [read_op(0, 1, 0, "iy")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "cross_key_version_read")

    def test_value_mismatch_same_version(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        # right version id, wrong value
        b.txn("R", 0, 10, 20, [read_op(0, 1, 7, "ix")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies"][0]["kind"], "value_mismatch")

    def test_json_scalar_values(self):
        b = HistoryBuilder()
        b.init(1, "hello", "is")
        b.init(2, True, "ib")
        b.init(3, None, "in")
        b.init(4, 2.5, "if")
        b.txn("R", 0, 10, 20, [
            read_op(0, 1, "hello", "is"),
            read_op(1, 2, True, "ib"),
            read_op(2, 3, None, "in", found=True),
            read_op(3, 4, 2.5, "if"),
        ])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")


if __name__ == "__main__":
    unittest.main()
