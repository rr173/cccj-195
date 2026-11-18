"""Missing evidence must yield 'unknown', never 'valid' - and never be
reported as 'no anomaly'."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)


class ContractGapTest(AuditCase):
    def test_incomplete_log_range_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.log_complete = False
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("R", 1, 10, 20, [read_op(0, 1, 1, "x1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("log_range_incomplete",
                      {g["kind"] for g in result["gaps"]})

    def test_log_range_pos_gap_unknown(self):
        # declared complete [1,3] but commit_pos 2 is missing
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T3", 3, [(1, "x3", "x1", 3)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("T3", 1, 3, 4, [put_op(0, 1, 3)])
        b.txn("R", 3, 10, 20, [read_op(0, 1, 3, "x3")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("log_range_gap", {g["kind"] for g in result["gaps"]})

    def test_missing_operations_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        # op seq jumps 0 -> 2: an operation is missing
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1), {"seq": 2, "type": "point_read",
                                                "key": 1, "found": True,
                                                "value": 1}])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("missing_operations", {g["kind"] for g in result["gaps"]})

    def test_unfinished_scan_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.txn("R", 0, 10, 20, [range_op(0, 0, 10, [(1, 0, "ix")], complete=False)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("incomplete_scan", {g["kind"] for g in result["gaps"]})

    def test_broken_version_chain_unknown(self):
        b = HistoryBuilder()
        b.commit("T1", 1, [(1, "x1", "no-such-version", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("R", 1, 10, 20, [read_op(0, 1, 1, "x1")])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("version_chain_broken", {g["kind"] for g in result["gaps"]})

    def test_duplicate_commit_pos_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 1, [(2, "y1", None, 2)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("T2", 0, 1, 2, [put_op(0, 2, 2)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("duplicate_commit_pos",
                      {g["kind"] for g in result["gaps"]})

    def test_committed_writes_missing_from_log_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        # T1 has a put op but no commit record
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("committed_writes_missing_from_log",
                      {g["kind"] for g in result["gaps"]})

    def test_key_written_twice_in_txn_unknown(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1), put_op(1, 1, 2)])
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertIn("key_written_twice_in_txn",
                      {g["kind"] for g in result["gaps"]})

    def test_no_false_valid_on_empty_history(self):
        b = HistoryBuilder()
        result = self.audit(b.doc())
        # empty but fully declared history is trivially valid
        self.assertVerdict(result, "valid")

    def test_unknown_never_reports_no_anomaly_as_valid(self):
        # a suspicious read with incomplete evidence: not an anomaly,
        # but also not proof of validity
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.log_complete = False
        b.txn("R", 0, 10, 20, [read_op(0, 1, 42)])  # no version id recorded
        result = self.audit(b.doc())
        self.assertVerdict(result, "unknown")
        self.assertEqual(result["anomalies_total"], 0)
        self.assertGreater(result["gaps_total"], 0)


if __name__ == "__main__":
    unittest.main()
