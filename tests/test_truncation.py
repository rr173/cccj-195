"""Conflict-evidence truncation: when anomalies exceed the report budget the
listing is truncated, but totals stay exact and the verdict is unaffected."""

import unittest

from tests.helpers import AuditCase, HistoryBuilder, put_op, read_op


class TruncationTest(AuditCase):
    def _many_bad_reads(self, n):
        b = HistoryBuilder()
        for k in range(n):
            b.init(k, 0, f"i{k}")
        ops = [read_op(i, i, 999) for i in range(n)]  # all wrong values
        b.txn("R", 0, 10, 20, ops)
        return b.doc()

    def test_anomaly_list_truncated_with_exact_total(self):
        result = self.audit(self._many_bad_reads(50), max_report=10)
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies_total"], 50)
        self.assertEqual(len(result["anomalies"]), 10)
        self.assertTrue(result["anomalies_truncated"])
        for a in result["anomalies"]:
            self.assertEqual(a["kind"], "value_mismatch")

    def test_no_truncation_under_budget(self):
        result = self.audit(self._many_bad_reads(5), max_report=10)
        self.assertVerdict(result, "invalid")
        self.assertEqual(result["anomalies_total"], 5)
        self.assertEqual(len(result["anomalies"]), 5)
        self.assertFalse(result["anomalies_truncated"])

    def test_cycle_witness_never_truncated(self):
        # many conflicts plus a cycle: the cycle witness stays complete
        b = HistoryBuilder()
        b.init(1, 0, "ix").init(2, 0, "iy")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(2, "y1", "iy", 1)])
        b.txn("T1", 0, 10, 20, [read_op(0, 2, 0, "iy"), put_op(1, 1, 1)])
        b.txn("T2", 0, 11, 21, [read_op(0, 1, 0, "ix"), put_op(1, 2, 1)])
        result = self.audit(b.doc(), max_report=1)
        self.assertVerdict(result, "invalid")
        self.assertEqual(len(result["graph"]["cycle"]["edges"]), 2)


if __name__ == "__main__":
    unittest.main()
