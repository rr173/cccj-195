"""Memory-budget fallback: when the graph cannot be held within the declared
budget the verdict must be unknown, never 'valid' by omission."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, put_op, range_op, read_op,
)


class MemoryBudgetTest(AuditCase):
    def _valid_history(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("R", 1, 10, 20, [read_op(0, 1, 1, "x1")])
        return b.doc()

    def test_zero_budget_yields_unknown_not_valid(self):
        result = self.audit(self._valid_history(), memory_budget_mb=0)
        self.assertVerdict(result, "unknown")
        kinds = {g["kind"] for g in result["gaps"]}
        self.assertIn("graph_exceeds_memory_budget", kinds)
        # crucially, "no cycle found" was not reported as valid
        self.assertIsNone(result["certificate"])

    def test_generous_budget_recovers_valid(self):
        result = self.audit(self._valid_history(), memory_budget_mb=512)
        self.assertVerdict(result, "valid")
        self.assertIsNotNone(result["certificate"])


if __name__ == "__main__":
    unittest.main()
