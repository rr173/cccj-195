"""Closed-interval range semantics at the endpoints, incl. int64 extremes."""

import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, put_op, range_op, read_op,
)

I64MIN = -(2**63)
I64MAX = 2**63 - 1


class RangeBoundsTest(AuditCase):
    def _builder_with_boundary_keys(self):
        # keys around the [10,20] interval: 9, 10, 20, 21
        b = HistoryBuilder()
        b.commit("T1", 1, [(9, "v9", None, 90)])
        b.commit("T2", 2, [(10, "v10", None, 100)])
        b.commit("T3", 3, [(20, "v20", None, 200)])
        b.commit("T4", 4, [(21, "v21", None, 210)])
        b.txn("T1", 0, 1, 2, [put_op(0, 9, 90)])
        b.txn("T2", 1, 3, 4, [put_op(0, 10, 100)])
        b.txn("T3", 2, 5, 6, [put_op(0, 20, 200)])
        b.txn("T4", 3, 7, 8, [put_op(0, 21, 210)])
        return b

    def test_closed_interval_includes_both_endpoints(self):
        b = self._builder_with_boundary_keys()
        b.txn("R", 4, 10, 20,
              [range_op(0, 10, 20, [(10, 100, "v10"), (20, 200, "v20")])])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_missing_endpoint_key_is_anomaly(self):
        b = self._builder_with_boundary_keys()
        # omits key 20 (the closed-interval upper endpoint)
        b.txn("R", 4, 10, 20, [range_op(0, 10, 20, [(10, 100, "v10")])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        a = result["anomalies"][0]
        self.assertEqual(a["kind"], "missing_key")
        self.assertEqual(a["key"], 20)

    def test_extra_key_just_outside_endpoint_is_anomaly(self):
        b = self._builder_with_boundary_keys()
        # wrongly includes key 21 (outside the closed interval)
        b.txn("R", 4, 10, 20,
              [range_op(0, 10, 20,
                        [(10, 100, "v10"), (20, 200, "v20"), (21, 210, "v21")])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "invalid")
        kinds = {a["kind"] for a in result["anomalies"]}
        self.assertIn("extra_key", kinds)
        keys = {a["key"] for a in result["anomalies"]}
        self.assertIn(21, keys)

    def test_int64_extreme_keys(self):
        b = HistoryBuilder()
        b.commit("T1", 1, [(I64MIN, "vmin", None, 1)])
        b.commit("T2", 2, [(I64MAX, "vmax", None, 2)])
        b.txn("T1", 0, 1, 2, [put_op(0, I64MIN, 1)])
        b.txn("T2", 1, 3, 4, [put_op(0, I64MAX, 2)])
        b.txn("R", 2, 10, 20,
              [range_op(0, I64MIN, I64MAX,
                        [(I64MIN, 1, "vmin"), (I64MAX, 2, "vmax")])])
        result = self.audit(b.doc(), want_bruteforce=True)
        self.assertVerdict(result, "valid")

    def test_full_domain_range_does_not_expand_domain(self):
        # a whole-domain scan must cost O(versions), not O(int64):
        # the number of scanned keys is proportional to the 4 versioned
        # keys, across the indexed passes (read verification, edge
        # generation, replay certificate: visible-set + key probes)
        b = self._builder_with_boundary_keys()
        b.txn("R", 4, 10, 20,
              [range_op(0, I64MIN, I64MAX,
                        [(9, 90, "v9"), (10, 100, "v10"),
                         (20, 200, "v20"), (21, 210, "v21")])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "valid")
        self.assertEqual(result["stats"]["range_keys_scanned"], 4 * 4)

    def test_single_point_interval(self):
        b = self._builder_with_boundary_keys()
        b.txn("R", 4, 10, 20, [range_op(0, 10, 10, [(10, 100, "v10")])])
        result = self.audit(b.doc())
        self.assertVerdict(result, "valid")


if __name__ == "__main__":
    unittest.main()
