"""verify-witness re-checks witnesses from the raw history without trusting
the auditor's stored conclusions: valid witnesses pass, tampered ones fail."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from tests.helpers import (
    AuditCase, HistoryBuilder, TOMBSTONE, audit_doc, put_op, range_op,
    read_op,
)
from snapshot_audit.store import Store
from snapshot_audit.model import load_history
from snapshot_audit.witness import (
    verify_certificate, verify_cycle_witness,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE = os.path.join(ROOT, "samples", "write_skew.json")


def _cli(args, cwd=ROOT):
    env = dict(os.environ)
    return subprocess.run(
        [sys.executable, "-m", "snapshot_audit"] + args,
        capture_output=True, text=True, cwd=cwd, env=env)


class VerifyWitnessCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, name, obj):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w") as fh:
            json.dump(obj, fh)
        return path

    def _audit(self, doc, name="h.json"):
        hpath = self._write(name, doc)
        wpath = os.path.join(self.tmp.name, "w.json")
        r = _cli(["audit", "--input", hpath, "--out", wpath])
        self.assertIn(r.returncode, (0, 1, 2), r.stderr)
        with open(wpath) as fh:
            return hpath, json.load(fh)

    def test_cycle_witness_accepted(self):
        r = _cli(["audit", "--input", SAMPLE, "--out",
                  os.path.join(self.tmp.name, "w.json")])
        self.assertEqual(r.returncode, 1, r.stderr)
        v = _cli(["verify-witness", "--input", SAMPLE, "--witness",
                  os.path.join(self.tmp.name, "w.json")])
        self.assertEqual(v.returncode, 0, v.stderr + v.stdout)
        self.assertIn('"verified": true', v.stdout)

    def test_tampered_cycle_edge_rejected(self):
        wpath = os.path.join(self.tmp.name, "w.json")
        _cli(["audit", "--input", SAMPLE, "--out", wpath])
        with open(wpath) as fh:
            witness = json.load(fh)
        # corrupt one edge: point the anti-dependency at the wrong key
        witness["graph"]["cycle"]["edges"][0]["key"] = 999
        bad = self._write("bad.json", witness)
        v = _cli(["verify-witness", "--input", SAMPLE, "--witness", bad])
        self.assertEqual(v.returncode, 1, v.stdout)
        self.assertIn('"verified": false', v.stdout)

    def test_tampered_cycle_direction_rejected(self):
        wpath = os.path.join(self.tmp.name, "w.json")
        _cli(["audit", "--input", SAMPLE, "--out", wpath])
        with open(wpath) as fh:
            witness = json.load(fh)
        for e in witness["graph"]["cycle"]["edges"]:
            e["src"], e["dst"] = e["dst"], e["src"]
        bad = self._write("bad2.json", witness)
        v = _cli(["verify-witness", "--input", SAMPLE, "--witness", bad])
        self.assertEqual(v.returncode, 1, v.stdout)

    def test_certificate_witness_accepted_and_tamper_rejected(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("R", 1, 10, 20, [read_op(0, 1, 1, "x1")])
        hpath, witness = self._audit(b.doc())
        self.assertEqual(witness["verdict"], "valid")
        wpath = self._write("cert.json", witness)
        v = _cli(["verify-witness", "--input", hpath, "--witness", wpath])
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)
        # tamper: reverse the order -> replay must fail
        witness["certificate"]["order"] = list(
            reversed(witness["certificate"]["order"]))
        bad = self._write("cert_bad.json", witness)
        v2 = _cli(["verify-witness", "--input", hpath, "--witness", bad])
        self.assertEqual(v2.returncode, 1, v2.stdout)

    def test_anomaly_witness_rechecked(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("R", 0, 10, 20, [read_op(0, 1, 1, "x1")])  # future version
        hpath, witness = self._audit(b.doc())
        self.assertEqual(witness["verdict"], "invalid")
        wpath = self._write("anom.json", witness)
        v = _cli(["verify-witness", "--input", hpath, "--witness", wpath])
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)
        # fabricate an anomaly that is not there
        witness["anomalies"] = [{
            "kind": "future_version_read", "txn_id": "R", "op_seq": 0,
            "key": 1, "observed": {"version_id": "ix"}, "loc": "x"}]
        bad = self._write("anom_bad.json", witness)
        v2 = _cli(["verify-witness", "--input", hpath, "--witness", bad])
        self.assertEqual(v2.returncode, 1, v2.stdout)

    def test_unknown_verdict_witness_inconclusive(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.log_complete = False
        b.txn("R", 0, 10, 20, [read_op(0, 1, 0, "ix")])
        hpath, witness = self._audit(b.doc())
        self.assertEqual(witness["verdict"], "unknown")
        wpath = self._write("unk.json", witness)
        v = _cli(["verify-witness", "--input", hpath, "--witness", wpath])
        self.assertEqual(v.returncode, 2, v.stdout + v.stderr)


class VerifyWitnessUnitTest(AuditCase):
    def test_cycle_witness_checks_each_edge_independently(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix").init(2, 0, "iy")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(2, "y1", "iy", 1)])
        b.txn("T1", 0, 10, 20, [read_op(0, 2, 0, "iy"), put_op(1, 1, 1)])
        b.txn("T2", 0, 11, 21, [read_op(0, 1, 0, "ix"), put_op(1, 2, 1)])
        result, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        cycle = result["graph"]["cycle"]["edges"]
        self.assertEqual(verify_cycle_witness(store, cycle), [])
        # break the read_version of one edge
        broken = [dict(e) for e in cycle]
        broken[0]["version_id"] = "y1"
        problems = verify_cycle_witness(store, broken)
        self.assertTrue(problems)
        # break closure
        broken2 = [dict(e) for e in cycle]
        broken2[0]["dst"] = "T1"
        problems2 = verify_cycle_witness(store, broken2)
        self.assertTrue(problems2)

    def test_certificate_rechecks_version_order_and_realtime(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(1, "x2", "x1", 2)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("T2", 1, 3, 4, [put_op(0, 1, 2)])
        result, store = audit_doc(b.doc())
        self.addCleanup(store.close)
        self.assertEqual(result["verdict"], "valid")
        order = result["certificate"]["order"]
        self.assertEqual(verify_certificate(store, order), [])
        # order violating the declared version order must be rejected
        bad = [t for t in order]
        i1, i2 = bad.index("T1"), bad.index("T2")
        bad[i1], bad[i2] = bad[i2], bad[i1]
        problems = verify_certificate(store, bad)
        self.assertTrue(any("version order" in p for p in problems))


if __name__ == "__main__":
    unittest.main()
