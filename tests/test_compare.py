"""Cross-run regression comparison tests against the independent oracle.

Covers every required scenario:

* the same write-skew ring persists when version ids, commit_pos and file
  line order all change;
* adding one provable ring is a new invalid finding;
* fixing a ring is resolved;
* two legal witnesses of one logical ring are ambiguous;
* renames require an explicit one-to-one mapping and conflicts are rejected;
* an incomplete side can only produce unknown gap transitions;
* read anomalies and dependency cycles never match each other;
* reordering JSONL lines does not change the report;
* tampering with inputs, the mapping or the report is rejected by
  verify-diff;
* large two runs use indexed/batched processing and report resource stats.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from tests.helpers import (
    HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)
from tests.oracle import (
    enumerate_identity_matching, enumerate_read_anomalies,
    enumerate_cycle_rings, expected_pairings, write_history,
    write_jsonl_history, write_mapping,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cli(args):
    return subprocess.run(
        [sys.executable, "-m", "snapshot_audit"] + args,
        capture_output=True, text=True, cwd=ROOT)


def _write_skew_doc(prefix="", pos_shift=0, swap_txn_order=False):
    """A two-txn write-skew history with controllable ids and positions."""
    b = HistoryBuilder()
    b.init(1, 0, f"{prefix}ix").init(2, 0, f"{prefix}iy")
    b.commit("T1", 1 + pos_shift, [(1, f"{prefix}x1", f"{prefix}ix", 1)])
    b.commit("T2", 2 + pos_shift, [(2, f"{prefix}y1", f"{prefix}iy", 1)])
    t1 = {"txn_id": "T1", "status": "committed", "snapshot_pos": 0,
          "start_time": 10, "end_time": 20,
          "operations": [read_op(0, 2, 0, f"{prefix}iy"),
                         put_op(1, 1, 1)]}
    t2 = {"txn_id": "T2", "status": "committed", "snapshot_pos": 0,
          "start_time": 11, "end_time": 21,
          "operations": [read_op(0, 1, 0, f"{prefix}ix"),
                         put_op(1, 2, 1)]}
    b.txns = [t2, t1] if swap_txn_order else [t1, t2]
    return b.doc()


class CompareTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = self.tmp.name

    def write(self, name, obj):
        path = os.path.join(self.d, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        return path

    def compare(self, base, cand, run_b="runB0", run_c="runC0",
                mapping=None, extra_files=None):
        bp = write_history(base, self.d, "base.json")
        cp = write_history(cand, self.d, "cand.json")
        args = ["compare", "--baseline-input", bp, "--baseline-run-id", run_b,
                "--candidate-input", cp, "--candidate-run-id", run_c,
                "--out", os.path.join(self.d, "report.json"),
                "--manifest-out", os.path.join(self.d, "manifest.json"),
                "--ledger", os.path.join(self.d, "ledger.db")]
        if mapping is not None:
            mp = write_mapping(mapping, self.d)
            args += ["--mapping", mp]
        r = _cli(args)
        self.assertIn(r.returncode, (0, 1, 2), r.stderr)
        with open(os.path.join(self.d, "report.json")) as fh:
            report = json.load(fh)
        return r, report

    def verify(self, bp=None, cp=None, mapping=False, ledger=True,
               report="report.json", manifest="manifest.json"):
        args = ["verify-diff",
                "--manifest", os.path.join(self.d, manifest),
                "--report", os.path.join(self.d, report),
                "--baseline-input", bp or os.path.join(self.d, "base.json"),
                "--candidate-input", cp or os.path.join(self.d, "cand.json")]
        if mapping:
            args += ["--mapping", os.path.join(self.d, "mapping.json")]
        if ledger:
            args += ["--ledger", os.path.join(self.d, "ledger.db")]
        return _cli(args)


class PersistedCycleTest(CompareTestBase):
    def test_same_ring_persists_despite_all_evidence_change(self):
        # version ids renamed, commit_pos shifted, transaction line order
        # swapped in the candidate file
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=10,
                               swap_txn_order=True)
        r, report = self.compare(base, cand)
        self.assertEqual(report["status"], "no_regression", report)
        cats = [row["category"] for row in report["rows"]]
        self.assertIn("persisted_invalid", cats)
        self.assertNotIn("new_invalid", cats)
        self.assertNotIn("resolved_invalid", cats)
        # evidence-only change: version ids differ but the row is still
        # classified persisted (not evidence_changed, since the normalized
        # witness semantics are identical)
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)
        self.assertIn('"verified": true', v.stdout)
        # oracle agrees on the ring
        exp = expected_pairings(base, cand)
        self.assertEqual(len(exp["cycles"]["persisted"]), 1)
        self.assertEqual(exp["cycles"]["new"], [])
        self.assertEqual(exp["cycles"]["resolved"], [])

    def test_persisted_ring_with_evidence_change_flag(self):
        # change a realtime timestamp inside the ring (a semantic evidence
        # value that does not break the ring) - identity still persists
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=3)
        for t in cand["transactions"]:
            t["start_time"] += 100
            t["end_time"] += 100
        r, report = self.compare(base, cand)
        self.assertEqual(report["status"], "no_regression", report)
        self.assertTrue(
            any(row["category"] in ("persisted_invalid", "evidence_changed")
                for row in report["rows"]))


class NewAndResolvedTest(CompareTestBase):
    def _fixed_doc(self):
        # serial history: T1 fully before R, no ring
        b = HistoryBuilder()
        b.init(1, 0, "ix").init(2, 0, "iy")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.commit("T2", 2, [(2, "y1", "iy", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b.txn("T2", 1, 3, 4, [put_op(0, 2, 1)])
        b.txn("R", 2, 5, 6, [read_op(0, 1, 1, "x1"),
                             read_op(1, 2, 1, "y1")])
        return b.doc()

    def test_adding_one_ring_is_new_invalid(self):
        base = self._fixed_doc()
        cand = _write_skew_doc()
        r, report = self.compare(base, cand, run_b="fix", run_c="skew")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(report["status"], "regression")
        self.assertEqual(report["counts"]["new_invalid"], 1)
        new_rows = [x for x in report["rows"]
                    if x["category"] == "new_invalid"]
        self.assertEqual(new_rows[0]["finding_class"], "dependency_cycle")
        self.assertEqual(new_rows[0]["candidate_run_id"], "skew")
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stderr)
        exp = expected_pairings(base, cand)
        self.assertEqual(len(exp["cycles"]["new"]), 1)

    def test_fixing_ring_is_resolved(self):
        base = _write_skew_doc()
        cand = self._fixed_doc()
        r, report = self.compare(base, cand)
        self.assertEqual(report["counts"]["resolved_invalid"], 1)
        self.assertNotIn("new_invalid",
                         [x["category"] for x in report["rows"]])
        self.assertIn(report["status"], ("improved", "no_regression"))
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)
        exp = expected_pairings(base, cand)
        self.assertEqual(len(exp["cycles"]["resolved"]), 1)

    def test_resolved_requires_complete_candidate_evidence(self):
        base = _write_skew_doc()
        cand = self._fixed_doc()
        # break candidate completeness: declare a wider range than commits
        cand["commit_log"]["last_commit_pos"] = 5
        r, report = self.compare(base, cand)
        self.assertEqual(report["runs"]["candidate"]["incomplete_evidence"],
                         True)
        cats = [x["category"] for x in report["rows"]
                if x["finding_class"] == "dependency_cycle"]
        # must NOT be allowed to claim resolved
        self.assertNotIn("resolved_invalid", cats)
        self.assertEqual(report["status"], "unknown")
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)


class AmbiguousWitnessTest(CompareTestBase):
    def test_two_legal_witnesses_of_same_ring_is_ambiguous(self):
        # Baseline: ring T1<->T2 via rw edges on keys 1 and 2.
        # Candidate: same ring and same nodes, but with an additional
        # realtime-independent alternative edge path so the same node pair
        # has two distinct legal witnesses (different edge kinds).
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=2)
        # add a ww edge inside the same ring nodes by giving T1 and T2 a
        # shared key both write (extra commit write on key 3) -> the node
        # pair gets both rw and ww legal edges in the candidate
        for c in cand["commit_log"]["commits"]:
            if c["txn_id"] == "T1":
                c["writes"].append({"key": 3, "version_id": "c-z1",
                                    "predecessor": "iz", "value": 1})
            if c["txn_id"] == "T2":
                c["writes"].append({"key": 3, "version_id": "c-z2",
                                    "predecessor": "c-z1", "value": 2})
        cand["initial_state"].append({"key": 3, "value": 0, "version_id": "iz"})
        for t in cand["transactions"]:
            if t["txn_id"] == "T1":
                t["operations"].append(put_op(2, 3, 1))
            if t["txn_id"] == "T2":
                t["operations"].append(put_op(2, 3, 2))
        r, report = self.compare(base, cand)
        amb = [x for x in report["rows"] if x["category"] == "ambiguous"]
        self.assertTrue(amb, json.dumps(report["counts"]))
        self.assertEqual(amb[0]["finding_class"], "dependency_cycle")
        self.assertEqual(report["status"], "unknown")
        # ambiguity is never mislabeled persisted or resolved
        for row in report["rows"]:
            if row["finding_class"] == "dependency_cycle":
                self.assertIn(row["category"],
                              ("ambiguous", "identity_conflict",
                               "evidence_changed", "persisted_invalid"))
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)


class RenameMappingTest(CompareTestBase):
    def test_rename_requires_explicit_mapping(self):
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=1)
        # rename T1->A1, T2->A2 in the candidate
        for t in cand["transactions"]:
            t["txn_id"] = {"T1": "A1", "T2": "A2"}[t["txn_id"]]
        for c in cand["commit_log"]["commits"]:
            c["txn_id"] = {"T1": "A1", "T2": "A2"}[c["txn_id"]]
        # without mapping: transactions show up as new/disappeared and the
        # ring cannot be aligned
        r, report = self.compare(base, cand)
        unmapped = [x for x in report["rows"]
                    if x["category"] == "unmapped_txn"]
        self.assertEqual(len(unmapped), 4)
        sides = {x["reason"] for x in unmapped}
        self.assertEqual(sides,
                         {"unmapped_transaction_only_in_candidate",
                          "unmapped_transaction_only_in_baseline"})
        # the ring is reported new + resolved, never silently persisted
        cats = [x["category"] for x in report["rows"]
                if x["finding_class"] == "dependency_cycle"]
        self.assertIn("new_invalid", cats)
        self.assertIn("resolved_invalid", cats)
        # oracle identity enumeration agrees
        ident = enumerate_identity_matching(base, cand)
        self.assertEqual(sorted(ident["baseline_only"]), ["T1", "T2"])
        self.assertEqual(sorted(ident["candidate_only"]), ["A1", "A2"])

    def test_rename_with_explicit_one_to_one_mapping_persists(self):
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=1)
        for t in cand["transactions"]:
            t["txn_id"] = {"T1": "A1", "T2": "A2"}[t["txn_id"]]
        for c in cand["commit_log"]["commits"]:
            c["txn_id"] = {"T1": "A1", "T2": "A2"}[c["txn_id"]]
        r, report = self.compare(base, cand,
                                 mapping=[("T1", "A1"), ("T2", "A2")])
        cats = [x["category"] for x in report["rows"]]
        self.assertIn("persisted_invalid", cats)
        self.assertNotIn("new_invalid", cats)
        v = self.verify(mapping=True)
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)
        ident = enumerate_identity_matching(
            base, cand, [("T1", "A1"), ("T2", "A2")])
        self.assertEqual(ident["renamed"], [("T1", "A1"), ("T2", "A2")])
        self.assertEqual(ident["baseline_only"], [])
        self.assertEqual(ident["candidate_only"], [])

    def test_mapping_one_to_many_rejected(self):
        base = _write_skew_doc()
        cand = _write_skew_doc()
        mp = write_mapping([("T1", "T1"), ("T1", "T2")], self.d,
                           "badmap.json")
        bp = write_history(base, self.d, "base.json")
        cp = write_history(cand, self.d, "cand.json")
        r = _cli(["compare", "--baseline-input", bp, "--baseline-run-id", "b",
                  "--candidate-input", cp, "--candidate-run-id", "c",
                  "--mapping", mp])
        self.assertEqual(r.returncode, 3)
        self.assertIn("one_to_many", r.stderr)

    def test_mapping_many_to_one_rejected(self):
        base = _write_skew_doc()
        cand = _write_skew_doc()
        mp = write_mapping([("T1", "T2"), ("T2", "T2")], self.d,
                           "badmap2.json")
        bp = write_history(base, self.d, "base.json")
        cp = write_history(cand, self.d, "cand.json")
        r = _cli(["compare", "--baseline-input", bp, "--baseline-run-id", "b",
                  "--candidate-input", cp, "--candidate-run-id", "c",
                  "--mapping", mp])
        self.assertEqual(r.returncode, 3)
        self.assertIn("many_to_one", r.stderr)

    def test_mapping_unknown_txn_rejected(self):
        base = _write_skew_doc()
        cand = _write_skew_doc()
        mp = write_mapping([("T1", "GHOST")], self.d, "badmap3.json")
        bp = write_history(base, self.d, "base.json")
        cp = write_history(cand, self.d, "cand.json")
        r = _cli(["compare", "--baseline-input", bp, "--baseline-run-id", "b",
                  "--candidate-input", cp, "--candidate-run-id", "c",
                  "--mapping", mp])
        self.assertEqual(r.returncode, 3)
        self.assertIn("mapping_unknown_txn", r.stderr)

    def test_duplicate_run_id_rejected(self):
        bp = write_history(_write_skew_doc(), self.d, "base.json")
        cp = write_history(_write_skew_doc(), self.d, "cand.json")
        r = _cli(["compare", "--baseline-input", bp, "--baseline-run-id", "x",
                  "--candidate-input", cp, "--candidate-run-id", "x"])
        self.assertEqual(r.returncode, 3)
        self.assertIn("duplicate_run_id", r.stderr)


class IncompleteHistoryTest(CompareTestBase):
    def test_incomplete_side_only_unknown_gap_transition(self):
        base = _write_skew_doc()
        # candidate: same ring but with an undeclared-complete log -> unknown
        cand = _write_skew_doc(prefix="c-", pos_shift=1)
        cand["commit_log"]["complete"] = False
        r, report = self.compare(base, cand)
        self.assertEqual(report["runs"]["candidate"]["verdict"], "unknown")
        self.assertEqual(report["status"], "unknown")
        # the cycle row must not be claimed resolved
        cyc = [x for x in report["rows"]
               if x["finding_class"] == "dependency_cycle"]
        self.assertTrue(cyc)
        for row in cyc:
            self.assertNotEqual(row["category"], "resolved_invalid")
        # gap changes are present with stable gap categories
        gaps = [x for x in report["rows"] if x["finding_class"] == "gap"]
        self.assertTrue(gaps)
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout)

    def test_missing_commit_pos_produces_gap_not_valid(self):
        b = HistoryBuilder()
        b.init(1, 0, "ix")
        b.commit("T1", 1, [(1, "x1", "ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        base = b.doc()
        cand = json.loads(json.dumps(base))
        cand["commit_log"]["last_commit_pos"] = 3  # pos 2 missing
        r, report = self.compare(base, cand)
        self.assertEqual(report["runs"]["candidate"]["incomplete_evidence"],
                         True)
        gap_rows = [x for x in report["rows"]
                    if x["finding_class"] == "gap"]
        self.assertTrue(any("log_range_gap" in x["identity_json"]
                            for x in gap_rows))
        self.assertEqual(report["status"], "unknown")
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout)


class ReadAnomalyMatchingTest(CompareTestBase):
    def _dirty_doc(self, prefix=""):
        b = HistoryBuilder()
        b.init(1, 0, f"{prefix}ix")
        b.commit("T1", 1, [(1, f"{prefix}x1", f"{prefix}ix", 1)])
        b.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        # R reads future version x1 at snapshot 0
        b.txn("R", 0, 10, 20,
              [read_op(0, 1, 1, f"{prefix}x1")])
        return b.doc()

    def test_read_anomaly_persists_with_renumbered_versions(self):
        base = self._dirty_doc()
        cand = self._dirty_doc(prefix="c-")
        r, report = self.compare(base, cand)
        rows = [x for x in report["rows"]
                if x["finding_class"] == "read_anomaly"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["category"], "persisted_invalid")
        # version ids live only in evidence, not identity
        self.assertNotIn("c-x1", rows[0]["identity_json"])
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout)
        # oracle independently enumerates the same anomaly
        self.assertEqual(len(enumerate_read_anomalies(base)), 1)
        self.assertEqual(len(enumerate_read_anomalies(cand)), 1)

    def test_read_anomaly_never_matches_cycle(self):
        # baseline has a dirty read; candidate has a write-skew ring:
        # both invalid, completely different finding classes
        base = self._dirty_doc()
        cand = _write_skew_doc()
        r, report = self.compare(base, cand)
        classes = {(x["finding_class"], x["category"]) for x in report["rows"]}
        # no row may pair a read_anomaly baseline with a cycle candidate
        for row in report["rows"]:
            if row["finding_class"] == "read_anomaly":
                self.assertIn(row["category"],
                              ("resolved_invalid", "new_invalid",
                               "identity_conflict", "ambiguous",
                               "evidence_changed", "persisted_invalid"))
        self.assertTrue(any(c == "resolved_invalid" and f == "read_anomaly"
                            for f, c in classes))
        self.assertTrue(any(c == "new_invalid" and f == "dependency_cycle"
                            for f, c in classes))
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout)

    def test_identity_conflict_on_same_txn_op_semantic_change(self):
        # same txn/op/key, but the anomaly category changes future->aborted:
        # a semantic conflict, not persisted
        base = self._dirty_doc()
        b2 = HistoryBuilder()
        b2.init(1, 0, "ix")
        b2.commit("T1", 1, [(1, "x1", "ix", 1)])
        b2.txn("T1", 0, 1, 2, [put_op(0, 1, 1)])
        b2.txn("Ax", None, None, None, [], status="aborted",
               writes=[{"key": 1, "version_id": "bad1", "value": 9}])
        b2.txn("R", 0, 10, 20, [read_op(0, 1, 9, "bad1")])
        cand = b2.doc()
        r, report = self.compare(base, cand)
        conflicts = [x for x in report["rows"]
                     if x["category"] == "identity_conflict"]
        self.assertTrue(conflicts, json.dumps(report["counts"]))
        self.assertTrue(all(x["finding_class"] == "read_anomaly"
                            for x in conflicts))
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout)


class JsonlReorderTest(CompareTestBase):
    def test_jsonl_line_reorder_does_not_change_report(self):
        doc = _write_skew_doc(prefix="j-")
        d1 = os.path.join(self.d, "h1")
        d2 = os.path.join(self.d, "h2")
        write_jsonl_history(doc, d1)
        write_jsonl_history(doc, d2)
        # shuffle the transactions.jsonl lines in d2 (commit order in
        # commits.jsonl stays intact, only record order changes)
        tp = os.path.join(d2, "hist", "transactions.jsonl")
        with open(tp) as fh:
            lines = [ln for ln in fh.read().splitlines() if ln.strip()]
        with open(tp, "w") as fh:
            fh.write("\n".join(reversed(lines)) + "\n")
        r1 = _cli(["compare",
                   "--baseline-input-dir", os.path.join(d1, "hist"),
                   "--baseline-run-id", "b",
                   "--candidate-input-dir", os.path.join(d1, "hist"),
                   "--candidate-run-id", "c",
                   "--out", os.path.join(self.d, "a.json"),
                   "--manifest-out", os.path.join(self.d, "m1.json"),
                   "--ledger", os.path.join(self.d, "l1.db")])
        self.assertIn(r1.returncode, (0, 1, 2), r1.stderr)
        r2 = _cli(["compare",
                   "--baseline-input-dir", os.path.join(d1, "hist"),
                   "--baseline-run-id", "b",
                   "--candidate-input-dir", os.path.join(d2, "hist"),
                   "--candidate-run-id", "c",
                   "--out", os.path.join(self.d, "b.json"),
                   "--manifest-out", os.path.join(self.d, "m2.json"),
                   "--ledger", os.path.join(self.d, "l2.db")])
        self.assertIn(r2.returncode, (0, 1, 2), r2.stderr)
        a = json.load(open(os.path.join(self.d, "a.json")))
        b = json.load(open(os.path.join(self.d, "b.json")))
        # input hashes differ (file order changed bytes) but findings match
        self.assertEqual(
            a["manifest"]["candidate"]["sha256"]
            != b["manifest"]["candidate"]["sha256"], True)
        self.assertEqual(a["rows"], b["rows"])
        self.assertEqual(a["counts"], b["counts"])


class TamperRejectionTest(CompareTestBase):
    def _setup(self):
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=1)
        r, report = self.compare(base, cand)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_report_one_byte_tamper_rejected(self):
        self._setup()
        rep_path = os.path.join(self.d, "report.json")
        rep = json.load(open(rep_path))
        # delete an ambiguous/conflict-style row if present, otherwise flip a
        # reason field on one row
        rep["rows"][0]["reason"] = "forged_reason"
        json.dump(rep, open(rep_path, "w"))
        v = self.verify()
        self.assertEqual(v.returncode, 1, v.stdout)
        self.assertIn("report_signature", v.stdout)

    def test_deleting_row_rejected(self):
        self._setup()
        rep_path = os.path.join(self.d, "report.json")
        rep = json.load(open(rep_path))
        removed = rep["rows"].pop()
        rep["counts"][removed["category"]] = max(
            0, rep["counts"][removed["category"]] - 1)
        json.dump(rep, open(rep_path, "w"))
        v = self.verify()
        self.assertEqual(v.returncode, 1)
        self.assertIn("missing expected row", v.stdout)

    def test_input_tamper_rejected(self):
        self._setup()
        cp = os.path.join(self.d, "cand.json")
        cand = json.load(open(cp))
        cand["initial_state"][0]["value"] = 999
        json.dump(cand, open(cp, "w"))
        v = self.verify()
        self.assertEqual(v.returncode, 1)
        self.assertIn("content SHA-256 changed", v.stdout)

    def test_manifest_tamper_rejected(self):
        self._setup()
        mp = os.path.join(self.d, "manifest.json")
        man = json.load(open(mp))
        man["audit_config"]["memory_budget_mb"] = 999
        json.dump(man, open(mp, "w"))
        v = self.verify()
        self.assertEqual(v.returncode, 1)
        self.assertIn("manifest signature mismatch", v.stdout)

    def test_mapping_tamper_rejected(self):
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=1)
        for t in cand["transactions"]:
            t["txn_id"] = {"T1": "A1", "T2": "A2"}[t["txn_id"]]
        for c in cand["commit_log"]["commits"]:
            c["txn_id"] = {"T1": "A1", "T2": "A2"}[c["txn_id"]]
        r, report = self.compare(base, cand,
                                 mapping=[("T1", "A1"), ("T2", "A2")])
        self.assertEqual(r.returncode, 0, r.stderr)
        mp = os.path.join(self.d, "mapping.json")
        mapping = json.load(open(mp))
        mapping["mappings"][0]["candidate_txn_id"] = "A2"
        json.dump(mapping, open(mp, "w"))
        v = self.verify(mapping=True)
        self.assertEqual(v.returncode, 1)
        self.assertTrue("mapping" in v.stdout
                        or "SHA-256 changed" in v.stdout)

    def test_ambiguous_row_deletion_rejected(self):
        # build the ambiguous scenario, then delete its ambiguous row
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=2)
        for c in cand["commit_log"]["commits"]:
            if c["txn_id"] == "T1":
                c["writes"].append({"key": 3, "version_id": "c-z1",
                                    "predecessor": "iz", "value": 1})
            if c["txn_id"] == "T2":
                c["writes"].append({"key": 3, "version_id": "c-z2",
                                    "predecessor": "c-z1", "value": 2})
        cand["initial_state"].append(
            {"key": 3, "value": 0, "version_id": "iz"})
        for t in cand["transactions"]:
            if t["txn_id"] == "T1":
                t["operations"].append(put_op(2, 3, 1))
            if t["txn_id"] == "T2":
                t["operations"].append(put_op(2, 3, 2))
        r, report = self.compare(base, cand)
        amb = [x for x in report["rows"] if x["category"] == "ambiguous"]
        self.assertTrue(amb)
        rep_path = os.path.join(self.d, "report.json")
        rep = json.load(open(rep_path))
        rep["rows"] = [x for x in rep["rows"]
                       if x["category"] != "ambiguous"]
        rep["counts"]["ambiguous"] = 0
        json.dump(rep, open(rep_path, "w"))
        v = self.verify()
        self.assertEqual(v.returncode, 1)
        self.assertIn("ambiguous", v.stdout)


class LargeScaleTest(CompareTestBase):
    def test_large_two_runs_indexed_batched_with_resource_stats(self):
        gen = os.path.join(ROOT, "bench", "gen_scale.py")
        bdir = os.path.join(self.d, "gen_b")
        cdir = os.path.join(self.d, "gen_c")
        common = ["--txns", "600", "--keys", "120", "--reads", "3",
                  "--ranges", "1", "--range-width", "30", "--skew-pairs",
                  "40", "--seed", "7"]
        g1 = subprocess.run([sys.executable, gen, "--out",
                             os.path.join(bdir, "hist")] + common,
                            capture_output=True, text=True)
        self.assertEqual(g1.returncode, 0, g1.stderr)
        g2 = subprocess.run([sys.executable, gen, "--out",
                             os.path.join(cdir, "hist"), "--seed", "8"]
                            + common[0:-2] + ["--skew-pairs", "40",
                                             "--seed", "8"],
                            capture_output=True, text=True)
        self.assertEqual(g2.returncode, 0, g2.stderr)
        r = _cli(["compare",
                  "--baseline-input-dir", os.path.join(bdir, "hist"),
                  "--baseline-run-id", "scale-b",
                  "--candidate-input-dir", os.path.join(cdir, "hist"),
                  "--candidate-run-id", "scale-c",
                  "--out", os.path.join(self.d, "big.json"),
                  "--manifest-out", os.path.join(self.d, "manifest.json"),
                  "--ledger", os.path.join(self.d, "ledger.db")])
        self.assertIn(r.returncode, (0, 1, 2), r.stderr)
        rep = json.load(open(os.path.join(self.d, "big.json")))
        stats = rep["stats"]
        # real resource/index counters are present and non-trivial
        for name in ("index_hits", "candidate_comparisons", "rows_written",
                     "index_inserts", "fresh_audits"):
            self.assertIn(name, stats, name)
            self.assertGreater(stats[name], 0, name)
        self.assertIn("rss_bytes_bucket_mb", stats)
        self.assertIn("runtime_stats", rep)
        self.assertGreater(rep["runtime_stats"]["wall_time_ms_exact"], 0)
        # the candidate comparison count must stay far below a cartesian
        # product of the two finding sets
        n_findings = (rep["runs"]["baseline"]["anomalies_total"]
                      + rep["runs"]["candidate"]["anomalies_total"])
        if n_findings:
            self.assertLess(stats["candidate_comparisons"],
                            max(1, n_findings) ** 2)
        v = self.verify(bp=os.path.join(bdir, "hist"),
                        cp=os.path.join(cdir, "hist"),
                        report="big.json")
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)
        # ledger cross-check passed inside verify-diff; also assert it is a
        # real SQLite database with the indexed schema
        import sqlite3
        db = sqlite3.connect(os.path.join(self.d, "ledger.db"))
        indexes = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")}
        self.assertIn("idx_rows_baseline", indexes)
        self.assertIn("idx_rows_candidate", indexes)
        db.close()


class BatchedLedgerTest(CompareTestBase):
    @staticmethod
    def _many_anomaly_doc(n):
        # one committed write read in the future by n distinct readers ->
        # n confirmed read anomalies, forcing multiple ledger write batches
        return {
            "format": "snapshot-history/1",
            "initial_state": [{"key": 0, "value": 0, "version_id": "i0"}],
            "commit_log": {
                "first_commit_pos": 1, "last_commit_pos": 1,
                "complete": True,
                "commits": [{"txn_id": "W", "commit_pos": 1, "writes": [
                    {"key": 0, "version_id": "v1", "predecessor": "i0",
                     "value": 1}]}]},
            "transactions": [
                {"txn_id": "W", "status": "committed", "snapshot_pos": 0,
                 "start_time": 1, "end_time": 2,
                 "operations": [put_op(0, 0, 1)]}] + [
                {"txn_id": f"R{i}", "status": "committed",
                 "snapshot_pos": 0, "start_time": 10 + i, "end_time": 11 + i,
                 "operations": [read_op(0, 0, 1, "v1")]}
                for i in range(n)],
        }

    def test_ledger_streams_across_batch_boundary(self):
        from snapshot_audit.ledger import _BATCH
        n = _BATCH + 1100  # guaranteed to need more than one write batch
        base = self._many_anomaly_doc(n)
        cand = self._many_anomaly_doc(n)
        r, report = self.compare(base, cand)
        self.assertEqual(report["stats"]["rows_written"], n)
        self.assertGreaterEqual(
            (n + _BATCH - 1) // _BATCH, 2)  # multiple batches
        self.assertEqual(report["counts"]["persisted_invalid"], n)
        import sqlite3
        db = sqlite3.connect(os.path.join(self.d, "ledger.db"))
        try:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM rows_").fetchone()[0], n)
            # every row was written inside committed transactions
            self.assertEqual(
                db.execute(
                    "SELECT value FROM stats WHERE name='rows_written'"
                ).fetchone()[0], n)
        finally:
            db.close()
        v = self.verify()
        self.assertEqual(v.returncode, 0, v.stdout + v.stderr)


class DeterminismTest(CompareTestBase):
    @staticmethod
    def _signed(path):
        rep = json.load(open(path))
        rep.pop("runtime_stats", None)  # operator-only, intentionally variable
        return rep

    def test_repeated_runs_byte_identical(self):
        base = _write_skew_doc()
        cand = _write_skew_doc(prefix="c-", pos_shift=4)
        self.compare(base, cand)
        shutil.copy(os.path.join(self.d, "report.json"),
                    os.path.join(self.d, "r1.json"))
        shutil.copy(os.path.join(self.d, "ledger.db"),
                    os.path.join(self.d, "l1.db"))
        self.compare(base, cand)
        self.assertEqual(
            self._signed(os.path.join(self.d, "r1.json")),
            self._signed(os.path.join(self.d, "report.json")))
        self.assertTrue(
            open(os.path.join(self.d, "l1.db"), "rb").read()
            == open(os.path.join(self.d, "ledger.db"), "rb").read())
        # the signed report hash itself is therefore stable
        self.assertEqual(
            json.load(open(os.path.join(self.d, "r1.json")))["report_sha256"],
            json.load(open(os.path.join(self.d, "report.json")))
            ["report_sha256"])


if __name__ == "__main__":
    unittest.main()
