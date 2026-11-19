"""CLI and behavior tests for ``lineage`` / ``verify-lineage``.

Covers the signed deterministic output, the exit-code contract, JSONL
directory inputs, input-drift refusal, and the tamper-detection guarantees
of verify-lineage (changed input, removed edge, unknown->complete flip,
changed range-predicate evidence, changed query, reordered arrays).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.helpers import (  # noqa: E402
    HistoryBuilder, TOMBSTONE, del_op, put_op, range_op, read_op,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cli(args, cwd=ROOT):
    return subprocess.run([sys.executable, "-m", "snapshot_audit"] + args,
                          capture_output=True, text=True, cwd=cwd)


def _skew_doc():
    b = HistoryBuilder()
    b.init(1, 0, "i1").init(2, 0, "i2")
    b.commit("T1", 1, [(1, "a1", "i1", 10)])
    b.commit("T2", 2, [(2, "b2", "i2", 20)])
    b.txn("T1", 0, 1, 2, [read_op(0, 2, 0, "i2"), put_op(1, 1, 10)])
    b.txn("T2", 1, 3, 4, [range_op(0, 1, 2, [(1, 10, "a1"), (2, 0, "i2")]),
                          put_op(1, 2, 20)])
    return b.doc()


def _unknown_doc():
    b = HistoryBuilder()
    b.init(1, 0, "i1")
    b.commit("T1", 1, [(2, "b2", None, 20)])
    b.txn("T1", 0, 1, 2, [range_op(0, 1, 5, [(1, 0, "i1")], complete=False),
                          put_op(1, 2, 20)])
    return b.doc()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = self.tmp.name

    def write(self, name, obj):
        path = os.path.join(self.d, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        return path

    def write_jsonl(self, doc):
        out = os.path.join(self.d, "hist")
        os.makedirs(out, exist_ok=True)
        log = doc["commit_log"]
        header = {"format": doc["format"],
                  "initial_state": doc["initial_state"],
                  "commit_log": {"first_commit_pos": log["first_commit_pos"],
                                 "last_commit_pos": log["last_commit_pos"],
                                 "complete": log["complete"]}}
        with open(os.path.join(out, "header.json"), "w") as fh:
            json.dump(header, fh)
        with open(os.path.join(out, "commits.jsonl"), "w") as fh:
            for c in log["commits"]:
                fh.write(json.dumps(c) + "\n")
        with open(os.path.join(out, "transactions.jsonl"), "w") as fh:
            for t in doc["transactions"]:
                fh.write(json.dumps(t) + "\n")
        return out

    def lineage(self, doc, *args, expect=0):
        hist = self.write("h.json", doc)
        out = os.path.join(self.d, "lin.json")
        r = _cli(["lineage", "--input", hist, "--out", out] + list(args))
        self.assertEqual(r.returncode, expect, r.stderr)
        with open(out) as fh:
            return json.load(fh), out, hist


class LineageCliTest(Base):
    def test_point_query_structure_and_binding(self):
        doc = _skew_doc()
        lin, _out, hist = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        self.assertEqual(lin["format"], "observation-lineage/1")
        self.assertEqual(lin["lineage_semantics"], "observation_dependency")
        self.assertIn("观察依赖血缘", lin["semantics_note"])
        self.assertEqual(lin["status"], "complete")
        self.assertEqual(lin["query"], {"commit_pos": 2, "key": 2})
        self.assertEqual(lin["input"]["path"], os.path.realpath(hist))
        self.assertEqual(len(lin["input"]["sha256"]), 64)
        self.assertEqual(lin["results"][0]["value"], 20)
        # stats report index hits, expanded nodes/edges, candidate checks
        st = lin["stats"]
        for k in ("version_lookups", "nodes_created", "nodes_expanded",
                  "edges_emitted", "read_ops_examined"):
            self.assertIn(k, st)
        self.assertGreaterEqual(st["read_ops_examined"], 1)
        self.assertIn("rss_bytes_exact", lin["runtime_stats"])
        self.assertIn("wall_time_ms_exact", lin["runtime_stats"])
        # signature covers the signed payload
        from snapshot_audit.hashing import canonical_json, sha256_text
        from snapshot_audit.lineage import _unsigned
        self.assertEqual(lin["lineage_sha256"],
                         sha256_text(canonical_json(_unsigned(lin))))

    def test_transaction_level_observation_set_marking(self):
        doc = _skew_doc()
        lin, _o, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        node = [n for n in lin["nodes"] if n["id"] == "v:b2"][0]
        obs = node["observation_set"]
        self.assertEqual(obs["scope"], "transaction_level")
        self.assertIn("does not imply", obs["note"])

    def test_range_query_exit_and_results(self):
        doc = _skew_doc()
        lin, _o, _h = self.lineage(doc, "--commit-pos", "2",
                                   "--lo", "1", "--hi", "2")
        self.assertEqual([r["key"] for r in lin["results"]], [1, 2])
        self.assertEqual(lin["status"], "complete")

    def test_unknown_exit_code(self):
        doc = _unknown_doc()
        lin, _o, _h = self.lineage(doc, "--commit-pos", "1", "--key", "2",
                                   expect=2)
        self.assertEqual(lin["status"], "unknown")
        pred = [e for e in lin["edges"]
                if e["kind"] == "range_predicate"][0]
        self.assertEqual(pred["status"], "unknown")

    def test_truncated_exit_code_and_cursor(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        for i in range(1, 5):
            b.commit(f"T{i}", i, [(i + 1, f"v{i+1}", None, i * 10)])
            b.txn(f"T{i}", i - 1, i, i + 0.5,
                  [read_op(0, i, (i - 1) * 10, f"v{i}" if i > 1 else "i1"),
                   put_op(1, i + 1, i * 10)])
        lin, _o, _h = self.lineage(b.doc(), "--commit-pos", "4", "--key", "5",
                                   "--max-depth", "1", expect=2)
        self.assertEqual(lin["status"], "truncated")
        self.assertEqual(lin["truncation"]["reasons"], ["max_depth"])
        self.assertTrue(lin["truncation"]["cursor"])

    def test_query_validation_errors(self):
        hist = self.write("h.json", _skew_doc())
        for args in (["--commit-pos", "-1", "--key", "1"],
                     ["--commit-pos", "1"],
                     ["--commit-pos", "1", "--key", "1", "--lo", "1",
                      "--hi", "2"],
                     ["--commit-pos", "1", "--lo", "5", "--hi", "2"]):
            r = _cli(["lineage", "--input", hist] + args)
            self.assertEqual(r.returncode, 3, (args, r.stderr))

    def test_jsonl_dir_input(self):
        doc = _skew_doc()
        hist = self.write_jsonl(doc)
        out = os.path.join(self.d, "lin.json")
        r = _cli(["lineage", "--input-dir", hist, "--commit-pos", "2",
                  "--key", "2", "--out", out])
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(out) as fh:
            lin = json.load(fh)
        self.assertEqual(lin["input"]["kind"], "dir")
        self.assertEqual(lin["status"], "complete")
        self.assertEqual(lin["results"][0]["value"], 20)
        # verify-lineage against the same directory input
        r2 = _cli(["verify-lineage", "--lineage", out])
        self.assertEqual(r2.returncode, 0, r2.stderr + r2.stdout)

    def test_deterministic_output(self):
        doc = _skew_doc()
        hist = self.write("h.json", doc)
        outs = []
        for i in range(2):
            out = os.path.join(self.d, f"lin{i}.json")
            r = _cli(["lineage", "--input", hist, "--commit-pos", "2",
                      "--key", "2", "--out", out])
            self.assertEqual(r.returncode, 0, r.stderr)
            with open(out) as fh:
                outs.append(json.load(fh))
        for doc_out in outs:
            doc_out.pop("runtime_stats")
        self.assertEqual(outs[0], outs[1])


class VerifyLineageTest(Base):
    def test_verify_ok(self):
        doc = _skew_doc()
        _lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        r = _cli(["verify-lineage", "--lineage", out])
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        report = json.loads(r.stdout)
        self.assertTrue(report["verified"])
        self.assertIn("recompute", report["checked"])

    def test_verify_ok_with_query_assertion(self):
        doc = _skew_doc()
        _lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        r = _cli(["verify-lineage", "--lineage", out,
                  "--commit-pos", "2", "--key", "2"])
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        r = _cli(["verify-lineage", "--lineage", out,
                  "--commit-pos", "1", "--key", "2"])
        self.assertEqual(r.returncode, 1)
        self.assertIn("query", r.stdout)

    def test_verify_truncated_and_unknown_lineages(self):
        b = HistoryBuilder()
        b.init(1, 0, "i1")
        for i in range(1, 4):
            b.commit(f"T{i}", i, [(i + 1, f"v{i+1}", None, i * 10)])
            b.txn(f"T{i}", i - 1, i, i + 0.5,
                  [read_op(0, i, (i - 1) * 10, f"v{i}" if i > 1 else "i1"),
                   put_op(1, i + 1, i * 10)])
        _lin, out, _h = self.lineage(b.doc(), "--commit-pos", "3",
                                     "--key", "4", "--max-depth", "1",
                                     expect=2)
        r = _cli(["verify-lineage", "--lineage", out])
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        _lin2, out2, _h2 = self.lineage(_unknown_doc(), "--commit-pos", "1",
                                        "--key", "2", expect=2)
        r2 = _cli(["verify-lineage", "--lineage", out2])
        self.assertEqual(r2.returncode, 0, r2.stderr + r2.stdout)

    def _tamper(self, lin, mutate):
        doc = json.loads(json.dumps(lin))  # deep copy
        mutate(doc)
        # re-sign so the self-signature is consistent: only the
        # recomputation-from-raw-history check may catch the change
        from snapshot_audit.hashing import canonical_json, sha256_text
        from snapshot_audit.lineage import _unsigned
        doc["lineage_sha256"] = sha256_text(canonical_json(_unsigned(doc)))
        path = os.path.join(self.d, "tampered.json")
        with open(path, "w") as fh:
            json.dump(doc, fh)
        return path

    def test_tamper_changed_input(self):
        doc = _skew_doc()
        _lin, out, hist = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        # modify the raw history after the lineage was produced
        doc["transactions"][0]["operations"][0]["value"] = 99
        self.write("h.json", doc)
        r = _cli(["verify-lineage", "--lineage", out])
        self.assertEqual(r.returncode, 1)
        self.assertIn("input.sha256", r.stdout)

    def test_tamper_removed_edge(self):
        doc = _skew_doc()
        lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        self.assertTrue(lin["edges"])
        path = self._tamper(lin, lambda d: d["edges"].pop(0))
        r = _cli(["verify-lineage", "--lineage", path])
        self.assertEqual(r.returncode, 1)
        self.assertIn("edges", r.stdout)

    def test_tamper_unknown_to_complete(self):
        lin, out, _h = self.lineage(_unknown_doc(), "--commit-pos", "1",
                                    "--key", "2", expect=2)
        self.assertEqual(lin["status"], "unknown")

        def mutate(d):
            d["status"] = "complete"
            for e in d["edges"]:
                e["status"] = "complete"
                e["gaps"] = []
            d["unknowns"] = []
        path = self._tamper(lin, mutate)
        r = _cli(["verify-lineage", "--lineage", path])
        self.assertEqual(r.returncode, 1)
        self.assertIn("unknown", r.stdout)

    def test_tamper_predicate_evidence(self):
        doc = _skew_doc()
        lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")

        def mutate(d):
            for e in d["edges"]:
                if e["kind"] == "range_predicate":
                    e["evidence"]["returned_keys"] = [1, 2, 99]
        path = self._tamper(lin, mutate)
        r = _cli(["verify-lineage", "--lineage", path])
        self.assertEqual(r.returncode, 1)
        self.assertIn("returned_keys", r.stdout)

    def test_tamper_query(self):
        doc = _skew_doc()
        lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        path = self._tamper(lin, lambda d: d["query"].__setitem__("key", 1))
        r = _cli(["verify-lineage", "--lineage", path])
        self.assertEqual(r.returncode, 1)

    def test_tamper_reordered_nodes(self):
        doc = _skew_doc()
        lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        path = self._tamper(lin, lambda d: d["nodes"].reverse())
        r = _cli(["verify-lineage", "--lineage", path])
        self.assertEqual(r.returncode, 1)
        self.assertIn("nodes", r.stdout)

    def test_tamper_signature_only(self):
        doc = _skew_doc()
        lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        lin["results"][0]["value"] = 999  # do not re-sign
        path = self.write("tampered2.json", lin)
        r = _cli(["verify-lineage", "--lineage", path])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lineage_sha256", r.stdout)

    def test_verify_missing_files(self):
        r = _cli(["verify-lineage", "--lineage",
                  os.path.join(self.d, "nope.json")])
        self.assertEqual(r.returncode, 3)
        doc = _skew_doc()
        _lin, out, _h = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        os.unlink(_h)
        r = _cli(["verify-lineage", "--lineage", out])
        self.assertEqual(r.returncode, 3)

    def test_verify_with_relocated_input_override(self):
        doc = _skew_doc()
        _lin, out, hist = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        # same content at a different path: explicit --input override verifies
        moved = self.write("moved.json", doc)
        r = _cli(["verify-lineage", "--lineage", out, "--input", moved])
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        # different content at the override path must fail
        doc["transactions"][0]["operations"][0]["value"] = 99
        bad = self.write("moved_bad.json", doc)
        r = _cli(["verify-lineage", "--lineage", out, "--input", bad])
        self.assertEqual(r.returncode, 1)


class InputDriftTest(Base):
    def test_input_changed_during_lineage(self):
        import contextlib
        import io

        import snapshot_audit.hashing as hashing
        from snapshot_audit.cli import main

        doc = _skew_doc()
        hist = self.write("h.json", doc)
        out = os.path.join(self.d, "lin.json")
        real_hash_input = hashing.hash_input
        calls = {"n": 0}

        def wrapped(path):
            calls["n"] += 1
            if calls["n"] == 2:
                with open(hist, "a", encoding="utf-8") as fh:
                    fh.write("\n")  # drift between the two hashes
            return real_hash_input(path)

        hashing.hash_input = wrapped
        try:
            with contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):
                rc = main(["lineage", "--input", hist, "--commit-pos", "2",
                           "--key", "2", "--out", out])
        finally:
            hashing.hash_input = real_hash_input
        self.assertEqual(rc, 3)
        self.assertFalse(os.path.exists(out))  # no success result emitted

    def test_input_changed_during_verify(self):
        import contextlib
        import io

        import snapshot_audit.hashing as hashing
        from snapshot_audit.cli import main

        doc = _skew_doc()
        _lin, out, hist = self.lineage(doc, "--commit-pos", "2", "--key", "2")
        real_hash_input = hashing.hash_input
        calls = {"n": 0}

        def wrapped(path):
            calls["n"] += 1
            if calls["n"] == 2:
                with open(hist, "a", encoding="utf-8") as fh:
                    fh.write("\n")
            return real_hash_input(path)

        hashing.hash_input = wrapped
        try:
            with contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):
                rc = main(["verify-lineage", "--lineage", out])
        finally:
            hashing.hash_input = real_hash_input
        self.assertEqual(rc, 3)


class SamplesUnchangedTest(unittest.TestCase):
    """The pre-existing commands keep their outputs and exit codes."""

    def test_audit_samples(self):
        for name, code in (("write_skew", 1), ("phantom_cycle", 1),
                           ("valid_serial", 0), ("dirty_reads", 1),
                           ("incomplete_log", 2)):
            r = _cli(["audit", "--input",
                      os.path.join(ROOT, "samples", f"{name}.json")])
            self.assertEqual(r.returncode, code, (name, r.stderr))
            self.assertIn('"verdict"', r.stdout)


if __name__ == "__main__":
    unittest.main()
