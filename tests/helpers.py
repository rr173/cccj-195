"""Shared helpers: history builder + audit runner for tests."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snapshot_audit.cli import run_audit  # noqa: E402
from snapshot_audit.model import load_history  # noqa: E402
from snapshot_audit.store import Store  # noqa: E402


class HistoryBuilder:
    def __init__(self) -> None:
        self.initial = []
        self.commits = []
        self.txns = []
        self.log_first = None
        self.log_last = None
        self.log_complete = True

    def init(self, key, value, version_id=None):
        self.initial.append({"key": key, "value": value,
                             "version_id": version_id or f"init@{key}"})
        return self

    def commit(self, txn_id, pos, writes):
        """writes: list of (key, version_id, predecessor, value) or
        (key, version_id, predecessor, TOMBSTONE)."""
        ws = []
        for (key, vid, pred, val) in writes:
            w = {"key": key, "version_id": vid, "predecessor": pred}
            if val is TOMBSTONE:
                w["tombstone"] = True
            else:
                w["value"] = val
            ws.append(w)
        self.commits.append({"txn_id": txn_id, "commit_pos": pos, "writes": ws})
        return self

    def txn(self, txn_id, snapshot_pos, start, end, ops, status="committed",
            writes=None):
        t = {"txn_id": txn_id, "status": status, "snapshot_pos": snapshot_pos,
             "start_time": start, "end_time": end, "operations": ops}
        if writes is not None:
            t["writes"] = writes
        self.txns.append(t)
        return self

    def auto_range(self):
        if self.commits:
            positions = [c["commit_pos"] for c in self.commits]
            self.log_first = self.log_first if self.log_first is not None \
                else min(positions)
            self.log_last = self.log_last if self.log_last is not None \
                else max(positions)
        return self

    def doc(self):
        self.auto_range()
        return {
            "format": "snapshot-history/1",
            "initial_state": self.initial,
            "commit_log": {
                "first_commit_pos": self.log_first,
                "last_commit_pos": self.log_last,
                "complete": self.log_complete,
                "commits": self.commits,
            },
            "transactions": self.txns,
        }


class _Tomb:
    pass


TOMBSTONE = _Tomb()


def read_op(seq, key, value=None, version_id=None, found=None):
    op = {"seq": seq, "type": "point_read", "key": key}
    op["found"] = value is not None if found is None else found
    if op["found"]:
        op["value"] = value
        if version_id is not None:
            op["version_id"] = version_id
    return op


def range_op(seq, lo, hi, entries, complete=True):
    return {"seq": seq, "type": "range_read", "lo": lo, "hi": hi,
            "entries": [{"key": k, "value": v,
                         **({"version_id": vid} if vid else {})}
                        for (k, v, vid) in entries],
            "complete": complete}


def put_op(seq, key, value):
    return {"seq": seq, "type": "put", "key": key, "value": value}


def del_op(seq, key):
    return {"seq": seq, "type": "delete", "key": key}


def audit_doc(doc, **kwargs):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(doc, fh)
        path = fh.name
    try:
        store = Store()
        try:
            header, commits, txns = load_history(path, None)
            store.ingest(header, commits, txns)
            return run_audit(store, **kwargs), store
        except Exception:
            store.close()
            raise
    finally:
        os.unlink(path)


class AuditCase(unittest.TestCase):
    def audit(self, doc, **kwargs):
        result, store = audit_doc(doc, **kwargs)
        self.addCleanup(store.close)
        return result

    def assertVerdict(self, result, verdict):
        self.assertEqual(result["verdict"], verdict,
                         f"expected {verdict}, got {result['verdict']}: "
                         f"{json.dumps(result, indent=1)[:2000]}")
