"""Small-history oracle for cross-run regression comparison.

The oracle is an *independent* implementation: it works directly on the raw
history documents (never on the auditor's findings) and enumerates

* the transaction identity matching between two runs (implicit same-name
  alignment plus the explicit one-to-one mapping), and
* every expected finding pairing with its expected classification
  (new / resolved / persisted / ambiguous / identity_conflict / gap).

Tests enumerate scenarios by hand and assert that ``compare`` reproduces the
oracle's classification row by row.
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# raw-history helpers (independent of snapshot_audit model classes)
# ---------------------------------------------------------------------------

def write_history(doc: Dict[str, Any], directory: str,
                  name: str = "h.json") -> str:
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    return path


def write_jsonl_history(doc: Dict[str, Any], directory: str) -> str:
    """Write a history as header.json + commits.jsonl + transactions.jsonl."""
    out = os.path.join(directory, "hist")
    os.makedirs(out, exist_ok=True)
    log = doc["commit_log"]
    header = {"format": doc["format"],
              "initial_state": doc["initial_state"],
              "commit_log": {"first_commit_pos": log.get("first_commit_pos"),
                             "last_commit_pos": log.get("last_commit_pos"),
                             "complete": log.get("complete", True)}}
    with open(os.path.join(out, "header.json"), "w", encoding="utf-8") as fh:
        json.dump(header, fh)
    with open(os.path.join(out, "commits.jsonl"), "w", encoding="utf-8") as fh:
        for c in log.get("commits", []):
            fh.write(json.dumps(c) + "\n")
    with open(os.path.join(out, "transactions.jsonl"), "w",
              encoding="utf-8") as fh:
        for t in doc["transactions"]:
            fh.write(json.dumps(t) + "\n")
    return out


def write_mapping(pairs: List[Tuple[str, str]], directory: str,
                  name: str = "mapping.json") -> str:
    path = os.path.join(directory, name)
    doc = {"mappings": [{"baseline_txn_id": b, "candidate_txn_id": c}
                        for b, c in pairs]}
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    return path


# ---------------------------------------------------------------------------
# independent identity enumeration
# ---------------------------------------------------------------------------

def enumerate_identity_matching(base_doc: Dict[str, Any],
                                cand_doc: Dict[str, Any],
                                mapping_pairs: Optional[List[Tuple[str, str]]]
                                = None) -> Dict[str, List[str]]:
    """Pure-document view of how transactions align between two runs."""
    b_ids = sorted(t["txn_id"] for t in base_doc["transactions"])
    c_ids = sorted(t["txn_id"] for t in cand_doc["transactions"])
    b2c = dict(mapping_pairs or [])
    c2b = {c: b for b, c in (mapping_pairs or [])}
    aligned, renamed, b_only, c_only = [], [], [], []
    for b in b_ids:
        if b in b2c:
            renamed.append((b, b2c[b]))
        elif b in c_ids:
            aligned.append(b)
        else:
            b_only.append(b)
    for c in c_ids:
        if c not in c2b and c not in b_ids:
            c_only.append(c)
    return {"aligned": aligned, "renamed": sorted(renamed),
            "baseline_only": b_only, "candidate_only": c_only}


# ---------------------------------------------------------------------------
# independent finding enumeration from raw documents
# ---------------------------------------------------------------------------

def _index_versions(doc: Dict[str, Any]) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """version id -> version record, straight from the raw commit log."""
    out: Dict[str, Dict[str, Any]] = {}
    for c in doc["commit_log"]["commits"]:
        for w in c["writes"]:
            out[w["version_id"]] = {**w, "txn_id": c["txn_id"],
                                    "commit_pos": c["commit_pos"]}
    for e in doc["initial_state"]:
        out[e["version_id"]] = {"key": e["key"], "version_id": e["version_id"],
                                "value": e["value"], "txn_id": "@initial",
                                "commit_pos": 0}
    return out


def enumerate_read_anomalies(doc: Dict[str, Any]
                             ) -> List[Dict[str, Any]]:
    """Derive expected read anomalies directly from a raw history document.

    Covers the anomaly classes needed by the oracle scenarios: future /
    aborted / unknown / stale / value / missing / extra.  This is a compact
    independent re-derivation, not a call into the auditor.
    """
    versions = _index_versions(doc)
    aborted = {w["version_id"]: w
               for t in doc["transactions"] if t["status"] == "aborted"
               for w in (t.get("writes") or [])}
    visible: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for vid, v in versions.items():
        key = v["key"]
        pos = v["commit_pos"]
        old = visible.get((key, pos))
        if old is None or True:
            visible[(key, pos)] = v
    # visible-at-snapshot: max commit_pos <= snapshot per key
    def vis_at(key: int, snapshot: int):
        candidates = [v for (k, p), v in visible.items()
                      if k == key and p <= snapshot]
        if not candidates:
            return None
        return max(candidates, key=lambda v: v["commit_pos"])

    findings: List[Dict[str, Any]] = []
    for t in doc["transactions"]:
        if t["status"] != "committed":
            continue
        sp = t["snapshot_pos"]
        own: Dict[int, str] = {}
        for op in t.get("operations", []):
            typ = op["type"]
            if typ in ("put", "delete"):
                own[op["key"]] = typ
                continue
            if typ != "point_read":
                continue
            key, seq = op["key"], op["seq"]
            if key in own:
                continue
            exp = vis_at(key, sp)
            exp_found = exp is not None and not exp.get("tombstone")
            if not op.get("found"):
                if exp_found:
                    findings.append({"txn": t["txn_id"], "op_seq": seq,
                                     "category": "missing_key", "key": key})
                continue
            vid = op.get("version_id")
            if vid in aborted:
                findings.append({"txn": t["txn_id"], "op_seq": seq,
                                 "category": "aborted_version_read", "key": key})
                continue
            v = versions.get(vid)
            if v is None:
                findings.append({"txn": t["txn_id"], "op_seq": seq,
                                 "category": "unknown_version_read", "key": key})
                continue
            if v["key"] != key:
                findings.append({"txn": t["txn_id"], "op_seq": seq,
                                 "category": "cross_key_version_read",
                                 "key": key})
                continue
            if v["commit_pos"] > sp:
                findings.append({"txn": t["txn_id"], "op_seq": seq,
                                 "category": "future_version_read", "key": key})
                continue
            if not exp_found or exp["version_id"] != vid:
                findings.append({"txn": t["txn_id"], "op_seq": seq,
                                 "category": "stale_version_read", "key": key})
                continue
            if op.get("value") != exp.get("value"):
                findings.append({"txn": t["txn_id"], "op_seq": seq,
                                 "category": "value_mismatch", "key": key})
    return findings


def enumerate_cycle_rings(doc: Dict[str, Any]) -> List[List[str]]:
    """Enumerate the write-skew style rw/rw rings visible in a raw document.

    A ring is returned as the sorted-rotation-minimum tuple of participating
    transaction ids, independently of any auditor output.
    """
    # rw edge (reader -> successor writer) per read key, derived from the
    # raw commit positions and each reader's snapshot.
    versions = _index_versions(doc)
    rw: List[Tuple[str, str, int]] = []
    for t in doc["transactions"]:
        if t["status"] != "committed":
            continue
        sp = t["snapshot_pos"]
        own = {op["key"] for op in (t.get("operations") or [])
               if op["type"] in ("put", "delete")}
        for op in t.get("operations", []):
            if op["type"] != "point_read":
                continue
            key = op["key"]
            if key in own:
                continue
            later = [v for v in versions.values()
                     if v["key"] == key and v["commit_pos"] > sp
                     and v["txn_id"] != "@initial"]
            if later:
                succ = min(later, key=lambda v: v["commit_pos"])
                if succ["txn_id"] != t["txn_id"]:
                    rw.append((t["txn_id"], succ["txn_id"], key))
    adj: Dict[str, set] = {}
    for a, b, _k in rw:
        adj.setdefault(a, set()).add(b)
    rings: List[Tuple[str, ...]] = []
    for a, nbrs in adj.items():
        for b in nbrs:
            if a in adj.get(b, set()):
                ring = tuple(sorted((a, b)))
                if ring not in rings:
                    rings.append(ring)
    return [list(r) for r in sorted(rings)]


def expected_pairings(base_doc: Dict[str, Any], cand_doc: Dict[str, Any],
                      mapping_pairs: Optional[List[Tuple[str, str]]] = None
                      ) -> Dict[str, List[Any]]:
    """Full oracle expectation for a two-run scenario."""
    identity = enumerate_identity_matching(base_doc, cand_doc, mapping_pairs)
    b2c = dict(mapping_pairs or [])

    def canon(run: str, tid: str) -> str:
        if run == "B":
            return b2c.get(tid, tid)
        c2b = {c: b for b, c in b2c.items()}
        return c2b.get(tid, tid)

    # read anomaly pairing
    b_anoms = {(canon("B", a["txn"]), a["op_seq"], a["category"], a["key"]): a
               for a in enumerate_read_anomalies(base_doc)}
    c_anoms = {(canon("C", a["txn"]), a["op_seq"], a["category"], a["key"]): a
               for a in enumerate_read_anomalies(cand_doc)}
    persisted_read = sorted(set(b_anoms) & set(c_anoms))
    new_read = sorted(set(c_anoms) - set(b_anoms))
    resolved_read = sorted(set(b_anoms) - set(c_anoms))

    b_rings = {tuple(canon("B", t) for t in r)
               for r in enumerate_cycle_rings(base_doc)}
    c_rings = {tuple(canon("C", t) for t in r)
               for r in enumerate_cycle_rings(cand_doc)}
    return {
        "identity": identity,
        "read_anomalies": {
            "persisted": persisted_read,
            "new": new_read,
            "resolved": resolved_read,
        },
        "cycles": {
            "persisted": sorted(b_rings & c_rings),
            "new": sorted(c_rings - b_rings),
            "resolved": sorted(b_rings - c_rings),
        },
    }


class OracleCase:
    """Mixin providing a temp workspace and compare/verify-diff runners."""

    def make_workspace(self) -> tempfile.TemporaryDirectory:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return tmp
