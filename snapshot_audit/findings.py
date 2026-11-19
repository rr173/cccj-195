"""Normalized finding identities for cross-run regression comparison.

A finding identity must not depend on anything that can change merely because
the same logical history was collected again:

* no file line numbers,
* no ``commit_pos`` (the same commit may land at another position),
* no SQLite rowids,
* no ``version_id`` values (ids may be renumbered) - they survive only inside
  the *evidence* block,
* dependency-cycle identities are rotated to a canonical minimum so the same
  ring starting at a different node has the same identity.

Three finding classes are produced:

* ``read_anomaly``     - identity pins the transaction (canonical id),
                         operation seq, anomaly category, key or closed
                         interval and the observed/expected *semantic* values;
* ``dependency_cycle`` - identity pins each edge's endpoints (canonical),
                         edge type, key/interval and predecessor/successor
                         write semantics, with canonical rotation;
* ``gap``               - identity pins a stable gap category plus the
                         transaction/operation/key/interval scope that can be
                         derived without line numbers or version ids.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from .hashing import canonical_json, sha256_text
from .manifest import IdentityMapping
from .store import Store

# cap on the cartesian product when enumerating the legal witnesses of a ring
WITNESS_VARIANT_CAP = 1024

_EVIDENCE_ONLY_KEYS = {"version_id", "succ_version_id", "predecessor"}


def _uid(identity: Dict[str, Any]) -> str:
    return sha256_text(canonical_json(identity))[:16]


def _strip_evidence_only(obj: Any) -> Any:
    """Remove version-id style fields; keep semantic values recursively."""
    if isinstance(obj, dict):
        return {k: _strip_evidence_only(v) for k, v in sorted(obj.items())
                if k not in _EVIDENCE_ONLY_KEYS}
    if isinstance(obj, list):
        return [_strip_evidence_only(v) for v in obj]
    return obj


def _interval(lo: Optional[int], hi: Optional[int],
              key: Optional[int]) -> Dict[str, Any]:
    if key is not None:
        return {"key": key}
    return {"lo": lo, "hi": hi}


# ---------------------------------------------------------------------------
# read anomalies
# ---------------------------------------------------------------------------

def read_anomaly_identity(anomaly: Dict[str, Any], mapping: IdentityMapping,
                          run: str) -> Dict[str, Any]:
    txn = mapping.canonical_id(run, anomaly["txn_id"])
    identity: Dict[str, Any] = {
        "finding": "read_anomaly",
        "category": anomaly["kind"],
        "txn": txn,
        "op_seq": anomaly["op_seq"],
        "target": _interval(anomaly.get("lo"), anomaly.get("hi"),
                            anomaly.get("key")),
        "expected_sem": _strip_evidence_only(anomaly.get("expected")),
        "observed_sem": _strip_evidence_only(anomaly.get("observed")),
    }
    return identity


# ---------------------------------------------------------------------------
# dependency cycles
# ---------------------------------------------------------------------------

def _write_sem(store: Store, vid: Optional[str]) -> Optional[Dict[str, Any]]:
    if vid is None:
        return None
    row = store.version_by_id(vid)
    if row is None:
        return None
    _key, _pos, _vid, _txn, tomb, val, _loc = row
    return {"tombstone": bool(tomb),
            "value": None if val is None else json.loads(val)}


def _read_sem(store: Store, read_txn: Optional[str],
              read_seq: Optional[int]) -> Optional[Dict[str, Any]]:
    if read_txn is None or read_seq is None:
        return None
    row = store.db.execute(
        "SELECT type,key,lo,hi,value,found FROM ops WHERE txn_id=? AND seq=?",
        (read_txn, read_seq)).fetchone()
    if row is None:
        return None
    typ, key, lo, hi, value, found = row
    sem: Dict[str, Any] = {"op_type": typ,
                           "target": _interval(lo, hi, key)}
    if typ == "point_read":
        sem["found"] = bool(found)
        if found and value is not None:
            sem["value"] = json.loads(value)
    return sem


def canonical_edge(store: Store, edge_row: tuple, mapping: IdentityMapping,
                   run: str) -> Dict[str, Any]:
    """Build the line/version-id independent identity of one graph edge."""
    (src, dst, kind, key, lo, hi, version_id, succ_version_id,
     read_txn, read_seq, evidence_json) = edge_row
    csrc = mapping.canonical_id(run, src)
    cdst = mapping.canonical_id(run, dst)
    out: Dict[str, Any] = {"edge": kind, "src": csrc, "dst": cdst}
    if kind in ("ww", "wr", "rw"):
        out["target"] = _interval(lo, hi, key)
    if kind == "ww":
        out["predecessor_write"] = _write_sem(store, version_id)
        out["successor_write"] = _write_sem(store, succ_version_id)
    elif kind == "wr":
        out["read_txn"] = mapping.canonical_id(run, read_txn)
        out["read_seq"] = read_seq
        out["read_sem"] = _read_sem(store, read_txn, read_seq)
        out["writer_write"] = _write_sem(store, version_id)
    elif kind == "rw":
        out["read_txn"] = mapping.canonical_id(run, read_txn)
        out["read_seq"] = read_seq
        out["read_sem"] = _read_sem(store, read_txn, read_seq)
        out["read_version_present"] = version_id is not None
        out["successor_write"] = _write_sem(store, succ_version_id)
    elif kind == "realtime":
        ev = json.loads(evidence_json) if evidence_json else {}
        out["a_end"] = ev.get("a_end")
        out["b_start"] = ev.get("b_start")
    else:  # pragma: no cover - defensive
        raise ValueError(f"unknown edge kind {kind!r}")
    return out


def canonical_rotation(edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rotate an ordered edge ring to its lexicographically smallest form.

    The same logical cycle reported from a different start node produces the
    same canonical edge sequence.
    """
    if not edges:
        return []
    keys = [canonical_json(e) for e in edges]
    best = 0
    best_key = keys[0]
    for i in range(1, len(keys)):
        if keys[i] < best_key:
            best = i
            best_key = keys[i]
    return edges[best:] + edges[:best]


def ring_signature(canonical_edges: List[Dict[str, Any]]) -> Tuple[str, ...]:
    """Canonical ordered node ring (rotation-normalized) for ring identity."""
    nodes = [e["src"] for e in canonical_edges]
    rotations = [tuple(nodes[i:] + nodes[:i]) for i in range(len(nodes))]
    return min(rotations)


def enumerate_witness_variants(store: Store, witness_edges: List[Dict[str, Any]],
                               mapping: IdentityMapping, run: str
                               ) -> Tuple[List[Dict[str, Any]], bool]:
    """All distinct legal witnesses for the emitted ring.

    Each consecutive node pair may have several concrete edges (different
    edge types/keys); every combination that closes the same ring is a legal
    witness.  Returns ``(variants, truncated)`` where each variant is a
    canonical, rotation-normalized edge list.
    """
    local_ring = [(e["src"], e["dst"]) for e in witness_edges]
    per_step: List[List[Dict[str, Any]]] = []
    for src, dst in local_ring:
        rows = store.db.execute(
            "SELECT src,dst,kind,key,lo,hi,version_id,succ_version_id,"
            "read_txn,read_seq,evidence FROM edges WHERE src=? AND dst=? "
            "ORDER BY kind, key, lo, hi, read_seq", (src, dst)).fetchall()
        if not rows:  # ring claimed by a witness whose edges no longer exist
            return [], False
        per_step.append([canonical_edge(store, r, mapping, run) for r in rows])
    combos = 1
    for options in per_step:
        combos *= len(options)
        if combos > WITNESS_VARIANT_CAP:
            break
    variants: List[Dict[str, Any]] = []
    if combos <= WITNESS_VARIANT_CAP:
        seen: set = set()

        def recurse(i: int, acc: List[Dict[str, Any]]) -> None:
            if i == len(per_step):
                rotated = canonical_rotation(acc)
                key = canonical_json(rotated)
                if key not in seen:
                    seen.add(key)
                    variants.append({"edges": rotated,
                                     "ring": list(ring_signature(rotated)),
                                     "uid": sha256_text(key)[:16]})
                return
            for option in per_step[i]:
                recurse(i + 1, acc + [option])

        recurse(0, [])
    return variants, combos > WITNESS_VARIANT_CAP


def cycle_finding(store: Store, witness_edges: List[Dict[str, Any]],
                  mapping: IdentityMapping, run: str) -> Dict[str, Any]:
    """Build the canonical finding for one emitted cycle witness."""
    raw_rows = []
    for e in witness_edges:
        row = store.db.execute(
            "SELECT src,dst,kind,key,lo,hi,version_id,succ_version_id,"
            "read_txn,read_seq,evidence FROM edges "
            "WHERE src=? AND dst=? ORDER BY kind, key LIMIT 1",
            (e["src"], e["dst"])).fetchone()
        raw_rows.append(row)
    emitted = [canonical_edge(store, r, mapping, run) for r in raw_rows]
    emitted_rotated = canonical_rotation(emitted)
    variants, truncated = enumerate_witness_variants(
        store, witness_edges, mapping, run)
    if not variants:
        # emitted witness itself must always be representable
        emitted_key = canonical_json(emitted_rotated)
        variants = [{"edges": emitted_rotated,
                     "ring": list(ring_signature(emitted_rotated)),
                     "uid": sha256_text(emitted_key)[:16]}]
    identity = {
        "finding": "dependency_cycle",
        "ring": list(ring_signature(emitted_rotated)),
        "witness": emitted_rotated,
    }
    evidence_locs: List[Dict[str, Any]] = []
    for e in witness_edges:
        ev = e.get("evidence") or {}
        evidence_locs.append({
            "src": e.get("src"), "dst": e.get("dst"), "kind": e.get("kind"),
            "key": e.get("key"), "lo": e.get("lo"), "hi": e.get("hi"),
            "version_id": e.get("version_id"),
            "succ_version_id": e.get("succ_version_id"),
            "read_txn": e.get("read_txn"), "read_seq": e.get("read_seq"),
            "loc": ev,
        })
    return {
        "finding_class": "dependency_cycle",
        "uid": _uid(identity),
        "identity": identity,
        "ring": identity["ring"],
        "variant_uids": sorted(v["uid"] for v in variants),
        "variants_truncated": truncated,
        "evidence": {"witness_locs": evidence_locs},
        "refs": {"txns": sorted({n for n in identity["ring"]})},
    }


# ---------------------------------------------------------------------------
# gaps
# ---------------------------------------------------------------------------

_RE_KEY = re.compile(r"key (-?\d+)")
_RE_POS = re.compile(r"commit_pos (\d+)")
_RE_OP = re.compile(r"op (\d+)")
_RE_INTERVAL = re.compile(r"\[(-?\d+),(-?\d+)\]")
_RE_SUSPECTED = re.compile(r"suspected (\w+)")
_RE_OP_LOC = re.compile(r"\.operations\[(\d+)\]")


def _loc_resolvers(store: Store) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    """Map record locs back to logical txn ids (line numbers never used)."""
    txn_locs: Dict[str, str] = {}
    pairs: List[Tuple[str, str]] = []
    for (tid, loc) in store.db.execute("SELECT txn_id, loc FROM txns"):
        if loc:
            txn_locs[loc] = tid
            pairs.append((loc, tid))
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return txn_locs, pairs


def _txn_from_loc(loc: str, pairs: List[Tuple[str, str]]) -> Optional[str]:
    for prefix, tid in pairs:
        if loc == prefix or loc.startswith(prefix + ".") or \
                loc.startswith(prefix + ".operations"):
            return tid
    return None


def gap_identity(gap: Dict[str, Any], store: Store,
                 mapping: IdentityMapping, run: str,
                 loc_pairs: List[Tuple[str, str]],
                 txn_ids: set) -> Dict[str, Any]:
    kind = gap["kind"]
    detail = gap.get("detail", "")
    loc = gap.get("loc", "")
    scope: Dict[str, Any] = {}

    def ctx_txn() -> Optional[str]:
        tid = _txn_from_loc(loc, loc_pairs)
        if tid is None:
            for t in sorted(txn_ids):
                if t in detail or t in loc:
                    tid = t
                    break
        return mapping.canonical_id(run, tid) if tid else None

    m_op = _RE_OP_LOC.search(loc) or _RE_OP.search(detail)
    if m_op:
        scope["op_seq"] = int(m_op.group(1))
    m_interval = _RE_INTERVAL.search(detail)
    if m_interval and kind in ("incomplete_scan", "unverifiable_read",
                               "bad_range"):
        scope["interval"] = [int(m_interval.group(1)), int(m_interval.group(2))]
    m_suspected = _RE_SUSPECTED.search(detail)
    if kind == "unverifiable_read" and m_suspected:
        scope["suspected_category"] = m_suspected.group(1)

    if kind in ("missing_snapshot_pos", "missing_times", "end_before_start",
                "missing_operations", "bad_op_seq", "unknown_op_type",
                "point_read_found_missing", "range_entries_missing",
                "incomplete_scan", "unverifiable_read",
                "key_written_twice_in_txn", "snapshot_not_before_commit",
                "committed_writes_missing_from_log",
                "txn_writes_vs_log_mismatch", "missing_write_ops"):
        t = ctx_txn()
        if t is not None:
            scope["txn"] = t
    if kind in ("duplicate_initial_key", "missing_version_id",
                "bad_predecessor", "key_written_twice_in_commit",
                "key_written_twice_in_txn", "txn_writes_vs_log_mismatch"):
        m = _RE_KEY.search(detail)
        if m:
            scope["key"] = int(m.group(1))
    if kind in ("duplicate_commit_pos",):
        m = _RE_POS.search(detail)
        if m:
            scope["commit_pos"] = int(m.group(1))
    if kind == "log_range_gap":
        nums = sorted(int(x) for x in re.findall(r"\d+", detail))
        if nums:
            scope["missing_commit_pos"] = nums
    if kind == "commit_outside_declared_range":
        nums = sorted(int(x) for x in re.findall(r"\d+", detail))
        if nums:
            scope["outside_commit_pos"] = nums
    if kind == "graph_exceeds_memory_budget":
        scope["component"] = loc or "graph"
    if kind == "duplicate_key_in_range_result":
        m = _RE_KEY.search(detail)
        if m:
            scope["key"] = int(m.group(1))
        t = ctx_txn()
        if t is not None:
            scope["txn"] = t
    if kind == "bad_range":
        t = ctx_txn()
        if t is not None:
            scope["txn"] = t
    # kinds with no stable scope beyond their category keep an empty scope:
    # log_range_incomplete, log_range_undeclared, commit_pos_not_increasing,
    # duplicate_version_id, version_chain_broken (version ids are evidence
    # only), commit_without_txn_record, ...
    identity = {"finding": "gap", "category": kind,
                "scope": dict(sorted(scope.items()))}
    return identity


# ---------------------------------------------------------------------------
# per-run finding extraction
# ---------------------------------------------------------------------------

def build_findings(store: Store, audit_result: Dict[str, Any],
                   mapping: IdentityMapping, run: str,
                   ) -> Dict[str, List[Dict[str, Any]]]:
    """Convert one fresh audit into normalized finding records."""
    txn_ids = {t for (t,) in store.db.execute(
        "SELECT txn_id FROM txns").fetchall()}
    if mapping.baseline_ids is None or mapping.candidate_ids is None:
        mapping.finalize(sorted(txn_ids), sorted(txn_ids))
    _txn_locs, loc_pairs = _loc_resolvers(store)

    read_findings: List[Dict[str, Any]] = []
    for a in audit_result["anomalies"]:
        identity = read_anomaly_identity(a, mapping, run)
        read_findings.append({
            "finding_class": "read_anomaly",
            "uid": _uid(identity),
            "identity": identity,
            "evidence": {
                "loc": a.get("loc", ""),
                "detail": a.get("detail", ""),
                "expected": a.get("expected"),
                "observed": a.get("observed"),
            },
            "refs": {"txn": a["txn_id"], "op_seq": a["op_seq"]},
        })

    cycle_findings: List[Dict[str, Any]] = []
    graph = audit_result.get("graph")
    if graph and graph.get("cycle") and graph["cycle"].get("edges"):
        cycle_findings.append(cycle_finding(
            store, graph["cycle"]["edges"], mapping, run))

    gap_findings: List[Dict[str, Any]] = []
    for g in audit_result["gaps"]:
        identity = gap_identity(g, store, mapping, run, loc_pairs, txn_ids)
        gap_findings.append({
            "finding_class": "gap",
            "uid": _uid(identity),
            "identity": identity,
            "evidence": {"loc": g.get("loc", ""), "detail": g.get("detail", "")},
            "refs": {"txn": identity["scope"].get("txn"),
                     "op_seq": identity["scope"].get("op_seq")},
        })
    return {"read_anomaly": read_findings,
            "dependency_cycle": cycle_findings,
            "gap": gap_findings}
