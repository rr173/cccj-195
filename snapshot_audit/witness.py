"""Independent witness verification.

``verify-witness`` never trusts the auditor's stored conclusions: every edge
of a cycle witness, every claimed anomaly, and every replay certificate is
re-derived from the raw history.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .model import (
    INITIAL_TXN, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ,
)
from .replay import replay_order
from .store import Store


class WitnessFailure(Exception):
    pass


# ---------------------------------------------------------------------------
# edge re-derivation
# ---------------------------------------------------------------------------

def _recheck_ww(store: Store, e: Dict[str, Any]) -> None:
    key = e.get("key")
    v1, v2 = e.get("version_id"), e.get("succ_version_id")
    if key is None or not v1 or not v2:
        raise WitnessFailure(f"ww edge missing fields: {e}")
    r1 = store.version_by_id(v1)
    r2 = store.version_by_id(v2)
    if r1 is None or r2 is None:
        raise WitnessFailure(f"ww edge references unknown versions {v1},{v2}")
    if r1[0] != key or r2[0] != key:
        raise WitnessFailure(f"ww edge key mismatch for {v1},{v2}")
    if r1[3] != e["src"] or r2[3] != e["dst"]:
        raise WitnessFailure("ww edge endpoints do not match version writers")
    if not r1[1] < r2[1]:
        raise WitnessFailure("ww edge versions out of commit order")
    gap = store.db.execute(
        "SELECT 1 FROM versions WHERE key=? AND commit_pos>? AND commit_pos<? "
        "LIMIT 1", (key, r1[1], r2[1])).fetchone()
    if gap:
        raise WitnessFailure(f"ww versions {v1},{v2} are not consecutive")


def _read_op(store: Store, txn_id: str, seq: int):
    return store.db.execute(
        "SELECT seq, type, key, lo, hi, value, found, version_id, entries, "
        "complete, loc FROM ops WHERE txn_id=? AND seq=?",
        (txn_id, seq)).fetchone()


def _self_written_before(store: Store, txn_id: str, seq: int, key: int) -> bool:
    return store.db.execute(
        "SELECT 1 FROM ops WHERE txn_id=? AND seq<? AND key=? "
        "AND type IN ('put','delete') LIMIT 1", (txn_id, seq, key)).fetchone() \
        is not None


def _check_read_covers(op, key: int) -> Tuple[int, int]:
    """Return (lo, hi) of the read op if it covers *key*."""
    _seq, typ, k, lo, hi, _v, _f, _vid, _e, _cf, _loc = op
    if typ == OP_POINT_READ and k == key:
        return (k, k)
    if typ == OP_RANGE_READ and lo <= key <= hi:
        return (lo, hi)
    raise WitnessFailure(f"read op does not cover key {key}")


def _reader_snapshot(store: Store, txn_id: str) -> int:
    row = store.db.execute(
        "SELECT snapshot_pos FROM txns WHERE txn_id=? AND status='committed'",
        (txn_id,)).fetchone()
    if row is None or row[0] is None:
        raise WitnessFailure(f"reader {txn_id} has no committed snapshot")
    return row[0]


def _recheck_wr(store: Store, e: Dict[str, Any]) -> None:
    key, vid = e.get("key"), e.get("version_id")
    reader, seq = e.get("read_txn"), e.get("read_seq")
    if key is None or not vid or reader is None or seq is None:
        raise WitnessFailure(f"wr edge missing fields: {e}")
    row = store.version_by_id(vid)
    if row is None or row[0] != key:
        raise WitnessFailure(f"wr edge version {vid} unknown or wrong key")
    if row[3] != e["src"]:
        raise WitnessFailure("wr edge src is not the version writer")
    if row[3] != INITIAL_TXN and store.db.execute(
            "SELECT 1 FROM txns WHERE txn_id=? AND status='committed'",
            (row[3],)).fetchone() is None:
        raise WitnessFailure("wr edge writer is not a committed txn")
    sp = _reader_snapshot(store, reader)
    op = _read_op(store, reader, seq)
    if op is None:
        raise WitnessFailure(f"read op {reader}/{seq} not found")
    lo, hi = _check_read_covers(op, key)
    if e.get("lo") is not None and (e["lo"], e["hi"]) != (lo, hi):
        raise WitnessFailure("wr edge interval does not match the read op")
    if row[1] > sp:
        raise WitnessFailure("wr version committed after the reader snapshot")
    newer = store.db.execute(
        "SELECT 1 FROM versions WHERE key=? AND commit_pos>? AND commit_pos<=? "
        "LIMIT 1", (key, row[1], sp)).fetchone()
    if newer:
        raise WitnessFailure("wr version is not the snapshot-visible one")
    if _self_written_before(store, reader, seq, key):
        raise WitnessFailure("read key was self-written; edge must not exist")


def _recheck_rw(store: Store, e: Dict[str, Any]) -> None:
    key = e.get("key")
    reader, seq = e.get("read_txn"), e.get("read_seq")
    succ_vid = e.get("succ_version_id")
    if key is None or reader is None or seq is None or not succ_vid:
        raise WitnessFailure(f"rw edge missing fields: {e}")
    sp = _reader_snapshot(store, reader)
    op = _read_op(store, reader, seq)
    if op is None:
        raise WitnessFailure(f"read op {reader}/{seq} not found")
    lo, hi = _check_read_covers(op, key)
    if e.get("lo") is not None and (e["lo"], e["hi"]) != (lo, hi):
        raise WitnessFailure("rw edge interval does not match the read op")
    # read version must be the snapshot-visible one (or none)
    vis = store.visible_version(key, sp)
    expect_vid = vis[2] if vis else None
    if e.get("version_id") != expect_vid:
        raise WitnessFailure(
            f"rw edge read_version {e.get('version_id')} != visible {expect_vid}")
    succ = store.successor_version(key, sp)
    if succ is None or succ[2] != succ_vid:
        raise WitnessFailure(f"rw edge successor {succ_vid} is not the first "
                             f"version after snapshot {sp}")
    if succ[3] != e["dst"]:
        raise WitnessFailure("rw edge dst is not the successor writer")
    if succ[3] == reader:
        raise WitnessFailure("rw edge is a self loop")
    if _self_written_before(store, reader, seq, key):
        raise WitnessFailure("read key was self-written; edge must not exist")


def _recheck_realtime(store: Store, e: Dict[str, Any]) -> None:
    a = store.db.execute(
        "SELECT end_time FROM txns WHERE txn_id=? AND status='committed'",
        (e["src"],)).fetchone()
    b = store.db.execute(
        "SELECT start_time FROM txns WHERE txn_id=? AND status='committed'",
        (e["dst"],)).fetchone()
    if not a or not b or a[0] is None or b[0] is None:
        raise WitnessFailure("realtime edge references txn without times")
    if not a[0] < b[0]:
        raise WitnessFailure(
            f"realtime edge false: {e['src']}.end={a[0]} !< {e['dst']}.start={b[0]}")


def recheck_edge(store: Store, e: Dict[str, Any]) -> None:
    kind = e.get("kind")
    if e.get("src") == e.get("dst"):
        raise WitnessFailure("self edge")
    if kind == "ww":
        _recheck_ww(store, e)
    elif kind == "wr":
        _recheck_wr(store, e)
    elif kind == "rw":
        _recheck_rw(store, e)
    elif kind == "realtime":
        _recheck_realtime(store, e)
    else:
        raise WitnessFailure(f"unknown edge kind {kind!r}")


# ---------------------------------------------------------------------------
# witnesses
# ---------------------------------------------------------------------------

def verify_cycle_witness(store: Store, cycle: List[Dict[str, Any]]) -> List[str]:
    """Re-derive every edge from the raw history; return list of problems."""
    problems: List[str] = []
    if not isinstance(cycle, list) or len(cycle) < 2:
        return ["cycle witness must contain at least 2 edges"]
    for i, e in enumerate(cycle):
        try:
            recheck_edge(store, e)
        except WitnessFailure as w:
            problems.append(f"edge {i} ({e.get('src')}->{e.get('dst')}): {w}")
    for i, e in enumerate(cycle):
        nxt = cycle[(i + 1) % len(cycle)]
        if e.get("dst") != nxt.get("src"):
            problems.append(f"cycle does not close between edge {i} and "
                            f"edge {(i + 1) % len(cycle)}")
    nodes = [e.get("src") for e in cycle]
    if len(set(nodes)) != len(nodes):
        problems.append("cycle is not simple (a node appears twice)")
    return problems


def verify_certificate(store: Store, order: List[str]) -> List[str]:
    """Replay the claimed serial order; also check WW and real-time respect."""
    problems: List[str] = []
    committed = {t for (t,) in store.db.execute(
        "SELECT txn_id FROM txns WHERE status='committed'")}
    claimed = [t for t in order if t != INITIAL_TXN]
    if set(claimed) != committed or len(claimed) != len(set(claimed)):
        problems.append("certificate order does not equal the committed txn set")
        return problems
    pos = {t: i for i, t in enumerate(order)}
    # declared version order
    prev = None
    for (key, _p, _vid, tid) in store.db.execute(
            "SELECT key, commit_pos, version_id, txn_id FROM versions "
            "ORDER BY key, commit_pos"):
        if prev is not None and prev[0] == key:
            a, b = prev[1], tid
            if a != INITIAL_TXN and b != INITIAL_TXN and a != b:
                if pos[a] > pos[b]:
                    problems.append(f"order violates declared version order on "
                                    f"key {key}: {a} after {b}")
        prev = (key, tid)
    # real time
    rows = store.db.execute(
        "SELECT txn_id, start_time, end_time FROM txns "
        "WHERE status='committed' AND start_time IS NOT NULL "
        "AND end_time IS NOT NULL").fetchall()
    for (a, _sa, ea) in rows:
        for (b, sb, _eb) in rows:
            if a != b and ea < sb and pos[a] > pos[b]:
                problems.append(f"order violates real time: {a} ended before "
                                f"{b} started but is placed after")
    failures = replay_order(store, order)
    for f in failures[:20]:
        problems.append(f"replay: {f.txn_id} op {f.op_seq}: {f.detail}")
    if len(failures) > 20:
        problems.append(f"replay: ... and {len(failures) - 20} more failures")
    return problems


def verify_anomaly(store: Store, a: Dict[str, Any]) -> List[str]:
    """Re-check one claimed anomaly from the raw history."""
    from .reads import LOG_GAP_KINDS, verify_reads
    problems: List[str] = []
    kind = a.get("kind")
    key = a.get("key")
    obs = a.get("observed") or {}
    vid = obs.get("version_id")
    tid = a.get("txn_id")
    row = store.db.execute(
        "SELECT snapshot_pos FROM txns WHERE txn_id=?", (tid,)).fetchone()
    if row is None:
        return [f"anomaly references unknown txn {tid}"]
    sp = row[0]
    if kind == "future_version_read":
        v = store.version_by_id(vid) if vid else None
        if v is None or v[1] <= sp:
            problems.append("claimed future version is not committed after "
                            "the reader snapshot")
    elif kind == "aborted_version_read":
        if not vid or store.aborted_by_id(vid) is None:
            problems.append("claimed aborted version not found among "
                            "rolled-back writes")
    elif kind == "unknown_version_read":
        if not vid:
            problems.append("no version id in claim")
        elif store.version_by_id(vid) is not None or \
                store.aborted_by_id(vid) is not None:
            problems.append("claimed unknown version actually exists")
        log_ok = not any(g.kind in LOG_GAP_KINDS for g in store.gaps)
        if not log_ok:
            problems.append("log incomplete: unknown-version claim not "
                            "confirmable")
    elif kind in ("stale_version_read", "value_mismatch", "missing_key",
                  "extra_key", "cross_key_version_read"):
        # re-run read verification for the affected transaction and require
        # the same anomaly to be reproduced at the same operation
        rep = verify_reads(store)
        match = [x for x in rep.anomalies
                 if x.kind == kind and x.txn_id == tid
                 and x.op_seq == a.get("op_seq") and x.key == key]
        if not match:
            problems.append(f"re-verification did not reproduce {kind} at "
                            f"{tid} op {a.get('op_seq')}")
    else:
        problems.append(f"unknown anomaly kind {kind!r}")
    return problems
