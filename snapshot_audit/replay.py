"""Replay a serial order and check that it explains every recorded read.

Replaying processes transactions in the given order and, inside each
transaction, operations in recorded seq order.  A read is *explained* if the
version visible at that point of the replay equals the version that is
snapshot-correct for the reader (its own earlier writes overlaying the
committed state at its ``snapshot_pos``) - which read verification already
matched against the recorded observation.  Range reads are evaluated against
the version index, never by scanning the key domain.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from .model import INITIAL_TXN, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ
from .store import Store

_TOMB = object()  # sentinel for "own delete" inside a replayed transaction


class ReplayFailure:
    def __init__(self, txn_id: str, op_seq: int, detail: str, loc: str) -> None:
        self.txn_id = txn_id
        self.op_seq = op_seq
        self.detail = detail
        self.loc = loc

    def to_json(self) -> Dict[str, Any]:
        return {"txn_id": self.txn_id, "op_seq": self.op_seq,
                "detail": self.detail, "loc": self.loc}


def replay_order(store: Store, order: List[str],
                 collect_failures: bool = True) -> List[ReplayFailure]:
    """Replay *order*; return the list of unexplained reads (empty = valid)."""
    db = store.db
    # seed with the initial state (commit_pos 0, virtual txn @initial)
    state: Dict[int, Tuple[str, str]] = {}
    for (key, vid, val) in db.execute(
            "SELECT key, version_id, value FROM versions WHERE commit_pos=0"):
        state[key] = (val, vid)

    failures: List[ReplayFailure] = []
    for tid in order:
        if tid == INITIAL_TXN:
            continue  # already seeded
        row = db.execute(
            "SELECT snapshot_pos FROM txns WHERE txn_id=?", (tid,)).fetchone()
        if row is None:
            failures.append(ReplayFailure(tid, -1, "txn has no record", ""))
            continue
        sp = row[0]
        # committed versions of this txn, for version ids of its writes
        own_versions: Dict[int, Tuple[str, bool]] = {}
        for (key, vid, tomb) in db.execute(
                "SELECT key, version_id, tombstone FROM versions WHERE txn_id=?",
                (tid,)):
            own_versions[key] = (vid, bool(tomb))
        own_written: Dict[int, Any] = {}  # key -> canon value or _TOMB
        for (seq, typ, key, lo, hi, value, found, vid, entries, _cf, loc) \
                in store.txn_ops(tid):
            if typ == OP_PUT:
                ovid = own_versions.get(key, (f"own:{tid}:{key}", False))[0]
                state[key] = (value, ovid)
                own_written[key] = value
            elif typ == OP_DELETE:
                state.pop(key, None)
                own_written[key] = _TOMB
            elif typ == OP_POINT_READ:
                ok, detail = _check_point(store, state, sp, own_written,
                                          key, tid, seq)
                if not ok:
                    failures.append(ReplayFailure(tid, seq, detail, loc))
                    if not collect_failures:
                        return failures
            elif typ == OP_RANGE_READ:
                ok, detail = _check_range(store, state, sp, own_written,
                                          lo, hi, tid, seq)
                if not ok:
                    failures.append(ReplayFailure(tid, seq, detail, loc))
                    if not collect_failures:
                        return failures
    return failures


def _expected_point(store: Store, sp: int, own: Dict[int, Any],
                    key: int) -> Tuple[bool, Optional[str], Optional[str]]:
    """Snapshot-correct observation for a point read."""
    if key in own:
        val = own[key]
        return (val is not _TOMB, None if val is _TOMB else val, None)
    vis = store.visible_version(key, sp)
    if vis is None or vis[4]:
        return (False, None, None)
    return (True, vis[5], vis[2])


def _check_point(store: Store, state: Dict[int, Tuple[str, str]], sp: int,
                 own: Dict[int, Any], key: int, tid: str, seq: int):
    exp_found, exp_val, exp_vid = _expected_point(store, sp, own, key)
    got = state.get(key)
    got_found = got is not None
    if got_found != exp_found:
        return False, (f"key {key}: replay shows "
                       f"{'present' if got_found else 'absent'}, snapshot expects "
                       f"{'present' if exp_found else 'absent'}")
    if got_found:
        gval, gvid = got
        if key in own:
            if gval != exp_val:
                return False, f"key {key}: replay value != own write"
        elif gvid != exp_vid or gval != exp_val:
            return False, (f"key {key}: replay sees version {gvid}, "
                           f"snapshot expects {exp_vid}")
    return True, ""


def _check_range(store: Store, state: Dict[int, Tuple[str, str]], sp: int,
                 own: Dict[int, Any], lo: int, hi: int, tid: str, seq: int):
    # expected entries at the snapshot, overlaid with own writes
    expected: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
    for (k, _pos, vvid, _txn, tomb, val, _loc) in \
            store.visible_entries_in_range(lo, hi, sp):
        if not tomb:
            expected[k] = (val, vvid)
    for k, val in own.items():
        if lo <= k <= hi:
            if val is _TOMB:
                expected.pop(k, None)
            else:
                expected[k] = (val, None)
    # replay-visible entries: probe the state only for keys that have versions
    replay: Dict[int, Tuple[str, str]] = {}
    for k in store.keys_with_versions_in_range(lo, hi):
        got = state.get(k)
        if got is not None:
            replay[k] = got
    missing = [k for k in expected if k not in replay]
    extra = [k for k in replay if k not in expected]
    if missing or extra:
        return False, (f"range [{lo},{hi}]: replay/snapshot key sets differ "
                       f"(missing={missing[:5]}, extra={extra[:5]})")
    for k, (eval_, evvid) in expected.items():
        gval, gvid = replay[k]
        if k in own:
            if gval != eval_:
                return False, f"range [{lo},{hi}] key {k}: replay != own write"
        elif gvid != evvid or gval != eval_:
            return False, (f"range [{lo},{hi}] key {k}: replay sees {gvid}, "
                           f"snapshot expects {evvid}")
    return True, ""
