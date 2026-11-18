"""Verify every recorded read against snapshot semantics.

The correct value for a read is: the transaction's own earlier writes
overlaying the committed state at its fixed ``snapshot_pos``.  Range reads
must return exactly the non-tombstone keys of the closed interval at that
logical time.

Anomaly classes (never conflated with each other):

* ``future_version_read``    - committed version with commit_pos > snapshot_pos
* ``aborted_version_read``   - version written by a rolled-back transaction
* ``unknown_version_read``   - version id nowhere in the committed log
                               (uncommitted); only confirmable when the log
                               range is provably complete
* ``cross_key_version_read`` - committed version of a *different* key
* ``stale_version_read``     - committed version <= snapshot, but not the
                               visible one (incl. tombstone read as present)
* ``value_mismatch``         - right version (or version not recorded), wrong
                               value; also covers present/absent disagreements
                               with no version evidence
* ``missing_key``            - key should have been observed but was not
* ``extra_key``              - key observed that should not have been there

Only evidence-complete mismatches become anomalies; everything else becomes
a gap.  ``future_version_read`` and ``aborted_version_read`` are confirmable
even when the log is incomplete, because the observed version is known to be
unreadable at that snapshot no matter what else is missing.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from .model import (
    Anomaly, Gap, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ,
)
from .store import Store

LOG_GAP_KINDS = {
    "log_range_incomplete", "log_range_gap", "log_range_undeclared",
    "duplicate_commit_pos", "commit_pos_not_increasing",
    "committed_writes_missing_from_log", "txn_writes_vs_log_mismatch",
    "missing_write_ops", "commit_without_txn_record", "duplicate_version_id",
    "missing_version_id", "bad_predecessor", "key_written_twice_in_commit",
}


def _dec(canon: Optional[str]) -> Any:
    return None if canon is None else json.loads(canon)


class ReadReport:
    def __init__(self) -> None:
        self.anomalies: List[Anomaly] = []
        self.gaps: List[Gap] = []
        self.checked = 0
        self.unverifiable = 0


class _Checker:
    def __init__(self, store: Store, log_ok: bool) -> None:
        self.store = store
        self.log_ok = log_ok
        self.report = ReadReport()

    # -- version classification -------------------------------------------

    def classify_version(self, key: int, vid: str, snapshot_pos: int):
        """Classify an observed version id for *key* at *snapshot_pos*."""
        row = self.store.version_by_id(vid)
        if row is not None:
            vkey, pos, _vid, txn, tomb, val, loc = row
            if vkey != key:
                return ("cross_key", row, f"version {vid} belongs to key {vkey}")
            if pos > snapshot_pos:
                return ("future", row,
                        f"version {vid} committed at pos {pos} > snapshot {snapshot_pos}")
            if tomb:
                return ("tombstone", row, f"version {vid} is a tombstone")
            return ("committed", row, "")
        arow = self.store.aborted_by_id(vid)
        if arow is not None:
            return ("aborted", arow,
                    f"version {vid} was written by rolled-back txn {arow[2]}")
        return ("unknown", None, f"version {vid} not found in committed log")

    # -- point reads -------------------------------------------------------

    def check_point_read(self, tid: str, snapshot_pos: int, ops_ok: bool,
                         own: Dict[int, Tuple[str, Optional[str]]],
                         seq: int, key: int, found: int, value: Optional[str],
                         vid: Optional[str], loc: str) -> None:
        st = self.store
        rep = self.report
        key_broken = st.db.execute(
            "SELECT 1 FROM broken_keys WHERE key=?", (key,)).fetchone() is not None
        confirmable = self.log_ok and ops_ok and not key_broken

        if key in own:
            # own earlier write decides; version ids are not meaningful here
            typ, val = own[key]
            exp_found = typ == "put"
            if bool(found) != exp_found or (exp_found and value != val):
                self._anomaly_or_gap(
                    confirmable=ops_ok,  # own writes need no log evidence
                    kind="value_mismatch", txn_id=tid, op_seq=seq, key=key,
                    expected={"found": exp_found, "value": _dec(val),
                              "source": "own_write"},
                    observed={"found": bool(found), "value": _dec(value),
                              "version_id": vid},
                    detail="read disagrees with the transaction's own earlier write",
                    loc=loc)
            rep.checked += 1
            return

        vis = st.visible_version(key, snapshot_pos)
        exp_found = vis is not None and not vis[4]
        exp_val = vis[5] if exp_found else None
        exp_vid = vis[2] if exp_found else None

        if not found:
            if exp_found:
                self._anomaly_or_gap(
                    confirmable, "missing_key", tid, seq, key,
                    expected={"found": True, "value": _dec(exp_val),
                              "version_id": exp_vid},
                    observed={"found": False},
                    detail="key visible at snapshot but read returned not-found",
                    loc=loc)
            rep.checked += 1
            return

        # observed found=True
        if vid is None:
            if not exp_found:
                self._anomaly_or_gap(
                    confirmable, "extra_key", tid, seq, key,
                    expected={"found": False},
                    observed={"found": True, "value": _dec(value)},
                    detail="key not visible at snapshot but read returned a value "
                           "(no version id recorded)", loc=loc)
            elif value != exp_val:
                self._anomaly_or_gap(
                    confirmable, "value_mismatch", tid, seq, key,
                    expected={"found": True, "value": _dec(exp_val),
                              "version_id": exp_vid},
                    observed={"found": True, "value": _dec(value)},
                    detail="value differs from snapshot-visible version "
                           "(no version id recorded)", loc=loc)
            rep.checked += 1
            return

        cat, row, detail = self.classify_version(key, vid, snapshot_pos)
        observed = {"found": True, "value": _dec(value), "version_id": vid}
        if cat == "future":
            self._anomaly_or_gap(True, "future_version_read", tid, seq, key,
                                 expected=self._exp_json(exp_found, exp_val, exp_vid),
                                 observed=observed, detail=detail, loc=loc)
        elif cat == "aborted":
            self._anomaly_or_gap(True, "aborted_version_read", tid, seq, key,
                                 expected=self._exp_json(exp_found, exp_val, exp_vid),
                                 observed=observed, detail=detail, loc=loc)
        elif cat == "cross_key":
            self._anomaly_or_gap(True, "cross_key_version_read", tid, seq, key,
                                 expected=self._exp_json(exp_found, exp_val, exp_vid),
                                 observed=observed, detail=detail, loc=loc)
        elif cat == "unknown":
            # confirmable only when the committed log is provably complete
            self._anomaly_or_gap(
                self.log_ok and ops_ok, "unknown_version_read", tid, seq, key,
                expected=self._exp_json(exp_found, exp_val, exp_vid),
                observed=observed, detail=detail, loc=loc)
        elif cat == "tombstone":
            self._anomaly_or_gap(
                confirmable, "stale_version_read", tid, seq, key,
                expected=self._exp_json(exp_found, exp_val, exp_vid),
                observed=observed,
                detail="observed version is a tombstone read as present; " + detail,
                loc=loc)
        else:  # committed, non-tombstone, <= snapshot
            if not exp_found:
                self._anomaly_or_gap(
                    confirmable, "stale_version_read", tid, seq, key,
                    expected={"found": False}, observed=observed,
                    detail="key deleted/never existed at snapshot; observed an "
                           "older committed version", loc=loc)
            elif vid != exp_vid:
                self._anomaly_or_gap(
                    confirmable, "stale_version_read", tid, seq, key,
                    expected=self._exp_json(True, exp_val, exp_vid),
                    observed=observed,
                    detail=f"visible version is {exp_vid}, observed {vid}", loc=loc)
            elif value != exp_val:
                self._anomaly_or_gap(
                    confirmable, "value_mismatch", tid, seq, key,
                    expected=self._exp_json(True, exp_val, exp_vid),
                    observed=observed,
                    detail="value differs from the observed version's stored value",
                    loc=loc)
        rep.checked += 1

    # -- range reads -------------------------------------------------------

    def check_range_read(self, tid: str, snapshot_pos: int, ops_ok: bool,
                         own: Dict[int, Tuple[str, Optional[str]]],
                         seq: int, lo: int, hi: int, entries_json: Optional[str],
                         complete_flag: int, loc: str) -> None:
        st = self.store
        rep = self.report
        scan_done = bool(complete_flag)
        if not scan_done:
            rep.gaps.append(Gap("incomplete_scan",
                                f"{tid} op {seq}: range scan [{lo},{hi}] did not "
                                f"record its end flag; result cannot prove validity",
                                loc))
        if entries_json is None:
            rep.gaps.append(Gap("range_entries_missing",
                                f"{tid} op {seq}: no result list recorded", loc))
            return
        observed_list = json.loads(entries_json)
        broken = st.broken_keys_in(lo, hi)
        confirmable = self.log_ok and ops_ok and scan_done and not broken

        # expected: snapshot-visible non-tombstone entries overlaid with own writes
        expected: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
        for (k, pos, vvid, txn, tomb, val, vloc) in \
                st.visible_entries_in_range(lo, hi, snapshot_pos):
            if not tomb:
                expected[k] = (val, vvid)
        own_in_range = {k: v for k, v in own.items() if lo <= k <= hi}
        for k, (typ, val) in own_in_range.items():
            if typ == "put":
                expected[k] = (val, None)
            else:
                expected.pop(k, None)

        observed: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
        for e in observed_list:
            observed[e["key"]] = (e["value"], e.get("version_id"))

        # per-entry version classification always runs: future/aborted reads
        # are confirmable even for unfinished scans or incomplete logs.
        for k, (oval, ovid) in observed.items():
            if k < lo or k > hi:
                self._anomaly_or_gap(
                    True, "extra_key", tid, seq, k, lo=lo, hi=hi,
                    expected={"in_range": [lo, hi]},
                    observed={"key": k, "value": _dec(oval), "version_id": ovid},
                    detail="returned key lies outside the scanned closed interval",
                    loc=loc)
                continue
            if k in own_in_range:
                continue  # own writes: value compared below, versions not meaningful
            if ovid is None:
                continue
            cat, row, detail = self.classify_version(k, ovid, snapshot_pos)
            obs = {"key": k, "value": _dec(oval), "version_id": ovid}
            if cat == "future":
                self._anomaly_or_gap(True, "future_version_read", tid, seq, k,
                                     lo=lo, hi=hi, observed=obs, detail=detail, loc=loc)
            elif cat == "aborted":
                self._anomaly_or_gap(True, "aborted_version_read", tid, seq, k,
                                     lo=lo, hi=hi, observed=obs, detail=detail, loc=loc)
            elif cat == "cross_key":
                self._anomaly_or_gap(True, "cross_key_version_read", tid, seq, k,
                                     lo=lo, hi=hi, observed=obs, detail=detail, loc=loc)
            elif cat == "unknown":
                self._anomaly_or_gap(self.log_ok and ops_ok, "unknown_version_read",
                                     tid, seq, k, lo=lo, hi=hi, observed=obs,
                                     detail=detail, loc=loc)

        if not scan_done:
            rep.unverifiable += 1
            return

        # full set comparison (only meaningful for finished scans)
        for k in sorted(expected):
            eval_, evvid = expected[k]
            if k not in observed:
                self._anomaly_or_gap(
                    confirmable, "missing_key", tid, seq, k, lo=lo, hi=hi,
                    expected={"key": k, "value": _dec(eval_), "version_id": evvid},
                    observed={"absent": True},
                    detail="key visible at snapshot missing from range result",
                    loc=loc)
                continue
            oval, ovid = observed[k]
            if k in own_in_range:
                if oval != eval_:
                    self._anomaly_or_gap(
                        ops_ok, "value_mismatch", tid, seq, k, lo=lo, hi=hi,
                        expected={"key": k, "value": _dec(eval_),
                                  "source": "own_write"},
                        observed={"key": k, "value": _dec(oval)},
                        detail="range result disagrees with own earlier write",
                        loc=loc)
                continue
            if ovid is not None and evvid is not None and ovid != evvid:
                cat, row, detail = self.classify_version(k, ovid, snapshot_pos)
                if cat == "committed":
                    self._anomaly_or_gap(
                        confirmable, "stale_version_read", tid, seq, k, lo=lo, hi=hi,
                        expected={"key": k, "value": _dec(eval_), "version_id": evvid},
                        observed={"key": k, "value": _dec(oval), "version_id": ovid},
                        detail=f"visible version is {evvid}, observed {ovid}",
                        loc=loc)
                # future/aborted/unknown already reported above
            elif oval != eval_:
                self._anomaly_or_gap(
                    confirmable, "value_mismatch", tid, seq, k, lo=lo, hi=hi,
                    expected={"key": k, "value": _dec(eval_), "version_id": evvid},
                    observed={"key": k, "value": _dec(oval), "version_id": ovid},
                    detail="value differs from snapshot-visible version", loc=loc)
        for k in sorted(observed):
            if k in expected or k < lo or k > hi:
                continue
            oval, ovid = observed[k]
            detail = "key not visible at snapshot but present in range result"
            kind = "extra_key"
            if ovid is not None:
                cat, row, cdetail = self.classify_version(k, ovid, snapshot_pos)
                if cat in ("future", "aborted", "cross_key", "unknown"):
                    continue  # already reported above with its specific class
                if cat in ("committed", "tombstone"):
                    kind = "stale_version_read"
                    detail = ("key deleted/never existed at snapshot; observed an "
                              "older committed version")
            self._anomaly_or_gap(
                confirmable, kind, tid, seq, k, lo=lo, hi=hi,
                expected={"absent": True},
                observed={"key": k, "value": _dec(oval), "version_id": ovid},
                detail=detail, loc=loc)
        rep.checked += 1

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _exp_json(found: bool, val: Optional[str], vid: Optional[str]) -> Dict[str, Any]:
        if not found:
            return {"found": False}
        return {"found": True, "value": _dec(val), "version_id": vid}

    def _anomaly_or_gap(self, confirmable: bool, kind: str, txn_id: str,
                        op_seq: int, key: Optional[int],
                        lo: Optional[int] = None, hi: Optional[int] = None,
                        expected=None, observed=None, detail: str = "",
                        loc: str = "") -> None:
        if confirmable:
            self.report.anomalies.append(Anomaly(
                kind=kind, txn_id=txn_id, op_seq=op_seq, key=key, lo=lo, hi=hi,
                expected=expected, observed=observed, detail=detail, loc=loc))
        else:
            self.report.gaps.append(Gap(
                "unverifiable_read",
                f"{txn_id} op {op_seq}: suspected {kind} but evidence is "
                f"incomplete ({detail})", loc))
            self.report.unverifiable += 1


def verify_reads(store: Store) -> ReadReport:
    """Verify all reads of committed transactions, streaming per transaction."""
    log_ok = not any(g.kind in LOG_GAP_KINDS for g in store.gaps)
    checker = _Checker(store, log_ok)
    for (tid, sp, _st, _et, _cp) in store.committed_txns():
        ops_ok = store.db.execute(
            "SELECT ops_complete FROM txns WHERE txn_id=?", (tid,)).fetchone()[0] == 1
        if sp is None:
            checker.report.gaps.append(Gap(
                "missing_snapshot_pos", f"{tid}: cannot verify reads", "transactions"))
            continue
        own: Dict[int, Tuple[str, Optional[str]]] = {}
        for (seq, typ, key, lo, hi, value, found, vid, entries, cflag, loc) \
                in store.txn_ops(tid):
            if typ == OP_PUT:
                own[key] = ("put", value)
            elif typ == OP_DELETE:
                own[key] = ("delete", None)
            elif typ == OP_POINT_READ:
                store.stats.add("point_reads_checked")
                checker.check_point_read(tid, sp, ops_ok, own, seq, key,
                                         found or 0, value, vid, loc)
            elif typ == OP_RANGE_READ:
                store.stats.add("range_reads_checked")
                checker.check_range_read(tid, sp, ops_ok, own, seq, lo, hi,
                                         entries, cflag or 0, loc)
    return checker.report
