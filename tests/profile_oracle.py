"""Independent workload-profile oracle for small histories.

This oracle is deliberately written *without* using any production code of
``snapshot_audit.profile`` and implements the specification by brute force:

* window assignment and per-window classification counts are recomputed
  straight from the raw history document;
* scan coverage is obtained by **enumerating every integer key of the finite
  test domain** and counting how many completed closed intervals contain it,
  then merging equal-valued adjacent keys;
* per-key heat uses the same event semantics, again by direct enumeration.

The production implementation must never enumerate keys: it aggregates
ordered end-point events in SQLite.  Tests cross-check the two on small
domains and separately prove that a single int64-wide sparse interval stays
cheap (cost in end points, not domain width).
"""

from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Tuple

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

MEMBERSHIP_GAP_KINDS = frozenset({
    "log_range_incomplete", "log_range_gap", "log_range_undeclared",
    "duplicate_commit_pos", "commit_pos_not_increasing",
    "committed_writes_missing_from_log", "commit_without_txn_record",
    "missing_operations",
})
CLASSIFICATION_GAP_KINDS = frozenset({
    "bad_op_seq", "unknown_op_type", "txn_writes_vs_log_mismatch",
    "missing_write_ops", "key_written_twice_in_txn",
    "key_written_twice_in_commit", "missing_version_id", "bad_predecessor",
    "duplicate_version_id", "missing_operations",
})
HEAT_GAP_KINDS = frozenset({
    "incomplete_scan", "range_entries_missing", "bad_range",
    "duplicate_key_in_range_result", "duplicate_range_entry_key",
    "entry_outside_interval", "duplicate_initial_key", "missing_operations",
})
DURATION_GAP_KINDS = frozenset({"missing_times", "end_before_start"})

BUCKET_EPS = 1e-12


def bucket_index(end_time: float, anchor: float, width: float) -> int:
    q = (end_time - anchor) / width
    k = math.floor(q)
    if abs(q - k) <= BUCKET_EPS:
        return k
    nxt = k + 1
    if abs(q - nxt) <= BUCKET_EPS:
        return nxt
    return k


def _effect_for(kind: str) -> List[str]:
    effects = []
    if kind in MEMBERSHIP_GAP_KINDS:
        effects.append("window_membership")
    if kind in CLASSIFICATION_GAP_KINDS:
        effects.append("operation_classification")
    if kind in HEAT_GAP_KINDS:
        effects.append("heat")
    if kind in DURATION_GAP_KINDS:
        effects.append("duration")
    if not effects:
        effects = ["window_membership", "operation_classification",
                   "heat", "duration"]
    return effects


class OracleProfiler:
    """Brute-force reference profile over an in-memory raw document."""

    def __init__(self, doc: Dict[str, Any], width: float,
                 top_n: int, domain: Optional[Any] = None) -> None:
        self.doc = doc
        self.width = float(width)
        self.top_n = top_n
        self.txns = doc.get("transactions", [])
        self.committed = [t for t in self.txns
                          if t.get("status") == "committed"]
        self.aborted = [t for t in self.txns
                        if t.get("status") == "aborted"]
        self.gap_effects: Dict[str, set] = {}
        self.gaps: List[Dict[str, Any]] = []
        if domain is None:
            domain = self._infer_domain()
        # domain is either (lo, hi) or a list of (lo, hi) sub-ranges: the
        # brute-force enumeration must stay on small finite domains (the
        # int64 endpoint tests probe only the keys near the endpoints)
        self.domains = [domain] if isinstance(domain, tuple) else list(domain)

    def _gap(self, kind: str, detail: str, loc: str = "") -> None:
        self.gaps.append({"kind": kind, "detail": detail, "loc": loc})
        self.gap_effects.setdefault(kind, set()).update(
            _effect_for(kind))

    def _infer_domain(self) -> Tuple[int, int]:
        keys: List[int] = []
        for e in self.doc.get("initial_state", []):
            keys.append(e["key"])
        for t in self.committed:
            for op in t.get("operations") or []:
                if "key" in op:
                    keys.append(op["key"])
                if "lo" in op:
                    keys.extend([op["lo"], op["hi"]])
        if not keys:
            return (0, 0)
        return (min(keys), max(keys))

    # -- gap detection mirroring the ingestion contract ---------------------

    def detect_gaps(self) -> None:
        log = self.doc.get("commit_log", {})
        if log.get("complete") is not True:
            self._gap("log_range_incomplete",
                      "commit log range not declared complete", "commit_log")
        positions = [c["commit_pos"] for c in log.get("commits", [])]
        if len(set(positions)) != len(positions):
            self._gap("duplicate_commit_pos",
                      "commit_pos appears twice", "commit_log")
        if any(positions[i] >= positions[i + 1]
               for i in range(len(positions) - 1)):
            self._gap("commit_pos_not_increasing",
                      "commit_pos not strictly increasing", "commit_log")
        if log.get("complete") is True and positions:
            first, last = log.get("first_commit_pos"), log.get("last_commit_pos")
            if first is None or last is None:
                self._gap("log_range_undeclared",
                          "complete log without range bounds", "commit_log")
            else:
                have = set(positions)
                if any(p not in have for p in range(first, last + 1)):
                    self._gap("log_range_gap",
                              "declared complete range is missing positions",
                              "commit_log")
        seen_init: set = set()
        for e in self.doc.get("initial_state", []):
            if e["key"] in seen_init:
                self._gap("duplicate_initial_key",
                          f"key {e['key']} twice in initial state",
                          "initial_state")
            seen_init.add(e["key"])
        # per-transaction evidence gaps
        for t in self.committed:
            tid = t["txn_id"]
            loc = f"transactions:{tid}"
            if t.get("start_time") is None or t.get("end_time") is None:
                self._gap("missing_times", f"{tid}: missing times", loc)
            elif t["end_time"] < t["start_time"]:
                self._gap("end_before_start",
                          f"{tid}: end before start", loc)
            ops = t.get("operations")
            if ops is None:
                self._gap("missing_operations",
                          f"{tid}: no operation record", loc)
                continue
            seqs = [op.get("seq") for op in ops]
            if sorted(seqs) != list(range(len(seqs))):
                self._gap("missing_operations",
                          f"{tid}: op seqs not contiguous", loc)
            written: Dict[int, str] = {}
            for op in ops:
                typ = op.get("type")
                if typ not in ("point_read", "range_read", "put", "delete"):
                    self._gap("unknown_op_type", f"type={typ!r}", loc)
                if typ in ("put", "delete"):
                    if op["key"] in written:
                        self._gap("key_written_twice_in_txn",
                                  f"{tid}: key {op['key']} written twice", loc)
                    written[op["key"]] = typ
                if typ == "range_read":
                    if op.get("hi", 0) < op.get("lo", 0):
                        self._gap("bad_range", "hi < lo", loc)
                    if op.get("complete") is not True:
                        self._gap("incomplete_scan",
                                  f"{tid}: unfinished scan", loc)
                    if not isinstance(op.get("entries"), list):
                        self._gap("range_entries_missing",
                                  f"{tid}: no entries list", loc)
                    else:
                        seen: set = set()
                        for e in op["entries"]:
                            if e["key"] in seen:
                                self._gap(
                                    "duplicate_key_in_range_result",
                                    f"{tid}: duplicate range entry "
                                    f"key {e['key']}", loc)
                            seen.add(e["key"])
                            if not (op["lo"] <= e["key"] <= op["hi"]):
                                self._gap("entry_outside_interval",
                                          f"{tid}: entry outside interval", loc)

    # -- windows -------------------------------------------------------------

    def build_windows(self) -> Tuple[List[Dict[str, Any]], Optional[float],
                                     List[Dict[str, Any]], int]:
        ends = [t["end_time"] for t in self.committed
                if t.get("end_time") is not None]
        unbinned: List[Dict[str, Any]] = []
        if not ends:
            for t in self.committed:
                unbinned.append({"txn_id": t["txn_id"],
                                 "reason": "missing_end_time",
                                 "loc": f"transactions:{t['txn_id']}"})
            return [], None, unbinned, -1
        anchor = min(ends)
        pre: Dict[int, Dict[str, Any]] = {}
        max_bucket = -1
        for t in self.committed:
            tid = t["txn_id"]
            et = t.get("end_time")
            if et is None:
                unbinned.append({"txn_id": tid,
                                 "reason": "missing_end_time",
                                 "loc": f"transactions:{tid}"})
                continue
            k = bucket_index(float(et), anchor, self.width)
            max_bucket = max(max_bucket, k)
            w = pre.setdefault(k, self._new_window(k, anchor))
            w["transactions"].append(tid)
            st = t.get("start_time")
            if st is None or et < st:
                w["duration"]["status"] = "unknown"
                w["duration"]["unexplained_transactions"].append(tid)
            else:
                d = float(et) - float(st)
                w["dur"].append(d)
            for op in t.get("operations") or []:
                w["op_total"] += 1
                typ = op.get("type")
                if typ == "point_read":
                    w["point_reads"] += 1
                elif typ == "range_read":
                    w["range_reads"] += 1
                elif typ in ("put", "delete"):
                    w["writes"] += 1
        windows = []
        membership = bool(self.gap_effects.keys() & MEMBERSHIP_GAP_KINDS)
        classification = bool(
            self.gap_effects.keys() & CLASSIFICATION_GAP_KINDS)
        heat = bool(self.gap_effects.keys() & HEAT_GAP_KINDS)
        for k in range(max_bucket + 1):
            w = pre.get(k)
            if w is None:
                windows.append({
                    "window_index": k,
                    "interval": {"lo": anchor + k * self.width,
                                 "hi": anchor + (k + 1) * self.width},
                    "transactions": [], "status": "complete", "gaps": [],
                    "transaction_count": 0, "operation_total": 0,
                    "point_reads": 0, "range_reads": 0, "writes": 0,
                    "duration": {"status": "complete",
                                 "explained_transactions": 0,
                                 "sum_ms": None, "min_ms": None,
                                 "max_ms": None, "mean_ms": None,
                                 "unexplained_transactions": []},
                })
                continue
            dur = w.pop("dur")
            durj = w["duration"]
            if dur:
                durj["explained_transactions"] = len(dur)
                durj["sum_ms"] = _n(sum(dur))
                durj["min_ms"] = _n(min(dur))
                durj["max_ms"] = _n(max(dur))
                durj["mean_ms"] = _n(sum(dur) / len(dur))
            else:
                durj["status"] = "unknown"
            w["transactions"].sort()
            durj["unexplained_transactions"].sort()
            gap_flags = []
            if membership:
                gap_flags.append("window_membership_unproven")
            if classification:
                gap_flags.append("operation_classification_unproven")
            if heat:
                gap_flags.append("heat_unproven")
            w["gaps"] = sorted(set(gap_flags) | set(w["gaps"]))
            w["status"] = "unknown" if w["gaps"] else "complete"
            w.pop("op_total", None)
            windows.append(self._finalize_window(k, anchor, w, dur))
        return windows, anchor, sorted(unbinned,
                                       key=lambda u: (u["txn_id"],
                                                      u["reason"])), max_bucket

    def _new_window(self, k: int, anchor: float) -> Dict[str, Any]:
        return {
            "window_index": k,
            "interval": {"lo": anchor + k * self.width,
                         "hi": anchor + (k + 1) * self.width},
            "transactions": [], "gaps": [],
            "transaction_count": 0, "op_total": 0,
            "point_reads": 0, "range_reads": 0, "writes": 0,
            "dur": [],
            "duration": {"status": "complete", "explained_transactions": 0,
                         "sum_ms": None, "min_ms": None, "max_ms": None,
                         "mean_ms": None, "unexplained_transactions": []},
        }

    def _finalize_window(self, k, anchor, w, dur) -> Dict[str, Any]:
        return {
            "window_index": k,
            "interval": {"lo": _n(w["interval"]["lo"]),
                         "hi": _n(w["interval"]["hi"])},
            "transactions": w["transactions"],
            "status": w["status"],
            "gaps": w["gaps"],
            "transaction_count": len(w["transactions"]),
            "operation_total": w["point_reads"] + w["range_reads"]
                               + w["writes"],
            "point_reads": w["point_reads"],
            "range_reads": w["range_reads"],
            "writes": w["writes"],
            "duration": w["duration"],
        }

    # -- heat (event semantics, direct counting) -----------------------------

    def build_key_heat(self) -> Dict[str, Any]:
        per_key: Dict[int, Dict[str, int]] = {}

        def add(key: int, kind: str) -> None:
            c = per_key.setdefault(
                key, {"point_read": 0, "range_item": 0, "write": 0})
            c[kind] += 1

        for t in self.committed:
            for op in t.get("operations") or []:
                typ = op.get("type")
                if typ == "point_read":
                    add(op["key"], "point_read")
                elif typ in ("put", "delete"):
                    add(op["key"], "write")
                elif typ == "range_read":
                    if op.get("complete") is not True:
                        continue  # unfinished scans contribute nothing
                    for e in op.get("entries") or []:
                        if e.get("tombstone") is not True:
                            add(e["key"], "range_item")
        entries = []
        for key in sorted(per_key):
            c = per_key[key]
            entries.append({
                "key": key,
                "point_read_events": c["point_read"],
                "range_returned_events": c["range_item"],
                "write_events": c["write"],
                "total_events": c["point_read"] + c["range_item"]
                                + c["write"],
            })
        ranked = sorted(entries, key=lambda e: (-e["total_events"], e["key"]))
        unknown = bool(self.gap_effects.keys() & HEAT_GAP_KINDS)
        return {
            "status": "unknown" if unknown else "complete",
            "top_n": self.top_n,
            "distinct_keys": len(entries),
            "ranked_keys": ranked[: self.top_n],
        }

    # -- coverage (brute force: enumerate every key in the domain) -----------

    def build_coverage_bruteforce(self) -> Dict[str, Any]:
        intervals: List[Tuple[int, int]] = []
        for t in self.committed:
            for op in t.get("operations") or []:
                if op.get("type") == "range_read" \
                        and op.get("complete") is True:
                    intervals.append((op["lo"], op["hi"]))
        # explicit per-key enumeration -- oracle only, never in production
        per_key: Dict[int, int] = {}
        for lo_d, hi_d in self.domains:
            for key in range(lo_d, hi_d + 1):
                n = sum(1 for (lo, hi) in intervals if lo <= key <= hi)
                if n:
                    per_key[key] = n
        segments: List[Dict[str, Any]] = []
        for key in sorted(per_key):
            n = per_key[key]
            if segments and segments[-1]["coverage"] == n \
                    and key == segments[-1]["hi"] + 1:
                segments[-1]["hi"] = key
            else:
                segments.append({"lo": key, "hi": key, "coverage": n})
        unknown = bool(self.gap_effects.keys() & HEAT_GAP_KINDS)
        return {
            "status": "unknown" if unknown else "complete",
            "completed_scans": len(intervals),
            "endpoint_events": 2 * len(intervals),
            "segments": segments,
        }

    def run(self) -> Dict[str, Any]:
        self.detect_gaps()
        windows, anchor, unbinned, max_bucket = self.build_windows()
        key_heat = self.build_key_heat()
        coverage = self.build_coverage_bruteforce()
        return {
            "status": "unknown" if self.gaps else "complete",
            "anchor_end_time": anchor,
            "window_count": max_bucket + 1,
            "windows": windows,
            "unbinned_transactions": unbinned,
            "key_heat": key_heat,
            "scan_coverage": coverage,
            "gap_kinds": sorted({g["kind"] for g in self.gaps}),
            "committed": len(self.committed),
            "aborted": len(self.aborted),
        }


def _n(x: float) -> float:
    return 0.0 if x == 0.0 else x


def oracle_sections(doc: Dict[str, Any], width: float, top_n: int,
                    domain: Optional[Tuple[int, int]] = None
                    ) -> Dict[str, Any]:
    """Convenience entry point used by the cross-check tests."""
    return OracleProfiler(doc, width, top_n, domain).run()
