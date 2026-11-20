"""Workload profiling (事务负载画像) from a single raw transaction history.

``profile`` answers the on-call question "when was the system busy, and on
which keys/intervals" directly from the recorded transaction history:

* fixed-width **time windows** anchored at the earliest ``end_time`` of any
  successfully committed transaction; every committed transaction is bucketed
  by its ``end_time`` arrival;
* per window: transaction count, total operation volume, point-read /
  range-read / write classification counts and an explainable transaction
  duration summary (only successfully committed transactions participate;
  rolled-back transactions never do);
* **per-key heat** on the event semantics: a point read counts its target
  key once, every ``put``/``delete`` write key counts once, and a range read
  contributes only the non-tombstone keys it actually returned from a
  *completed* scan;
* **range-scan coverage heat**: one completed scan contributes one coverage
  count to its whole closed interval even when it returned zero rows; the
  key domain is rendered as disjoint segments with equal adjacent coverage
  merged.

The two heat quantities are deliberately different things: a per-key event
count ("this key was returned N times") versus interval coverage ("this key
lay inside a scanned interval N times").  Neither is derived from audit
anomaly counts, lineage node counts or any other renamed metric.

Missing evidence never becomes zero.  Missing logs, conflicting operation
evidence or scans that never finished mark the affected windows/sections as
``unknown`` (with an explicit gap); a window with zero transactions is a
normal, *complete* window and stays distinguishable from a window whose load
cannot be known.

Production scan-coverage construction writes the interval end-point events
into SQLite and aggregates them in key order (an external sort of the
end-point events is the equivalent alternative).  Closed end points are
preserved exactly and the int64 key domain is never expanded into per-key
records: cost grows with the number of intervals/end points, not with key
domain width.  The exhaustive per-key enumeration oracle lives only in the
test suite (``tests/profile_oracle.py``).
"""

from __future__ import annotations

import json
import math
import resource
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from . import __version__
from .hashing import canonical_json, sha256_text
from .model import (
    INT64_MAX, INT64_MIN, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ,
    STATUS_ABORTED, STATUS_COMMITTED, ContractError,
)
from .store import Store

PROFILE_FORMAT = "workload-profile/1"

# Gap kinds whose effect is "the recorded transaction set itself may be
# incomplete", so no window membership/count can be proven exhaustive.
MEMBERSHIP_GAP_KINDS = frozenset({
    "log_range_incomplete", "log_range_gap", "log_range_undeclared",
    "duplicate_commit_pos", "commit_pos_not_increasing",
    "committed_writes_missing_from_log", "commit_without_txn_record",
    "missing_operations",
})

# Gap kinds whose effect is "operation classification evidence conflicts",
# so per-window operation classification totals cannot be trusted.
CLASSIFICATION_GAP_KINDS = frozenset({
    "bad_op_seq", "unknown_op_type", "txn_writes_vs_log_mismatch",
    "missing_write_ops", "key_written_twice_in_txn",
    "key_written_twice_in_commit", "missing_version_id", "bad_predecessor",
    "duplicate_version_id", "missing_operations",
})

# Gap kinds whose effect is "some range/heat evidence is unusable", so the
# heat sections can only be lower bounds.
HEAT_GAP_KINDS = frozenset({
    "incomplete_scan", "range_entries_missing", "bad_range",
    "duplicate_key_in_range_result", "duplicate_range_entry_key",
    "entry_outside_interval", "duplicate_initial_key", "missing_operations",
})

# Gap kinds whose effect is "a transaction duration cannot be explained".
DURATION_GAP_KINDS = frozenset({
    "missing_times", "end_before_start",
})

_BATCH = 4096
# A computed (end_time - anchor) / width only lands exactly on an integer
# boundary in finite floating-point precision; snap when the distance to an
# integer boundary is at most this many ULPs of the bucket index, so genuine
# boundary arrivals (e.g. 10.0 + 10.0) are stable while ordinary near-boundary
# times keep their floor bucket.
BUCKET_EPS = 1e-9


class ProfileError(Exception):
    """Hard failure: no profile may be produced (bad usage/input)."""


@dataclass
class _WindowAcc:
    index: int
    lo: float
    hi: float
    txn_ids: List[str] = field(default_factory=list)
    txn_count: int = 0
    duration_sum: float = 0.0
    duration_min: Optional[float] = None
    duration_max: Optional[float] = None
    duration_explained: int = 0
    duration_unexplained: List[str] = field(default_factory=list)
    duration_unknown: bool = False
    gap_kinds: set = field(default_factory=set)


def _rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss
    return rss * 1024


def _gap(kind: str, detail: str, loc: str = "") -> Dict[str, Any]:
    return {"kind": kind, "detail": detail, "loc": loc}


def _num(value: float) -> float:
    """Normalize a float for deterministic JSON (collapse -0.0)."""
    if value == 0.0:
        return 0.0
    return value


def validate_window_width(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) \
            or (isinstance(value, float)
                and (math.isnan(value) or math.isinf(value))):
        raise ProfileError(
            f"window width must be a finite positive number, got {value!r}")
    w = float(value)
    if not (w > 0.0):
        raise ProfileError(
            f"window width must be strictly positive, got {value!r}")
    return w


def validate_top_n(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProfileError(
            f"top-n must be a positive integer, got {value!r}")
    return value


def bucket_index(end_time: float, anchor: float, width: float) -> int:
    """Fixed-width bucket of *end_time* measured from *anchor*.

    Buckets are half-open internally, ``[anchor + k*w, anchor + (k+1)*w)``,
    and an arrival computed within ``BUCKET_EPS`` of an integer boundary is
    treated as lying on that boundary (finite-precision rule for genuine
    boundary arrivals); ordinary near-boundary times keep their floor bucket.
    """
    q = (end_time - anchor) / width
    k = math.floor(q)
    if abs(q - k) <= BUCKET_EPS:
        return k
    nxt = k + 1
    if abs(q - nxt) <= BUCKET_EPS:
        return nxt
    return k


# ---------------------------------------------------------------------------
# bounded on-disk profile tables
# ---------------------------------------------------------------------------

_PROFILE_SCHEMA = """
CREATE TABLE pf_key_heat(
  key   INTEGER NOT NULL,
  kind  TEXT NOT NULL,          -- point_read | range_item | write
  n     INTEGER NOT NULL,
  PRIMARY KEY (key, kind)
) WITHOUT ROWID;

CREATE TABLE pf_scan_events(
  pos         INTEGER NOT NULL,
  start_event INTEGER NOT NULL, -- 1 = closed interval start at lo
                                -- 0 = interval end event placed at hi
  lo          INTEGER NOT NULL,
  hi          INTEGER NOT NULL
);
CREATE INDEX idx_pf_events ON pf_scan_events(pos, start_event);

CREATE TABLE pf_txn_bucket(
  txn_id TEXT PRIMARY KEY,
  bucket INTEGER NOT NULL
);
CREATE INDEX idx_pf_bucket ON pf_txn_bucket(bucket);

CREATE TABLE pf_committed(
  txn_id TEXT PRIMARY KEY
);
"""

# Upper bound on in-memory (key, kind) heat counters before the aggregated
# counters are spilled to SQLite in one upsert batch.  Distinct pairs can
# never outnumber the operations, but the cap makes memory bounded even for
# adversarial high-cardinality inputs; spills carry aggregated rows, never
# one row per event.
HEAT_DICT_CAP = 200_000


class WorkloadProfiler:
    """Builds the profile from an ingested on-disk :class:`Store`.

    All growing state (per-key heat events, scan end-point events, txn->
    bucket membership) is kept in SQLite; memory holds only the fixed number
    of window accumulators plus bounded batches.
    """

    def __init__(self, store: Store, width: float, top_n: int) -> None:
        self.store = store
        self.width = width
        self.top_n = top_n
        # reuse the store's single connection: a second connection to the
        # same file can be locked out while the store's schema is in use
        self.db = store.db
        self.db.executescript(_PROFILE_SCHEMA)
        self.gap_effects: Dict[str, set] = {}
        self.record_gaps: List[Dict[str, Any]] = []
        # per-key heat is aggregated in bounded counters: the number of
        # distinct (key, kind) pairs can never exceed the number of
        # operations, so it scales with record size, never with key domain
        self.key_heat: Dict[Tuple[int, str], int] = {}
        self._event_batch: List[Tuple[int, int, int, int]] = []
        self._bucket_batch: List[Tuple[str, int]] = []
        self.heat_events = 0
        self.scan_intervals = 0
        self.endpoint_events = 0
        self.records_seen = 0
        self.committed_txns = 0
        self.aborted_txns = 0
        self.unbinned_txns: List[Dict[str, Any]] = []
        self._pending_windows: Dict[int, _WindowAcc] = {}

    def close(self) -> None:
        # the connection belongs to the store; nothing extra to close here
        return

    # -- gap bookkeeping ---------------------------------------------------

    def _record_gap(self, kind: str, detail: str, loc: str,
                    effects: Tuple[str, ...]) -> None:
        self.record_gaps.append(_gap(kind, detail, loc))
        self.gap_effects.setdefault(kind, set()).update(effects)

    def _ingest_store_gaps(self) -> None:
        """Classify every gap the ingestion cross-validation already found."""
        for g in self.store.gaps:
            effects: List[str] = []
            if g.kind in MEMBERSHIP_GAP_KINDS:
                effects.append("window_membership")
            if g.kind in CLASSIFICATION_GAP_KINDS:
                effects.append("operation_classification")
            if g.kind in HEAT_GAP_KINDS:
                effects.append("heat")
            if g.kind in DURATION_GAP_KINDS:
                effects.append("duration")
            if not effects:
                # unknown gap kind: conservatively pollute every metric it
                # could possibly touch rather than silently ignoring it
                effects = ["window_membership", "operation_classification",
                           "heat", "duration"]
            self._record_gap(g.kind, g.detail, g.loc, tuple(effects))

    # -- bounded batch sinks ------------------------------------------------

    def _add_heat(self, key: int, kind: str, n: int = 1) -> None:
        pair = (key, kind)
        self.key_heat[pair] = self.key_heat.get(pair, 0) + n
        self.heat_events += n
        if len(self.key_heat) >= HEAT_DICT_CAP:
            self._spill_heat()

    def _spill_heat(self) -> None:
        """Merge the in-memory heat counters into the disk table."""
        if not self.key_heat:
            return
        rows = [(key, kind, n) for (key, kind), n in self.key_heat.items()]
        self.db.executemany(
            "INSERT INTO pf_key_heat(key, kind, n) VALUES (?,?,?) "
            "ON CONFLICT(key, kind) DO UPDATE SET n = n + excluded.n",
            rows)
        self.key_heat.clear()

    def _add_bucket(self, txn_id: str, bucket: int) -> None:
        self._bucket_batch.append((txn_id, bucket))
        if len(self._bucket_batch) >= _BATCH:
            self._flush_buckets()

    def _flush_buckets(self) -> None:
        if self._bucket_batch:
            self.db.executemany(
                "INSERT OR REPLACE INTO pf_txn_bucket VALUES (?,?)",
                self._bucket_batch)
            self._bucket_batch.clear()

    def _add_scan(self, lo: int, hi: int) -> None:
        # closed interval: a start event at lo and an end event placed at hi
        # (ends are applied after starts at the same coordinate), so coverage
        # is constant on [lo, hi] and drops only from hi+1 onwards.
        if hi < lo:
            # ingestion already records a bad_range gap; never emit inverted
            # end-point events that would corrupt the sweep invariants
            return
        self._event_batch.extend([
            (lo, 1, lo, hi),
            (hi, 0, lo, hi),
        ])
        self.scan_intervals += 1
        self.endpoint_events += 2
        if len(self._event_batch) >= _BATCH:
            self._flush_events()

    def _flush_events(self) -> None:
        if self._event_batch:
            self.db.executemany(
                "INSERT INTO pf_scan_events VALUES (?,?,?,?)",
                self._event_batch)
            self._event_batch.clear()

    # -- main pass ----------------------------------------------------------

    def build(self) -> Dict[str, Any]:
        self._ingest_store_gaps()
        db = self.store.db
        rows = db.execute(
            "SELECT txn_id, status, snapshot_pos, start_time, end_time, "
            "ops_complete, loc FROM txns ORDER BY txn_id").fetchall()
        self.records_seen = len(rows)
        committed = [r for r in rows if r[1] == STATUS_COMMITTED]
        self.aborted_txns = sum(1 for r in rows if r[1] == STATUS_ABORTED)
        self.committed_txns = len(committed)

        anchor: Optional[float] = None
        for (_tid, _st, _sp, _stt, et, _oc, _loc) in committed:
            if et is not None and (anchor is None or et < anchor):
                anchor = et

        max_bucket = -1
        if anchor is None:
            if committed:
                # committed transactions exist but none carries an end_time:
                # window membership genuinely cannot be established
                self._record_gap(
                    "missing_times",
                    "no committed transaction records an end_time; time "
                    "windows cannot be anchored", "transactions",
                    ("window_membership", "duration"))
                for r in committed:
                    self.unbinned_txns.append(
                        {"txn_id": r[0], "reason": "missing_end_time",
                         "loc": r[6]})
            # with zero committed transactions the empty window set is a
            # known fact, not missing evidence
        else:
            max_bucket = self._bucket_transactions(committed, anchor)
        self._flush_buckets()
        # disk-side committed-id set: heat queries join against it instead of
        # binding tens of thousands of host parameters
        id_rows = [(r[0],) for r in committed]
        for i in range(0, len(id_rows), _BATCH):
            self.db.executemany(
                "INSERT INTO pf_committed(txn_id) VALUES (?)",
                id_rows[i:i + _BATCH])

        scan_unknown, _heat_conflict = self._build_heat(committed)
        self._spill_heat()
        self._flush_events()
        self.db.commit()

        windows_json = self._windows_to_json(
            anchor, max_bucket, self.width)
        key_heat = self._key_heat_section(scan_unknown)
        coverage = self._coverage_section(scan_unknown)
        gaps = sorted(self.record_gaps,
                      key=lambda g: (g["kind"], g["loc"], g["detail"]))
        status = "unknown" if self.record_gaps else "complete"
        return {
            "status": status,
            "anchor_end_time": anchor,
            "windowing": {
                "anchor": "min_committed_end_time",
                "width": self.width,
                "bucket_rule": "floor((end_time - anchor) / width); "
                               "arrivals exactly on a boundary snap to the "
                               "nearest bucket",
                "boundary_snap_epsilon": BUCKET_EPS,
                "window_count": max_bucket + 1,
            },
            "windows": windows_json,
            "unbinned_transactions": sorted(
                self.unbinned_txns, key=lambda u: (u["txn_id"], u["reason"])),
            "key_heat": key_heat,
            "scan_coverage": coverage,
            "gaps": gaps,
        }

    def _bucket_transactions(self, committed: List[tuple],
                             anchor: float) -> int:
        width = self.width
        pre: Dict[int, _WindowAcc] = {}
        max_bucket = -1
        for (tid, _st, _sp, stt, et, _oc, loc) in committed:
            if et is None:
                self.unbinned_txns.append(
                    {"txn_id": tid, "reason": "missing_end_time", "loc": loc})
                self._record_gap(
                    "missing_times",
                    f"{tid}: committed transaction without end_time cannot be "
                    f"assigned to a window", loc,
                    ("window_membership", "duration"))
                continue
            k = bucket_index(et, anchor, width)
            if k < 0:
                self.unbinned_txns.append(
                    {"txn_id": tid, "reason": f"end_time_before_anchor",
                     "loc": loc})
                self._record_gap(
                    "end_before_anchor",
                    f"{tid}: end_time {et} precedes the anchor {anchor}", loc,
                    ("window_membership",))
                continue
            max_bucket = max(max_bucket, k)
            self._add_bucket(tid, k)
            acc = pre.get(k)
            if acc is None:
                acc = _WindowAcc(k, anchor + k * width,
                                 anchor + (k + 1) * width)
                pre[k] = acc
            acc.txn_ids.append(tid)
            acc.txn_count += 1
            if stt is None:
                acc.duration_unknown = True
                acc.duration_unexplained.append(tid)
            else:
                d = et - stt
                if d < 0:
                    acc.duration_unknown = True
                    acc.duration_unexplained.append(tid)
                    self._record_gap(
                        "end_before_start",
                        f"{tid}: end_time {et} < start_time {stt}; duration "
                        f"is not explainable", loc, ("duration",))
                else:
                    acc.duration_sum += d
                    acc.duration_explained += 1
                    acc.duration_min = d if acc.duration_min is None \
                        else min(acc.duration_min, d)
                    acc.duration_max = d if acc.duration_max is None \
                        else max(acc.duration_max, d)
        self._pending_windows = pre
        return max_bucket

    def _build_heat(self, committed: List[tuple]) -> Tuple[bool, bool]:
        """Stream every committed transaction's operations into heat sinks.

        Operations are read in one ordered, batched cursor joined against the
        disk-side committed-id set, instead of one query per transaction, so
        the pass stays O(ops) with bounded memory and no host-parameter limit.
        """
        db = self.store.db
        scan_unknown = False
        heat_conflict = False
        cur = db.execute(
            "SELECT o.txn_id, o.seq, o.type, o.key, o.lo, o.hi, "
            "o.entries, o.complete, o.loc "
            "FROM ops o JOIN pf_committed c ON c.txn_id = o.txn_id "
            "ORDER BY o.txn_id, o.seq")
        while True:
            batch = cur.fetchmany(_BATCH)
            if not batch:
                break
            for (tid, seq, typ, key, lo, hi, entries_json, cflag,
                 loc) in batch:
                if typ == OP_POINT_READ:
                    # event semantics: every point read counts its target once
                    self._add_heat(key, "point_read")
                elif typ in (OP_PUT, OP_DELETE):
                    # every write key counts once per write event
                    self._add_heat(key, "write")
                elif typ == OP_RANGE_READ:
                    if not bool(cflag):
                        scan_unknown = heat_conflict = True
                        self._record_gap(
                            "incomplete_scan",
                            f"{tid} op {seq}: range scan [{lo},{hi}] never "
                            f"recorded its end flag; its returned keys and "
                            f"interval coverage are excluded from heat", loc,
                            ("heat",))
                        continue
                    if entries_json is None:
                        # ingestion also records range_entries_missing; the
                        # finished scan still covered the interval, but no
                        # returned-key heat can be derived
                        scan_unknown = heat_conflict = True
                        self._add_scan(lo, hi)
                        continue
                    entries = json.loads(entries_json)
                    seen: set = set()
                    for e in entries:
                        ek = e["key"]
                        if ek in seen:
                            heat_conflict = True
                            self._record_gap(
                                "duplicate_key_in_range_result",
                                f"{tid} op {seq}: key {ek} appears twice in "
                                f"the range result", f"{loc}.entries",
                                ("heat",))
                            continue
                        seen.add(ek)
                        if ek < lo or ek > hi:
                            heat_conflict = True
                            self._record_gap(
                                "entry_outside_interval",
                                f"{tid} op {seq}: returned key {ek} lies "
                                f"outside [{lo},{hi}]", loc, ("heat",))
                        # only non-tombstone returned keys enter per-key heat
                        if e.get("tombstone") is not True:
                            self._add_heat(ek, "range_item")
                    # a completed scan always covers its closed interval,
                    # even when the result list is empty
                    self._add_scan(lo, hi)
        return scan_unknown, heat_conflict

    # -- window JSON --------------------------------------------------------

    def _windows_to_json(self, anchor: Optional[float], max_bucket: int,
                         width: float) -> List[Dict[str, Any]]:
        pre: Dict[int, _WindowAcc] = getattr(self, "_pending_windows", {})
        membership_uncertain = bool(
            self.gap_effects.keys() & MEMBERSHIP_GAP_KINDS)
        classification_uncertain = bool(
            self.gap_effects.keys() & CLASSIFICATION_GAP_KINDS)
        heat_uncertain = bool(self.gap_effects.keys() & HEAT_GAP_KINDS)
        counts = self._per_window_op_counts()
        out: List[Dict[str, Any]] = []
        for k in range(max_bucket + 1):
            acc = pre.get(k)
            if acc is None:
                lo_t = anchor + k * width
                hi_t = anchor + (k + 1) * width
                # an explicitly empty window: known zero, never unknown
                out.append({
                    "window_index": k,
                    "interval": {"lo": _num(lo_t), "hi": _num(hi_t)},
                    "transactions": [],
                    "status": "complete",
                    "gaps": [],
                    "transaction_count": 0,
                    "operation_total": 0,
                    "point_reads": 0,
                    "range_reads": 0,
                    "writes": 0,
                    "duration": self._empty_duration(),
                })
                continue
            c = counts.get(k, {"total": 0, "point_read": 0,
                               "range_read": 0, "write": 0})
            wgap_kinds = set(acc.gap_kinds)
            if membership_uncertain:
                wgap_kinds.add("window_membership_unproven")
            if classification_uncertain:
                wgap_kinds.add("operation_classification_unproven")
            if heat_uncertain:
                wgap_kinds.add("heat_unproven")
            out.append({
                "window_index": k,
                "interval": {"lo": _num(acc.lo), "hi": _num(acc.hi)},
                "transactions": sorted(acc.txn_ids),
                "status": "unknown" if wgap_kinds else "complete",
                "gaps": sorted(wgap_kinds),
                # observable record counts are exact even when the record's
                # completeness is unproven; the gap flags carry that fact
                "transaction_count": acc.txn_count,
                "operation_total": c["total"],
                "point_reads": c["point_read"],
                "range_reads": c["range_read"],
                "writes": c["write"],
                "duration": self._duration_json(acc),
            })
        return out

    def _per_window_op_counts(self) -> Dict[int, Dict[str, int]]:
        """Exact op classification counts per bucket via a disk-side join."""
        out: Dict[int, Dict[str, int]] = {}
        cur = self.db.execute(
            "SELECT b.bucket, o.type, COUNT(*) FROM pf_txn_bucket b "
            "JOIN ops o ON o.txn_id = b.txn_id GROUP BY b.bucket, o.type")
        for bucket, typ, n in cur:
            c = out.setdefault(bucket, {"total": 0, "point_read": 0,
                                        "range_read": 0, "write": 0})
            c["total"] += int(n)
            if typ == OP_POINT_READ:
                c["point_read"] += int(n)
            elif typ == OP_RANGE_READ:
                c["range_read"] += int(n)
            else:
                c["write"] += int(n)
        return out

    @staticmethod
    def _empty_duration() -> Dict[str, Any]:
        return {"status": "complete", "explained_transactions": 0,
                "sum_ms": None, "min_ms": None, "max_ms": None,
                "mean_ms": None, "unexplained_transactions": []}

    def _duration_json(self, acc: _WindowAcc) -> Dict[str, Any]:
        if acc.duration_explained == 0:
            return {
                "status": "unknown",
                "explained_transactions": 0,
                "sum_ms": None, "min_ms": None, "max_ms": None,
                "mean_ms": None,
                "unexplained_transactions": sorted(acc.duration_unexplained),
            }
        return {
            "status": "unknown" if acc.duration_unknown else "complete",
            "explained_transactions": acc.duration_explained,
            "sum_ms": _num(acc.duration_sum),
            "min_ms": _num(acc.duration_min),
            "max_ms": _num(acc.duration_max),
            "mean_ms": _num(acc.duration_sum / acc.duration_explained),
            "unexplained_transactions": sorted(acc.duration_unexplained),
        }

    # -- key heat -----------------------------------------------------------

    def _key_heat_section(self, scan_unknown: bool) -> Dict[str, Any]:
        # merge any counters still held in memory with spilled disk rows
        merged: Dict[Tuple[int, str], int] = dict(self.key_heat)
        for key, kind, n in self.db.execute(
                "SELECT key, kind, n FROM pf_key_heat"):
            pair = (key, kind)
            merged[pair] = merged.get(pair, 0) + int(n)
        per_key: Dict[int, Dict[str, int]] = {}
        for (key, kind), n in merged.items():
            c = per_key.setdefault(
                key, {"point_read": 0, "range_item": 0, "write": 0})
            c[kind] += n
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
        # stable ranking: total desc, then key asc (deterministic tie order)
        ranked = sorted(entries, key=lambda e: (-e["total_events"], e["key"]))
        unknown = bool(self.gap_effects.keys() & HEAT_GAP_KINDS) or \
            scan_unknown
        return {
            "status": "unknown" if unknown else "complete",
            "semantics": "event counts: point_read target once per read; "
                         "put/delete key once per write; range_read only "
                         "counts non-tombstone keys returned by a completed "
                         "scan",
            "top_n": self.top_n,
            "distinct_keys": len(entries),
            "ranked_keys": ranked[: self.top_n],
            "note": "ranked_keys are provable lower bounds under status=unknown"
                    if unknown else "ranked_keys are exact",
        }

    # -- scan coverage ------------------------------------------------------

    def _coverage_section(self, scan_unknown: bool) -> Dict[str, Any]:
        segments = self._sweep_segments()
        unknown = bool(self.gap_effects.keys() & HEAT_GAP_KINDS) or \
            scan_unknown
        return {
            "status": "unknown" if unknown else "complete",
            "semantics": "one completed range scan contributes one coverage "
                         "count to every key in its closed interval, "
                         "including scans returning zero rows; segments are "
                         "disjoint and equal-coverage neighbors are merged",
            "completed_scans": self.scan_intervals,
            "endpoint_events": self.endpoint_events,
            "segments": segments,
            "note": "coverage is a provable lower bound under status=unknown"
                    if unknown else "coverage is exact",
        }

    def _sweep_segments(self) -> List[Dict[str, Any]]:
        """Closed-interval coverage via ordered end-point aggregation.

        Events are streamed from the SQLite index ordered by
        ``(pos, start_event DESC)``: at one coordinate all starts are applied
        before all ends, so an interval ending at x and another starting at x
        both cover x.  For every distinct event coordinate *p* with *s*
        starts and *e* ends:

        * the coverage of *p* itself is ``active + s`` (intervals ending at p
          still cover their closed end point, so ends are not subtracted yet);
        * the integer keys strictly between *p* and the next event coordinate
          carry the count ``active + s - e`` reached after the group;
        * then ``active += s - e``.

        Only positive-coverage runs are emitted and equal-coverage neighbors
        are merged by ``_merge_segments``.  The int64 domain is never
        enumerated and memory stays bounded by the fetch batch size.
        """
        cur = self.db.execute(
            "SELECT pos, start_event FROM pf_scan_events "
            "ORDER BY pos, start_event DESC")
        raw: List[Dict[str, Any]] = []
        active = 0
        prev_pos: Optional[int] = None
        active_after_prev = 0
        group_pos: Optional[int] = None
        starts = 0
        ends = 0

        def emit(lo: int, hi: int, coverage: int) -> None:
            if coverage > 0:
                raw.append({"lo": lo, "hi": hi, "coverage": coverage})

        def finish_group() -> None:
            nonlocal active, prev_pos, active_after_prev
            nonlocal group_pos, starts, ends
            if group_pos is None:
                return
            if prev_pos is not None:
                gap_lo = prev_pos + 1
                if gap_lo < group_pos:
                    emit(gap_lo, group_pos - 1, active_after_prev)
            count_here = active + starts
            emit(group_pos, group_pos, count_here)
            active += starts - ends
            prev_pos, active_after_prev = group_pos, active
            group_pos, starts, ends = None, 0, 0

        while True:
            batch = cur.fetchmany(_BATCH)
            if not batch:
                break
            for pos, start_event in batch:
                if group_pos is not None and pos != group_pos:
                    finish_group()
                if group_pos is None:
                    group_pos = pos
                if start_event:
                    starts += 1
                else:
                    ends += 1
        finish_group()
        # every interval has a matching end event, so the sweep closes flat
        if active != 0:
            raise ProfileError(
                f"internal: scan event sweep ended with active={active}")
        return self._merge_segments(raw)

    @staticmethod
    def _merge_segments(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge adjacent segments carrying the same coverage count."""
        merged: List[Dict[str, Any]] = []
        for seg in raw:
            if seg["lo"] < INT64_MIN or seg["hi"] > INT64_MAX or \
                    seg["lo"] > seg["hi"]:
                raise ProfileError(
                    f"internal: segment outside int64 domain: {seg}")
            if merged and merged[-1]["coverage"] == seg["coverage"] \
                    and seg["lo"] == merged[-1]["hi"] + 1:
                merged[-1]["hi"] = seg["hi"]
            else:
                merged.append(dict(seg))
        return merged


# ---------------------------------------------------------------------------
# top-level runner
# ---------------------------------------------------------------------------

def _unsigned(result: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in result.items()
            if k not in ("profile_sha256", "runtime_stats")}


def run(store: Store, input_info: Dict[str, Any], width: float,
        top_n: int) -> Dict[str, Any]:
    """Build the deterministic, signed workload profile."""
    t0 = time.monotonic()
    profiler = WorkloadProfiler(store, width, top_n)
    try:
        core = profiler.build()
    finally:
        profiler.close()
    stats = {
        "input_records": profiler.records_seen,
        "committed_transactions": profiler.committed_txns,
        "aborted_transactions_excluded": profiler.aborted_txns,
        "unbinned_transactions": len(profiler.unbinned_txns),
        "heat_events_processed": profiler.heat_events,
        "scan_intervals_processed": profiler.scan_intervals,
        "endpoint_events_processed": profiler.endpoint_events,
        "segments_emitted": len(core["scan_coverage"]["segments"]),
        "distinct_heat_keys": core["key_heat"]["distinct_keys"],
    }
    result: Dict[str, Any] = {
        "format": PROFILE_FORMAT,
        "generator": f"snapshot_audit/{__version__}",
        "input": input_info,
        "parameters": {"window_width": width, "top_n": top_n},
    }
    result.update(core)
    result["stats"] = dict(sorted(stats.items()))
    result["profile_sha256"] = sha256_text(
        canonical_json(_unsigned(result)))
    result["runtime_stats"] = {
        "rss_bytes_exact": _rss_bytes(),
        "wall_time_ms_exact": int((time.monotonic() - t0) * 1000),
        "measurement": "ru_maxrss of the profile process; monotonic wall "
                       "clock around profiling only",
    }
    return result


# ---------------------------------------------------------------------------
# independent verification
# ---------------------------------------------------------------------------

class ProfileVerification:
    def __init__(self) -> None:
        self.problems: List[str] = []
        self.checked: List[str] = []
        self.fatal = False

    def fail(self, where: str, detail: str, fatal: bool = False) -> None:
        self.problems.append(f"{where}: {detail}")
        self.fatal = self.fatal or fatal

    @property
    def ok(self) -> bool:
        return not self.problems


_DIFF_CAP = 80


def _diff(path: str, claimed: Any, recomputed: Any,
          problems: List[str]) -> None:
    if len(problems) >= _DIFF_CAP:
        return
    if type(claimed) is not type(recomputed):
        problems.append(
            f"{path}: type {type(claimed).__name__} in the claim != "
            f"{type(recomputed).__name__} recomputed")
        return
    if isinstance(claimed, dict):
        for k in sorted(set(claimed) | set(recomputed)):
            if k not in claimed:
                problems.append(f"{path}.{k}: missing in the claim")
            elif k not in recomputed:
                problems.append(f"{path}.{k}: not reproducible from the raw "
                                f"history")
            else:
                _diff(f"{path}.{k}", claimed[k], recomputed[k], problems)
    elif isinstance(claimed, list):
        if len(claimed) != len(recomputed):
            problems.append(f"{path}: claimed {len(claimed)} item(s), "
                            f"recomputed {len(recomputed)}")
        for i, (x, y) in enumerate(zip(claimed, recomputed)):
            _diff(f"{path}[{i}]", x, y, problems)
    elif claimed != recomputed:
        problems.append(f"{path}: claimed {claimed!r}, "
                        f"recomputed {recomputed!r}")


# Runtime stats are machine-state measurements, not semantic facts: they are
# unsigned and verified with explicit tolerance/exclusion rules instead of
# equality.
_RSS_FACTOR = 2  # recomputed peak RSS must stay within a factor of two


def _verify_runtime_stats(doc: Dict[str, Any],
                          v: ProfileVerification) -> None:
    rt = doc.get("runtime_stats")
    if not isinstance(rt, dict):
        v.fail("runtime_stats", "missing or malformed runtime_stats")
        return
    rss = rt.get("rss_bytes_exact")
    wall = rt.get("wall_time_ms_exact")
    if not isinstance(rss, int) or not isinstance(wall, int) or rss < 0 \
            or wall < 0:
        v.fail("runtime_stats",
               "rss_bytes_exact/wall_time_ms_exact must be non-negative ints")
        return
    peak = _rss_bytes()
    # tolerance rule: within a factor of two either direction (the
    # recomputation process legitimately differs in allocator behavior)
    if not (rss // _RSS_FACTOR <= peak <= rss * _RSS_FACTOR + 1):
        v.fail("runtime_stats.rss_bytes_exact",
               f"claimed peak RSS {rss} differs beyond the 2x tolerance from "
               f"the recomputation peak {peak}")
    # exclusion rule: wall time is environment dependent and never fails
    # verification; only its presence/type is checked above.


def verify_profile(doc: Any, input_path: Optional[str] = None,
                   input_dir: Optional[str] = None
                   ) -> ProfileVerification:
    """Recompute the whole profile from the raw history and compare.

    The verifier never trusts the supplied document's conclusions or its
    signature: the raw history bound in ``input`` is re-hashed, re-ingested
    and profiled independently, then every signed field is diffed.
    """
    from .hashing import hash_input, normalized_path
    from .model import load_history

    v = ProfileVerification()
    if not isinstance(doc, dict) or doc.get("format") != PROFILE_FORMAT:
        v.fail("format", f"not a {PROFILE_FORMAT} document", fatal=True)
        return v
    v.checked.append("format")

    stored_sig = doc.get("profile_sha256")
    actual_sig = sha256_text(canonical_json(_unsigned(doc)))
    v.checked.append("result_signature")
    if stored_sig != actual_sig:
        v.fail("profile_sha256",
               f"signature mismatch (stored {stored_sig}, recomputed over "
               f"the claim {actual_sig}); the document was modified")

    params = doc.get("parameters") or {}
    try:
        width = validate_window_width(params.get("window_width"))
        top_n = validate_top_n(params.get("top_n"))
    except ProfileError as e:
        v.fail("parameters", str(e), fatal=True)
        return v
    v.checked.append("parameters")

    rec = doc.get("input") or {}
    path = input_path or input_dir or rec.get("path")
    if not path:
        v.fail("input", "no input path available (give --input/--input-dir)",
               fatal=True)
        return v
    try:
        info0 = hash_input(path)
    except (OSError, ValueError) as e:
        v.fail("input", f"cannot load raw history: {e}", fatal=True)
        return v
    v.checked.append("input_binding")
    if info0["sha256"] != rec.get("sha256"):
        v.fail("input.sha256",
               f"raw history SHA-256 is {info0['sha256']}, the profile was "
               f"built for {rec.get('sha256')}")
    if info0["kind"] != rec.get("kind"):
        v.fail("input.kind",
               f"raw history kind is {info0['kind']!r}, bound "
               f"{rec.get('kind')!r}")
    if input_path is None and input_dir is None and \
            normalized_path(path) != rec.get("path"):
        v.fail("input.path",
               f"input moved: bound {rec.get('path')!r}, now "
               f"{normalized_path(path)!r}")

    store = Store()
    try:
        try:
            header, commits, txns = load_history(
                path if info0["kind"] == "file" else None,
                path if info0["kind"] == "dir" else None)
            store.ingest(header, commits, txns)
        except (ContractError, OSError, ValueError) as e:
            v.fail("input", f"cannot parse raw history: {e}", fatal=True)
            return v
        recompute_input = info0
        if (input_path is not None or input_dir is not None) and \
                info0["sha256"] == rec.get("sha256") and \
                info0["kind"] == rec.get("kind"):
            recompute_input = dict(info0)
            recompute_input["path"] = rec.get("path")
        recomputed = run(store, recompute_input, width, top_n)
    finally:
        store.close()
    v.checked.append("recompute")
    try:
        info1 = hash_input(path)
    except (OSError, ValueError) as e:
        v.fail("input", f"cannot re-hash raw history: {e}", fatal=True)
        return v
    if info1["sha256"] != info0["sha256"]:
        v.fail("input", "raw history changed during verification",
               fatal=True)
        return v

    v.checked.append("windows_classification_heat_segments_unknowns_order")
    _diff("profile", _unsigned(doc), _unsigned(recomputed), v.problems)
    if len(v.problems) >= _DIFF_CAP:
        v.problems.append(f"... comparison stopped after {_DIFF_CAP} "
                          f"differences")
    v.checked.append("runtime_stats")
    _verify_runtime_stats(doc, v)
    return v
