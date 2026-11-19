"""Observation-dependency lineage (观察依赖血缘) by historical position.

Given a *raw* history and a ``commit_pos``, ``lineage`` answers "what is the
current value of this key (or of every live key in a closed interval) at
that position" and then traces, for every returned version, which committed
transaction wrote it and which versions that transaction had observed
before the write - recursively, by version and by the reader's
``snapshot_pos`` at read time.  It never walks per-key latest-version
chains backwards and never uses the auditor's dependency graph, an audit
report or a regression ledger as a fact.

Semantics boundary (important): the input format records *what each
transaction observed*, not *how values were computed*.  There is no
expression- or output-column-level mapping in the format, so every read a
transaction performed before a write is reported as a member of the
**transaction-level observation set**.  An edge "V's writer observed U"
must never be read as "U causally produced V" in the business sense.

Graph model
-----------

* **Nodes** are committed versions (including initial-state versions and
  tombstones).  A node keeps the writer transaction, the write operation
  sequence number, the version semantics (initial / committed value /
  tombstone) and the original-record location.
* **Edges** are the read observations of the writer transaction that
  precede the write operation.  Edge kinds: ``point_read``,
  ``range_read_item``, ``range_predicate`` (interval-completeness
  observation, incl. tombstone-absence items), ``own_write``
  (intra-transaction write-then-read; never a transaction-level
  self-loop).  Absence observations (point read ``found=false``) and
  range predicates have no fabricated value node: a range query key that
  does not exist yields no value node, only the predicate observation of
  the scanning transaction is kept.
* Every node and edge carries its own ``status`` (``complete`` /
  ``unknown``) with explicit gaps: missing/conflicting version chains,
  undeclared-complete log ranges, unfinished scans, unattributable
  observations, ...  Gaps are localized - an unrelated branch's gap never
  pollutes a provably independent node or edge.
* Expansion is a deterministic best-first traversal (depth, then version
  id) over the on-disk indexes; each version is expanded at most once
  (paths deduplicate), while the same upstream version observed by
  several operations keeps one edge per operation (each with its own
  evidence).  Cycles and conflicting evidence yield stable references /
  boundaries, never infinite recursion.  ``--max-depth`` / ``--max-nodes``
  truncate with an explicit resumption cursor; a truncated result is
  never reported as complete.

The result binds the input spec path, SHA-256, the query position, the
key/interval and the format version, and is deterministically signed.
``verify-lineage`` reloads the raw history, rebuilds the target state and
the observation-dependency graph from scratch and re-checks everything
without trusting the JSON under test.
"""

from __future__ import annotations

import heapq
import json
import resource
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from . import __version__
from .hashing import canonical_json, sha256_text
from .model import (
    INITIAL_TXN, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ,
    STATUS_COMMITTED, ContractError, check_int64,
)
from .reads import LOG_GAP_KINDS
from .store import Store

LINEAGE_FORMAT = "observation-lineage/1"

SEMANTICS_NOTE = (
    "观察依赖血缘：边仅表示写入事务在写操作之前实际观察到的版本"
    "（事务级观察集合）。本格式未记录计算表达式或输出列映射，"
    "因此不声称某次读取在业务因果上产生了某个写入。"
)

OBSERVATION_SET_NOTE = (
    "transaction-level observation set: every read the writer transaction "
    "performed before the write operation is an observable input; the "
    "format records no expression-level association, so membership does "
    "not imply the read participated in computing the value"
)

# edge kinds
KIND_POINT = "point_read"
KIND_RANGE_ITEM = "range_read_item"
KIND_RANGE_PREDICATE = "range_predicate"
KIND_OWN_WRITE = "own_write"

# observed classifications
OBS_VERSION = "version"
OBS_ABSENT_TOMBSTONE = "absent_tombstone"
OBS_ABSENT_NO_VERSION = "absent_no_version"
OBS_PREDICATE = "predicate"
OBS_OWN_VALUE = "own_value"
OBS_OWN_ABSENT = "own_absent"

# node version semantics
SEM_INITIAL = "initial"
SEM_COMMITTED = "committed_value"
SEM_TOMBSTONE = "tombstone"

# truncation reasons
REASON_MAX_DEPTH = "max_depth"
REASON_MAX_NODES = "max_nodes"


class InputChangedError(Exception):
    """The input history changed while the lineage was being computed."""


def _rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss
    return rss * 1024


def _dec(canon: Optional[str]) -> Any:
    return None if canon is None else json.loads(canon)


def _gap(kind: str, detail: str, loc: str = "") -> Dict[str, Any]:
    return {"kind": kind, "detail": detail, "loc": loc}


def build_query(commit_pos: Any, key: Any = None, lo: Any = None,
                hi: Any = None) -> Dict[str, Any]:
    """Validate and normalize a lineage query (point or closed interval)."""
    if isinstance(commit_pos, bool) or not isinstance(commit_pos, int) \
            or commit_pos < 0:
        raise ContractError(
            f"query: commit_pos must be a non-negative integer, got {commit_pos!r}")
    if key is not None and (lo is not None or hi is not None):
        raise ContractError("query: give either a key or an interval, not both")
    if key is not None:
        check_int64(key, "query")
        return {"commit_pos": commit_pos, "key": key}
    if lo is None or hi is None:
        raise ContractError("query: an interval query needs both lo and hi")
    check_int64(lo, "query")
    check_int64(hi, "query")
    if hi < lo:
        raise ContractError(f"query: interval hi {hi} < lo {lo}")
    return {"commit_pos": commit_pos, "lo": lo, "hi": hi}


class _TxnCtx:
    """On-demand per-transaction context for lineage expansion."""

    __slots__ = ("txn_id", "status", "snapshot_pos", "ops_complete",
                 "commit_pos", "loc", "ops", "write_ops")

    def __init__(self, txn_id: str, row: tuple, ops: List[tuple]) -> None:
        self.txn_id = txn_id
        self.status, self.snapshot_pos, self.ops_complete, self.commit_pos, \
            self.loc = row
        self.ops = ops  # (seq,type,key,lo,hi,value,found,vid,entries,complete,loc)
        write_ops: Dict[int, List[Tuple[int, str, Optional[str]]]] = {}
        for (seq, typ, key, _lo, _hi, value, _f, _v, _e, _c, _l) in ops:
            if typ in (OP_PUT, OP_DELETE):
                write_ops.setdefault(key, []).append((seq, typ, value))
        self.write_ops = write_ops


class LineageBuilder:
    """Deterministic observation-dependency expansion over the disk store."""

    def __init__(self, store: Store, max_depth: int, max_nodes: int) -> None:
        if max_depth < 0:
            raise ContractError(f"max_depth must be >= 0, got {max_depth}")
        if max_nodes < 0:
            raise ContractError(f"max_nodes must be >= 0, got {max_nodes}")
        self.store = store
        self.max_depth = max_depth
        self.max_nodes = max_nodes
        self.log_gaps = [g for g in store.gaps if g.kind in LOG_GAP_KINDS]
        self.log_ok = not self.log_gaps
        self.broken_keys = store.broken_key_set()
        self.dup_vids = store.duplicate_version_ids() if any(
            g.kind == "duplicate_version_id" for g in store.gaps) else set()
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Dict[str, Any]] = []
        self.heap: List[Tuple[int, str]] = []
        self.expanded: set = set()
        self.cursor: List[Dict[str, Any]] = []
        self._cursor_seen: set = set()
        self.txn_ctxs: Dict[str, Optional[_TxnCtx]] = {}
        self.counters = {"read_ops_examined": 0}

    # -- context -----------------------------------------------------------

    def _txn_ctx(self, txn_id: str) -> Optional[_TxnCtx]:
        if txn_id in self.txn_ctxs:
            return self.txn_ctxs[txn_id]
        row = self.store.txn_row(txn_id)
        ctx = None
        if row is not None:
            ctx = _TxnCtx(txn_id, row, self.store.txn_ops(txn_id))
        self.txn_ctxs[txn_id] = ctx
        return ctx

    def _log_gap_jsons(self) -> List[Dict[str, Any]]:
        return [g.to_json() for g in self.log_gaps]

    # -- nodes ---------------------------------------------------------------

    @staticmethod
    def _node_id(vid: str) -> str:
        return "v:" + vid

    def _defer(self, vid: str, depth: int, reason: str,
               node: Optional[str] = None) -> None:
        key = (vid, depth, reason)
        if key in self._cursor_seen:
            return
        self._cursor_seen.add(key)
        self.cursor.append({"version_id": vid, "node": node, "depth": depth,
                            "reason": reason})

    def _make_node(self, vrow: tuple, depth: int) -> Tuple[Optional[str], bool]:
        """Materialize a version node; returns (node_id, deferred)."""
        key, pos, vid, txn, tomb, val, loc = vrow
        nid = self._node_id(vid)
        if nid in self.nodes:
            return nid, False
        if len(self.nodes) >= self.max_nodes:
            self._defer(vid, depth, REASON_MAX_NODES)
            return None, True
        gaps: List[Dict[str, Any]] = []
        node: Dict[str, Any] = {
            "id": nid,
            "key": key,
            "version_id": vid,
            "commit_pos": pos,
            "writer_txn": txn,
            "write_op_seq": None,
            "version_semantics": SEM_TOMBSTONE if tomb else SEM_COMMITTED,
            "loc": loc,
            "depth": depth,
            "expanded": False,
            "observation_set": None,
            "status": "complete",
            "gaps": gaps,
        }
        if not tomb:
            node["value"] = _dec(val)
        expandable = False
        if txn == INITIAL_TXN:
            node["version_semantics"] = SEM_INITIAL
            node["expanded"] = True
            node["observation_set"] = {
                "scope": "none", "complete": True, "reads_before_write": 0,
                "note": "initial state; no writer transaction"}
        elif vid in self.dup_vids:
            # conflicting evidence: stable boundary, never expanded
            gaps.append(_gap("duplicate_version_id",
                             f"version id {vid!r} names more than one committed "
                             f"version; lineage cannot be attributed uniquely",
                             "commit_log"))
        else:
            ctx = self._txn_ctx(txn)
            if ctx is None:
                gaps.append(_gap("txn_record_missing",
                                 f"writer {txn!r} has no transaction record; "
                                 f"its read observations are unknown", loc))
            else:
                if ctx.status != STATUS_COMMITTED:
                    gaps.append(_gap("txn_not_committed",
                                     f"writer {txn!r} is recorded as "
                                     f"{ctx.status!r} but appears in the commit "
                                     f"log", ctx.loc))
                wops = ctx.write_ops.get(key, [])
                if len(wops) == 1:
                    wseq, wtyp, wval = wops[0]
                    node["write_op_seq"] = wseq
                    if (wtyp == OP_DELETE) != bool(tomb) or \
                            (wtyp == OP_PUT and wval != val):
                        gaps.append(_gap(
                            "txn_writes_vs_log_mismatch",
                            f"{txn}: write op for key {key} disagrees with the "
                            f"logged version", ctx.loc))
                elif not wops:
                    gaps.append(_gap(
                        "write_op_unattributed",
                        f"{txn}: no put/delete op for key {key}; the set of "
                        f"reads before the write is unbounded (all recorded "
                        f"reads are included)", ctx.loc))
                else:
                    gaps.append(_gap(
                        "key_written_twice_in_txn",
                        f"{txn}: key {key} written {len(wops)} times; write op "
                        f"attribution is ambiguous", ctx.loc))
                if not ctx.ops_complete:
                    gaps.append(_gap("missing_operations",
                                     f"{txn}: operation record incomplete; the "
                                     f"observation set may be incomplete",
                                     ctx.loc))
                if ctx.snapshot_pos is None:
                    gaps.append(_gap("missing_snapshot_pos",
                                     f"{txn}: no snapshot_pos; snapshot-relative "
                                     f"observations cannot be resolved", ctx.loc))
                if ctx.snapshot_pos is not None and ctx.commit_pos is not None \
                        and ctx.snapshot_pos >= ctx.commit_pos:
                    gaps.append(_gap(
                        "snapshot_not_before_commit",
                        f"{txn}: snapshot_pos {ctx.snapshot_pos} >= own "
                        f"commit_pos {ctx.commit_pos}", ctx.loc))
            expandable = True
        node["status"] = "unknown" if gaps else "complete"
        self.nodes[nid] = node
        if expandable:
            if depth < self.max_depth:
                heapq.heappush(self.heap, (depth, nid))
            else:
                self._defer(vid, depth, REASON_MAX_DEPTH, node=nid)
        return nid, False

    # -- edges ---------------------------------------------------------------

    def _add_edge(self, node: Dict[str, Any], depth: int, *, kind: str,
                  reader_txn: str, read_seq: int, sp: Optional[int],
                  key: Optional[int], lo: Optional[int], hi: Optional[int],
                  observed: str, target_row: Optional[tuple],
                  target_vid: Optional[str], version_recorded: bool,
                  intra_txn: bool, evidence: Dict[str, Any],
                  gaps: List[Dict[str, Any]]) -> None:
        deferred = False
        target = None
        if target_row is not None:
            target, deferred = self._make_node(target_row, depth + 1)
        edge = {
            "src": node["id"],
            "kind": kind,
            "reader_txn": reader_txn,
            "read_seq": read_seq,
            "reader_snapshot_pos": sp,
            "key": key,
            "lo": lo,
            "hi": hi,
            "observed": observed,
            "target": target,
            "target_version_id": target_vid,
            "version_recorded": version_recorded,
            "intra_txn": intra_txn,
            "deferred": deferred,
            "evidence": evidence,
            "status": "unknown" if gaps else "complete",
            "gaps": gaps,
        }
        self.edges.append(edge)

    # -- read observation resolution -----------------------------------------

    def _recorded_version_edge(self, node, ctx, depth, common, kind,
                               vid, value, loc, extra_gaps):
        """Edge for a read that recorded an explicit version id."""
        gaps = list(extra_gaps)
        ev: Dict[str, Any] = {"read_loc": loc, "recorded_version_id": vid,
                              "value": _dec(value)}
        vrow = self.store.version_by_id(vid)
        if vrow is not None:
            ev["version_loc"] = vrow[6]
            if vrow[0] != common["key"]:
                gaps.append(_gap(
                    "cross_key_version_observed",
                    f"recorded version {vid} belongs to key {vrow[0]}, not "
                    f"key {common['key']}", loc))
            if vid in self.dup_vids:
                gaps.append(_gap("duplicate_version_id",
                                 f"version id {vid!r} is ambiguous",
                                 "commit_log"))
            self._add_edge(node, depth, kind=kind, **common, observed=OBS_VERSION,
                           target_row=vrow, target_vid=vid,
                           version_recorded=True, intra_txn=False,
                           evidence=ev, gaps=gaps)
            return
        arow = self.store.aborted_by_id(vid)
        if arow is not None:
            ev["aborted_txn"] = arow[2]
            ev["aborted_loc"] = arow[5]
            gaps.append(_gap(
                "aborted_version_observed",
                f"version {vid} was written by rolled-back txn {arow[2]}; "
                f"it has no committed lineage", loc))
        else:
            gaps.append(_gap(
                "unknown_version_observed",
                f"version {vid} not found in the committed log", loc))
        self._add_edge(node, depth, kind=kind, **common, observed=OBS_VERSION,
                       target_row=None, target_vid=vid,
                       version_recorded=True, intra_txn=False,
                       evidence=ev, gaps=gaps)

    def _inferred_version_edge(self, node, ctx, depth, common, kind,
                               value, loc, extra_gaps):
        """Edge for a found-read without a recorded version id.

        The observation is resolved against the version visible at the
        reader's snapshot; that resolution needs a provably complete log
        and an intact version chain for the key.
        """
        key = common["key"]
        sp = common["sp"]
        gaps = list(extra_gaps)
        if sp is None:
            gaps.append(_gap("missing_snapshot_pos",
                             f"{ctx.txn_id}: no snapshot_pos; cannot resolve "
                             f"the snapshot-visible version", ctx.loc))
        if not self.log_ok:
            gaps.extend(self._log_gap_jsons())
        if key in self.broken_keys:
            gaps.append(_gap("version_chain_broken",
                             f"key {key} has a broken version chain", ""))
        ev: Dict[str, Any] = {"read_loc": loc, "recorded_version_id": None,
                              "value": _dec(value)}
        vis = self.store.visible_version(key, sp) if sp is not None else None
        target_row, tvid = None, None
        if vis is not None:
            tvid = vis[2]
            if vis[4]:
                gaps.append(_gap(
                    "observed_value_unattributable",
                    f"read recorded a value for key {key} but the "
                    f"snapshot-visible version {vis[2]} is a tombstone", loc))
            elif value != vis[5]:
                gaps.append(_gap(
                    "observed_value_unattributable",
                    f"recorded value does not match snapshot-visible version "
                    f"{vis[2]}; the actually observed version is unknown", loc))
            else:
                ev["version_loc"] = vis[6]
                target_row = vis
                if vis[2] in self.dup_vids:
                    gaps.append(_gap("duplicate_version_id",
                                     f"version id {vis[2]!r} is ambiguous",
                                     "commit_log"))
        elif sp is not None:
            gaps.append(_gap(
                "observed_value_unattributable",
                f"read recorded a value for key {key} but no version is "
                f"visible at snapshot {sp}", loc))
        self._add_edge(node, depth, kind=kind, **common, observed=OBS_VERSION,
                       target_row=target_row, target_vid=tvid,
                       version_recorded=False, intra_txn=False,
                       evidence=ev, gaps=gaps)

    def _absence_edge(self, node, ctx, depth, common, kind, loc, extra_gaps):
        """Edge for a point read that recorded found=false."""
        key = common["key"]
        sp = common["sp"]
        gaps = list(extra_gaps)
        if sp is None:
            gaps.append(_gap("missing_snapshot_pos",
                             f"{ctx.txn_id}: no snapshot_pos; cannot resolve "
                             f"the absence observation", ctx.loc))
        if not self.log_ok:
            gaps.extend(self._log_gap_jsons())
        if key in self.broken_keys:
            gaps.append(_gap("version_chain_broken",
                             f"key {key} has a broken version chain", ""))
        ev: Dict[str, Any] = {"read_loc": loc}
        vis = self.store.visible_version(key, sp) if sp is not None else None
        observed = OBS_ABSENT_NO_VERSION
        target_row, tvid = None, None
        if vis is not None:
            if vis[4]:
                observed = OBS_ABSENT_TOMBSTONE
                target_row, tvid = vis, vis[2]
                ev["version_loc"] = vis[6]
                if vis[2] in self.dup_vids:
                    gaps.append(_gap("duplicate_version_id",
                                     f"version id {vis[2]!r} is ambiguous",
                                     "commit_log"))
            else:
                gaps.append(_gap(
                    "absence_contradicted",
                    f"read recorded absence of key {key} but version {vis[2]} "
                    f"is live at the snapshot", loc))
        self._add_edge(node, depth, kind=kind, **common, observed=observed,
                       target_row=target_row, target_vid=tvid,
                       version_recorded=False, intra_txn=False,
                       evidence=ev, gaps=gaps)

    def _own_write_edge(self, node, ctx, depth, common, own_rec,
                        found, value, loc, extra_gaps=()):
        """Intra-transaction write-then-read edge (never a txn self-loop)."""
        key = common["key"]
        wseq, wtyp, wval = own_rec
        gaps = list(extra_gaps)
        exp_found = wtyp == OP_PUT
        if bool(found) != exp_found or (exp_found and value != wval):
            gaps.append(_gap(
                "own_write_read_mismatch",
                f"{ctx.txn_id}: read of key {key} disagrees with the "
                f"transaction's own earlier {wtyp}", loc))
        if len(ctx.write_ops.get(key, [])) > 1:
            gaps.append(_gap("key_written_twice_in_txn",
                             f"{ctx.txn_id}: key {key} written more than once",
                             ctx.loc))
        vrow = self.store.version_by_txn_key(ctx.txn_id, key)
        tvid = None
        if vrow is None:
            gaps.append(_gap(
                "own_write_version_missing",
                f"{ctx.txn_id}: own write of key {key} has no committed "
                f"version in the log", loc))
        else:
            tvid = vrow[2]
            if tvid in self.dup_vids:
                gaps.append(_gap("duplicate_version_id",
                                 f"version id {tvid!r} is ambiguous",
                                 "commit_log"))
        ev = {"read_loc": loc, "write_seq": wseq,
              "value": _dec(wval) if wtyp == OP_PUT else None,
              "version_loc": vrow[6] if vrow else None}
        self._add_edge(node, depth, kind=KIND_OWN_WRITE, **common,
                       observed=OBS_OWN_VALUE if exp_found else OBS_OWN_ABSENT,
                       target_row=vrow, target_vid=tvid,
                       version_recorded=False, intra_txn=True,
                       evidence=ev, gaps=gaps)

    # -- expansion -------------------------------------------------------------

    def _expand(self, node: Dict[str, Any], depth: int) -> None:
        txn = node["writer_txn"]
        ctx = self._txn_ctx(txn)
        reads_seen = 0
        if ctx is not None and ctx.status == STATUS_COMMITTED:
            wseq = node["write_op_seq"]
            own: Dict[int, Tuple[int, str, Optional[str]]] = {}
            for (seq, typ, key, lo, hi, value, found, vid, entries, cflag,
                 loc) in ctx.ops:
                if typ in (OP_PUT, OP_DELETE):
                    own[key] = (seq, typ, value)
                    continue
                if wseq is not None and seq >= wseq:
                    continue  # only reads *before* the write are inputs
                reads_seen += 1
                self.counters["read_ops_examined"] += 1
                if typ == OP_POINT_READ:
                    self._point_read_edges(node, ctx, own, depth, seq, key,
                                           found or 0, value, vid, loc)
                elif typ == OP_RANGE_READ:
                    self._range_read_edges(node, ctx, own, depth, seq, lo, hi,
                                           entries, cflag or 0, loc)
            obs_complete = bool(ctx.ops_complete) and wseq is not None
        else:
            obs_complete = False
        node["observation_set"] = {
            "scope": "transaction_level",
            "complete": obs_complete,
            "reads_before_write": reads_seen,
            "note": OBSERVATION_SET_NOTE,
        }

    def _point_read_edges(self, node, ctx, own, depth, seq, key, found,
                          value, vid, loc):
        sp = ctx.snapshot_pos
        common = {"reader_txn": ctx.txn_id, "read_seq": seq, "sp": sp,
                  "key": key, "lo": None, "hi": None}
        if key in own:
            self._own_write_edge(node, ctx, depth, common, own[key],
                                 found, value, loc)
            return
        if found:
            if vid is not None:
                self._recorded_version_edge(node, ctx, depth, common,
                                            KIND_POINT, vid, value, loc, ())
            else:
                self._inferred_version_edge(node, ctx, depth, common,
                                            KIND_POINT, value, loc, ())
        else:
            self._absence_edge(node, ctx, depth, common, KIND_POINT, loc, ())

    def _range_read_edges(self, node, ctx, own, depth, seq, lo, hi,
                          entries_json, cflag, loc):
        sp = ctx.snapshot_pos
        scan_complete = bool(cflag)
        entries = json.loads(entries_json) if entries_json is not None else None
        returned: List[Tuple[int, Optional[str], Optional[str]]] = []
        dup_keys: set = set()
        seen_keys: set = set()
        if entries is not None:
            for e in entries:
                k = e["key"]
                if k in seen_keys:
                    dup_keys.add(k)
                seen_keys.add(k)
                returned.append((k, e["value"], e.get("version_id")))
        returned_set = {k for k, _v, _i in returned}
        returned_keys = sorted(returned_set)
        own_in_range = {k: v for k, v in own.items() if lo <= k <= hi}

        # returned items
        for (k, val, evid) in returned:
            common = {"reader_txn": ctx.txn_id, "read_seq": seq, "sp": sp,
                      "key": k, "lo": lo, "hi": hi}
            extra: List[Dict[str, Any]] = []
            if k < lo or k > hi:
                extra.append(_gap("entry_outside_interval",
                                  f"returned key {k} lies outside the scanned "
                                  f"interval [{lo},{hi}]", loc))
            if k in dup_keys:
                extra.append(_gap("duplicate_range_entry_key",
                                  f"key {k} appears twice in the range result",
                                  loc))
            if k in own_in_range:
                self._own_write_edge(node, ctx, depth, common, own_in_range[k],
                                     1, val, loc, extra)
            elif evid is not None:
                self._recorded_version_edge(node, ctx, depth, common,
                                            KIND_RANGE_ITEM, evid, val, loc,
                                            extra)
            else:
                self._inferred_version_edge(node, ctx, depth, common,
                                            KIND_RANGE_ITEM, val, loc, extra)

        # own keys inside the interval that the scan did not return
        if scan_complete and entries is not None:
            for k in sorted(own_in_range):
                if k in returned_set:
                    continue
                common = {"reader_txn": ctx.txn_id, "read_seq": seq, "sp": sp,
                          "key": k, "lo": lo, "hi": hi}
                self._own_write_edge(node, ctx, depth, common, own_in_range[k],
                                     0, None, loc)

        # interval-completeness predicate observation (no fabricated values)
        pgaps: List[Dict[str, Any]] = []
        if hi < lo:
            pgaps.append(_gap("bad_range", f"hi {hi} < lo {lo}", loc))
        if not scan_complete:
            pgaps.append(_gap(
                "incomplete_scan",
                f"{ctx.txn_id} op {seq}: range scan [{lo},{hi}] did not "
                f"record its end flag; the predicate observation is unproven",
                loc))
        if entries is None:
            pgaps.append(_gap("range_entries_missing",
                              f"{ctx.txn_id} op {seq}: no result list recorded",
                              loc))
        if sp is None:
            pgaps.append(_gap("missing_snapshot_pos",
                              f"{ctx.txn_id}: no snapshot_pos; cannot resolve "
                              f"the predicate observation", ctx.loc))
        if not self.log_ok:
            pgaps.extend(self._log_gap_jsons())
        broken_in = any(lo <= k <= hi for k in self.broken_keys)
        if broken_in:
            pgaps.append(_gap("version_chain_broken",
                              f"a key inside [{lo},{hi}] has a broken version "
                              f"chain", ""))
        if dup_keys:
            pgaps.append(_gap("duplicate_range_entry_key",
                              f"range result repeats key(s) "
                              f"{sorted(dup_keys)[:5]}", loc))
        vis_rows = None
        if sp is not None and scan_complete and entries is not None:
            vis_rows = list(self.store.visible_entries_in_range(lo, hi, sp))
        if not pgaps and vis_rows is not None:
            contradicted = [r[0] for r in vis_rows
                            if not r[4] and r[0] not in returned_set
                            and r[0] not in own_in_range]
            if contradicted:
                pgaps.append(_gap(
                    "predicate_contradicted",
                    f"scan [{lo},{hi}] is complete but live key(s) "
                    f"{contradicted[:5]} visible at the snapshot are missing "
                    f"from the result", loc))
        pev = {"read_loc": loc,
               "returned_keys": returned_keys if entries is not None else None,
               "own_keys_in_range": sorted(own_in_range),
               "scan_complete": scan_complete}
        self._add_edge(node, depth, kind=KIND_RANGE_PREDICATE,
                       reader_txn=ctx.txn_id, read_seq=seq, sp=sp,
                       key=None, lo=lo, hi=hi, observed=OBS_PREDICATE,
                       target_row=None, target_vid=None,
                       version_recorded=False, intra_txn=False,
                       evidence=pev, gaps=pgaps)

        # per-tombstone absence observations inside the scanned interval
        if vis_rows is not None and scan_complete:
            for r in vis_rows:
                k3, _pos, vvid, _txn, tomb, _val, vloc = r
                if not tomb or k3 in returned_set or k3 in own_in_range:
                    continue
                tgaps: List[Dict[str, Any]] = []
                if not self.log_ok:
                    tgaps.extend(self._log_gap_jsons())
                if k3 in self.broken_keys:
                    tgaps.append(_gap("version_chain_broken",
                                      f"key {k3} has a broken version chain",
                                      ""))
                if vvid in self.dup_vids:
                    tgaps.append(_gap("duplicate_version_id",
                                      f"version id {vvid!r} is ambiguous",
                                      "commit_log"))
                self._add_edge(node, depth, kind=KIND_RANGE_PREDICATE,
                               reader_txn=ctx.txn_id, read_seq=seq, sp=sp,
                               key=k3, lo=lo, hi=hi,
                               observed=OBS_ABSENT_TOMBSTONE,
                               target_row=r, target_vid=vvid,
                               version_recorded=False, intra_txn=False,
                               evidence={"read_loc": loc, "version_loc": vloc},
                               gaps=tgaps)

    # -- driver ----------------------------------------------------------------

    def _run_expansion(self) -> None:
        while self.heap:
            depth, nid = heapq.heappop(self.heap)
            if nid in self.expanded:
                continue
            node = self.nodes[nid]
            self._expand(node, depth)
            self.expanded.add(nid)
            node["expanded"] = True

    def build(self, query: Dict[str, Any]) -> Dict[str, Any]:
        pos = query["commit_pos"]
        results: List[Dict[str, Any]] = []
        results_gaps: List[Dict[str, Any]] = []
        if "key" in query:
            key = query["key"]
            egaps: List[Dict[str, Any]] = []
            if not self.log_ok:
                egaps.extend(self._log_gap_jsons())
            if key in self.broken_keys:
                egaps.append(_gap("version_chain_broken",
                                  f"key {key} has a broken version chain", ""))
            vis = self.store.visible_version(key, pos)
            entry: Dict[str, Any] = {"key": key}
            if vis is None:
                entry.update({"found": False, "reason": "no_version",
                              "version_id": None, "node": None})
            elif vis[4]:
                nid, _ = self._make_node(vis, 0)
                if vis[2] in self.dup_vids:
                    egaps.append(_gap("duplicate_version_id",
                                      f"version id {vis[2]!r} is ambiguous",
                                      "commit_log"))
                entry.update({"found": False, "reason": "tombstone",
                              "version_id": vis[2], "node": nid})
            else:
                nid, _ = self._make_node(vis, 0)
                if vis[2] in self.dup_vids:
                    egaps.append(_gap("duplicate_version_id",
                                      f"version id {vis[2]!r} is ambiguous",
                                      "commit_log"))
                entry.update({"found": True, "value": _dec(vis[5]),
                              "version_id": vis[2], "node": nid})
            entry["status"] = "unknown" if egaps else "complete"
            entry["gaps"] = egaps
            results.append(entry)
            results_complete = not egaps
        else:
            lo, hi = query["lo"], query["hi"]
            if not self.log_ok:
                results_gaps.extend(self._log_gap_jsons())
            if any(lo <= k <= hi for k in self.broken_keys):
                results_gaps.append(_gap(
                    "version_chain_broken",
                    f"a key inside [{lo},{hi}] has a broken version chain", ""))
            for row in self.store.visible_entries_in_range(lo, hi, pos):
                k, _p, vid, _txn, tomb, val, _loc = row
                if tomb:
                    continue  # non-existent keys get no value node
                egaps = list(results_gaps)
                if vid in self.dup_vids:
                    egaps.append(_gap("duplicate_version_id",
                                      f"version id {vid!r} is ambiguous",
                                      "commit_log"))
                nid, _ = self._make_node(row, 0)
                results.append({"key": k, "found": True, "value": _dec(val),
                                "version_id": vid, "node": nid,
                                "status": "unknown" if egaps else "complete",
                                "gaps": egaps})
            results_complete = not results_gaps and \
                all(r["status"] == "complete" for r in results)

        self._run_expansion()

        nodes = [self.nodes[nid] for nid in sorted(self.nodes)]
        edges = sorted(self.edges, key=_edge_sort_key)
        cycles = _find_cycles([n["id"] for n in nodes], edges)
        unknowns = _unknown_summary(results, results_gaps, nodes, edges)
        truncation = None
        if self.cursor:
            cursor = sorted(self.cursor,
                            key=lambda c: (c["depth"], c["version_id"],
                                           c["reason"]))
            truncation = {
                "reasons": sorted({c["reason"] for c in cursor}),
                "cursor": cursor,
                "note": "lineage is partial; re-query with higher limits to "
                        "resume from the cursor versions",
            }
        any_unknown = (
            not results_complete
            or any(r["status"] == "unknown" for r in results)
            or any(n["status"] == "unknown" for n in nodes)
            or any(e["status"] == "unknown" for e in edges))
        if truncation is not None:
            status = "truncated"
        elif any_unknown:
            status = "unknown"
        else:
            status = "complete"
        return {
            "status": status,
            "results": results,
            "results_complete": results_complete,
            "results_gaps": results_gaps,
            "nodes": nodes,
            "edges": edges,
            "cycles": cycles,
            "unknowns": unknowns,
            "truncation": truncation,
        }

    def stats_json(self, core: Dict[str, Any]) -> Dict[str, int]:
        c = self.store.stats.counters
        return dict(sorted({
            "version_lookups": c.get("version_lookups", 0),
            "txn_lookups": c.get("txn_lookups", 0),
            "op_lookups": c.get("op_lookups", 0),
            "range_probes": c.get("range_probes", 0),
            "range_keys_scanned": c.get("range_keys_scanned", 0),
            "read_ops_examined": self.counters["read_ops_examined"],
            "nodes_created": len(self.nodes),
            "nodes_expanded": len(self.expanded),
            "edges_emitted": len(self.edges),
            "unknown_nodes": sum(1 for n in core["nodes"]
                                 if n["status"] == "unknown"),
            "unknown_edges": sum(1 for e in core["edges"]
                                 if e["status"] == "unknown"),
            "boundary_nodes": sum(1 for n in core["nodes"]
                                  if not n["expanded"]),
        }.items()))


# ---------------------------------------------------------------------------
# ordering / cycles / unknowns
# ---------------------------------------------------------------------------

def _none_last(v: Optional[int]) -> Tuple[int, int]:
    return (1, 0) if v is None else (0, v)


def _edge_sort_key(e: Dict[str, Any]):
    return (e["src"], e["reader_txn"], e["read_seq"], e["kind"],
            _none_last(e["key"]), _none_last(e["lo"]), _none_last(e["hi"]),
            e["observed"], e["target"] or "", e["target_version_id"] or "")


def _find_cycles(node_ids: List[str],
                 edges: List[Dict[str, Any]]) -> List[List[str]]:
    """Strongly connected components of size > 1 (plus self-loops)."""
    adj: Dict[str, List[str]] = {n: [] for n in node_ids}
    self_loops: set = set()
    for e in edges:
        t = e["target"]
        if t is None or e["src"] not in adj or t not in adj:
            continue
        if e["src"] == t:
            self_loops.add(t)
        else:
            adj[e["src"]].append(t)
    index: Dict[str, int] = {}
    low: Dict[str, int] = {}
    onstk: set = set()
    stack: List[str] = []
    sccs: List[List[str]] = []
    counter = [0]
    for root in node_ids:
        if root in index:
            continue
        work = [(root, 0)]
        while work:
            v, ci = work[-1]
            if ci == 0:
                index[v] = low[v] = counter[0]
                counter[0] += 1
                stack.append(v)
                onstk.add(v)
            recursed = False
            nbrs = adj[v]
            i = ci
            while i < len(nbrs):
                w = nbrs[i]
                if w not in index:
                    work[-1] = (v, i + 1)
                    work.append((w, 0))
                    recursed = True
                    break
                if w in onstk and index[w] < low[v]:
                    low[v] = index[w]
                i += 1
            if recursed:
                continue
            work.pop()
            if work:
                pv = work[-1][0]
                if low[v] < low[pv]:
                    low[pv] = low[v]
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    onstk.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                if len(comp) > 1:
                    sccs.append(sorted(comp))
    for n in sorted(self_loops):
        sccs.append([n])
    return sorted(sccs)


def _unknown_summary(results, results_gaps, nodes, edges):
    unknowns: List[Dict[str, Any]] = []
    if results_gaps:
        unknowns.append({"ref": "results",
                         "gap_kinds": sorted({g["kind"]
                                              for g in results_gaps})})
    for r in results:
        if r["status"] == "unknown":
            unknowns.append({"ref": f"result:{r['key']}",
                             "gap_kinds": sorted({g["kind"]
                                                  for g in r["gaps"]})})
    for n in nodes:
        if n["status"] == "unknown":
            unknowns.append({"ref": f"node:{n['id']}",
                             "gap_kinds": sorted({g["kind"]
                                                  for g in n["gaps"]})})
    for i, e in enumerate(edges):
        if e["status"] == "unknown":
            unknowns.append({"ref": f"edge:{i}",
                             "gap_kinds": sorted({g["kind"]
                                                  for g in e["gaps"]})})
    unknowns.sort(key=lambda u: u["ref"])
    return unknowns


# ---------------------------------------------------------------------------
# top-level runner
# ---------------------------------------------------------------------------

def _unsigned(result: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in result.items()
            if k not in ("lineage_sha256", "runtime_stats")}


def run(store: Store, input_info: Dict[str, Any], query: Dict[str, Any],
        max_depth: int = 64, max_nodes: int = 8192) -> Dict[str, Any]:
    """Build the signed observation-dependency lineage for *query*.

    Everything is derived from the on-disk store built from the raw
    history; audit reports and regression ledgers are never consulted.
    """
    t0 = time.monotonic()
    builder = LineageBuilder(store, max_depth, max_nodes)
    core = builder.build(query)
    result: Dict[str, Any] = {
        "format": LINEAGE_FORMAT,
        "generator": f"snapshot_audit/{__version__}",
        "lineage_semantics": "observation_dependency",
        "semantics_note": SEMANTICS_NOTE,
        "input": input_info,
        "query": query,
        "limits": {"max_depth": max_depth, "max_nodes": max_nodes},
    }
    result.update(core)
    result["stats"] = builder.stats_json(core)
    result["lineage_sha256"] = sha256_text(canonical_json(_unsigned(result)))
    result["runtime_stats"] = {
        "rss_bytes_exact": _rss_bytes(),
        "wall_time_ms_exact": int((time.monotonic() - t0) * 1000),
    }
    return result


# ---------------------------------------------------------------------------
# independent verification
# ---------------------------------------------------------------------------

class LineageVerification:
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


_DIFF_CAP = 50


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


def verify_lineage(doc: Any, input_path: Optional[str] = None,
                   input_dir: Optional[str] = None,
                   expect_query: Optional[Dict[str, Any]] = None) -> LineageVerification:
    """Re-derive the lineage from the raw history and compare, never
    trusting the conclusions stored in *doc*."""
    from .hashing import hash_input, normalized_path
    from .model import load_history

    v = LineageVerification()
    if not isinstance(doc, dict) or doc.get("format") != LINEAGE_FORMAT:
        v.fail("format", f"not an {LINEAGE_FORMAT} document", fatal=True)
        return v
    v.checked.append("format")

    # result self-signature (integrity of the signed payload)
    stored_sig = doc.get("lineage_sha256")
    actual_sig = sha256_text(canonical_json(_unsigned(doc)))
    v.checked.append("result_signature")
    if stored_sig != actual_sig:
        v.fail("lineage_sha256",
               f"signature mismatch (stored {stored_sig}, recomputed over "
               f"the claim {actual_sig}); the document was modified")

    # query and limits
    query = doc.get("query")
    limits = doc.get("limits") or {}
    try:
        if not isinstance(query, dict):
            raise ContractError("query missing")
        query = build_query(query.get("commit_pos"), query.get("key"),
                            query.get("lo"), query.get("hi"))
        max_depth = limits.get("max_depth")
        max_nodes = limits.get("max_nodes")
        if not isinstance(max_depth, int) or not isinstance(max_nodes, int) \
                or max_depth < 0 or max_nodes < 0:
            raise ContractError(f"bad limits {limits!r}")
    except ContractError as e:
        v.fail("query", str(e), fatal=True)
        return v
    v.checked.append("query")
    if expect_query is not None and expect_query != query:
        v.fail("query", f"caller asked for {expect_query} but the document "
                        f"answers {query}")

    # input binding: re-hash the raw history
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
               f"raw history SHA-256 is {info0['sha256']}, the lineage was "
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

    # rebuild target state and the observation-dependency graph from scratch
    store = Store()
    try:
        header, commits, txns = load_history(
            path if info0["kind"] == "file" else None,
            path if info0["kind"] == "dir" else None)
        store.ingest(header, commits, txns)
    except (ContractError, OSError, ValueError) as e:
        store.close()
        v.fail("input", f"cannot parse raw history: {e}", fatal=True)
        return v
    # under an explicit --input/--input-dir override the content identity
    # (SHA-256) is what binds; the stored path is waived for the comparison
    recompute_input = info0
    if (input_path is not None or input_dir is not None) and \
            info0["sha256"] == rec.get("sha256") and \
            info0["kind"] == rec.get("kind"):
        recompute_input = dict(info0)
        recompute_input["path"] = rec.get("path")
    try:
        recomputed = run(store, recompute_input, query, max_depth, max_nodes)
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

    # full comparison of the signed payloads (nodes, edges, unknown
    # propagation, ordering, truncation status, deterministic stats)
    v.checked.append("nodes_edges_unknowns_truncation_order")
    _diff("lineage", _unsigned(doc), _unsigned(recomputed), v.problems)
    if len(v.problems) >= _DIFF_CAP:
        v.problems.append(f"... comparison stopped after {_DIFF_CAP} "
                          f"differences")

    # runtime stats are unsigned but must be present and well-formed
    v.checked.append("runtime_stats")
    rt = doc.get("runtime_stats")
    if not isinstance(rt, dict) or \
            not isinstance(rt.get("rss_bytes_exact"), int) or \
            not isinstance(rt.get("wall_time_ms_exact"), int) or \
            rt["rss_bytes_exact"] < 0 or rt["wall_time_ms_exact"] < 0:
        v.fail("runtime_stats", "missing or malformed runtime_stats")
    return v
