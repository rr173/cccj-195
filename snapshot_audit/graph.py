"""Dependency graph over declared version order, read observations, real time.

Edges (all between committed transactions; ``@initial`` is a virtual source):

* ``ww``       - same-key committed versions in commit_pos order
                 (consecutive pairs; declared version order),
* ``wr``       - read-from: the reader observed the version of a key that is
                 visible at its snapshot (including tombstone/absence, which
                 orders the deleting writer before the reader),
* ``rw``       - anti-dependency: the reader observed the snapshot state of a
                 key and a later committed version (the successor) exists;
                 for range reads this also covers keys that did not exist at
                 the snapshot and were inserted afterwards (phantoms) - such
                 edges are derived from actual key changes inside the
                 interval, never from expanding the key domain,
* ``realtime`` - A ended before B started.

A transaction's own writes never create self-edges.  The history is
serial-explainable iff this graph is acyclic; a topological order then
replays to exactly the recorded observations (see replay.py).
"""

from __future__ import annotations

import bisect
import json
from typing import Any, Dict, List, Optional, Tuple

from .model import Gap, INITIAL_TXN, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ
from .store import Store

# approximate in-memory footprint per graph node / edge for budget checks
_NODE_BYTES = 160
_EDGE_BYTES = 96
_CYCLE_SEARCH_BUDGET = 4_000_000  # BFS expansions while minimising a cycle


def build_edges(store: Store) -> None:
    """Generate all dependency edges into the edges table (streaming)."""
    _ww_edges(store)
    _read_edges(store)
    _realtime_edges(store)
    store.db.commit()


def _emit(store: Store, rows: List[tuple]) -> None:
    store.db.executemany(
        "INSERT INTO edges (src,dst,kind,key,lo,hi,version_id,succ_version_id,"
        "read_txn,read_seq,evidence) VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    store.stats.add("edges_emitted", len(rows))


def _ww_edges(store: Store) -> None:
    cur = store.db.execute(
        "SELECT key, commit_pos, version_id, txn_id, loc FROM versions "
        "ORDER BY key, commit_pos")
    prev: Optional[tuple] = None
    batch: List[tuple] = []
    for (key, pos, vid, tid, loc) in cur:
        store.stats.add("ww_candidates")
        if prev is not None and prev[0] == key:
            _pkey, _ppos, pvid, ptid, ploc = prev
            if ptid != tid:
                batch.append((ptid, tid, "ww", key, None, None, pvid, vid,
                              None, None, json.dumps({
                                  "key": key, "v1": pvid, "v2": vid,
                                  "loc1": ploc, "loc2": loc})))
            else:
                store.stats.add("self_edges_skipped")
        prev = (key, pos, vid, tid, loc)
        if len(batch) >= 8192:
            _emit(store, batch)
            batch.clear()
    _emit(store, batch)


def _read_edges(store: Store) -> None:
    for (tid, sp, _st, _et, _cp) in store.committed_txns():
        if sp is None:
            continue
        own: set = set()
        batch: List[tuple] = []
        for (seq, typ, key, lo, hi, _val, _f, _vid, _e, _cf, loc) in store.txn_ops(tid):
            if typ in (OP_PUT, OP_DELETE):
                own.add(key)
                continue
            if typ == OP_POINT_READ:
                if key in own:
                    continue
                store.stats.add("edge_candidates", 2)
                _key_edges(store, tid, sp, seq, key, None, None, loc, batch)
            elif typ == OP_RANGE_READ:
                for k in store.keys_with_versions_in_range(lo, hi):
                    if k in own:
                        continue
                    store.stats.add("edge_candidates", 2)
                    _key_edges(store, tid, sp, seq, k, lo, hi, loc, batch)
            if len(batch) >= 8192:
                _emit(store, batch)
                batch.clear()
        _emit(store, batch)


def _key_edges(store: Store, tid: str, sp: int, seq: int, key: int,
               lo: Optional[int], hi: Optional[int], loc: str,
               batch: List[tuple]) -> None:
    vis = store.visible_version(key, sp)
    succ = store.successor_version(key, sp)
    if vis is not None:
        _k, _pos, vid, writer, _t, _v, vloc = vis
        if writer != tid:
            batch.append((writer, tid, "wr", key, lo, hi, vid, None, tid, seq,
                          json.dumps({"key": key, "version": vid,
                                      "read": {"txn": tid, "seq": seq, "loc": loc},
                                      "version_loc": vloc,
                                      **({"lo": lo, "hi": hi} if lo is not None else {})})))
        else:
            store.stats.add("self_edges_skipped")
    if succ is not None:
        _k, _pos, svid, swriter, _t, _v, sloc = succ
        if swriter != tid:
            batch.append((tid, swriter, "rw", key, lo, hi,
                          vis[2] if vis else None, svid, tid, seq,
                          json.dumps({"key": key,
                                      "read_version": vis[2] if vis else None,
                                      "succ_version": svid,
                                      "read": {"txn": tid, "seq": seq, "loc": loc},
                                      "succ_loc": sloc,
                                      **({"lo": lo, "hi": hi} if lo is not None else {})})))
        else:
            store.stats.add("self_edges_skipped")


def _realtime_edges(store: Store) -> None:
    """A.end_time < B.start_time  ==>  A -> B.

    The relation R(A,B) = (A.end < B.start) is already transitive
    (A.end < B.start <= B.end < C.start implies A.end < C.start), so its
    transitive reduction keeps exactly the edges A->C for which no B has
    A.end < B.start and B.end < C.start.  For each C (swept in start
    order) let M(C) = max{ B.start : B.end < C.start }; then A->C is
    irredundant iff M(C) <= A.end < C.start.  Reachability - and hence
    the acyclicity verdict and every emitted witness edge - is preserved
    exactly, while sequential workloads drop from O(n^2) to O(n) edges.

    Sweep with a sorted end-time list: O(n log n) candidate comparisons
    plus one emitted edge per irredundant pair - never an n x n scan.
    """
    txns = [(t, s, e) for (t, s, e) in store.db.execute(
        "SELECT txn_id, start_time, end_time FROM txns "
        "WHERE status='committed' AND start_time IS NOT NULL "
        "AND end_time IS NOT NULL")]
    store.stats.add("realtime_candidates", len(txns))
    by_start = sorted(txns, key=lambda r: (r[1], r[0]))
    by_end = sorted(txns, key=lambda r: (r[2], r[0]))
    ends: List[float] = []          # sorted end times inserted so far
    end_txns: List[List[str]] = []  # parallel: txns ending at each end time
    max_start_inserted: Optional[float] = None
    scan = 0
    batch: List[tuple] = []
    for (tid, start, _end) in by_start:
        # insert every txn whose end falls before this start
        while scan < len(by_end) and by_end[scan][2] < start:
            _t, s2, e2 = by_end[scan]
            pos = bisect.bisect_left(ends, e2)
            ends.insert(pos, e2)
            end_txns.insert(pos, [_t])
            if max_start_inserted is None or s2 > max_start_inserted:
                max_start_inserted = s2
            scan += 1
        # irredundant predecessors: end in [max_start_inserted, start)
        if max_start_inserted is not None:
            lo = bisect.bisect_left(ends, max_start_inserted)
            hi = bisect.bisect_left(ends, start)
            for i in range(lo, hi):
                for src in end_txns[i]:
                    batch.append((src, tid, "realtime", None, None, None,
                                  None, None, None, None, json.dumps({
                                      "a_end": ends[i], "b_start": start})))
                    store.stats.add("realtime_edges")
            store.stats.add("realtime_edges_reduced_away", lo)
        if len(batch) >= 8192:
            _emit(store, batch)
            batch.clear()
    _emit(store, batch)


# ---------------------------------------------------------------------------
# Cycle detection / topological order (memory-budgeted, CSR representation)
# ---------------------------------------------------------------------------

# in-memory footprint model for the budget check (bytes)
_NODE_BYTES = 200   # name, index dict entry, per-node arrays
_EDGE_BYTES = 16    # two int32 columns + CSR target
_CYCLE_SEARCH_BUDGET = 4_000_000  # BFS expansions while minimising a cycle


class GraphResult:
    def __init__(self) -> None:
        self.built = False
        self.acyclic: Optional[bool] = None
        self.cycle: Optional[List[Dict[str, Any]]] = None  # edge witness list
        self.cycle_minimal: Optional[bool] = None
        self.topo_order: Optional[List[str]] = None
        self.nodes = 0
        self.distinct_edges = 0
        self.gap: Optional[Gap] = None


def analyze_graph(store: Store, memory_budget_bytes: int) -> GraphResult:
    from array import array

    res = GraphResult()
    db = store.db
    res.nodes = db.execute(
        "SELECT COUNT(*) FROM txns WHERE status='committed'").fetchone()[0]
    res.distinct_edges = db.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT src, dst FROM edges)").fetchone()[0]
    est = res.nodes * _NODE_BYTES + res.distinct_edges * _EDGE_BYTES
    if est > memory_budget_bytes:
        res.gap = Gap("graph_exceeds_memory_budget",
                      f"estimated {est} bytes for {res.nodes} nodes / "
                      f"{res.distinct_edges} distinct edges exceeds budget "
                      f"{memory_budget_bytes}", "edges")
        return res

    # node set: committed txns plus every edge endpoint
    names = set()
    for (t,) in db.execute("SELECT txn_id FROM txns WHERE status='committed'"):
        names.add(t)
    for (s,) in db.execute("SELECT DISTINCT src FROM edges"):
        names.add(s)
    for (d,) in db.execute("SELECT DISTINCT dst FROM edges"):
        names.add(d)
    names = sorted(names)
    index = {nm: i for i, nm in enumerate(names)}
    n = len(names)

    # stream distinct edges (sorted by src, which matches index order) into
    # int32 columns, then build CSR offsets by counting
    src_col = array("i")
    tgt = array("i")
    for (s, d) in db.execute(
            "SELECT DISTINCT src, dst FROM edges ORDER BY src, dst"):
        src_col.append(index[s])
        tgt.append(index[d])
    off = array("i", [0]) * (n + 1)
    for s in src_col:
        off[s + 1] += 1
    for i in range(n):
        off[i + 1] += off[i]
    indeg = array("i", [0]) * n
    for d in tgt:
        indeg[d] += 1
    del src_col

    sccs = _tarjan_csr(n, off, tgt)
    cyclic = [c for c in sccs if len(c) > 1]
    res.built = True
    if not cyclic:
        res.acyclic = True
        res.topo_order = _kahn_csr(names, off, tgt, indeg)
        return res

    res.acyclic = False
    # pick the smallest cyclic SCC for a compact witness
    scc = min(cyclic, key=len)
    cyc_nodes, minimal = _shortest_cycle_csr(n, off, tgt, scc,
                                             _CYCLE_SEARCH_BUDGET)
    res.cycle_minimal = minimal
    cyc_names = [names[i] for i in cyc_nodes]
    # attach one concrete evidence edge per step
    witness: List[Dict[str, Any]] = []
    for i, src in enumerate(cyc_names):
        dst = cyc_names[(i + 1) % len(cyc_names)]
        row = db.execute(
            "SELECT src,dst,kind,key,lo,hi,version_id,succ_version_id,"
            "read_txn,read_seq,evidence FROM edges "
            "WHERE src=? AND dst=? ORDER BY kind, key LIMIT 1", (src, dst)).fetchone()
        witness.append({
            "src": row[0], "dst": row[1], "kind": row[2], "key": row[3],
            "lo": row[4], "hi": row[5], "version_id": row[6],
            "succ_version_id": row[7], "read_txn": row[8], "read_seq": row[9],
            "evidence": json.loads(row[10]) if row[10] else {},
        })
    res.cycle = witness
    return res


def _tarjan_csr(n: int, off, tgt) -> List[List[int]]:
    from array import array
    idx = array("i", [-1]) * n
    low = array("i", [0]) * n
    onstk = array("b", [0]) * n
    stack = array("i")
    sccs: List[List[int]] = []
    counter = 0
    for root in range(n):
        if idx[root] != -1:
            continue
        work = [(root, off[root])]
        while work:
            v, ci = work[-1]
            if ci == off[v]:  # first visit
                idx[v] = low[v] = counter
                counter += 1
                stack.append(v)
                onstk[v] = 1
            recursed = False
            end = off[v + 1]
            i = ci
            while i < end:
                w = tgt[i]
                if idx[w] == -1:
                    work[-1] = (v, i + 1)
                    work.append((w, off[w]))
                    recursed = True
                    break
                if onstk[w] and idx[w] < low[v]:
                    low[v] = idx[w]
                i += 1
            if recursed:
                continue
            work.pop()
            if work:
                pv = work[-1][0]
                if low[v] < low[pv]:
                    low[pv] = low[v]
            if low[v] == idx[v]:
                comp = []
                while True:
                    w = stack.pop()
                    onstk[w] = 0
                    comp.append(w)
                    if w == v:
                        break
                sccs.append(comp)
    return sccs


def _kahn_csr(names: List[str], off, tgt, indeg) -> List[str]:
    import heapq
    from array import array
    deg = array("i", indeg)
    heap = [i for i in range(len(names)) if deg[i] == 0]
    heapq.heapify(heap)  # index order == sorted-name order: deterministic
    order: List[str] = []
    while heap:
        v = heapq.heappop(heap)
        order.append(names[v])
        for i in range(off[v], off[v + 1]):
            w = tgt[i]
            deg[w] -= 1
            if deg[w] == 0:
                heapq.heappush(heap, w)
    return order


def _shortest_cycle_csr(n: int, off, tgt, scc: List[int],
                        budget: int) -> Tuple[List[int], bool]:
    """Shortest directed cycle inside one SCC (BFS from each member)."""
    inset = set(scc)
    best: Optional[List[int]] = None
    expansions = 0
    exhausted = False
    for start in sorted(scc):
        if best is not None and len(best) == 2:
            break  # cannot beat a 2-cycle
        parent: Dict[int, int] = {start: -1}
        depth = {start: 0}
        frontier = [start]
        limit = len(best) - 1 if best else len(scc)
        found: Optional[int] = None
        while frontier and found is None and not exhausted:
            nxt = []
            for v in frontier:
                if depth[v] >= limit:
                    continue
                for i in range(off[v], off[v + 1]):
                    w = tgt[i]
                    expansions += 1
                    if expansions > budget:
                        exhausted = True
                        break
                    if w not in inset:
                        continue
                    if w == start and depth[v] + 1 >= 2:
                        found = v
                        break
                    if w not in parent:
                        parent[w] = v
                        depth[w] = depth[v] + 1
                        nxt.append(w)
                if found is not None or exhausted:
                    break
            frontier = nxt
        if exhausted:
            break
        if found is not None:
            path = [found]
            while path[-1] != start:
                path.append(parent[path[-1]])
            path.reverse()
            if best is None or len(path) < len(best):
                best = path
    if best is None:
        best = _dfs_cycle_csr(off, tgt, scc)
        exhausted = True
    return best, not exhausted


def _dfs_cycle_csr(off, tgt, scc: List[int]) -> List[int]:
    inset = set(scc)
    start = scc[0]
    color: Dict[int, int] = {}
    parent: Dict[int, int] = {}
    stack = [start]
    while stack:
        v = stack[-1]
        if color.get(v, 0) == 0:
            color[v] = 1
        advanced = False
        for i in range(off[v], off[v + 1]):
            w = tgt[i]
            if w not in inset:
                continue
            if color.get(w, 0) == 0:
                parent[w] = v
                stack.append(w)
                advanced = True
                break
            if color.get(w) == 1:
                path = [v]
                while path[-1] != w:
                    path.append(parent[path[-1]])
                path.reverse()
                return path
        if not advanced:
            color[v] = 2
            stack.pop()
    return scc[:1]
