"""Independent small reference implementation for observation-dependency
lineage (对拍 oracle).

This module deliberately shares no code with ``snapshot_audit``: it works
directly on the raw history document (plain dicts/lists), replays commits
in fixed order to hand-compute the visible state at a ``commit_pos``, and
then computes the recursive observation closure by walking each writer
transaction's operations in recorded seq order.  Tests cross-check the
SQLite-backed engine against this oracle node-by-node and edge-by-edge on
point queries, interval queries, tombstone reconstruction, same-transaction
multi-reads, write-then-read, version reordering, truncation, unknown gaps,
unrelated-branch gaps, cycle/conflict evidence and large sparse reachable
subgraphs.
"""

from __future__ import annotations

import heapq
import json

INIT = "@initial"

LOG_GAP_KINDS = {
    "log_range_incomplete", "log_range_gap", "log_range_undeclared",
    "duplicate_commit_pos", "commit_pos_not_increasing",
    "committed_writes_missing_from_log", "txn_writes_vs_log_mismatch",
    "missing_write_ops", "commit_without_txn_record", "duplicate_version_id",
    "missing_version_id", "bad_predecessor", "key_written_twice_in_commit",
}

OBS_SET_NOTE = (
    "transaction-level observation set: every read the writer transaction "
    "performed before the write operation is an observable input; the "
    "format records no expression-level association, so membership does "
    "not imply the read participated in computing the value"
)


def _canon(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def _dec(c):
    return None if c is None else json.loads(c)


def _gap(kind):
    return {"kind": kind}


# ---------------------------------------------------------------------------
# raw-document analysis
# ---------------------------------------------------------------------------

def _versions(doc):
    out = []
    for i, e in enumerate(doc["initial_state"]):
        vid = e.get("version_id") or f"init@{e['key']}"
        out.append({"key": e["key"], "pos": 0, "vid": vid, "pred": None,
                    "txn": INIT, "tomb": False, "value": _canon(e["value"]),
                    "loc": f"initial_state[{i}]"})
    for i, c in enumerate(doc["commit_log"].get("commits", [])):
        for j, w in enumerate(c["writes"]):
            vid = w.get("version_id")
            if not isinstance(vid, str):
                continue
            pred = w.get("predecessor")
            if pred is not None and not isinstance(pred, str):
                pred = None
            tomb = bool(w.get("tombstone"))
            out.append({"key": w["key"], "pos": c["commit_pos"], "vid": vid,
                        "pred": pred, "txn": c["txn_id"], "tomb": tomb,
                        "value": None if tomb else _canon(w["value"]),
                        "loc": f"commit_log.commits[{i}].writes[{j}]"})
    return out


def _aborted(doc):
    out = {}
    for i, t in enumerate(doc["transactions"]):
        if t.get("status") != "aborted":
            continue
        for j, w in enumerate(t.get("writes") or []):
            vid = w.get("version_id")
            if not isinstance(vid, str):
                continue
            tomb = bool(w.get("tombstone"))
            out.setdefault(vid, {"key": w["key"], "txn": t["txn_id"],
                                 "tomb": tomb,
                                 "value": None if tomb else _canon(w["value"]),
                                 "loc": f"transactions[{i}].writes[{j}]"})
    return out


def _ops_complete(ops):
    if ops is None:
        return False
    complete = True
    seqs = []
    written = set()
    for op in ops:
        seq = op.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool):
            complete = False
            continue
        seqs.append(seq)
        typ = op.get("type")
        if typ not in ("point_read", "range_read", "put", "delete"):
            complete = False
            continue
        if typ == "range_read":
            if op.get("hi") is not None and op.get("lo") is not None \
                    and op["hi"] < op["lo"]:
                complete = False
            if not isinstance(op.get("entries"), list):
                complete = False
        if typ == "point_read" and op.get("found") not in (True, False):
            complete = False
        if typ in ("put", "delete"):
            k = op.get("key")
            if k in written:
                complete = False
            written.add(k)
    if sorted(seqs) != list(range(len(seqs))):
        complete = False
    return complete


def _parse_ops(t, i):
    ops = []
    for k, op in enumerate(t.get("operations") or []):
        loc = f"transactions[{i}].operations[{k}]"
        seq = op.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool):
            continue
        typ = op.get("type")
        if typ not in ("point_read", "range_read", "put", "delete"):
            continue
        rec = {"seq": seq, "type": typ, "key": op.get("key"),
               "lo": op.get("lo"), "hi": op.get("hi"), "value": None,
               "found": False, "vid": None, "entries": None,
               "complete": False, "loc": loc}
        if typ == "point_read":
            f = op.get("found") is True
            rec["found"] = f
            if f:
                rec["value"] = _canon(op.get("value"))
                rec["vid"] = op.get("version_id")
        elif typ == "range_read":
            rec["complete"] = op.get("complete") is True
            ent = op.get("entries")
            if isinstance(ent, list):
                rec["entries"] = [
                    {"key": e["key"], "value": _canon(e["value"]),
                     "version_id": e.get("version_id")} for e in ent]
        elif typ == "put":
            rec["value"] = _canon(op.get("value"))
        ops.append(rec)
    ops.sort(key=lambda o: o["seq"])
    return ops


def _txn_ctxs(doc):
    """Merge commit-log entries and transaction records (store semantics)."""
    ctxs = {}
    for c in doc["commit_log"].get("commits", []):
        e = ctxs.setdefault(c["txn_id"], {"txn_id": c["txn_id"],
                                          "snapshot_pos": None, "ops": [],
                                          "ops_complete": False,
                                          "commit_pos": None, "loc": ""})
        e["status"] = "committed"
        e["commit_pos"] = c["commit_pos"]
    for i, t in enumerate(doc["transactions"]):
        tid = t["txn_id"]
        e = ctxs.setdefault(tid, {"txn_id": tid, "snapshot_pos": None,
                                  "ops": [], "ops_complete": False,
                                  "commit_pos": None, "loc": ""})
        e["status"] = t["status"]
        e["snapshot_pos"] = t.get("snapshot_pos")
        e["loc"] = f"transactions[{i}]"
        ops = t.get("operations")
        oc = True
        if t["status"] == "committed" and ops is None:
            oc = False
        if ops is not None:
            oc = _ops_complete(ops) and oc
            e["ops"] = _parse_ops(t, i)
        e["ops_complete"] = oc
    return ctxs


def _log_gap_kinds(doc, versions, ctxs):
    kinds = set()
    log = doc["commit_log"]
    commits = log.get("commits", [])
    complete = log.get("complete") is True
    first, last = log.get("first_commit_pos"), log.get("last_commit_pos")
    positions = [c["commit_pos"] for c in commits]
    if not complete:
        kinds.add("log_range_incomplete")
    elif first is None and last is None and not commits:
        pass
    elif first is None or last is None:
        kinds.add("log_range_undeclared")
    else:
        have = set(positions)
        if any(p not in have for p in range(first, last + 1)):
            kinds.add("log_range_gap")
    if len(set(positions)) != len(positions):
        kinds.add("duplicate_commit_pos")
    if any(positions[i + 1] <= positions[i] for i in range(len(positions) - 1)):
        kinds.add("commit_pos_not_increasing")
    for c in commits:
        seen = set()
        for w in c["writes"]:
            if w["key"] in seen:
                kinds.add("key_written_twice_in_commit")
            seen.add(w["key"])
            if not isinstance(w.get("version_id"), str):
                kinds.add("missing_version_id")
            pred = w.get("predecessor")
            if pred is not None and not isinstance(pred, str):
                kinds.add("bad_predecessor")
    counts = {}
    for v in versions:
        counts[v["vid"]] = counts.get(v["vid"], 0) + 1
    if any(c > 1 for c in counts.values()):
        kinds.add("duplicate_version_id")
    txn_writes = {}
    for tid, ctx in ctxs.items():
        for op in ctx["ops"]:
            if op["type"] in ("put", "delete"):
                txn_writes.setdefault(tid, {})[op["key"]] = \
                    (op["type"], op["value"])
    log_writes = {}
    for v in versions:
        if v["txn"] == INIT:
            continue
        log_writes.setdefault(v["txn"], {})[v["key"]] = (v["tomb"], v["value"])
    for tid, writes in txn_writes.items():
        if ctxs[tid]["status"] != "committed":
            continue
        lw = log_writes.get(tid)
        if lw is None:
            kinds.add("committed_writes_missing_from_log")
            continue
        for key, (typ, val) in writes.items():
            ent = lw.get(key)
            if ent is None or (typ == "delete") != ent[0] or \
                    (typ == "put" and ent[1] != val):
                kinds.add("txn_writes_vs_log_mismatch")
    for tid, lw in log_writes.items():
        ctx = ctxs.get(tid)
        if ctx is None or ctx["status"] != "committed":
            kinds.add("commit_without_txn_record")
        else:
            tw = txn_writes.get(tid)
            if tw is None or set(tw) != set(lw):
                kinds.add("missing_write_ops")
    return kinds


def _broken_keys(versions):
    by_vid = {}
    for v in versions:
        by_vid.setdefault(v["vid"], v)
    broken = set()
    for v in versions:
        pred = v["pred"]
        if pred is None:
            continue
        p = by_vid.get(pred)
        if p is None or p["key"] != v["key"] or p["pos"] >= v["pos"]:
            broken.add(v["key"])
    return broken


# ---------------------------------------------------------------------------
# oracle lineage
# ---------------------------------------------------------------------------

def _edge_sort_key(e):
    def nl(v):
        return (1, 0) if v is None else (0, v)
    return (e["src"], e["reader_txn"], e["read_seq"], e["kind"],
            nl(e["key"]), nl(e["lo"]), nl(e["hi"]), e["observed"],
            e["target"] or "", e["target_version_id"] or "")


def _find_cycles(node_ids, edges):
    adj = {n: [] for n in node_ids}
    self_loops = set()
    for e in edges:
        t = e["target"]
        if t is None or e["src"] not in adj or t not in adj:
            continue
        if e["src"] == t:
            self_loops.add(t)
        else:
            adj[e["src"]].append(t)
    # simple iterative Tarjan
    index, low, onstk, stack, sccs = {}, {}, set(), [], []
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
            i = ci
            while i < len(adj[v]):
                w = adj[v][i]
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
            if work and low[v] < low[work[-1][0]]:
                low[work[-1][0]] = low[v]
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


class _Oracle:
    def __init__(self, doc, max_depth, max_nodes):
        self.doc = doc
        self.max_depth = max_depth
        self.max_nodes = max_nodes
        self.versions = _versions(doc)
        self.aborted = _aborted(doc)
        self.ctxs = _txn_ctxs(doc)
        kinds = _log_gap_kinds(doc, self.versions, self.ctxs)
        self.log_gaps = sorted(k for k in kinds if k in LOG_GAP_KINDS)
        self.log_ok = not self.log_gaps
        self.broken = _broken_keys(self.versions)
        counts = {}
        for v in self.versions:
            counts[v["vid"]] = counts.get(v["vid"], 0) + 1
        self.dup_vids = {v for v, c in counts.items() if c > 1}
        self.nodes = {}
        self.edges = []
        self.heap = []
        self.expanded = set()
        self.cursor = []
        self.cursor_seen = set()

    # -- state probes --------------------------------------------------------

    def visible(self, key, sp):
        best = None
        for v in self.versions:
            # ties on (key, pos) resolve to the later row in document
            # order, mirroring the engine's index scan (highest rowid)
            if v["key"] == key and v["pos"] <= sp and \
                    (best is None or v["pos"] >= best["pos"]):
                best = v
        return best

    def visible_entries(self, lo, hi, sp):
        best = {}
        for v in self.versions:
            k = v["key"]
            if lo <= k <= hi and v["pos"] <= sp and \
                    (k not in best or v["pos"] >= best[k]["pos"]):
                best[k] = v
        return [best[k] for k in sorted(best)]

    def by_vid(self, vid):
        for v in self.versions:
            if v["vid"] == vid:
                return v
        return None

    def by_txn_key(self, txn, key):
        for v in self.versions:
            if v["txn"] == txn and v["key"] == key:
                return v
        return None

    # -- nodes ---------------------------------------------------------------

    def _defer(self, vid, depth, reason, node=None):
        k = (vid, depth, reason)
        if k in self.cursor_seen:
            return
        self.cursor_seen.add(k)
        self.cursor.append({"version_id": vid, "node": node, "depth": depth,
                            "reason": reason})

    def make_node(self, v, depth):
        nid = "v:" + v["vid"]
        if nid in self.nodes:
            return nid, False
        if len(self.nodes) >= self.max_nodes:
            self._defer(v["vid"], depth, "max_nodes")
            return None, True
        gaps = []
        node = {"id": nid, "key": v["key"], "version_id": v["vid"],
                "commit_pos": v["pos"], "writer_txn": v["txn"],
                "write_op_seq": None,
                "version_semantics": "tombstone" if v["tomb"]
                else "committed_value",
                "loc": v["loc"], "depth": depth, "expanded": False,
                "observation_set": None, "status": "complete", "gaps": gaps}
        if not v["tomb"]:
            node["value"] = _dec(v["value"])
        expandable = False
        if v["txn"] == INIT:
            node["version_semantics"] = "initial"
            node["expanded"] = True
            node["observation_set"] = {
                "scope": "none", "complete": True, "reads_before_write": 0,
                "note": "initial state; no writer transaction"}
        elif v["vid"] in self.dup_vids:
            gaps.append(_gap("duplicate_version_id"))
        else:
            ctx = self.ctxs.get(v["txn"])
            if ctx is None:
                gaps.append(_gap("txn_record_missing"))
            else:
                if ctx["status"] != "committed":
                    gaps.append(_gap("txn_not_committed"))
                wops = [(o["seq"], o["type"], o["value"]) for o in ctx["ops"]
                        if o["type"] in ("put", "delete")
                        and o["key"] == v["key"]]
                if len(wops) == 1:
                    wseq, wtyp, wval = wops[0]
                    node["write_op_seq"] = wseq
                    if (wtyp == "delete") != bool(v["tomb"]) or \
                            (wtyp == "put" and wval != v["value"]):
                        gaps.append(_gap("txn_writes_vs_log_mismatch"))
                elif not wops:
                    gaps.append(_gap("write_op_unattributed"))
                else:
                    gaps.append(_gap("key_written_twice_in_txn"))
                if not ctx["ops_complete"]:
                    gaps.append(_gap("missing_operations"))
                if ctx["snapshot_pos"] is None:
                    gaps.append(_gap("missing_snapshot_pos"))
                if ctx["snapshot_pos"] is not None and \
                        ctx["commit_pos"] is not None and \
                        ctx["snapshot_pos"] >= ctx["commit_pos"]:
                    gaps.append(_gap("snapshot_not_before_commit"))
            expandable = True
        node["status"] = "unknown" if gaps else "complete"
        self.nodes[nid] = node
        if expandable:
            if depth < self.max_depth:
                heapq.heappush(self.heap, (depth, nid))
            else:
                self._defer(v["vid"], depth, "max_depth", node=nid)
        return nid, False

    # -- edges ---------------------------------------------------------------

    def add_edge(self, node, depth, kind, reader_txn, read_seq, sp, key, lo,
                 hi, observed, target_row, target_vid, version_recorded,
                 intra_txn, evidence, gaps):
        deferred = False
        target = None
        if target_row is not None:
            target, deferred = self.make_node(target_row, depth + 1)
        self.edges.append({
            "src": node["id"], "kind": kind, "reader_txn": reader_txn,
            "read_seq": read_seq, "reader_snapshot_pos": sp, "key": key,
            "lo": lo, "hi": hi, "observed": observed, "target": target,
            "target_version_id": target_vid,
            "version_recorded": version_recorded, "intra_txn": intra_txn,
            "deferred": deferred, "evidence": evidence,
            "status": "unknown" if gaps else "complete", "gaps": gaps})

    def _log_gaps(self):
        return [_gap(k) for k in self.log_gaps]

    def recorded_edge(self, node, ctx, depth, common, kind, vid, value, loc,
                      extra):
        gaps = list(extra)
        ev = {"read_loc": loc, "recorded_version_id": vid,
              "value": _dec(value)}
        v = self.by_vid(vid)
        if v is not None:
            ev["version_loc"] = v["loc"]
            if v["key"] != common["key"]:
                gaps.append(_gap("cross_key_version_observed"))
            if vid in self.dup_vids:
                gaps.append(_gap("duplicate_version_id"))
            self.add_edge(node, depth, kind=kind, **common,
                          observed="version", target_row=v, target_vid=vid,
                          version_recorded=True, intra_txn=False,
                          evidence=ev, gaps=gaps)
            return
        ar = self.aborted.get(vid)
        if ar is not None:
            ev["aborted_txn"] = ar["txn"]
            ev["aborted_loc"] = ar["loc"]
            gaps.append(_gap("aborted_version_observed"))
        else:
            gaps.append(_gap("unknown_version_observed"))
        self.add_edge(node, depth, kind=kind, **common, observed="version",
                      target_row=None, target_vid=vid, version_recorded=True,
                      intra_txn=False, evidence=ev, gaps=gaps)

    def inferred_edge(self, node, ctx, depth, common, kind, value, loc,
                      extra):
        key, sp = common["key"], common["sp"]
        gaps = list(extra)
        if sp is None:
            gaps.append(_gap("missing_snapshot_pos"))
        if not self.log_ok:
            gaps.extend(self._log_gaps())
        if key in self.broken:
            gaps.append(_gap("version_chain_broken"))
        ev = {"read_loc": loc, "recorded_version_id": None,
              "value": _dec(value)}
        vis = self.visible(key, sp) if sp is not None else None
        target_row, tvid = None, None
        if vis is not None:
            tvid = vis["vid"]
            if vis["tomb"]:
                gaps.append(_gap("observed_value_unattributable"))
            elif value != vis["value"]:
                gaps.append(_gap("observed_value_unattributable"))
            else:
                ev["version_loc"] = vis["loc"]
                target_row = vis
                if vis["vid"] in self.dup_vids:
                    gaps.append(_gap("duplicate_version_id"))
        elif sp is not None:
            gaps.append(_gap("observed_value_unattributable"))
        self.add_edge(node, depth, kind=kind, **common, observed="version",
                      target_row=target_row, target_vid=tvid,
                      version_recorded=False, intra_txn=False,
                      evidence=ev, gaps=gaps)

    def absence_edge(self, node, ctx, depth, common, kind, loc, extra):
        key, sp = common["key"], common["sp"]
        gaps = list(extra)
        if sp is None:
            gaps.append(_gap("missing_snapshot_pos"))
        if not self.log_ok:
            gaps.extend(self._log_gaps())
        if key in self.broken:
            gaps.append(_gap("version_chain_broken"))
        ev = {"read_loc": loc}
        vis = self.visible(key, sp) if sp is not None else None
        observed = "absent_no_version"
        target_row, tvid = None, None
        if vis is not None:
            if vis["tomb"]:
                observed = "absent_tombstone"
                target_row, tvid = vis, vis["vid"]
                ev["version_loc"] = vis["loc"]
                if vis["vid"] in self.dup_vids:
                    gaps.append(_gap("duplicate_version_id"))
            else:
                gaps.append(_gap("absence_contradicted"))
        self.add_edge(node, depth, kind=kind, **common, observed=observed,
                      target_row=target_row, target_vid=tvid,
                      version_recorded=False, intra_txn=False,
                      evidence=ev, gaps=gaps)

    def own_write_edge(self, node, ctx, depth, common, own_rec, found, value,
                       loc, extra=()):
        key = common["key"]
        wseq, wtyp, wval = own_rec
        gaps = list(extra)
        exp_found = wtyp == "put"
        if bool(found) != exp_found or (exp_found and value != wval):
            gaps.append(_gap("own_write_read_mismatch"))
        n_writes = sum(1 for o in ctx["ops"]
                       if o["type"] in ("put", "delete") and o["key"] == key)
        if n_writes > 1:
            gaps.append(_gap("key_written_twice_in_txn"))
        v = self.by_txn_key(ctx["txn_id"], key)
        tvid = None
        if v is None:
            gaps.append(_gap("own_write_version_missing"))
        else:
            tvid = v["vid"]
            if tvid in self.dup_vids:
                gaps.append(_gap("duplicate_version_id"))
        ev = {"read_loc": loc, "write_seq": wseq,
              "value": _dec(wval) if wtyp == "put" else None,
              "version_loc": v["loc"] if v else None}
        self.add_edge(node, depth, kind="own_write", **common,
                      observed="own_value" if exp_found else "own_absent",
                      target_row=v, target_vid=tvid, version_recorded=False,
                      intra_txn=True, evidence=ev, gaps=gaps)

    # -- expansion -------------------------------------------------------------

    def expand(self, node, depth):
        ctx = self.ctxs.get(node["writer_txn"])
        reads_seen = 0
        if ctx is not None and ctx["status"] == "committed":
            wseq = node["write_op_seq"]
            own = {}
            for op in ctx["ops"]:
                if op["type"] in ("put", "delete"):
                    own[op["key"]] = (op["seq"], op["type"], op["value"])
                    continue
                if wseq is not None and op["seq"] >= wseq:
                    continue
                reads_seen += 1
                if op["type"] == "point_read":
                    self.point_edges(node, ctx, own, depth, op)
                elif op["type"] == "range_read":
                    self.range_edges(node, ctx, own, depth, op)
            obs_complete = bool(ctx["ops_complete"]) and wseq is not None
        else:
            obs_complete = False
        node["observation_set"] = {
            "scope": "transaction_level", "complete": obs_complete,
            "reads_before_write": reads_seen, "note": OBS_SET_NOTE}

    def point_edges(self, node, ctx, own, depth, op):
        sp = ctx["snapshot_pos"]
        common = {"reader_txn": ctx["txn_id"], "read_seq": op["seq"],
                  "sp": sp, "key": op["key"], "lo": None, "hi": None}
        key = op["key"]
        if key in own:
            self.own_write_edge(node, ctx, depth, common, own[key],
                                op["found"], op["value"], op["loc"])
            return
        if op["found"]:
            if op["vid"] is not None:
                self.recorded_edge(node, ctx, depth, common, "point_read",
                                   op["vid"], op["value"], op["loc"], ())
            else:
                self.inferred_edge(node, ctx, depth, common, "point_read",
                                   op["value"], op["loc"], ())
        else:
            self.absence_edge(node, ctx, depth, common, "point_read",
                              op["loc"], ())

    def range_edges(self, node, ctx, own, depth, op):
        sp = ctx["snapshot_pos"]
        lo, hi = op["lo"], op["hi"]
        loc = op["loc"]
        scan_complete = bool(op["complete"])
        entries = op["entries"]
        returned = []
        dup_keys, seen = set(), set()
        if entries is not None:
            for e in entries:
                k = e["key"]
                if k in seen:
                    dup_keys.add(k)
                seen.add(k)
                returned.append((k, e["value"], e.get("version_id")))
        returned_set = {k for k, _v, _i in returned}
        returned_keys = sorted(returned_set)
        own_in_range = {k: v for k, v in own.items() if lo <= k <= hi}
        for (k, val, evid) in returned:
            common = {"reader_txn": ctx["txn_id"], "read_seq": op["seq"],
                      "sp": sp, "key": k, "lo": lo, "hi": hi}
            extra = []
            if k < lo or k > hi:
                extra.append(_gap("entry_outside_interval"))
            if k in dup_keys:
                extra.append(_gap("duplicate_range_entry_key"))
            if k in own_in_range:
                self.own_write_edge(node, ctx, depth, common, own_in_range[k],
                                    1, val, loc, extra)
            elif evid is not None:
                self.recorded_edge(node, ctx, depth, common, "range_read_item",
                                   evid, val, loc, extra)
            else:
                self.inferred_edge(node, ctx, depth, common, "range_read_item",
                                   val, loc, extra)
        if scan_complete and entries is not None:
            for k in sorted(own_in_range):
                if k in returned_set:
                    continue
                common = {"reader_txn": ctx["txn_id"], "read_seq": op["seq"],
                          "sp": sp, "key": k, "lo": lo, "hi": hi}
                self.own_write_edge(node, ctx, depth, common, own_in_range[k],
                                    0, None, loc)
        pgaps = []
        if hi < lo:
            pgaps.append(_gap("bad_range"))
        if not scan_complete:
            pgaps.append(_gap("incomplete_scan"))
        if entries is None:
            pgaps.append(_gap("range_entries_missing"))
        if sp is None:
            pgaps.append(_gap("missing_snapshot_pos"))
        if not self.log_ok:
            pgaps.extend(self._log_gaps())
        if any(lo <= k <= hi for k in self.broken):
            pgaps.append(_gap("version_chain_broken"))
        if dup_keys:
            pgaps.append(_gap("duplicate_range_entry_key"))
        vis_rows = None
        if sp is not None and scan_complete and entries is not None:
            vis_rows = self.visible_entries(lo, hi, sp)
        if not pgaps and vis_rows is not None:
            contra = [r["key"] for r in vis_rows
                      if not r["tomb"] and r["key"] not in returned_set
                      and r["key"] not in own_in_range]
            if contra:
                pgaps.append(_gap("predicate_contradicted"))
        pev = {"read_loc": loc,
               "returned_keys": returned_keys if entries is not None else None,
               "own_keys_in_range": sorted(own_in_range),
               "scan_complete": scan_complete}
        self.add_edge(node, depth, kind="range_predicate",
                      reader_txn=ctx["txn_id"], read_seq=op["seq"], sp=sp,
                      key=None, lo=lo, hi=hi, observed="predicate",
                      target_row=None, target_vid=None, version_recorded=False,
                      intra_txn=False, evidence=pev, gaps=pgaps)
        if vis_rows is not None and scan_complete:
            for r in vis_rows:
                k3 = r["key"]
                if not r["tomb"] or k3 in returned_set or k3 in own_in_range:
                    continue
                tgaps = []
                if not self.log_ok:
                    tgaps.extend(self._log_gaps())
                if k3 in self.broken:
                    tgaps.append(_gap("version_chain_broken"))
                if r["vid"] in self.dup_vids:
                    tgaps.append(_gap("duplicate_version_id"))
                self.add_edge(node, depth, kind="range_predicate",
                              reader_txn=ctx["txn_id"], read_seq=op["seq"],
                              sp=sp, key=k3, lo=lo, hi=hi,
                              observed="absent_tombstone", target_row=r,
                              target_vid=r["vid"], version_recorded=False,
                              intra_txn=False,
                              evidence={"read_loc": loc,
                                        "version_loc": r["loc"]},
                              gaps=tgaps)

    # -- driver ----------------------------------------------------------------

    def run_expansion(self):
        while self.heap:
            depth, nid = heapq.heappop(self.heap)
            if nid in self.expanded:
                continue
            node = self.nodes[nid]
            self.expand(node, depth)
            self.expanded.add(nid)
            node["expanded"] = True

    def build(self, query):
        pos = query["commit_pos"]
        results = []
        results_gaps = []
        if "key" in query:
            key = query["key"]
            egaps = []
            if not self.log_ok:
                egaps.extend(self._log_gaps())
            if key in self.broken:
                egaps.append(_gap("version_chain_broken"))
            vis = self.visible(key, pos)
            entry = {"key": key}
            if vis is None:
                entry.update({"found": False, "reason": "no_version",
                              "version_id": None, "node": None})
            elif vis["tomb"]:
                nid, _ = self.make_node(vis, 0)
                if vis["vid"] in self.dup_vids:
                    egaps.append(_gap("duplicate_version_id"))
                entry.update({"found": False, "reason": "tombstone",
                              "version_id": vis["vid"], "node": nid})
            else:
                nid, _ = self.make_node(vis, 0)
                if vis["vid"] in self.dup_vids:
                    egaps.append(_gap("duplicate_version_id"))
                entry.update({"found": True, "value": _dec(vis["value"]),
                              "version_id": vis["vid"], "node": nid})
            entry["status"] = "unknown" if egaps else "complete"
            entry["gaps"] = egaps
            results.append(entry)
            results_complete = not egaps
        else:
            lo, hi = query["lo"], query["hi"]
            if not self.log_ok:
                results_gaps.extend(self._log_gaps())
            if any(lo <= k <= hi for k in self.broken):
                results_gaps.append(_gap("version_chain_broken"))
            for v in self.visible_entries(lo, hi, pos):
                if v["tomb"]:
                    continue
                egaps = list(results_gaps)
                if v["vid"] in self.dup_vids:
                    egaps.append(_gap("duplicate_version_id"))
                nid, _ = self.make_node(v, 0)
                results.append({"key": v["key"], "found": True,
                                "value": _dec(v["value"]),
                                "version_id": v["vid"], "node": nid,
                                "status": "unknown" if egaps else "complete",
                                "gaps": egaps})
            results_complete = not results_gaps and \
                all(r["status"] == "complete" for r in results)

        self.run_expansion()

        nodes = [self.nodes[nid] for nid in sorted(self.nodes)]
        edges = sorted(self.edges, key=_edge_sort_key)
        cycles = _find_cycles([n["id"] for n in nodes], edges)
        unknowns = []
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
        return {"status": status, "results": results,
                "results_complete": results_complete,
                "results_gaps": results_gaps, "nodes": nodes, "edges": edges,
                "cycles": cycles, "unknowns": unknowns,
                "truncation": truncation}


def oracle_lineage(doc, query, max_depth=64, max_nodes=8192):
    """Hand-computed observation-dependency lineage core (reference)."""
    return _Oracle(doc, max_depth, max_nodes).build(query)
