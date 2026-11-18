"""On-disk storage for versions, transactions, operations and edges.

Everything that can grow with the history lives in SQLite tables with an
explicit index plan, so point/range probes are indexed lookups instead of
domain expansions or Cartesian products.  The auditor streams ingestion in
batches and keeps only per-transaction state in memory.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .model import (
    FORMAT_ID, INITIAL_TXN, OP_DELETE, OP_POINT_READ, OP_PUT, OP_RANGE_READ,
    STATUS_ABORTED, STATUS_COMMITTED, Gap, canon_scalar, check_int64,
)

SCHEMA = """
CREATE TABLE versions(
  key         INTEGER NOT NULL,
  commit_pos  INTEGER NOT NULL,
  version_id  TEXT NOT NULL,
  predecessor TEXT,
  txn_id      TEXT NOT NULL,
  tombstone   INTEGER NOT NULL,
  value       TEXT,
  loc         TEXT
);
CREATE UNIQUE INDEX idx_versions_vid ON versions(version_id);
CREATE INDEX idx_versions_key_pos ON versions(key, commit_pos);
CREATE INDEX idx_versions_pos ON versions(commit_pos);

CREATE TABLE aborted_versions(
  version_id TEXT PRIMARY KEY,
  key INTEGER NOT NULL,
  txn_id TEXT NOT NULL,
  tombstone INTEGER NOT NULL,
  value TEXT,
  loc TEXT
);

CREATE TABLE txns(
  txn_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  snapshot_pos INTEGER,
  start_time REAL,
  end_time REAL,
  commit_pos INTEGER,
  ops_complete INTEGER NOT NULL DEFAULT 0,
  loc TEXT
);

CREATE TABLE ops(
  txn_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  key INTEGER,
  lo INTEGER,
  hi INTEGER,
  value TEXT,          -- canonical JSON scalar (put / observed read value)
  found INTEGER,       -- point_read observed found flag
  version_id TEXT,     -- observed version_id (reads)
  entries TEXT,        -- range_read observed entries (canonical JSON list)
  complete INTEGER,    -- range_read end-of-scan flag
  loc TEXT
);
CREATE INDEX idx_ops_txn ON ops(txn_id, seq);
CREATE INDEX idx_ops_key ON ops(key) WHERE type = 'point_read';

CREATE TABLE edges(
  src TEXT NOT NULL,
  dst TEXT NOT NULL,
  kind TEXT NOT NULL,        -- ww | wr | rw | realtime
  key INTEGER,
  lo INTEGER,
  hi INTEGER,
  version_id TEXT,
  succ_version_id TEXT,
  read_txn TEXT,
  read_seq INTEGER,
  evidence TEXT              -- JSON: locations and versions involved
);
CREATE INDEX idx_edges_src ON edges(src, dst);
CREATE INDEX idx_edges_dst ON edges(dst, src);
"""

INDEX_PLAN = [
    "versions(version_id) UNIQUE          -- resolve version ids",
    "versions(key, commit_pos)            -- visible-version and successor probes, per-key chains",
    "versions(commit_pos)                 -- log-range completeness checks",
    "aborted_versions(version_id)         -- aborted-version attribution",
    "txns(txn_id)                         -- transaction records",
    "ops(txn_id, seq)                     -- per-transaction operation replay",
    "ops(key) WHERE type='point_read'     -- point-read lookup by key",
    "edges(src,dst), edges(dst,src)       -- adjacency streaming for SCC/topo",
]

_BATCH = 8192


class Stats:
    """Candidate-comparison and work counters (reported in the audit)."""

    def __init__(self) -> None:
        self.counters: Dict[str, int] = {}

    def add(self, name: str, n: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + n

    def to_json(self) -> Dict[str, int]:
        return dict(sorted(self.counters.items()))


class Store:
    def __init__(self, work_dir: Optional[str] = None, keep: bool = False) -> None:
        self._tmp: Optional[tempfile.TemporaryDirectory] = None
        if work_dir is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="snapaudit-")
            work_dir = self._tmp.name
        os.makedirs(work_dir, exist_ok=True)
        self.db_path = os.path.join(work_dir, "audit.db")
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.executescript(SCHEMA)
        self.keep = keep
        self.stats = Stats()
        self.gaps: List[Gap] = []

    def close(self) -> None:
        self.db.close()
        if self._tmp is not None and not self.keep:
            self._tmp.cleanup()

    # -- ingestion ---------------------------------------------------------

    def ingest(self, header, commits, txns) -> None:
        self._ingest_initial(header)
        self._ingest_commits(header, commits)
        self._ingest_txns(txns)
        self._cross_validate(header)
        self.db.commit()

    def _ingest_initial(self, header) -> None:
        rows = []
        seen = set()
        for i, e in enumerate(header.initial_state):
            loc = f"initial_state[{i}]"
            key = check_int64(e.get("key"), loc)
            if key in seen:
                self.gaps.append(Gap("duplicate_initial_key", f"key {key} twice in initial state", loc))
                continue
            seen.add(key)
            vid = e.get("version_id") or f"init@{key}"
            val = canon_scalar(e.get("value"), loc)
            rows.append((key, 0, vid, None, INITIAL_TXN, 0, val, loc))
        self.db.executemany(
            "INSERT OR IGNORE INTO versions VALUES (?,?,?,?,?,?,?,?)", rows)
        self.stats.add("initial_keys", len(rows))

    def _ingest_commits(self, header, commits) -> None:
        positions: List[int] = []
        vrows: List[tuple] = []
        commit_txn: List[Tuple[str, int, str]] = []
        seen_pos = set()
        monotonic = True
        last_pos = -1
        n_commits = 0
        for crec in commits:
            n_commits += 1
            pos = crec.commit_pos
            if pos in seen_pos:
                self.gaps.append(Gap("duplicate_commit_pos",
                                     f"commit_pos {pos} appears twice", crec.loc))
            seen_pos.add(pos)
            if pos <= last_pos:
                monotonic = False
            last_pos = pos
            positions.append(pos)
            commit_txn.append((crec.txn_id, pos, crec.loc))
            seen_keys = set()
            for j, w in enumerate(crec.writes):
                wloc = f"{crec.loc}.writes[{j}]"
                key = check_int64(w.get("key"), wloc)
                if key in seen_keys:
                    self.gaps.append(Gap("key_written_twice_in_commit",
                                         f"key {key} written twice in one commit", wloc))
                seen_keys.add(key)
                vid = w.get("version_id")
                if not isinstance(vid, str):
                    self.gaps.append(Gap("missing_version_id", f"key {key}", wloc))
                    continue
                pred = w.get("predecessor")
                if pred is not None and not isinstance(pred, str):
                    self.gaps.append(Gap("bad_predecessor", f"key {key}", wloc))
                    pred = None
                tomb = 1 if w.get("tombstone") else 0
                val = None if tomb else canon_scalar(w.get("value"), wloc)
                vrows.append((key, pos, vid, pred, crec.txn_id, tomb, val, wloc))
            if len(vrows) >= _BATCH:
                self._flush_versions(vrows)
                vrows.clear()
        self._flush_versions(vrows)
        if not monotonic:
            self.gaps.append(Gap("commit_pos_not_increasing",
                                 "commit_pos values are not strictly increasing in log order",
                                 "commit_log"))
        # declared-range completeness
        if header.log_complete:
            if header.log_first is None and header.log_last is None \
                    and n_commits == 0:
                pass  # empty log, declared complete: nothing to check
            elif header.log_first is None or header.log_last is None:
                self.gaps.append(Gap("log_range_undeclared",
                                     "log declared complete but range bounds missing",
                                     "commit_log"))
            else:
                missing = [p for p in range(header.log_first, header.log_last + 1)
                           if p not in seen_pos]
                if missing:
                    self.gaps.append(Gap(
                        "log_range_gap",
                        f"declared complete range [{header.log_first},{header.log_last}] "
                        f"is missing {len(missing)} commit_pos (e.g. {missing[:5]})",
                        "commit_log"))
                outside = [p for p in positions
                           if p < header.log_first or p > header.log_last]
                if outside:
                    self.gaps.append(Gap(
                        "commit_outside_declared_range",
                        f"{len(outside)} commit(s) outside declared range "
                        f"[{header.log_first},{header.log_last}] (e.g. {outside[:5]})",
                        "commit_log"))
        else:
            self.gaps.append(Gap("log_range_incomplete",
                                 "commit log range not declared complete; reads cannot be "
                                 "used to prove validity", "commit_log"))
        self.stats.add("commits", n_commits)
        # remember commit_pos per txn
        self.db.executemany(
            "INSERT INTO txns(txn_id,status,commit_pos,loc) VALUES (?,?,?,?) "
            "ON CONFLICT(txn_id) DO UPDATE SET commit_pos=excluded.commit_pos",
            [(t, "committed", p, loc) for (t, p, loc) in commit_txn])

    def _flush_versions(self, vrows: List[tuple]) -> None:
        if not vrows:
            return
        self.db.executemany("INSERT INTO versions VALUES (?,?,?,?,?,?,?,?)", vrows)
        self.stats.add("versions", len(vrows))

    def _ingest_txns(self, txns) -> None:
        n = 0
        for trec in txns:
            n += 1
            loc = trec.loc
            ops_complete = 1
            ops = trec.operations
            if trec.status == STATUS_COMMITTED:
                if trec.snapshot_pos is None:
                    self.gaps.append(Gap("missing_snapshot_pos", trec.txn_id, loc))
                if trec.start_time is None or trec.end_time is None:
                    self.gaps.append(Gap("missing_times", trec.txn_id, loc))
                if trec.start_time is not None and trec.end_time is not None \
                        and trec.end_time < trec.start_time:
                    self.gaps.append(Gap("end_before_start", trec.txn_id, loc))
                if ops is None:
                    ops_complete = 0
                    self.gaps.append(Gap("missing_operations",
                                         f"{trec.txn_id}: no operation record", loc))
            self.db.execute(
                "INSERT INTO txns(txn_id,status,snapshot_pos,start_time,end_time,ops_complete,loc) "
                "VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(txn_id) DO UPDATE SET status=excluded.status, "
                "snapshot_pos=excluded.snapshot_pos, start_time=excluded.start_time, "
                "end_time=excluded.end_time, ops_complete=excluded.ops_complete, "
                "loc=excluded.loc",
                (trec.txn_id, trec.status, trec.snapshot_pos, trec.start_time,
                 trec.end_time, ops_complete, loc))
            if ops is not None:
                ops_complete = self._ingest_ops(trec, ops) and ops_complete
                self.db.execute("UPDATE txns SET ops_complete=? WHERE txn_id=?",
                                (ops_complete, trec.txn_id))
            if trec.status == STATUS_ABORTED and trec.writes:
                self._ingest_aborted_writes(trec)
        self.stats.add("transactions", n)

    def _ingest_ops(self, trec, ops: List[Dict[str, Any]]) -> bool:
        complete = True
        seqs = []
        rows = []
        written: Dict[int, str] = {}
        for k, op in enumerate(ops):
            loc = f"{trec.loc}.operations[{k}]"
            seq = op.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool):
                self.gaps.append(Gap("bad_op_seq", f"op without integer seq", loc))
                complete = False
                continue
            seqs.append(seq)
            typ = op.get("type")
            if typ not in (OP_POINT_READ, OP_RANGE_READ, OP_PUT, OP_DELETE):
                self.gaps.append(Gap("unknown_op_type", f"type={typ!r}", loc))
                complete = False
                continue
            key = lo = hi = None
            value = entries = vid = None
            found = complete_flag = None
            if typ in (OP_POINT_READ, OP_PUT, OP_DELETE):
                key = check_int64(op.get("key"), loc)
            if typ == OP_RANGE_READ:
                lo = check_int64(op.get("lo"), loc)
                hi = check_int64(op.get("hi"), loc)
                if hi < lo:
                    self.gaps.append(Gap("bad_range", f"hi {hi} < lo {lo}", loc))
                    complete = False
                complete_flag = 1 if op.get("complete") is True else 0
                ent = op.get("entries")
                if not isinstance(ent, list):
                    self.gaps.append(Gap("range_entries_missing",
                                         "range_read without recorded result list", loc))
                    complete = False
                    ent = []
                canon_entries = []
                seen_ekeys = set()
                for m, e in enumerate(ent):
                    eloc = f"{loc}.entries[{m}]"
                    ek = check_int64(e.get("key"), eloc)
                    if ek in seen_ekeys:
                        self.gaps.append(Gap("duplicate_key_in_range_result",
                                             f"key {ek} twice in range result", eloc))
                    seen_ekeys.add(ek)
                    ev = canon_scalar(e.get("value"), eloc)
                    canon_entries.append({"key": ek, "value": ev,
                                          "version_id": e.get("version_id")})
                entries = json.dumps(canon_entries, sort_keys=True)
            if typ == OP_POINT_READ:
                f = op.get("found")
                if f is not True and f is not False:
                    self.gaps.append(Gap("point_read_found_missing",
                                         "point_read without found flag", loc))
                    complete = False
                    f = False
                found = 1 if f else 0
                if f:
                    value = canon_scalar(op.get("value"), loc)
                    vid = op.get("version_id")
            if typ == OP_PUT:
                value = canon_scalar(op.get("value"), loc)
                if key in written:
                    self.gaps.append(Gap("key_written_twice_in_txn",
                                         f"{trec.txn_id} writes key {key} twice", loc))
                    complete = False
                written[key] = "put"
            if typ == OP_DELETE:
                if key in written:
                    self.gaps.append(Gap("key_written_twice_in_txn",
                                         f"{trec.txn_id} writes key {key} twice", loc))
                    complete = False
                written[key] = "delete"
            rows.append((trec.txn_id, seq, typ, key, lo, hi, value, found, vid,
                         entries, complete_flag, loc))
        if sorted(seqs) != list(range(len(seqs))):
            self.gaps.append(Gap("missing_operations",
                                 f"{trec.txn_id}: op seqs are not 0..{len(seqs)-1} "
                                 f"(gaps or duplicates)", trec.loc))
            complete = False
        self.db.executemany("INSERT INTO ops VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        self.stats.add("operations", len(rows))
        return complete

    def _ingest_aborted_writes(self, trec) -> None:
        rows = []
        for j, w in enumerate(trec.writes or []):
            wloc = f"{trec.loc}.writes[{j}]"
            key = check_int64(w.get("key"), wloc)
            vid = w.get("version_id")
            if not isinstance(vid, str):
                continue
            tomb = 1 if w.get("tombstone") else 0
            val = None if tomb else canon_scalar(w.get("value"), wloc)
            rows.append((vid, key, trec.txn_id, tomb, val, wloc))
        self.db.executemany(
            "INSERT OR IGNORE INTO aborted_versions VALUES (?,?,?,?,?,?)", rows)
        self.stats.add("aborted_versions", len(rows))

    # -- cross validation --------------------------------------------------

    def _cross_validate(self, header) -> None:
        db = self.db
        # duplicate version ids
        for (vid, c) in db.execute(
                "SELECT version_id, COUNT(*) FROM versions GROUP BY version_id "
                "HAVING COUNT(*) > 1"):
            self.gaps.append(Gap("duplicate_version_id", f"version {vid} x{c}", "commit_log"))
        # version chains: predecessor must exist, be same key, older commit_pos
        broken_keys = set()
        for (key, pos, vid, pred, loc) in db.execute(
                "SELECT key, commit_pos, version_id, predecessor, loc FROM versions"):
            if pred is None:
                continue
            row = db.execute(
                "SELECT key, commit_pos FROM versions WHERE version_id=?", (pred,)).fetchone()
            if row is None:
                # predecessor may legitimately point at an initial version; those are rows too
                self.gaps.append(Gap("version_chain_broken",
                                     f"version {vid} (key {key}) has unknown predecessor {pred}",
                                     loc))
                broken_keys.add(key)
            elif row[0] != key:
                self.gaps.append(Gap("version_chain_broken",
                                     f"version {vid} (key {key}) predecessor {pred} "
                                     f"belongs to key {row[0]}", loc))
                broken_keys.add(key)
            elif row[1] >= pos:
                self.gaps.append(Gap("version_chain_broken",
                                     f"version {vid} predecessor {pred} is not older "
                                     f"(commit_pos {row[1]} >= {pos})", loc))
                broken_keys.add(key)
        if broken_keys:
            db.execute("CREATE TABLE broken_keys(key INTEGER PRIMARY KEY)")
            db.executemany("INSERT INTO broken_keys VALUES (?)",
                           [(k,) for k in broken_keys])
        else:
            db.execute("CREATE TABLE broken_keys(key INTEGER PRIMARY KEY)")
        # committed txns: snapshot strictly before own commit
        for (tid, sp, cp) in db.execute(
                "SELECT txn_id, snapshot_pos, commit_pos FROM txns "
                "WHERE status='committed' AND commit_pos IS NOT NULL"):
            if sp is not None and cp is not None and sp >= cp:
                self.gaps.append(Gap("snapshot_not_before_commit",
                                     f"{tid}: snapshot_pos {sp} >= own commit_pos {cp}",
                                     "transactions"))
        # committed txns with writes must appear in the log and match it
        txn_writes: Dict[str, Dict[int, Tuple[str, Optional[str]]]] = {}
        for (tid, typ, key, val) in db.execute(
                "SELECT txn_id, type, key, value FROM ops "
                "WHERE type IN ('put','delete') ORDER BY txn_id, seq"):
            txn_writes.setdefault(tid, {})[key] = (typ, val)
        log_writes: Dict[str, Dict[int, Tuple[int, Optional[str]]]] = {}
        for (tid, key, tomb, val) in db.execute(
                "SELECT txn_id, key, tombstone, value FROM versions "
                "WHERE txn_id != ?", (INITIAL_TXN,)):
            log_writes.setdefault(tid, {})[key] = (tomb, val)
        for tid, writes in txn_writes.items():
            st = db.execute("SELECT status FROM txns WHERE txn_id=?", (tid,)).fetchone()
            if not st or st[0] != STATUS_COMMITTED:
                continue
            lw = log_writes.get(tid)
            if lw is None:
                self.gaps.append(Gap("committed_writes_missing_from_log",
                                     f"{tid}: {len(writes)} write op(s) but no commit record",
                                     "commit_log"))
                continue
            for key, (typ, val) in writes.items():
                ent = lw.get(key)
                if ent is None:
                    self.gaps.append(Gap("txn_writes_vs_log_mismatch",
                                         f"{tid}: wrote key {key} but log has no such write",
                                         "commit_log"))
                else:
                    tomb, lval = ent
                    if (typ == "delete") != bool(tomb) or (typ == "put" and lval != val):
                        self.gaps.append(Gap("txn_writes_vs_log_mismatch",
                                             f"{tid}: write op for key {key} disagrees with log",
                                             "commit_log"))
        for tid, lw in log_writes.items():
            tw = txn_writes.get(tid)
            st = db.execute("SELECT status FROM txns WHERE txn_id=?", (tid,)).fetchone()
            if not st or st[0] != STATUS_COMMITTED:
                self.gaps.append(Gap("commit_without_txn_record",
                                     f"{tid}: commit in log but no committed txn record",
                                     "commit_log"))
            elif tw is None or set(tw) != set(lw):
                missing = set(lw) - set(tw or {})
                self.gaps.append(Gap("missing_write_ops",
                                     f"{tid}: log records {len(missing)} write(s) with no "
                                     f"matching put/delete op", "transactions"))
        # read-only committed txns are fine without a log entry.

    # -- queries -----------------------------------------------------------

    def visible_version(self, key: int, snapshot_pos: int):
        """Version of *key* visible at *snapshot_pos* (or None)."""
        self.stats.add("version_lookups")
        return self.db.execute(
            "SELECT key, commit_pos, version_id, txn_id, tombstone, value, loc "
            "FROM versions WHERE key=? AND commit_pos<=? "
            "ORDER BY commit_pos DESC LIMIT 1", (key, snapshot_pos)).fetchone()

    def successor_version(self, key: int, snapshot_pos: int):
        """First committed version of *key* after *snapshot_pos* (or None)."""
        self.stats.add("version_lookups")
        return self.db.execute(
            "SELECT key, commit_pos, version_id, txn_id, tombstone, value, loc "
            "FROM versions WHERE key=? AND commit_pos>? "
            "ORDER BY commit_pos ASC LIMIT 1", (key, snapshot_pos)).fetchone()

    def visible_entries_in_range(self, lo: int, hi: int, snapshot_pos: int):
        """All key -> visible version rows inside [lo,hi] at snapshot_pos."""
        self.stats.add("range_probes")
        cur = self.db.execute(
            "SELECT key, commit_pos, version_id, txn_id, tombstone, value, loc FROM ("
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY commit_pos DESC) rn "
            "  FROM versions WHERE key BETWEEN ? AND ? AND commit_pos <= ?"
            ") WHERE rn = 1 ORDER BY key", (lo, hi, snapshot_pos))
        while True:
            rows = cur.fetchmany(4096)
            if not rows:
                break
            self.stats.add("range_keys_scanned", len(rows))
            for r in rows:
                yield r

    def keys_with_versions_in_range(self, lo: int, hi: int) -> Iterator[int]:
        self.stats.add("range_probes")
        cur = self.db.execute(
            "SELECT DISTINCT key FROM versions WHERE key BETWEEN ? AND ? ORDER BY key",
            (lo, hi))
        while True:
            rows = cur.fetchmany(4096)
            if not rows:
                break
            self.stats.add("range_keys_scanned", len(rows))
            for (k,) in rows:
                yield k

    def version_by_id(self, vid: str):
        self.stats.add("version_lookups")
        return self.db.execute(
            "SELECT key, commit_pos, version_id, txn_id, tombstone, value, loc "
            "FROM versions WHERE version_id=?", (vid,)).fetchone()

    def aborted_by_id(self, vid: str):
        self.stats.add("version_lookups")
        return self.db.execute(
            "SELECT version_id, key, txn_id, tombstone, value, loc "
            "FROM aborted_versions WHERE version_id=?", (vid,)).fetchone()

    def committed_txns(self) -> List[tuple]:
        return self.db.execute(
            "SELECT txn_id, snapshot_pos, start_time, end_time, commit_pos "
            "FROM txns WHERE status='committed' ORDER BY txn_id").fetchall()

    def txn_ops(self, txn_id: str) -> List[tuple]:
        return self.db.execute(
            "SELECT seq, type, key, lo, hi, value, found, version_id, entries, complete, loc "
            "FROM ops WHERE txn_id=? ORDER BY seq", (txn_id,)).fetchall()

    def broken_keys_in(self, lo: int, hi: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM broken_keys WHERE key BETWEEN ? AND ? LIMIT 1",
            (lo, hi)).fetchone() is not None

    def index_plan(self) -> List[str]:
        return list(INDEX_PLAN)
