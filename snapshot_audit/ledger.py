"""SQLite regression ledger.

All comparison rows are written inside transactions in bounded batches and
looked up through indexes on the normalized finding ids - never through an
unindexed Cartesian product of the two finding sets.  Row order is fixed by
explicit stable sort keys, so a fresh ledger built from identical inputs is
byte-identical.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional

LEDGER_SCHEMA = """
CREATE TABLE runs(
  side              TEXT PRIMARY KEY,
  run_id            TEXT NOT NULL,
  input_path        TEXT NOT NULL,
  input_sha256      TEXT NOT NULL,
  verdict           TEXT NOT NULL,
  anomalies_total   INTEGER NOT NULL,
  gaps_total        INTEGER NOT NULL,
  incomplete        INTEGER NOT NULL
);
CREATE TABLE rows_(
  row_key           TEXT PRIMARY KEY,
  category          TEXT NOT NULL,
  finding_class     TEXT NOT NULL,
  baseline_uid      TEXT,
  candidate_uid     TEXT,
  baseline_run_id   TEXT,
  candidate_run_id  TEXT,
  canonical_txn     TEXT,
  op_seq            INTEGER,
  baseline_evidence TEXT,
  candidate_evidence TEXT,
  evidence_changed  INTEGER NOT NULL DEFAULT 0,
  reason            TEXT NOT NULL,
  identity_json     TEXT NOT NULL
);
CREATE INDEX idx_rows_baseline ON rows_(baseline_uid);
CREATE INDEX idx_rows_candidate ON rows_(candidate_uid);
CREATE INDEX idx_rows_category ON rows_(category, row_key);
CREATE TABLE stats(
  name  TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
"""

# categories allowed in the ledger / report rows
CAT_NEW_INVALID = "new_invalid"
CAT_RESOLVED_INVALID = "resolved_invalid"
CAT_PERSISTED_INVALID = "persisted_invalid"
CAT_EVIDENCE_CHANGED = "evidence_changed"
CAT_VALID_UNKNOWN_TRANSITION = "valid_unknown_transition"
CAT_GAP_CHANGE = "gap_change"
CAT_UNMAPPED = "unmapped_txn"
CAT_AMBIGUOUS = "ambiguous"
CAT_IDENTITY_CONFLICT = "identity_conflict"

ROW_CATEGORIES = (
    CAT_NEW_INVALID, CAT_RESOLVED_INVALID, CAT_PERSISTED_INVALID,
    CAT_EVIDENCE_CHANGED, CAT_VALID_UNKNOWN_TRANSITION, CAT_GAP_CHANGE,
    CAT_UNMAPPED, CAT_AMBIGUOUS, CAT_IDENTITY_CONFLICT,
)

_BATCH = 4096


class Ledger:
    def __init__(self, path: str) -> None:
        self.path = path
        if os.path.exists(path):
            os.remove(path)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(LEDGER_SCHEMA)
        self._pending: List[tuple] = []
        self._stats: Dict[str, int] = {}

    def add_stat(self, name: str, value: int) -> None:
        self._stats[name] = self._stats.get(name, 0) + value

    def set_stat(self, name: str, value: int) -> None:
        self._stats[name] = value

    def add_run(self, side: str, summary: Dict[str, Any]) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?)",
                (side, summary["run_id"], summary["input"]["path"],
                 summary["input"]["sha256"], summary["verdict"],
                 summary["anomalies_total"], summary["gaps_total"],
                 1 if summary["incomplete_evidence"] else 0))

    def add_row(self, row: Dict[str, Any]) -> None:
        self._pending.append((
            row["row_key"], row["category"], row["finding_class"],
            row.get("baseline_uid"), row.get("candidate_uid"),
            row.get("baseline_run_id"), row.get("candidate_run_id"),
            row.get("canonical_txn"), row.get("op_seq"),
            _jd(row.get("baseline_evidence")),
            _jd(row.get("candidate_evidence")),
            1 if row.get("evidence_changed") else 0,
            row["reason"], row["identity_json"]))
        if len(self._pending) >= _BATCH:
            self.flush()

    def flush(self) -> None:
        if not self._pending:
            return
        with self.db:  # one transaction per batch
            self.db.executemany(
                "INSERT INTO rows_ VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                self._pending)
        self._pending.clear()

    def finalize(self, extra_stats: Optional[Dict[str, int]] = None) -> None:
        self.flush()
        if extra_stats:
            for k, v in extra_stats.items():
                self._stats[k] = v
        with self.db:
            self.db.executemany(
                "INSERT INTO stats VALUES (?,?) "
                "ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                sorted(self._stats.items()))
        # compact deterministic physical layout in stable key order
        with self.db:
            self.db.execute(
                "CREATE TABLE rows_sorted AS SELECT * FROM rows_ "
                "ORDER BY row_key")
            self.db.execute("DROP TABLE rows_")
            self.db.execute(
                "CREATE TABLE rows_ ("
                "row_key TEXT PRIMARY KEY, category TEXT NOT NULL, "
                "finding_class TEXT NOT NULL, baseline_uid TEXT, "
                "candidate_uid TEXT, baseline_run_id TEXT, "
                "candidate_run_id TEXT, canonical_txn TEXT, op_seq INTEGER, "
                "baseline_evidence TEXT, candidate_evidence TEXT, "
                "evidence_changed INTEGER NOT NULL DEFAULT 0, "
                "reason TEXT NOT NULL, identity_json TEXT NOT NULL)")
            self.db.execute(
                "INSERT INTO rows_ SELECT row_key,category,finding_class,"
                "baseline_uid,candidate_uid,baseline_run_id,candidate_run_id,"
                "canonical_txn,op_seq,baseline_evidence,candidate_evidence,"
                "evidence_changed,reason,identity_json FROM rows_sorted")
            self.db.execute("DROP TABLE rows_sorted")
            self.db.execute(
                "CREATE INDEX idx_rows_baseline ON rows_(baseline_uid)")
            self.db.execute("CREATE INDEX idx_rows_candidate ON rows_(candidate_uid)")
            self.db.execute(
                "CREATE INDEX idx_rows_category ON rows_(category, row_key)")
        self.db.commit()

    def all_rows(self) -> List[Dict[str, Any]]:
        cur = self.db.execute(
            "SELECT row_key,category,finding_class,baseline_uid,candidate_uid,"
            "baseline_run_id,candidate_run_id,canonical_txn,op_seq,"
            "baseline_evidence,candidate_evidence,reason,identity_json "
            "FROM rows_ ORDER BY row_key")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def stats(self) -> Dict[str, int]:
        return dict(self.db.execute("SELECT name,value FROM stats").fetchall())

    def close(self) -> None:
        self.db.close()


def _jd(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    from .hashing import canonical_json
    return canonical_json(obj)
