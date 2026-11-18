"""Data model, input contract validation and streaming loaders.

Input contract (see README.md for the full schema):

* initial key/values (absence of a key has one uniform semantics),
* a commit log with a declared *complete* range of unique, increasing
  ``commit_pos`` and the full write set of every commit in that range,
* committed versions carry explicit ``version_id`` and ``predecessor``;
  deletes are tombstones,
* per transaction: start/end time, fixed ``snapshot_pos`` and operations
  recorded by sequence number: ``point_read``, closed-interval
  ``range_read`` (full result list + end-of-scan flag), ``put``, ``delete``,
* keys are int64, values are JSON scalars preserved as-is,
* a transaction writes a key at most once (write-then-read is legal),
* rolled-back transactions, when present, are marked ``aborted`` and never
  participate in the commit graph; their writes are not readable versions.

Every contract violation that prevents a definite conclusion is recorded as
a *gap*; missing evidence is never reported as "no anomaly".
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

FORMAT_ID = "snapshot-history/1"

# Operation types
OP_POINT_READ = "point_read"
OP_RANGE_READ = "range_read"
OP_PUT = "put"
OP_DELETE = "delete"
OP_TYPES = {OP_POINT_READ, OP_RANGE_READ, OP_PUT, OP_DELETE}

STATUS_COMMITTED = "committed"
STATUS_ABORTED = "aborted"

INITIAL_TXN = "@initial"  # virtual transaction that wrote the initial state


class ContractError(Exception):
    """Raised when the input cannot be parsed at all (not an audit gap)."""


def canon_scalar(value: Any, loc: str) -> str:
    """Validate that *value* is a JSON scalar and return its canonical text.

    Scalars are compared by canonical encoding, so ``1`` and ``1.0`` are
    distinct values ("values preserved as-is").
    """
    if value is None or isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, (int, float, str)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    raise ContractError(f"{loc}: value is not a JSON scalar: {type(value).__name__}")


def check_int64(key: Any, loc: str) -> int:
    if isinstance(key, bool) or not isinstance(key, int):
        raise ContractError(f"{loc}: key is not an integer: {key!r}")
    if key < INT64_MIN or key > INT64_MAX:
        raise ContractError(f"{loc}: key outside int64 range: {key}")
    return key


def check_nonneg_int(value: Any, what: str, loc: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractError(f"{loc}: {what} must be a non-negative integer, got {value!r}")
    return value


def check_time(value: Any, loc: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"{loc}: time must be a number, got {value!r}")
    return float(value)


@dataclass
class Gap:
    """A piece of missing/broken evidence.  Gaps block any 'valid' verdict."""
    kind: str
    detail: str
    loc: str = ""

    def to_json(self) -> Dict[str, Any]:
        return {"kind": self.kind, "detail": self.detail, "loc": self.loc}


@dataclass
class Anomaly:
    """A confirmed violation, located at a specific operation."""
    kind: str               # future_version_read | aborted_version_read | ...
    txn_id: str
    op_seq: int
    key: Optional[int] = None
    lo: Optional[int] = None
    hi: Optional[int] = None
    expected: Optional[Dict[str, Any]] = None
    observed: Optional[Dict[str, Any]] = None
    detail: str = ""
    loc: str = ""

    def to_json(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "kind": self.kind,
            "txn_id": self.txn_id,
            "op_seq": self.op_seq,
            "detail": self.detail,
            "loc": self.loc,
        }
        if self.key is not None:
            d["key"] = self.key
        if self.lo is not None:
            d["lo"] = self.lo
            d["hi"] = self.hi
        if self.expected is not None:
            d["expected"] = self.expected
        if self.observed is not None:
            d["observed"] = self.observed
        return d


# ---------------------------------------------------------------------------
# Streaming record loaders
# ---------------------------------------------------------------------------

@dataclass
class CommitRec:
    txn_id: str
    commit_pos: int
    writes: List[Dict[str, Any]]
    loc: str


@dataclass
class TxnRec:
    txn_id: str
    status: str
    snapshot_pos: Optional[int]
    start_time: Optional[float]
    end_time: Optional[float]
    operations: Optional[List[Dict[str, Any]]]
    writes: Optional[List[Dict[str, Any]]]  # aborted-txn write attribution
    loc: str


@dataclass
class Header:
    initial_state: List[Dict[str, Any]]
    log_first: Optional[int]
    log_last: Optional[int]
    log_complete: bool
    loc: str


def _parse_header(doc: Dict[str, Any], loc: str) -> Header:
    if doc.get("format") != FORMAT_ID:
        raise ContractError(f"{loc}: missing or unknown 'format' (want {FORMAT_ID!r})")
    initial = doc.get("initial_state")
    if not isinstance(initial, list):
        raise ContractError(f"{loc}: 'initial_state' must be a list")
    log = doc.get("commit_log")
    if not isinstance(log, dict):
        raise ContractError(f"{loc}: 'commit_log' declaration missing")
    first = log.get("first_commit_pos")
    last = log.get("last_commit_pos")
    complete = log.get("complete")
    if complete is not True and complete is not False:
        raise ContractError(f"{loc}: commit_log.complete must be true or false")
    for name, val in (("first_commit_pos", first), ("last_commit_pos", last)):
        if val is not None:
            check_nonneg_int(val, name, loc)
    return Header(initial, first, last, bool(complete), loc)


def _parse_commit(obj: Dict[str, Any], loc: str) -> CommitRec:
    txn_id = obj.get("txn_id")
    if not isinstance(txn_id, str):
        raise ContractError(f"{loc}: commit missing txn_id")
    pos = check_nonneg_int(obj.get("commit_pos"), "commit_pos", loc)
    writes = obj.get("writes")
    if not isinstance(writes, list):
        raise ContractError(f"{loc}: commit missing writes list")
    return CommitRec(txn_id, pos, writes, loc)


def _parse_txn(obj: Dict[str, Any], loc: str) -> TxnRec:
    txn_id = obj.get("txn_id")
    if not isinstance(txn_id, str):
        raise ContractError(f"{loc}: transaction missing txn_id")
    status = obj.get("status")
    if status not in (STATUS_COMMITTED, STATUS_ABORTED):
        raise ContractError(f"{loc}: transaction status must be "
                            f"{STATUS_COMMITTED!r} or {STATUS_ABORTED!r}")
    sp = obj.get("snapshot_pos")
    if sp is not None:
        sp = check_nonneg_int(sp, "snapshot_pos", loc)
    st = obj.get("start_time")
    et = obj.get("end_time")
    if st is not None:
        st = check_time(st, loc)
    if et is not None:
        et = check_time(et, loc)
    ops = obj.get("operations")
    if ops is not None and not isinstance(ops, list):
        raise ContractError(f"{loc}: operations must be a list")
    writes = obj.get("writes")
    if writes is not None and not isinstance(writes, list):
        raise ContractError(f"{loc}: writes must be a list")
    return TxnRec(txn_id, status, sp, st, et, ops, writes, loc)


def load_single_file(path: str) -> Tuple[Header, Iterator[CommitRec], Iterator[TxnRec]]:
    """Load a one-document history file."""
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    if not isinstance(doc, dict):
        raise ContractError("top-level JSON value must be an object")
    header = _parse_header(doc, "commit_log")
    commits = doc.get("commit_log", {}).get("commits", [])
    txns = doc.get("transactions", [])
    if not isinstance(commits, list) or not isinstance(txns, list):
        raise ContractError("commit_log.commits and transactions must be lists")

    def commit_iter() -> Iterator[CommitRec]:
        for i, c in enumerate(commits):
            yield _parse_commit(c, f"commit_log.commits[{i}]")

    def txn_iter() -> Iterator[TxnRec]:
        for i, t in enumerate(txns):
            yield _parse_txn(t, f"transactions[{i}]")

    return header, commit_iter(), txn_iter()


def load_dir(path: str) -> Tuple[Header, Iterator[CommitRec], Iterator[TxnRec]]:
    """Stream a directory history: header.json, commits.jsonl, transactions.jsonl."""
    header_path = os.path.join(path, "header.json")
    commits_path = os.path.join(path, "commits.jsonl")
    txns_path = os.path.join(path, "transactions.jsonl")
    with open(header_path, "r", encoding="utf-8") as fh:
        hdoc = json.load(fh)
    header = _parse_header(hdoc, "header.json")

    def commit_iter() -> Iterator[CommitRec]:
        if not os.path.exists(commits_path):
            return
        with open(commits_path, "r", encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                line = line.strip()
                if line:
                    yield _parse_commit(json.loads(line), f"commits.jsonl:{n}")

    def txn_iter() -> Iterator[TxnRec]:
        if not os.path.exists(txns_path):
            return
        with open(txns_path, "r", encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                line = line.strip()
                if line:
                    yield _parse_txn(json.loads(line), f"transactions.jsonl:{n}")

    return header, commit_iter(), txn_iter()


def load_history(input_path: Optional[str], input_dir: Optional[str]):
    if input_dir:
        return load_dir(input_dir)
    if input_path:
        return load_single_file(input_path)
    raise ContractError("no input given (use --input or --input-dir)")
