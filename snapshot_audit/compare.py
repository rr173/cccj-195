"""Cross-run regression comparison engine.

Both sides are audited freshly from their raw histories (see
``audit_run.fresh_audit``) and normalized into line/position/version-id
independent finding identities (see ``findings``).  Matching then proceeds
through id-keyed indexes, never an unindexed Cartesian product:

* read anomalies match on their full semantic identity; the same
  transaction/operation with a conflicting identity is ``identity_conflict``;
  several candidates that cannot be disambiguated are ``ambiguous``;
* dependency cycles match on the rotation-normalized ring and their sets of
  legal witness variants; two legal witnesses of one logical ring produce
  ``ambiguous``, never a wrong "resolved"/"persisted";
* gaps match on stable category + scope; missing evidence is never promoted
  to ``valid``/``resolved``;
* unmapped transactions still enter new/disappeared statistics;
* a finding can only be ``resolved`` when the candidate evidence is complete
  and the finding genuinely no longer occurs.
"""

from __future__ import annotations

import re
import resource
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from . import __version__
from .audit_run import fresh_audit, run_summary
from .findings import build_findings
from .hashing import canonical_json, sha256_text
from .ledger import (
    CAT_AMBIGUOUS, CAT_EVIDENCE_CHANGED, CAT_GAP_CHANGE,
    CAT_IDENTITY_CONFLICT, CAT_NEW_INVALID, CAT_PERSISTED_INVALID,
    CAT_RESOLVED_INVALID, CAT_UNMAPPED, CAT_VALID_UNKNOWN_TRANSITION,
    Ledger,
)
from .manifest import (
    IdentityMapping, ManifestError, recheck_manifest, validate_mapping_refs,
)

# file line numbers inside evidence locs (e.g. "transactions.jsonl:3") are
# evidence-only coordinates: reordering JSONL records must not change the
# comparison result
_LOC_LINE = re.compile(r":\d+(?=($|[.\]]))")


def _normalize_evidence(obj: Any) -> Any:
    """Strip volatile line-number coordinates from evidence structures."""
    if isinstance(obj, str):
        return _LOC_LINE.sub("", obj)
    if isinstance(obj, dict):
        return {k: _normalize_evidence(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_normalize_evidence(v) for v in obj]
    return obj


def _ev_hash(finding: Optional[Dict[str, Any]]) -> Optional[str]:
    if finding is None:
        return None
    return sha256_text(canonical_json(
        _normalize_evidence(finding.get("evidence"))))


REPORT_FORMAT = "snapshot-regression-report/1"

# reasons attached to every ledger row
REASON_READ_MATCH = "read_anomaly_same_identity"
REASON_READ_EVIDENCE = "read_anomaly_evidence_changed"
REASON_READ_CONFLICT = "same_txn_op_semantic_conflict"
REASON_READ_AMBIGUOUS = "multiple_candidate_findings_same_identity"
REASON_CYCLE_PERSISTED = "cycle_ring_and_witness_persist"
REASON_CYCLE_EVIDENCE = "cycle_witness_evidence_changed"
REASON_CYCLE_AMBIGUOUS = "cycle_has_multiple_legal_witnesses"
REASON_CYCLE_CONFLICT = "same_ring_conflicting_witness_semantics"
REASON_CYCLE_RING_CONFLICT = "cycle_node_ring_conflict"
REASON_GAP_NEW = "new_evidence_gap"
REASON_GAP_RESOLVED = "evidence_gap_cleared"
REASON_GAP_PERSISTED = "evidence_gap_persists"
REASON_GAP_UNRESOLVABLE = "candidate_evidence_incomplete_cannot_resolve"
REASON_TXN_NEW = "unmapped_transaction_only_in_candidate"
REASON_TXN_DISAPPEARED = "unmapped_transaction_only_in_baseline"
REASON_VERDICT_TRANSITION = "run_level_valid_unknown_transition"


def _rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss
    return rss * 1024


class CompareStats:
    def __init__(self) -> None:
        self.values: Dict[str, int] = {}

    def add(self, name: str, n: int = 1) -> None:
        self.values[name] = self.values.get(name, 0) + n

    def set(self, name: str, n: int) -> None:
        self.values[name] = n

    def as_json(self) -> Dict[str, int]:
        return dict(sorted(self.values.items()))


def _ev_hash(finding: Optional[Dict[str, Any]]) -> Optional[str]:
    if finding is None:
        return None
    # normalize away volatile file-line coordinates so reordering JSONL
    # records cannot look like an evidence change
    return sha256_text(canonical_json(
        _normalize_evidence(finding.get("evidence"))))


def _group_key_read(finding: Dict[str, Any]) -> Tuple[str, Any, ...]:
    # location only: findings at the same transaction/operation/target must
    # be compared even when their anomaly category changed (that change is
    # itself the semantic conflict)
    ident = finding["identity"]
    tgt = canonical_json(ident["target"])
    return (ident["txn"], ident["op_seq"], tgt)


def _group_key_gap(finding: Dict[str, Any]) -> str:
    return sha256_text(canonical_json(finding["identity"]))[:16]


class Comparison:
    def __init__(self, manifest: Dict[str, Any], mapping: IdentityMapping,
                 memory_budget_mb: int) -> None:
        self.manifest = manifest
        self.mapping = mapping
        self.memory_budget_mb = memory_budget_mb
        self.stats = CompareStats()
        self.rows: List[Dict[str, Any]] = []

    # -- row construction --------------------------------------------------

    def _row_key(self, category: str, finding_class: str,
                 parts: List[Any]) -> str:
        return sha256_text(canonical_json(
            [category, finding_class] + parts))[:16]

    def _emit(self, category: str, finding_class: str, reason: str,
              row_key_parts: List[Any],
              baseline: Optional[Dict[str, Any]] = None,
              candidate: Optional[Dict[str, Any]] = None,
              canonical_txn: Optional[str] = None,
              op_seq: Optional[int] = None,
              identity: Optional[Dict[str, Any]] = None,
              evidence_changed: bool = False) -> None:
        ident = identity or (baseline or candidate)["identity"]
        if canonical_txn is None:
            if baseline is not None:
                canonical_txn = baseline["identity"].get("txn")
            elif candidate is not None:
                canonical_txn = candidate["identity"].get("txn")
        self.rows.append({
            "row_key": self._row_key(category, finding_class, row_key_parts),
            "category": category,
            "finding_class": finding_class,
            "reason": reason,
            "baseline_uid": baseline["uid"] if baseline else None,
            "candidate_uid": candidate["uid"] if candidate else None,
            "baseline_run_id": self.manifest["baseline"]["run_id"]
            if baseline else None,
            "candidate_run_id": self.manifest["candidate"]["run_id"]
            if candidate else None,
            "canonical_txn": canonical_txn,
            "op_seq": op_seq,
            "baseline_evidence": _normalize_evidence(baseline["evidence"])
            if baseline else None,
            "candidate_evidence": _normalize_evidence(candidate["evidence"])
            if candidate else None,
            "evidence_changed": bool(evidence_changed),
            "identity_json": canonical_json(ident),
        })

    # -- main entry --------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        t0 = time.monotonic()
        m = self.manifest
        b_rec, c_rec = m["baseline"], m["candidate"]
        # the raw histories must not change while the comparison runs:
        # re-hash every input after the audits and refuse to sign otherwise
        b_bundle = fresh_audit(
            input_path=b_rec["path"] if b_rec["kind"] == "file" else None,
            input_dir=b_rec["path"] if b_rec["kind"] == "dir" else None,
            memory_budget_mb=self.memory_budget_mb)
        c_bundle = fresh_audit(
            input_path=c_rec["path"] if c_rec["kind"] == "file" else None,
            input_dir=c_rec["path"] if c_rec["kind"] == "dir" else None,
            memory_budget_mb=self.memory_budget_mb)
        self.stats.add("fresh_audits", 2)
        drift = recheck_manifest(m)
        if drift:
            b_bundle["store"].close()
            c_bundle["store"].close()
            raise ManifestError("input_changed_during_run",
                                "; ".join(drift))
        try:
            validate_mapping_refs(self.mapping, b_bundle["txn_ids"],
                                  c_bundle["txn_ids"])
            self.mapping.finalize(b_bundle["txn_ids"], c_bundle["txn_ids"])
            b_findings = build_findings(
                b_bundle["store"], b_bundle["result"], self.mapping, "baseline")
            c_findings = build_findings(
                c_bundle["store"], c_bundle["result"], self.mapping, "candidate")
            self._index_and_match(b_bundle, c_bundle, b_findings, c_findings)
        finally:
            b_bundle["store"].close()
            c_bundle["store"].close()

        b_summary = run_summary(b_bundle, b_rec["run_id"], b_rec)
        c_summary = run_summary(c_bundle, c_rec["run_id"], c_rec)
        self._unmapped_txns(b_bundle["txn_ids"], c_bundle["txn_ids"])
        self._verdict_transition(b_summary, c_summary)

        counts = self._counts()
        status, status_reason = self._overall_status(
            b_summary, c_summary, counts)
        self.rows.sort(key=lambda r: (r["category"], r["row_key"]))
        self.stats.set("rows_written", len(self.rows))
        # Wall time and exact RSS vary between runs on a shared machine.
        # Keep an exact measurement for the console/operator but put only a
        # coarsely bucketed, stable resource figure into the signed report so
        # repeated runs on identical inputs are byte-identical.
        exact_rss = _rss_bytes()
        exact_wall = int((time.monotonic() - t0) * 1000)
        self.stats.set("rss_bytes_bucket_mb",
                       (exact_rss // (16 * 1024 * 1024)) * 16)
        self._exact_stats = {"rss_bytes_exact": exact_rss,
                             "wall_time_ms_exact": exact_wall}
        report = {
            "format": REPORT_FORMAT,
            "generator": f"snapshot_audit/{__version__}",
            "manifest_sha256": m["manifest_sha256"],
            "manifest": m,
            "status": status,
            "status_reason": status_reason,
            "runs": {"baseline": b_summary, "candidate": c_summary},
            "counts": counts,
            "rows": self.rows,
            "stats": self.stats.as_json(),
        }
        report["report_sha256"] = sha256_text(
            canonical_json(_unsigned_report(report)))
        report["runtime_stats"] = self._exact_stats
        return report

    # -- matching ----------------------------------------------------------

    def _index_and_match(self, b_bundle, c_bundle, b_findings,
                         c_findings) -> None:
        self._match_read_anomalies(b_findings["read_anomaly"],
                                   c_findings["read_anomaly"])
        self._match_cycles(b_findings["dependency_cycle"],
                           c_findings["dependency_cycle"],
                           c_bundle["incomplete"])
        self._match_gaps(b_findings["gap"], c_findings["gap"],
                         c_bundle["incomplete"])

    def _match_read_anomalies(self, b_list: List[Dict[str, Any]],
                              c_list: List[Dict[str, Any]]) -> None:
        # Location index: (canonical txn, op, target) -> findings.
        # Both are hash indexes; matching never scans the cross product of
        # the two finding sets.
        b_groups: Dict[Tuple, List[Dict[str, Any]]] = {}
        c_groups: Dict[Tuple, List[Dict[str, Any]]] = {}
        for f in b_list:
            b_groups.setdefault(_group_key_read(f), []).append(f)
            self.stats.add("index_inserts")
        for f in c_list:
            c_groups.setdefault(_group_key_read(f), []).append(f)
            self.stats.add("index_inserts")
        for gk in sorted(set(b_groups) | set(c_groups)):
            bs = b_groups.get(gk, [])
            cs = c_groups.get(gk, [])
            self.stats.add("index_hits", len(bs) + len(cs))
            self.stats.add("candidate_comparisons",
                           max(1, len(bs)) * max(1, len(cs)))
            b_uids = {f["uid"]: f for f in bs}
            c_uids = {f["uid"]: f for f in cs}
            shared = sorted(set(b_uids) & set(c_uids))
            if shared:
                if len(bs) > 1 or len(cs) > 1:
                    # several distinct findings at the same location: the
                    # pairing cannot be made unique
                    for uid in shared:
                        self._emit(
                            CAT_AMBIGUOUS, "read_anomaly",
                            REASON_READ_AMBIGUOUS, ["read", gk, uid],
                            b_uids[uid], c_uids[uid],
                            op_seq=b_uids[uid]["identity"]["op_seq"])
                    for uid in sorted(set(b_uids) - set(shared)):
                        self._emit(CAT_IDENTITY_CONFLICT, "read_anomaly",
                                   REASON_READ_CONFLICT,
                                   ["read", gk, uid], baseline=b_uids[uid],
                                   op_seq=b_uids[uid]["identity"]["op_seq"])
                    for uid in sorted(set(c_uids) - set(shared)):
                        self._emit(CAT_IDENTITY_CONFLICT, "read_anomaly",
                                   REASON_READ_CONFLICT,
                                   ["read", gk, uid], candidate=c_uids[uid],
                                   op_seq=c_uids[uid]["identity"]["op_seq"])
                    continue
                bf, cf = b_uids[shared[0]], c_uids[shared[0]]
                ev_changed = _ev_hash(bf) != _ev_hash(cf)
                # same normalized identity: the invalid finding persists;
                # version ids / source locations can only mark an evidence
                # change, never a new fault by themselves
                self._emit(
                    CAT_PERSISTED_INVALID, "read_anomaly",
                    REASON_READ_MATCH,
                    ["read", gk, bf["uid"]], bf, cf,
                    op_seq=bf["identity"]["op_seq"],
                    evidence_changed=ev_changed)
                if ev_changed:
                    self.stats.add("evidence_changes")
                continue
            # same transaction/op/target but disjoint semantic identities:
            # a real semantic conflict, never silently "resolved + new"
            if bs and cs:
                for f in bs:
                    self._emit(CAT_IDENTITY_CONFLICT, "read_anomaly",
                               REASON_READ_CONFLICT, ["read", gk, f["uid"]],
                               baseline=f,
                               op_seq=f["identity"]["op_seq"])
                for f in cs:
                    self._emit(CAT_IDENTITY_CONFLICT, "read_anomaly",
                               REASON_READ_CONFLICT, ["read", gk, f["uid"]],
                               candidate=f,
                               op_seq=f["identity"]["op_seq"])
            elif bs:
                for f in bs:
                    self._emit(CAT_RESOLVED_INVALID, "read_anomaly",
                               "read_anomaly_no_longer_observed",
                               ["read", gk, f["uid"]], baseline=f,
                               op_seq=f["identity"]["op_seq"])
            else:
                for f in cs:
                    self._emit(CAT_NEW_INVALID, "read_anomaly",
                               "read_anomaly_newly_observed",
                               ["read", gk, f["uid"]], candidate=f,
                               op_seq=f["identity"]["op_seq"])

    def _match_cycles(self, b_list: List[Dict[str, Any]],
                      c_list: List[Dict[str, Any]],
                      candidate_incomplete: bool) -> None:
        # index cycles by canonical node ring (rotation normalized)
        b_by_ring: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        c_by_ring: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        for f in b_list:
            b_by_ring[tuple(f["ring"])] = f
            self.stats.add("index_inserts")
        for f in c_list:
            c_by_ring[tuple(f["ring"])] = f
            self.stats.add("index_inserts")
        for ring in sorted(set(b_by_ring) | set(c_by_ring)):
            self.stats.add("index_hits")
            bf = b_by_ring.get(ring)
            cf = c_by_ring.get(ring)
            self.stats.add("candidate_comparisons", 1)
            if bf is None:
                self._emit(CAT_NEW_INVALID, "dependency_cycle",
                           "cycle_ring_newly_present",
                           ["cycle", ring], candidate=cf,
                           canonical_txn=ring[0] if ring else None)
                continue
            if cf is None:
                if candidate_incomplete:
                    self._emit(CAT_GAP_CHANGE, "dependency_cycle",
                               REASON_GAP_UNRESOLVABLE,
                               ["cycle", ring], baseline=bf,
                               canonical_txn=ring[0] if ring else None)
                else:
                    self._emit(CAT_RESOLVED_INVALID, "dependency_cycle",
                               "cycle_ring_no_longer_present",
                               ["cycle", ring], baseline=bf,
                               canonical_txn=ring[0] if ring else None)
                continue
            # same canonical ring on both sides: compare witness variants
            b_variants = set(bf["variant_uids"])
            c_variants = set(cf["variant_uids"])
            shared = b_variants & c_variants
            if len(b_variants) > 1 or len(c_variants) > 1:
                if shared:
                    self._emit(CAT_AMBIGUOUS, "dependency_cycle",
                               REASON_CYCLE_AMBIGUOUS,
                               ["cycle", ring, "ambiguous"], bf, cf,
                               canonical_txn=ring[0])
                else:
                    self._emit(CAT_IDENTITY_CONFLICT, "dependency_cycle",
                               REASON_CYCLE_CONFLICT,
                               ["cycle", ring, "conflict"], bf, cf,
                               canonical_txn=ring[0])
                continue
            if not shared:
                self._emit(CAT_IDENTITY_CONFLICT, "dependency_cycle",
                           REASON_CYCLE_CONFLICT,
                           ["cycle", ring, "conflict"], bf, cf,
                           canonical_txn=ring[0])
                continue
            ev_changed = _ev_hash(bf) != _ev_hash(cf)
            # the ring and a legal witness persist; version ids and source
            # locations only mark an evidence change, never a new fault
            self._emit(CAT_PERSISTED_INVALID, "dependency_cycle",
                       REASON_CYCLE_PERSISTED,
                       ["cycle", ring, "persist"], bf, cf,
                       canonical_txn=ring[0],
                       evidence_changed=ev_changed)
            if ev_changed:
                self.stats.add("evidence_changes")
        # rings present only on one side with disjoint node sets were handled
        # above; a same-emitted-node but reordered ring is a distinct tuple
        # and would land here as new+resolved pairs - flag as conflict instead
        b_rings = set(b_by_ring)
        c_rings = set(c_by_ring)
        for br in sorted(b_rings - c_rings):
            for cr in sorted(c_rings - b_rings):
                if set(br) == set(cr):
                    bf, cf = b_by_ring[br], c_by_ring[cr]
                    # remove the earlier new/resolved pair if it was emitted
                    self.rows = [
                        r for r in self.rows
                        if not (r["finding_class"] == "dependency_cycle"
                                and (r["baseline_uid"] == bf["uid"]
                                     or r["candidate_uid"] == cf["uid"])
                                and r["category"] in (CAT_NEW_INVALID,
                                                      CAT_RESOLVED_INVALID,
                                                      CAT_GAP_CHANGE))]
                    self._emit(CAT_IDENTITY_CONFLICT, "dependency_cycle",
                               REASON_CYCLE_RING_CONFLICT,
                               ["cycle", sorted(br), "ring-order"],
                               bf, cf, canonical_txn=br[0])

    def _match_gaps(self, b_list: List[Dict[str, Any]],
                    c_list: List[Dict[str, Any]],
                    candidate_incomplete: bool) -> None:
        b_by_uid = {f["uid"]: f for f in b_list}
        c_by_uid = {f["uid"]: f for f in c_list}
        for f in b_list + c_list:
            self.stats.add("index_inserts")
        for uid in sorted(set(b_by_uid) | set(c_by_uid)):
            self.stats.add("index_hits")
            self.stats.add("candidate_comparisons", 1)
            bf, cf = b_by_uid.get(uid), c_by_uid.get(uid)
            scope = (bf or cf)["identity"]["scope"]
            txn = scope.get("txn")
            op_seq = scope.get("op_seq")
            if bf and cf:
                self._emit(CAT_GAP_CHANGE, "gap", REASON_GAP_PERSISTED,
                           ["gap", uid], bf, cf, canonical_txn=txn,
                           op_seq=op_seq)
            elif bf:
                if candidate_incomplete:
                    self._emit(CAT_GAP_CHANGE, "gap",
                               REASON_GAP_UNRESOLVABLE,
                               ["gap", uid, "unresolved"], baseline=bf,
                               canonical_txn=txn, op_seq=op_seq)
                else:
                    self._emit(CAT_GAP_CHANGE, "gap", REASON_GAP_RESOLVED,
                               ["gap", uid, "resolved"], baseline=bf,
                               canonical_txn=txn, op_seq=op_seq)
            else:
                self._emit(CAT_GAP_CHANGE, "gap", REASON_GAP_NEW,
                           ["gap", uid, "new"], candidate=cf,
                           canonical_txn=txn, op_seq=op_seq)

    # -- run-level rows ----------------------------------------------------

    def _unmapped_txns(self, b_ids: List[str], c_ids: List[str]) -> None:
        mapped_b = set(self.mapping.b2c)
        mapped_c = set(self.mapping.c2b)
        b_set, c_set = set(b_ids), set(c_ids)
        for tid in sorted(b_set - mapped_b - c_set):
            canon = self.mapping.canonical_id("baseline", tid)
            self._emit(CAT_UNMAPPED, "transaction", REASON_TXN_DISAPPEARED,
                       ["txn", "baseline", tid], canonical_txn=canon,
                       identity={"finding": "unmapped_transaction",
                                 "side": "baseline", "txn": canon,
                                 "local_txn_id": tid})
        for tid in sorted(c_set - mapped_c - b_set):
            canon = self.mapping.canonical_id("candidate", tid)
            self._emit(CAT_UNMAPPED, "transaction", REASON_TXN_NEW,
                       ["txn", "candidate", tid], canonical_txn=canon,
                       identity={"finding": "unmapped_transaction",
                                 "side": "candidate", "txn": canon,
                                 "local_txn_id": tid})

    def _verdict_transition(self, b_summary: Dict[str, Any],
                            c_summary: Dict[str, Any]) -> None:
        pair = (b_summary["verdict"], c_summary["verdict"])
        if pair[0] == pair[1]:
            return
        # only movement between the non-invalid, evidence-limited verdicts
        # counts as a valid/unknown transition; invalid<->valid is already
        # explained by the concrete new/resolved finding rows
        if set(pair) <= {"valid", "unknown"}:
            self._emit(
                CAT_VALID_UNKNOWN_TRANSITION, "run_verdict",
                REASON_VERDICT_TRANSITION,
                ["verdict", pair[0], pair[1]],
                canonical_txn=None,
                identity={"finding": "verdict_transition",
                          "baseline_verdict": pair[0],
                          "candidate_verdict": pair[1],
                          "baseline_gap_kinds": b_summary["gap_kinds"],
                          "candidate_gap_kinds": c_summary["gap_kinds"]})

    # -- aggregation -------------------------------------------------------

    def _counts(self) -> Dict[str, int]:
        counts = {cat: 0 for cat in (
            CAT_NEW_INVALID, CAT_RESOLVED_INVALID, CAT_PERSISTED_INVALID,
            CAT_EVIDENCE_CHANGED, CAT_VALID_UNKNOWN_TRANSITION,
            CAT_GAP_CHANGE, CAT_UNMAPPED, CAT_AMBIGUOUS,
            CAT_IDENTITY_CONFLICT)}
        for r in self.rows:
            counts[r["category"]] += 1
        # evidence changes are a flagged subset of persisted findings, not a
        # separate fault category; reported separately for visibility
        counts[CAT_EVIDENCE_CHANGED] = sum(
            1 for r in self.rows if r.get("evidence_changed"))
        counts["unmapped_new"] = sum(
            1 for r in self.rows if r["category"] == CAT_UNMAPPED
            and r["reason"] == REASON_TXN_NEW)
        counts["unmapped_disappeared"] = sum(
            1 for r in self.rows if r["category"] == CAT_UNMAPPED
            and r["reason"] == REASON_TXN_DISAPPEARED)
        return dict(sorted(counts.items()))

    def _overall_status(self, b_summary: Dict[str, Any],
                        c_summary: Dict[str, Any],
                        counts: Dict[str, int]) -> Tuple[str, str]:
        if counts[CAT_NEW_INVALID] or counts[CAT_IDENTITY_CONFLICT]:
            return "regression", (
                f"new_invalid={counts[CAT_NEW_INVALID]} "
                f"identity_conflict={counts[CAT_IDENTITY_CONFLICT]}")
        if counts[CAT_AMBIGUOUS]:
            return "unknown", f"ambiguous findings={counts[CAT_AMBIGUOUS]}"
        if b_summary["incomplete_evidence"] or c_summary["incomplete_evidence"]:
            return "unknown", "at least one side has incomplete evidence"
        if counts[CAT_GAP_CHANGE] or counts[CAT_VALID_UNKNOWN_TRANSITION]:
            return "unknown", "evidence gaps or valid/unknown transitions present"
        if counts[CAT_RESOLVED_INVALID]:
            return "improved", f"resolved_invalid={counts[CAT_RESOLVED_INVALID]}"
        return "no_regression", "no new invalid findings"


def _unsigned_report(report: Dict[str, Any]) -> Dict[str, Any]:
    # report_sha256 is the signature itself; runtime_stats is attached after
    # signing and must not be part of the signed payload either
    return {k: v for k, v in report.items()
            if k not in ("report_sha256", "runtime_stats")}


def write_ledger(ledger_path: str, report: Dict[str, Any],
                 stats: Dict[str, int]) -> Ledger:
    ledger = Ledger(ledger_path)
    try:
        ledger.add_run("baseline", report["runs"]["baseline"])
        ledger.add_run("candidate", report["runs"]["candidate"])
        for row in report["rows"]:
            ledger.add_row(row)
        ledger.finalize(stats)
    except Exception:
        ledger.close()
        raise
    return ledger


def run_comparison(manifest: Dict[str, Any], mapping: IdentityMapping,
                   memory_budget_mb: int = 512) -> Dict[str, Any]:
    comp = Comparison(manifest, mapping, memory_budget_mb)
    return comp.run()
