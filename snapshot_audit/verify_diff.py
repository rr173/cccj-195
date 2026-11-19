"""Independent verification of a regression report.

``verify-diff`` trusts nothing stored in the report or the ledger: given the
manifest, the report and both *raw* histories, it

1. re-hashes every input and the manifest,
2. re-audits both raw histories from scratch,
3. rebuilds every normalized finding identity and re-applies the identity
   mapping and the matching rules,
4. re-checks the report signature, row categories/counts, evidence locations
   and ordering, and the ledger contents,
5. independently confirms that every ``resolved_invalid`` row is real: the
   candidate evidence is complete and the finding genuinely no longer occurs.

A single changed byte in an input, the mapping or the report makes
verification fail with a locatable inconsistency.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from .audit_run import fresh_audit, run_summary
from .compare import REPORT_FORMAT, Comparison, _unsigned_report
from .findings import build_findings
from .hashing import canonical_json, sha256_text
from .ledger import CAT_RESOLVED_INVALID
from .manifest import (
    MANIFEST_FORMAT, ManifestError, load_mapping, recheck_manifest,
    validate_mapping_refs,
)


class Verification:
    def __init__(self) -> None:
        self.problems: List[str] = []
        self.checked: List[str] = []

    def fail(self, where: str, detail: str) -> None:
        self.problems.append(f"{where}: {detail}")

    @property
    def ok(self) -> bool:
        return not self.problems


def _load_json(path: str) -> Any:
    import json
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def verify_diff(manifest_path: str, report_path: str,
                baseline_override: Optional[str] = None,
                candidate_override: Optional[str] = None,
                mapping_override: Optional[str] = None,
                ledger_path: Optional[str] = None,
                memory_budget_mb: int = 512) -> Verification:
    v = Verification()
    # ---- load artifacts ---------------------------------------------------
    try:
        manifest = _load_json(manifest_path)
    except (OSError, ValueError) as e:
        v.fail("manifest", f"cannot load: {e}")
        return v
    try:
        report = _load_json(report_path)
    except (OSError, ValueError) as e:
        v.fail("report", f"cannot load: {e}")
        return v

    # ---- manifest integrity and input/mapping digests ---------------------
    v.checked.append("manifest_signature")
    if not isinstance(manifest, dict) or \
            manifest.get("format") != MANIFEST_FORMAT:
        v.fail("manifest", "unknown format")
        return v
    for p in recheck_manifest(manifest, baseline_override,
                              candidate_override, mapping_override):
        v.fail("inputs", p)

    # ---- report self-signature -------------------------------------------
    v.checked.append("report_signature")
    if not isinstance(report, dict) or report.get("format") != REPORT_FORMAT:
        v.fail("report", "unknown format")
        return v
    stored_sig = report.get("report_sha256")
    actual_sig = sha256_text(canonical_json(_unsigned_report(report)))
    if stored_sig != actual_sig:
        v.fail("report_signature",
               f"report signature mismatch (stored {stored_sig}, "
               f"recomputed {actual_sig}); the report was modified")
    if report.get("manifest_sha256") != manifest.get("manifest_sha256"):
        v.fail("manifest_link",
               "report references a different manifest than the one given")

    # ---- mapping ----------------------------------------------------------
    v.checked.append("identity_mapping")
    mrec = manifest.get("identity_mapping")
    mapping_path = mapping_override or (mrec or {}).get("path")
    try:
        mapping = load_mapping(mapping_path)
    except ManifestError as e:
        v.fail("identity_mapping", str(e))
        return v
    if mrec is None and mapping.source is not None:
        v.fail("identity_mapping",
               "mapping supplied but manifest was built without one")
    if mrec is not None and mapping.source is not None and \
            mapping.source["sha256"] != mrec.get("sha256"):
        v.fail("identity_mapping",
               "mapping digest does not match the manifest record")

    # ---- re-audit both raw histories and re-run matching ------------------
    v.checked.append("fresh_reaudit")
    b_rec, c_rec = manifest["baseline"], manifest["candidate"]
    b_bundle = fresh_audit(
        input_path=b_rec["path"] if b_rec["kind"] == "file" else None,
        input_dir=b_rec["path"] if b_rec["kind"] == "dir" else None,
        memory_budget_mb=memory_budget_mb)
    c_bundle = fresh_audit(
        input_path=c_rec["path"] if c_rec["kind"] == "file" else None,
        input_dir=c_rec["path"] if c_rec["kind"] == "dir" else None,
        memory_budget_mb=memory_budget_mb)
    try:
        try:
            validate_mapping_refs(mapping, b_bundle["txn_ids"],
                                  c_bundle["txn_ids"])
        except ManifestError as e:
            v.fail("identity_mapping", str(e))
        mapping.finalize(b_bundle["txn_ids"], c_bundle["txn_ids"])
        b_findings = build_findings(b_bundle["store"], b_bundle["result"],
                                    mapping, "baseline")
        c_findings = build_findings(c_bundle["store"], c_bundle["result"],
                                    mapping, "candidate")
        v.checked.append("finding_identities")

        # rebuild the expected report through the real matching engine
        expected = Comparison(manifest, mapping, memory_budget_mb)
        expected._index_and_match(b_bundle, c_bundle, b_findings, c_findings)
        b_summary = run_summary(b_bundle, b_rec["run_id"], b_rec)
        c_summary = run_summary(c_bundle, c_rec["run_id"], c_rec)
        expected._unmapped_txns(b_bundle["txn_ids"], c_bundle["txn_ids"])
        expected._verdict_transition(b_summary, c_summary)
        expected.rows.sort(key=lambda r: (r["category"], r["row_key"]))
    finally:
        b_bundle["store"].close()
        c_bundle["store"].close()

    v.checked.append("rows_and_classification")
    _compare_row_sets(v, expected.rows, report.get("rows") or [])
    _compare_counts(v, expected._counts(), report.get("counts") or {})
    _compare_summaries(v, b_summary, c_summary, report)

    # ---- ordering and evidence locations ---------------------------------
    v.checked.append("ordering")
    rows = report.get("rows") or []
    if rows != sorted(rows, key=lambda r: (r.get("category"),
                                           r.get("row_key"))):
        v.fail("ordering", "report rows are not in stable sorted order")
    _check_evidence_locations(v, report, b_bundle, c_bundle)

    # ---- resolved claims need complete candidate evidence -----------------
    v.checked.append("resolved_claims")
    _check_resolved(v, report, c_summary)

    # ---- ledger cross-check ----------------------------------------------
    if ledger_path is not None:
        v.checked.append("ledger")
        _check_ledger(v, ledger_path, report)

    return v


def _compare_row_sets(v: Verification, expected: List[Dict[str, Any]],
                      actual: List[Dict[str, Any]]) -> None:
    exp_keys = {(r["category"], r["row_key"]): r for r in expected}
    act_keys = {(r["category"], r["row_key"]): r for r in actual}
    missing = sorted(set(exp_keys) - set(act_keys))
    extra = sorted(set(act_keys) - set(exp_keys))
    for key in missing:
        v.fail("rows", f"missing expected row category={key[0]} "
                       f"row_key={key[1]} ({exp_keys[key]['reason']})")
    for key in extra:
        v.fail("rows", f"unexpected row category={key[0]} "
                       f"row_key={key[1]} ({act_keys[key].get('reason')})")
    for key in sorted(set(exp_keys) & set(act_keys)):
        e, a = exp_keys[key], act_keys[key]
        for field in ("finding_class", "reason", "baseline_uid",
                      "candidate_uid", "canonical_txn", "op_seq",
                      "identity_json", "baseline_run_id",
                      "candidate_run_id", "evidence_changed"):
            if e.get(field) != a.get(field):
                v.fail("rows",
                       f"row {key[0]}/{key[1]} field {field!r} differs: "
                       f"expected {e.get(field)!r}, report has {a.get(field)!r}")
        for field in ("baseline_evidence", "candidate_evidence"):
            if canonical_json(e.get(field)) != canonical_json(a.get(field)):
                v.fail("rows",
                       f"row {key[0]}/{key[1]} field {field!r} evidence differs")


def _compare_counts(v: Verification, expected: Dict[str, int],
                    actual: Dict[str, int]) -> None:
    if expected != actual:
        v.fail("counts", f"counts differ: expected {expected}, got {actual}")


def _compare_summaries(v: Verification, b_summary: Dict[str, Any],
                       c_summary: Dict[str, Any], report: Dict[str, Any]) -> None:
    runs = report.get("runs") or {}
    for side, summary in (("baseline", b_summary), ("candidate", c_summary)):
        got = runs.get(side)
        if got is None:
            v.fail("runs", f"missing {side} run summary")
            continue
        for field in ("run_id", "verdict", "anomalies_total", "gaps_total",
                      "incomplete_evidence", "transactions"):
            if got.get(field) != summary.get(field):
                v.fail("runs",
                       f"{side}.{field}: report {got.get(field)!r} != "
                       f"recomputed {summary.get(field)!r}")
        if (got.get("input") or {}).get("sha256") != \
                summary["input"]["sha256"]:
            v.fail("runs", f"{side} input hash mismatch")


def _check_evidence_locations(v: Verification, report: Dict[str, Any],
                              b_bundle, c_bundle) -> None:
    # every evidence loc referenced by the report must exist verbatim in the
    # freshly re-audited finding evidence (line-number coordinates are
    # normalized: JSONL record order is not part of a finding identity)
    from .compare import _normalize_evidence
    from .hashing import canonical_json

    def locs(bundle) -> set:
        out = set()
        for a in bundle["result"]["anomalies"]:
            out.add(canonical_json(_normalize_evidence(a.get("loc", ""))))
        for g in bundle["result"]["gaps"]:
            out.add(canonical_json(_normalize_evidence(g.get("loc", ""))))
        graph = bundle["result"].get("graph")
        if graph and graph.get("cycle"):
            for e in graph["cycle"].get("edges", []):
                out.add(canonical_json(
                    _normalize_evidence(e.get("evidence") or {})))
        return out

    b_locs, c_locs = locs(b_bundle), locs(c_bundle)
    for row in report.get("rows") or []:
        be = row.get("baseline_evidence")
        ce = row.get("candidate_evidence")
        if be and be.get("loc") and canonical_json(
                _normalize_evidence(be["loc"])) not in b_locs:
            v.fail("evidence",
                   f"row {row.get('row_key')}: baseline evidence loc "
                   f"{be['loc']!r} not found in the fresh baseline audit")
        if ce and ce.get("loc") and canonical_json(
                _normalize_evidence(ce["loc"])) not in c_locs:
            v.fail("evidence",
                   f"row {row.get('row_key')}: candidate evidence loc "
                   f"{ce['loc']!r} not found in the fresh candidate audit")
        wl = (be or {}).get("witness_locs") if be else None
        if wl:
            for w in wl:
                if canonical_json(_normalize_evidence(w.get("loc") or {})) \
                        not in b_locs:
                    v.fail("evidence",
                           f"row {row.get('row_key')}: baseline cycle witness "
                           f"evidence not found in the fresh audit")


def _check_resolved(v: Verification, report: Dict[str, Any],
                    c_summary: Dict[str, Any]) -> None:
    resolved = [r for r in report.get("rows") or []
                if r.get("category") == CAT_RESOLVED_INVALID]
    if not resolved:
        return
    if c_summary["incomplete_evidence"]:
        v.fail("resolved",
               "report claims resolved findings but the candidate history "
               "has incomplete evidence; 'no change' cannot be proven")
    # the recomputed matching in _compare_row_sets already proves the finding
    # no longer occurs in the candidate; additionally require the candidate
    # verdict not to be unknown for the same evidence domain
    if c_summary["verdict"] == "unknown":
        v.fail("resolved",
               "candidate verdict is unknown: resolved claims require a "
               "definite (valid/invalid) candidate audit")


def _check_ledger(v: Verification, ledger_path: str,
                  report: Dict[str, Any]) -> None:
    import sqlite3
    if not os.path.isfile(ledger_path):
        v.fail("ledger", f"ledger file missing: {ledger_path}")
        return
    db = sqlite3.connect(ledger_path)
    try:
        n = db.execute("SELECT COUNT(*) FROM rows_").fetchone()[0]
        if n != len(report.get("rows") or []):
            v.fail("ledger",
                   f"ledger has {n} rows, report has "
                   f"{len(report.get('rows') or [])}")
        for (rk, cat, ident) in db.execute(
                "SELECT row_key,category,identity_json FROM rows_ "
                "ORDER BY row_key"):
            match = [r for r in report["rows"] if r["row_key"] == rk]
            if not match:
                v.fail("ledger", f"ledger row {rk} absent from report")
            elif match[0]["category"] != cat or \
                    match[0]["identity_json"] != ident:
                v.fail("ledger", f"ledger row {rk} disagrees with report")
        prev = None
        for (rk,) in db.execute("SELECT row_key FROM rows_"):
            if prev is not None and rk < prev:
                v.fail("ledger", "ledger rows are not in stable sorted order")
                break
            prev = rk
        for side in ("baseline", "candidate"):
            row = db.execute("SELECT run_id,input_sha256 FROM runs "
                             "WHERE side=?", (side,)).fetchone()
            rec = report["runs"][side]
            if row is None:
                v.fail("ledger", f"missing runs.{side}")
            elif row[0] != rec["run_id"] or row[1] != rec["input"]["sha256"]:
                v.fail("ledger", f"runs.{side} disagrees with report")
    finally:
        db.close()
