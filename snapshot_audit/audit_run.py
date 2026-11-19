"""Re-audit one raw history for regression comparison.

Every comparison side is audited from its *raw* history through the same
read-only pipeline as ``audit``.  Old ``result.json`` files are never read as
facts.  Unlike the CLI audit, the finding lists are kept complete (the CLI
truncation limit is presentation only) so matching is exact.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .graph import analyze_graph, build_edges
from .model import load_history
from .reads import verify_reads
from .replay import replay_order
from .store import Store
from .witness import verify_cycle_witness

# gap categories that make a history record-incomplete; such a side can never
# support 'resolved' and may only participate in unknown transitions
INCOMPLETENESS_GAP_KINDS = {
    "log_range_incomplete", "log_range_gap", "log_range_undeclared",
    "duplicate_commit_pos", "commit_pos_not_increasing",
    "missing_operations", "incomplete_scan", "range_entries_missing",
    "version_chain_broken", "missing_version_id", "bad_predecessor",
    "committed_writes_missing_from_log", "txn_writes_vs_log_mismatch",
    "missing_write_ops", "commit_without_txn_record",
    "duplicate_version_id", "graph_exceeds_memory_budget",
    "unverifiable_read",
}


def fresh_audit(input_path: str = None, input_dir: str = None,
                memory_budget_mb: int = 512,
                work_dir: str = None) -> Dict[str, Any]:
    """Ingest and audit a raw history; return store + full audit result.

    The caller owns the returned store and must close it.
    """
    store = Store(work_dir=work_dir)
    try:
        header, commits, txns = load_history(input_path, input_dir)
        store.ingest(header, commits, txns)
    except Exception:
        store.close()
        raise

    result: Dict[str, Any] = {
        "verdict": "unknown",
        "anomalies": [],
        "gaps": [],
        "graph": None,
        "certificate": None,
        "stats": {},
    }
    read_report = verify_reads(store)
    anomalies = read_report.anomalies
    gaps = list(store.gaps) + read_report.gaps
    # full, untruncated finding lists for exact identity matching
    result["anomalies"] = [a.to_json() for a in anomalies]
    result["gaps"] = [g.to_json() for g in gaps]
    result["anomalies_total"] = len(anomalies)
    result["gaps_total"] = len(gaps)

    if anomalies:
        result["verdict"] = "invalid"
        result["invalid_reason"] = "read_anomalies"
    elif gaps:
        result["verdict"] = "unknown"
        result["unknown_reason"] = "evidence gaps prevent a definite verdict"
    else:
        build_edges(store)
        gres = analyze_graph(store, memory_budget_mb * 1024 * 1024)
        if gres.gap is not None:
            result["verdict"] = "unknown"
            result["unknown_reason"] = gres.gap.detail
            result["gaps"].append(gres.gap.to_json())
            result["gaps_total"] += 1
        elif gres.acyclic is False:
            problems = verify_cycle_witness(store, gres.cycle or [])
            if problems:
                result["verdict"] = "unknown"
                result["unknown_reason"] = (
                    "internal: cycle witness failed verification: "
                    + "; ".join(problems))
            else:
                result["verdict"] = "invalid"
                result["invalid_reason"] = "dependency_cycle"
                result["graph"] = {
                    "nodes": gres.nodes,
                    "distinct_edges": gres.distinct_edges,
                    "acyclic": False,
                    "cycle": {"edges": gres.cycle,
                              "minimal": gres.cycle_minimal,
                              "independently_verified": True},
                }
        else:
            order = gres.topo_order or []
            failures = replay_order(store, order)
            if failures:
                result["verdict"] = "unknown"
                result["unknown_reason"] = (
                    "internal: topological order failed replay: "
                    + "; ".join(f"{f.txn_id}#{f.op_seq}: {f.detail}"
                                for f in failures[:5]))
            else:
                result["verdict"] = "valid"
                result["graph"] = {"nodes": gres.nodes,
                                   "distinct_edges": gres.distinct_edges,
                                   "acyclic": True}
                result["certificate"] = {
                    "kind": "replay_certificate",
                    "order": order,
                    "reads_checked": store.stats.counters.get(
                        "point_reads_checked", 0)
                        + store.stats.counters.get("range_reads_checked", 0),
                    "replayed": True}
    result["stats"] = store.stats.to_json()
    txn_ids = [t for (t,) in store.db.execute(
        "SELECT txn_id FROM txns ORDER BY txn_id").fetchall()]
    gap_kinds = sorted({g["kind"] for g in result["gaps"]})
    incomplete = any(k in INCOMPLETENESS_GAP_KINDS for k in gap_kinds)
    bundle = {
        "store": store,
        "result": result,
        "txn_ids": txn_ids,
        "gap_kinds": gap_kinds,
        "incomplete": incomplete,
    }
    return bundle


def run_summary(bundle: Dict[str, Any], run_id: str,
                input_record: Dict[str, Any]) -> Dict[str, Any]:
    r = bundle["result"]
    return {
        "run_id": run_id,
        "input": {"path": input_record["path"],
                  "kind": input_record["kind"],
                  "sha256": input_record["sha256"]},
        "verdict": r["verdict"],
        "invalid_reason": r.get("invalid_reason"),
        "unknown_reason": r.get("unknown_reason"),
        "anomalies_total": r["anomalies_total"],
        "gaps_total": r["gaps_total"],
        "gap_kinds": bundle["gap_kinds"],
        "incomplete_evidence": bundle["incomplete"],
        "transactions": len(bundle["txn_ids"]),
        "audit_stats": r["stats"],
    }
