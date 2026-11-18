"""Command line interface: ``audit`` and ``verify-witness``."""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from typing import Any, Dict, List, Optional

from . import __version__
from .graph import analyze_graph, build_edges
from .model import ContractError, load_history
from .reads import verify_reads
from .replay import replay_order
from .store import Store
from .witness import (
    verify_anomaly, verify_certificate, verify_cycle_witness,
)

EXIT_VALID = 0
EXIT_INVALID = 1
EXIT_UNKNOWN = 2
EXIT_ERROR = 3


def _rss_bytes() -> int:
    # ru_maxrss is KiB on Linux, bytes on macOS
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss
    return rss * 1024


def run_audit(store: Store, memory_budget_mb: int = 512,
              max_report: int = 100, want_bruteforce: bool = False,
              ) -> Dict[str, Any]:
    t0 = time.monotonic()
    result: Dict[str, Any] = {
        "auditor": f"snapshot_audit/{__version__}",
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

    total_anomalies = len(anomalies)
    total_gaps = len(gaps)
    result["anomalies"] = [a.to_json() for a in anomalies[:max_report]]
    result["gaps"] = [g.to_json() for g in gaps[:max_report]]
    result["anomalies_total"] = total_anomalies
    result["gaps_total"] = total_gaps
    result["anomalies_truncated"] = total_anomalies > max_report
    result["gaps_truncated"] = total_gaps > max_report

    if anomalies:
        # Confirmed violations exist; report them (reads already located the
        # exact operations).  No graph is needed for an invalid verdict.
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
                # never emit an unverified cycle
                result["verdict"] = "unknown"
                result["unknown_reason"] = ("internal: cycle witness failed "
                                            "verification: " + "; ".join(problems))
            else:
                result["verdict"] = "invalid"
                result["invalid_reason"] = "dependency_cycle"
                result["graph"] = {
                    "nodes": gres.nodes,
                    "distinct_edges": gres.distinct_edges,
                    "acyclic": False,
                    "cycle": {
                        "edges": gres.cycle,
                        "minimal": gres.cycle_minimal,
                        "independently_verified": True,
                    },
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
                result["graph"] = {
                    "nodes": gres.nodes,
                    "distinct_edges": gres.distinct_edges,
                    "acyclic": True,
                }
                result["certificate"] = {
                    "kind": "replay_certificate",
                    "order": order,
                    "reads_checked": store.stats.counters.get(
                        "point_reads_checked", 0)
                        + store.stats.counters.get("range_reads_checked", 0),
                    "replayed": True,
                }

    if want_bruteforce and result["verdict"] in ("valid", "invalid"):
        from .bruteforce import brute_force
        bf_verdict, bf_order, note = brute_force(store)
        bf = {"verdict": bf_verdict, "order": bf_order, "note": note}
        if bf_verdict is not None:
            graph_valid = result["verdict"] == "valid"
            bf["agrees_with_graph"] = (bf_verdict == graph_valid)
        result["bruteforce"] = bf

    result["stats"] = store.stats.to_json()
    result["stats"]["rss_bytes"] = _rss_bytes()
    result["stats"]["wall_time_ms"] = int((time.monotonic() - t0) * 1000)
    result["index_plan"] = store.index_plan()
    return result


def _open_store(args) -> Store:
    store = Store(work_dir=getattr(args, "work_dir", None),
                  keep=getattr(args, "keep_db", False))
    header, commits, txns = load_history(args.input, args.input_dir)
    store.ingest(header, commits, txns)
    return store


def cmd_audit(args) -> int:
    try:
        store = _open_store(args)
    except (ContractError, OSError, json.JSONDecodeError) as e:
        print(f"audit: input error: {e}", file=sys.stderr)
        return EXIT_ERROR
    try:
        result = run_audit(store, memory_budget_mb=args.memory_budget_mb,
                           max_report=args.max_report,
                           want_bruteforce=args.bruteforce)
    finally:
        db_path = store.db_path
        store.close()
    out = json.dumps(result, indent=2, ensure_ascii=False, sort_keys=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(out + "\n")
    else:
        print(out)
    if args.keep_db:
        print(f"audit db kept at {db_path}", file=sys.stderr)
    return {"valid": EXIT_VALID, "invalid": EXIT_INVALID,
            "unknown": EXIT_UNKNOWN}[result["verdict"]]


def cmd_verify_witness(args) -> int:
    try:
        store = _open_store(args)
    except (ContractError, OSError, json.JSONDecodeError) as e:
        print(f"verify-witness: input error: {e}", file=sys.stderr)
        return EXIT_ERROR
    try:
        with open(args.witness, "r", encoding="utf-8") as fh:
            witness = json.load(fh)
    except (OSError, json.JSONDecodeError) as e:
        print(f"verify-witness: cannot read witness: {e}", file=sys.stderr)
        store.close()
        return EXIT_ERROR
    try:
        problems: List[str] = []
        checked: List[str] = []
        # accept either a full audit result or a bare witness object
        cycle = None
        cert = None
        anomalies: List[Dict[str, Any]] = []
        if isinstance(witness, dict) and "verdict" in witness:
            if witness.get("graph") and witness["graph"].get("cycle"):
                cycle = witness["graph"]["cycle"].get("edges")
            cert = witness.get("certificate")
            anomalies = witness.get("anomalies") or []
        elif isinstance(witness, dict) and witness.get("kind") == "cycle":
            cycle = witness.get("edges")
        elif isinstance(witness, dict) and \
                witness.get("kind") == "replay_certificate":
            cert = witness
        elif isinstance(witness, dict) and witness.get("kind"):
            anomalies = [witness]

        if cycle:
            checked.append("cycle")
            problems.extend(verify_cycle_witness(store, cycle))
        if cert:
            checked.append("certificate")
            problems.extend(verify_certificate(store, cert.get("order") or []))
        for a in anomalies:
            checked.append(f"anomaly:{a.get('kind')}")
            problems.extend(verify_anomaly(store, a))
        if not checked:
            print("verify-witness: nothing verifiable in witness "
                  "(verdict may have been 'unknown')", file=sys.stderr)
            store.close()
            return EXIT_UNKNOWN
    finally:
        store.close()
    report = {"checked": checked, "problems": problems,
              "verified": not problems}
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return EXIT_VALID if not problems else EXIT_INVALID


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="snapshot-audit",
        description="Read-only offline auditor for snapshot transaction "
                    "histories. It never schedules transactions, never "
                    "modifies the source, and never repairs history.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_input(p) -> None:
        g = p.add_mutually_exclusive_group(required=True)
        g.add_argument("--input", help="single-document history JSON")
        g.add_argument("--input-dir",
                       help="directory with header.json, commits.jsonl, "
                            "transactions.jsonl (streaming)")

    p_audit = sub.add_parser("audit", help="audit a history")
    add_input(p_audit)
    p_audit.add_argument("--memory-budget-mb", type=int, default=512,
                         help="memory budget for in-memory graph analysis")
    p_audit.add_argument("--max-report", type=int, default=100,
                         help="max anomalies/gaps listed (counts stay exact)")
    p_audit.add_argument("--bruteforce", action="store_true",
                         help="also run the exhaustive serial-order check "
                              "(small histories) and compare")
    p_audit.add_argument("--work-dir", help="directory for the on-disk store")
    p_audit.add_argument("--keep-db", action="store_true",
                         help="keep the on-disk store after the run")
    p_audit.add_argument("--out", help="write the audit result JSON here")
    p_audit.set_defaults(func=cmd_audit)

    p_verify = sub.add_parser("verify-witness",
                              help="re-check a witness from the raw history")
    add_input(p_verify)
    p_verify.add_argument("--witness", required=True,
                          help="audit result JSON or bare witness JSON")
    p_verify.add_argument("--work-dir", help="directory for the on-disk store")
    p_verify.set_defaults(func=cmd_verify_witness)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
