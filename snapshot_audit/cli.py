"""Command line interface: ``audit``, ``verify-witness``, ``compare``,
``verify-diff``, ``lineage`` and ``verify-lineage``."""

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
# compare-specific exit codes reuse the same numeric convention:
# 0 no regression / improved, 1 regression, 2 unknown, 3 usage/input error


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


def _input_path(args, prefix: str) -> Optional[str]:
    single = getattr(args, f"{prefix}_input", None)
    directory = getattr(args, f"{prefix}_input_dir", None)
    return single or directory


def cmd_compare(args) -> int:
    from .compare import run_comparison, write_ledger
    from .manifest import ManifestError, build_manifest, load_mapping
    b_path, c_path = _input_path(args, "baseline"), _input_path(args, "candidate")
    if not b_path or not c_path:
        print("compare: both --baseline-input/--baseline-input-dir and "
              "--candidate-input/--candidate-input-dir are required",
              file=sys.stderr)
        return EXIT_ERROR
    try:
        mapping = load_mapping(args.mapping)
        audit_config = {"memory_budget_mb": args.memory_budget_mb,
                        "tool": f"snapshot_audit/{__version__}"}
        manifest = build_manifest(
            b_path, args.baseline_run_id, c_path, args.candidate_run_id,
            mapping, audit_config)
    except (ManifestError, OSError) as e:
        print(f"compare: manifest error: {e}", file=sys.stderr)
        return EXIT_ERROR
    try:
        report = run_comparison(manifest, mapping,
                                memory_budget_mb=args.memory_budget_mb)
    except (ContractError, OSError, json.JSONDecodeError, ManifestError) as e:
        print(f"compare: input error: {e}", file=sys.stderr)
        return EXIT_ERROR
    out = json.dumps(report, indent=2, ensure_ascii=False, sort_keys=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(out + "\n")
    else:
        print(out)
    if args.manifest_out:
        with open(args.manifest_out, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    if args.ledger:
        ledger = write_ledger(args.ledger, report, report["stats"])
        ledger.close()
        print(f"compare: ledger written to {args.ledger}", file=sys.stderr)
    return {"no_regression": EXIT_VALID, "improved": EXIT_VALID,
            "regression": EXIT_INVALID, "unknown": EXIT_UNKNOWN}[
        report["status"]]


def cmd_verify_diff(args) -> int:
    from .verify_diff import verify_diff
    try:
        verification = verify_diff(
            args.manifest, args.report,
            baseline_override=args.baseline_input or args.baseline_input_dir,
            candidate_override=args.candidate_input or args.candidate_input_dir,
            mapping_override=args.mapping,
            ledger_path=args.ledger,
            memory_budget_mb=args.memory_budget_mb)
    except (OSError, json.JSONDecodeError) as e:
        print(f"verify-diff: error: {e}", file=sys.stderr)
        return EXIT_ERROR
    out = {"verified": verification.ok, "checked": verification.checked,
           "problems": verification.problems}
    print(json.dumps(out, indent=2, ensure_ascii=False))
    if not verification.ok:
        return EXIT_INVALID
    return EXIT_VALID


def _lineage_query(args) -> Dict[str, Any]:
    from .lineage import build_query
    return build_query(args.commit_pos, key=args.key, lo=args.lo, hi=args.hi)


def cmd_lineage(args) -> int:
    from .hashing import hash_input
    from .lineage import run
    try:
        query = _lineage_query(args)
    except ContractError as e:
        print(f"lineage: query error: {e}", file=sys.stderr)
        return EXIT_ERROR
    path = args.input or args.input_dir
    try:
        info0 = hash_input(path)
    except (OSError, ValueError) as e:
        print(f"lineage: input error: {e}", file=sys.stderr)
        return EXIT_ERROR
    try:
        store = _open_store(args)
    except (ContractError, OSError, json.JSONDecodeError) as e:
        print(f"lineage: input error: {e}", file=sys.stderr)
        return EXIT_ERROR
    try:
        result = run(store, info0, query, max_depth=args.max_depth,
                     max_nodes=args.max_nodes)
    finally:
        db_path = store.db_path
        store.close()
    # the raw history must not change while the lineage is computed:
    # re-hash and refuse to emit a success result on any drift
    try:
        info1 = hash_input(path)
    except (OSError, ValueError) as e:
        print(f"lineage: input error: {e}", file=sys.stderr)
        return EXIT_ERROR
    if info1["sha256"] != info0["sha256"]:
        print("lineage: input changed during execution; refusing to emit "
              "a success result", file=sys.stderr)
        return EXIT_ERROR
    out = json.dumps(result, indent=2, ensure_ascii=False, sort_keys=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(out + "\n")
    else:
        print(out)
    if args.keep_db:
        print(f"lineage db kept at {db_path}", file=sys.stderr)
    return EXIT_VALID if result["status"] == "complete" else EXIT_UNKNOWN


def cmd_verify_lineage(args) -> int:
    from .lineage import verify_lineage
    try:
        with open(args.lineage, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as e:
        print(f"verify-lineage: cannot read lineage document: {e}",
              file=sys.stderr)
        return EXIT_ERROR
    expect = None
    if args.commit_pos is not None:
        try:
            expect = _lineage_query(args)
        except ContractError as e:
            print(f"verify-lineage: query error: {e}", file=sys.stderr)
            return EXIT_ERROR
    verification = verify_lineage(
        doc, input_path=args.input, input_dir=args.input_dir,
        expect_query=expect)
    out = {"verified": verification.ok, "checked": verification.checked,
           "problems": verification.problems}
    print(json.dumps(out, indent=2, ensure_ascii=False))
    if verification.ok:
        return EXIT_VALID
    return EXIT_ERROR if verification.fatal else EXIT_INVALID


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

    def add_side_input(p, prefix: str, label: str) -> None:
        g = p.add_mutually_exclusive_group(required=True)
        g.add_argument(f"--{prefix}-input",
                       help=f"{label} single-document history JSON")
        g.add_argument(f"--{prefix}-input-dir",
                       help=f"{label} JSONL history directory")

    p_compare = sub.add_parser(
        "compare",
        help="cross-run regression comparison of two independent raw histories")
    add_side_input(p_compare, "baseline", "baseline")
    add_side_input(p_compare, "candidate", "candidate")
    p_compare.add_argument("--baseline-run-id", required=True)
    p_compare.add_argument("--candidate-run-id", required=True)
    p_compare.add_argument(
        "--mapping",
        help="optional explicit one-to-one txn identity mapping JSON "
             "(list of {baseline_txn_id, candidate_txn_id})")
    p_compare.add_argument("--out", help="write the regression report JSON here")
    p_compare.add_argument("--manifest-out",
                           help="write the comparison manifest JSON here")
    p_compare.add_argument("--ledger", help="write the SQLite ledger here")
    p_compare.add_argument("--memory-budget-mb", type=int, default=512)
    p_compare.set_defaults(func=cmd_compare)

    p_vdiff = sub.add_parser(
        "verify-diff",
        help="re-audit raw histories and re-verify a regression report")
    p_vdiff.add_argument("--manifest", required=True,
                         help="comparison manifest produced by compare")
    p_vdiff.add_argument("--report", required=True,
                         help="regression report produced by compare")
    add_side_input(p_vdiff, "baseline", "baseline")
    add_side_input(p_vdiff, "candidate", "candidate")
    p_vdiff.add_argument("--mapping",
                         help="mapping file (must match the manifest digest)")
    p_vdiff.add_argument("--ledger", help="optional SQLite ledger to cross-check")
    p_vdiff.add_argument("--memory-budget-mb", type=int, default=512)
    p_vdiff.set_defaults(func=cmd_verify_diff)

    def add_query(p) -> None:
        p.add_argument("--commit-pos", type=int, required=True,
                       help="historical position to reconstruct")
        p.add_argument("--key", type=int,
                       help="point query: current value of this key")
        p.add_argument("--lo", type=int,
                       help="range query: closed interval lower bound "
                            "(needs --hi)")
        p.add_argument("--hi", type=int,
                       help="range query: closed interval upper bound "
                            "(needs --lo)")

    p_lin = sub.add_parser(
        "lineage",
        help="observation-dependency lineage (观察依赖血缘) of the value(s) "
             "at a historical commit_pos")
    add_input(p_lin)
    add_query(p_lin)
    p_lin.add_argument("--max-depth", type=int, default=64,
                       help="max expansion depth from the result versions")
    p_lin.add_argument("--max-nodes", type=int, default=8192,
                       help="max version nodes materialized")
    p_lin.add_argument("--work-dir", help="directory for the on-disk store")
    p_lin.add_argument("--keep-db", action="store_true",
                       help="keep the on-disk store after the run")
    p_lin.add_argument("--out", help="write the lineage JSON here")
    p_lin.set_defaults(func=cmd_lineage)

    p_vlin = sub.add_parser(
        "verify-lineage",
        help="re-derive a lineage from the raw history and re-check it")
    g = p_vlin.add_mutually_exclusive_group()
    g.add_argument("--input", help="single-document history JSON "
                                   "(default: the path bound in the lineage)")
    g.add_argument("--input-dir", help="JSONL history directory")
    p_vlin.add_argument("--lineage", required=True,
                        help="lineage JSON produced by the lineage command")
    p_vlin.add_argument("--commit-pos", type=int,
                        help="assert the lineage answers this commit_pos")
    p_vlin.add_argument("--key", type=int,
                        help="assert the lineage answers this key")
    p_vlin.add_argument("--lo", type=int,
                        help="assert the lineage answers this interval lo")
    p_vlin.add_argument("--hi", type=int,
                        help="assert the lineage answers this interval hi")
    p_vlin.set_defaults(func=cmd_verify_lineage)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
