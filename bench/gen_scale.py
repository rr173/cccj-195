#!/usr/bin/env python3
"""Generate scale histories (JSONL directory format) for benchmarking.

The generator simulates snapshot-isolated execution, so every recorded read
is snapshot-correct by construction.  A configurable number of write-skew
pairs injects real conflicts (cycles).  Output layout:

    OUT/header.json         format id, initial state, log range declaration
    OUT/commits.jsonl       one commit per line
    OUT/transactions.jsonl  one transaction per line

Example:
    python3 bench/gen_scale.py --out /tmp/h1 --txns 2000 --keys 500 \
        --reads 3 --ranges 1 --range-width 40 --skew-pairs 50 --seed 7
"""

import argparse
import bisect
import json
import os
import random


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--txns", type=int, default=1000)
    ap.add_argument("--keys", type=int, default=200)
    ap.add_argument("--reads", type=int, default=3, help="point reads per txn")
    ap.add_argument("--ranges", type=int, default=1, help="range reads per txn")
    ap.add_argument("--range-width", type=int, default=50)
    ap.add_argument("--writes", type=int, default=1, help="writes per txn")
    ap.add_argument("--skew-pairs", type=int, default=0,
                    help="write-skew cycle pairs to inject")
    ap.add_argument("--snapshot-mode", choices=["random", "current"],
                    default="random",
                    help="'current' gives every txn the latest snapshot")
    ap.add_argument("--no-overlap", action="store_true",
                    help="disjoint txn time windows (with current snapshots "
                         "this yields serial-explainable histories)")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)

    n_keys = args.keys
    # committed versions per key: list of (commit_pos, version_id, value|None)
    committed = {k: [(0, f"i{k}", 0)] for k in range(n_keys)}
    sorted_keys = list(range(n_keys))

    commits = []
    txns = []
    pos = 0
    time_cursor = 0

    # pre-plan skew pairs on distinct txns
    skew_txns = {}
    if args.skew_pairs:
        pool = list(range(args.txns))
        rng.shuffle(pool)
        for i in range(min(args.skew_pairs, args.txns // 2)):
            a, b = pool[2 * i], pool[2 * i + 1]
            kx = rng.randrange(n_keys)
            ky = rng.randrange(n_keys)
            while ky == kx and n_keys > 1:
                ky = rng.randrange(n_keys)
            skew_txns[a] = (kx, ky, b)
            skew_txns[b] = (ky, kx, a)

    def visible(key, snapshot):
        vis = None
        for (p, vid, val) in committed[key]:
            if p <= snapshot:
                vis = (vid, val)
            else:
                break
        return vis

    for t in range(args.txns):
        tid = f"T{t}"
        if args.snapshot_mode == "current":
            snapshot = pos
        else:
            snapshot = rng.randint(0, pos) if pos else 0
        ops = []
        writes = {}
        seq = 0

        skew = skew_txns.get(t)
        if skew is not None:
            k_read, k_write, _other = skew
            vis = visible(k_read, snapshot)
            if vis is not None and vis[1] is not None:
                ops.append({"seq": seq, "type": "point_read", "key": k_read,
                            "found": True, "value": vis[1],
                            "version_id": vis[0]})
            else:
                ops.append({"seq": seq, "type": "point_read", "key": k_read,
                            "found": False})
            seq += 1
            writes[k_write] = rng.randint(1, 10**6)

        for _ in range(args.reads):
            key = rng.randrange(n_keys)
            if key in writes:
                continue
            vis = visible(key, snapshot)
            if vis is not None and vis[1] is not None:
                ops.append({"seq": seq, "type": "point_read", "key": key,
                            "found": True, "value": vis[1],
                            "version_id": vis[0]})
            else:
                ops.append({"seq": seq, "type": "point_read", "key": key,
                            "found": False})
            seq += 1

        for _ in range(args.ranges):
            lo = rng.randrange(n_keys)
            hi = min(n_keys - 1, lo + args.range_width)
            entries = []
            i = bisect.bisect_left(sorted_keys, lo)
            while i < len(sorted_keys) and sorted_keys[i] <= hi:
                k = sorted_keys[i]
                # puts are emitted after the scans (see below), so at scan
                # time the expected result is purely the snapshot state
                vis = visible(k, snapshot)
                if vis is not None and vis[1] is not None:
                    entries.append({"key": k, "value": vis[1],
                                    "version_id": vis[0]})
                i += 1
            ops.append({"seq": seq, "type": "range_read", "lo": lo, "hi": hi,
                        "entries": entries, "complete": True})
            seq += 1

        for _ in range(args.writes):
            key = rng.randrange(n_keys)
            if key not in writes:
                writes[key] = rng.randint(1, 10**6)
        for key, val in writes.items():
            ops.append({"seq": seq, "type": "put", "key": key, "value": val})
            seq += 1

        if args.no_overlap:
            start = time_cursor + 1
            end = start + 1
        else:
            start = time_cursor + rng.choice([0, 0, 0, 1])
            end = start + rng.randint(1, 4)
        time_cursor = max(time_cursor, end)
        txns.append({"txn_id": tid, "status": "committed",
                     "snapshot_pos": snapshot, "start_time": start,
                     "end_time": end, "operations": ops})
        if writes:
            pos += 1
            ws = []
            for key, val in writes.items():
                pred = committed[key][-1][1]
                vid = f"v{pos}k{key}"
                ws.append({"key": key, "version_id": vid,
                           "predecessor": pred, "value": val})
                committed[key].append((pos, vid, val))
            commits.append({"txn_id": tid, "commit_pos": pos, "writes": ws})

    header = {
        "format": "snapshot-history/1",
        "initial_state": [{"key": k, "value": 0, "version_id": f"i{k}"}
                          for k in range(n_keys)],
        "commit_log": {"first_commit_pos": 1 if commits else None,
                       "last_commit_pos": pos if commits else None,
                       "complete": True},
    }
    with open(os.path.join(args.out, "header.json"), "w") as fh:
        json.dump(header, fh)
    with open(os.path.join(args.out, "commits.jsonl"), "w") as fh:
        for c in commits:
            fh.write(json.dumps(c) + "\n")
    with open(os.path.join(args.out, "transactions.jsonl"), "w") as fh:
        for t in txns:
            fh.write(json.dumps(t) + "\n")
    print(f"wrote {args.out}: {len(txns)} txns, {len(commits)} commits, "
          f"{sum(len(c['writes']) for c in commits)} versions, "
          f"{args.skew_pairs} skew pairs")


if __name__ == "__main__":
    main()
