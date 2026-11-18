"""Exhaustive serial-order checker for small histories (ground truth).

Enumerates every serial permutation of the committed transactions that
respects the declared version order (write-write chains) and the real-time
relation, replays each one in fixed operation order, and reports whether any
permutation explains all recorded observations.  Used to cross-check the
graph-based verdict on small histories.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from .model import INITIAL_TXN
from .replay import replay_order
from .store import Store

MAX_BRUTE_FORCE_TXNS = 10


def _precedence_pairs(store: Store) -> Tuple[List[str], Set[Tuple[str, str]]]:
    txns = [t for (t,) in store.db.execute(
        "SELECT txn_id FROM txns WHERE status='committed' ORDER BY txn_id")]
    pairs: Set[Tuple[str, str]] = set()
    # declared version order: consecutive committed versions per key
    prev = None
    for (key, _pos, _vid, tid) in store.db.execute(
            "SELECT key, commit_pos, version_id, txn_id FROM versions "
            "ORDER BY key, commit_pos"):
        if prev is not None and prev[0] == key and prev[1] != tid \
                and prev[1] != INITIAL_TXN and tid != INITIAL_TXN:
            pairs.add((prev[1], tid))
        prev = (key, tid)
    # real time
    rows = store.db.execute(
        "SELECT txn_id, start_time, end_time FROM txns "
        "WHERE status='committed' AND start_time IS NOT NULL "
        "AND end_time IS NOT NULL").fetchall()
    for (a, _sa, ea) in rows:
        for (b, sb, _eb) in rows:
            if a != b and ea < sb:
                pairs.add((a, b))
    return txns, pairs


def brute_force(store: Store) -> Tuple[Optional[bool], Optional[List[str]], str]:
    """Return (serializable?, explaining_order, note)."""
    txns, pairs = _precedence_pairs(store)
    if len(txns) > MAX_BRUTE_FORCE_TXNS:
        return None, None, f"{len(txns)} txns exceed brute-force limit"
    succ: Dict[str, Set[str]] = {t: set() for t in txns}
    pred_count: Dict[str, int] = {t: 0 for t in txns}
    for (a, b) in pairs:
        if b not in succ[a]:
            succ[a].add(b)
            pred_count[b] += 1
    order: List[str] = []
    remaining = set(txns)

    def search() -> Optional[List[str]]:
        if not remaining:
            return list(order)
        for t in sorted(remaining):
            if pred_count[t] == 0:
                remaining.discard(t)
                order.append(t)
                for u in succ[t]:
                    pred_count[u] -= 1
                # replay the prefix only at the leaf; prefixes are cheap enough
                if not remaining:
                    failures = replay_order(store, order, collect_failures=False)
                    if not failures:
                        return list(order)
                else:
                    r = search()
                    if r is not None:
                        return r
                for u in succ[t]:
                    pred_count[u] += 1
                order.pop()
                remaining.add(t)
        return None

    found = search()
    if found is not None:
        return True, found, ""
    return False, None, ""
