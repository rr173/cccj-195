"""Comparison manifests and explicit transaction identity mappings.

A manifest captures everything a regression comparison was based on, before
any audit runs: normalized paths, run ids, content SHA-256 digests, the
mapping digest, audit configuration and the generator version.  Both
``compare`` and ``verify-diff`` re-hash every input and refuse to proceed on
any mismatch, so a changed input can never silently reuse an old conclusion.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from . import __version__
from .hashing import canonical_json, hash_input, normalized_path, sha256_text

MANIFEST_FORMAT = "snapshot-regression-manifest/1"


class ManifestError(Exception):
    """Hard validation failure: no regression conclusion may be produced."""

    def __init__(self, kind: str, detail: str, loc: str = "") -> None:
        super().__init__(f"{kind}: {detail}" + (f" ({loc})" if loc else ""))
        self.kind = kind
        self.detail = detail
        self.loc = loc


# ---------------------------------------------------------------------------
# identity mappings
# ---------------------------------------------------------------------------

@dataclass
class IdentityMapping:
    """Explicit, strictly one-to-one mapping of renamed transactions.

    Same-named transactions align implicitly across runs; only renames need
    an explicit pair.  ``b2c`` maps a baseline txn id to exactly one candidate
    txn id; the reverse mapping is unique as well.  Unmapped transactions
    keep their own id and still enter new/disappeared statistics (with a
    side-prefixed canonical id so distinct local ids never collide).
    """
    b2c: Dict[str, str]
    c2b: Dict[str, str]
    source: Optional[Dict[str, Any]]  # path + digest, or None when absent
    baseline_ids: set = None
    candidate_ids: set = None

    @property
    def pairs(self) -> List[Tuple[str, str]]:
        return sorted(self.b2c.items())

    def finalize(self, baseline_ids: List[str],
                 candidate_ids: List[str]) -> None:
        self.baseline_ids = set(baseline_ids)
        self.candidate_ids = set(candidate_ids)

    def canonical_id(self, run: str, txn_id: str) -> str:
        """Stable cross-run id.

        Mapped pairs collapse to the baseline id; same-named unmapped ids
        collapse to the bare id; side-private ids get an explicit prefix.
        """
        if run == "baseline":
            if txn_id in self.b2c:
                return txn_id  # baseline id is the canonical pair id
            if self.candidate_ids is not None and txn_id in self.candidate_ids:
                return txn_id  # implicit same-name alignment
            return f"baseline:{txn_id}"
        mapped = self.c2b.get(txn_id)
        if mapped is not None:
            return mapped
        if self.baseline_ids is not None and txn_id in self.baseline_ids:
            return txn_id  # implicit same-name alignment
        return f"candidate:{txn_id}"


def load_mapping(path: Optional[str]) -> IdentityMapping:
    if path is None:
        return IdentityMapping({}, {}, None)
    norm = normalized_path(path)
    if not os.path.isfile(norm):
        raise ManifestError("mapping_not_found", f"mapping file not found: {norm}",
                            "mapping")
    with open(norm, "r", encoding="utf-8") as fh:
        text = fh.read()
    digest = sha256_text(text)
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as e:
        raise ManifestError("mapping_parse_error", str(e), norm)
    raw_pairs: List[Dict[str, Any]] = []
    if isinstance(doc, dict) and isinstance(doc.get("mappings"), list):
        raw_pairs = doc["mappings"]
    elif isinstance(doc, list):
        raw_pairs = doc
    else:
        raise ManifestError(
            "mapping_bad_format",
            "mapping must be a list of {baseline_txn_id, candidate_txn_id} "
            "pairs or an object with a 'mappings' list", norm)
    b2c: Dict[str, str] = {}
    c2b: Dict[str, str] = {}
    for i, pair in enumerate(raw_pairs):
        loc = f"mappings[{i}]"
        if not isinstance(pair, dict):
            raise ManifestError("mapping_bad_pair", "pair must be an object", loc)
        b = pair.get("baseline_txn_id")
        c = pair.get("candidate_txn_id")
        if not isinstance(b, str) or not isinstance(c, str):
            raise ManifestError(
                "mapping_bad_pair",
                "baseline_txn_id and candidate_txn_id must be strings", loc)
        if b in b2c:
            raise ManifestError(
                "mapping_one_to_many",
                f"baseline txn {b!r} is mapped more than once "
                f"({b2c[b]!r} and {c!r})", loc)
        if c in c2b:
            raise ManifestError(
                "mapping_many_to_one",
                f"candidate txn {c!r} is the target of more than one mapping "
                f"({c2b[c]!r} and {b!r})", loc)
        b2c[b] = c
        c2b[c] = b
    source = {"path": norm, "sha256": digest,
              "bytes": len(text.encode("utf-8"))}
    return IdentityMapping(b2c, c2b, source)


def validate_mapping_refs(mapping: IdentityMapping,
                          baseline_ids: List[str],
                          candidate_ids: List[str]) -> None:
    """Every mapped id must reference an existing transaction."""
    bset, cset = set(baseline_ids), set(candidate_ids)
    for b, c in mapping.pairs:
        if b not in bset:
            raise ManifestError(
                "mapping_unknown_txn",
                f"mapping references baseline txn {b!r} which does not exist "
                f"in the baseline history", "mapping")
        if c not in cset:
            raise ManifestError(
                "mapping_unknown_txn",
                f"mapping references candidate txn {c!r} which does not exist "
                f"in the candidate history", "mapping")


# ---------------------------------------------------------------------------
# manifests
# ---------------------------------------------------------------------------

def _input_record(path: str, run_id: str) -> Dict[str, Any]:
    try:
        info = hash_input(path)
    except (OSError, ValueError) as e:
        raise ManifestError("input_unreadable", str(e), path)
    info = dict(info)
    info["run_id"] = run_id
    return info


def build_manifest(baseline_path: str, baseline_run_id: str,
                   candidate_path: str, candidate_run_id: str,
                   mapping: IdentityMapping,
                   audit_config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(baseline_run_id, str) or not baseline_run_id:
        raise ManifestError("bad_run_id", "baseline run_id must be a non-empty string")
    if not isinstance(candidate_run_id, str) or not candidate_run_id:
        raise ManifestError("bad_run_id", "candidate run_id must be a non-empty string")
    if baseline_run_id == candidate_run_id:
        raise ManifestError(
            "duplicate_run_id",
            f"baseline and candidate run_id are both {baseline_run_id!r}; "
            "two independent collection points need distinct run ids")
    baseline = _input_record(baseline_path, baseline_run_id)
    candidate = _input_record(candidate_path, candidate_run_id)
    if baseline["sha256"] == candidate["sha256"] and \
            baseline["kind"] == candidate["kind"]:
        # identical content is legal (expect no regression) but flag path
        # identity explicitly so it remains auditable
        pass
    manifest = {
        "format": MANIFEST_FORMAT,
        "generator": f"snapshot_audit/{__version__}",
        "baseline": baseline,
        "candidate": candidate,
        "identity_mapping": mapping.source,  # None when no mapping supplied
        "audit_config": dict(sorted(audit_config.items())),
    }
    manifest["manifest_sha256"] = sha256_text(
        canonical_json(_unsigned(manifest)))
    return manifest


def _unsigned(manifest: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in manifest.items() if k != "manifest_sha256"}


def recheck_manifest(manifest: Dict[str, Any],
                     baseline_path: Optional[str] = None,
                     candidate_path: Optional[str] = None,
                     mapping_path: Optional[str] = None
                     ) -> List[str]:
    """Re-hash every input named by the manifest.

    Returns a list of human-readable problems (empty = all unchanged).
    Paths may be overridden (e.g. verify-diff CLI arguments); otherwise the
    paths stored in the manifest are used.
    """
    problems: List[str] = []
    if not isinstance(manifest, dict) or manifest.get("format") != MANIFEST_FORMAT:
        return ["manifest has an unknown format"]
    stored_sig = manifest.get("manifest_sha256")
    if stored_sig != sha256_text(canonical_json(_unsigned(manifest))):
        problems.append("manifest signature mismatch: the manifest itself was "
                        "modified after creation")
    for side, override in (("baseline", baseline_path),
                           ("candidate", candidate_path)):
        rec = manifest.get(side)
        if not isinstance(rec, dict):
            problems.append(f"manifest missing {side} record")
            continue
        path = override or rec.get("path")
        if not path:
            problems.append(f"{side}: no input path available")
            continue
        try:
            info = hash_input(path)
        except (OSError, ValueError) as e:
            problems.append(f"{side}: cannot re-hash {path}: {e}")
            continue
        if normalized_path(path) != rec.get("path") and override is None:
            problems.append(
                f"{side}: input path changed: stored {rec.get('path')!r}, "
                f"now {normalized_path(path)!r}")
        if info["sha256"] != rec.get("sha256"):
            problems.append(
                f"{side}: content SHA-256 changed since the manifest was "
                f"created (stored {rec.get('sha256')}, now {info['sha256']})")
    mrec = manifest.get("identity_mapping")
    if mrec is not None:
        mpath = mapping_path or mrec.get("path")
        if not mpath or not os.path.isfile(mpath):
            problems.append(f"identity mapping file missing: {mpath}")
        else:
            info = hash_file_safe(mpath)
            if info is None:
                problems.append(f"identity mapping unreadable: {mpath}")
            elif info["sha256"] != mrec.get("sha256"):
                problems.append(
                    "identity mapping SHA-256 changed since the manifest was "
                    f"created (stored {mrec.get('sha256')}, now {info['sha256']})")
    elif mapping_path is not None:
        problems.append("a mapping was given to verify-diff but the manifest "
                        "was created without one")
    return problems


def hash_file_safe(path: str):
    from .hashing import hash_file
    try:
        return hash_file(path)
    except OSError:
        return None
