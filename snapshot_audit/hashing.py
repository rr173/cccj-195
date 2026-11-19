"""Deterministic content hashing and canonical JSON encoding.

The regression tool identifies inputs purely by content (SHA-256), never by
path or mtime alone, so re-running against byte-identical data yields
byte-identical reports.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, List

CHUNK = 1024 * 1024


def canonical_json(obj: Any) -> str:
    """Canonical JSON text: sorted keys, no insignificant whitespace."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def hash_file(path: str) -> Dict[str, Any]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return {"sha256": h.hexdigest(), "bytes": size}


def normalized_path(path: str) -> str:
    """Absolute, symlink-resolved normal path."""
    return os.path.realpath(os.path.abspath(path))


def hash_input(path: str) -> Dict[str, Any]:
    """Hash a single-file history or a JSONL directory history.

    Directories are hashed as the sorted list of ``relpath\\tdigest\\tsize``
    records, so file-system iteration order can never influence the digest.
    """
    norm = normalized_path(path)
    if not os.path.exists(norm):
        raise FileNotFoundError(norm)
    if os.path.isfile(norm):
        info = hash_file(norm)
        return {"kind": "file", "path": norm, "sha256": info["sha256"],
                "bytes": info["bytes"], "files": []}
    if not os.path.isdir(norm):
        raise NotADirectoryError(norm)
    files: List[Dict[str, Any]] = []
    for root, dirnames, filenames in os.walk(norm):
        dirnames.sort()
        for name in sorted(filenames):
            full = os.path.join(root, name)
            if not os.path.isfile(full) or os.path.islink(full):
                # symlinks inside an input dir are rejected: their target may
                # change without the directory itself changing
                raise ValueError(f"refusing to hash non-regular file: {full}")
            rel = os.path.relpath(full, norm).replace(os.sep, "/")
            info = hash_file(full)
            files.append({"relpath": rel, "sha256": info["sha256"],
                          "bytes": info["bytes"]})
    files.sort(key=lambda f: f["relpath"])
    listing = "".join(f"{f['relpath']}\t{f['sha256']}\t{f['bytes']}\n"
                      for f in files)
    return {"kind": "dir", "path": norm,
            "sha256": sha256_text(listing), "bytes": None, "files": files}
