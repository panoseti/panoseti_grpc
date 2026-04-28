"""
Manifest generation for DAQ run directories.

Computes per-file digests and writes a manifest file atomically.
"""

from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import hashlib
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

_manifest_logger = logging.getLogger(__name__)

# Try fast hash libraries, fall back to hashlib.sha256
try:
    import blake3 as _blake3

    def _digest_bytes(data: bytes, algo: str) -> str:
        if algo == "blake3":
            return str(_blake3.blake3(data).hexdigest())
        return _digest_xxh3(data)

    _HAS_BLAKE3 = True
except ImportError:
    _HAS_BLAKE3 = False

    def _digest_bytes(data: bytes, algo: str) -> str:
        return _digest_xxh3(data) if algo == "xxh3_128" else hashlib.sha256(data).hexdigest()


try:
    import xxhash as _xxhash

    def _digest_xxh3(data: bytes) -> str:
        return str(_xxhash.xxh3_128(data).hexdigest())

    _HAS_XXHASH = True
except ImportError:
    _HAS_XXHASH = False

    def _digest_xxh3(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()


def _effective_algo(algo: str) -> str:
    """Return the actual algorithm used (may differ from requested if library unavailable)."""
    if algo == "blake3":
        if not _HAS_BLAKE3:
            _manifest_logger.warning("blake3 not available, falling back to xxh3_128")
            algo = "xxh3_128"
        else:
            return "blake3"
    if algo == "xxh3_128":
        if not _HAS_XXHASH:
            _manifest_logger.warning("xxhash not available, falling back to sha256")
            return "sha256"
        return "xxh3_128"
    return algo


@dataclass
class ManifestEntry:
    relative_path: str
    digest_hex: str
    size_bytes: int
    mtime_ns: int


@dataclass
class ManifestResult:
    manifest_path: Path
    file_count: int
    total_bytes: int
    elapsed_seconds: float
    algorithm: str
    entries: list[ManifestEntry] = field(default_factory=list)


def _compute_manifest_sync(
    run_dir: Path,
    patterns: list[str],
    algo: str,
) -> ManifestResult:
    """Blocking implementation — call via asyncio.to_thread."""
    t0 = time.monotonic()
    effective = _effective_algo(algo)

    entries: list[ManifestEntry] = []

    for dirpath, _dirnames, filenames in os.walk(run_dir):
        for filename in sorted(filenames):
            if not any(fnmatch.fnmatch(filename, pat) for pat in patterns):
                continue
            abs_path = Path(dirpath) / filename
            rel_path = abs_path.relative_to(run_dir)
            stat = abs_path.stat()
            data = abs_path.read_bytes()
            digest = _digest_bytes(data, effective)
            entries.append(
                ManifestEntry(
                    relative_path=str(rel_path),
                    digest_hex=digest,
                    size_bytes=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                )
            )

    total_bytes = sum(e.size_bytes for e in entries)
    manifest_path = run_dir / f"manifest.{effective}"

    # Atomic write
    fd, tmp_path = tempfile.mkstemp(dir=run_dir, prefix=".manifest_tmp_")
    try:
        with os.fdopen(fd, "w") as f:
            for entry in entries:
                f.write(f"{entry.digest_hex}  {entry.size_bytes}  {entry.mtime_ns}  {entry.relative_path}\n")
        os.replace(tmp_path, manifest_path)
        # Ensure rsync (non-root) can read the manifest created by the root gRPC server
        os.chmod(manifest_path, 0o666)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise

    elapsed = time.monotonic() - t0
    return ManifestResult(
        manifest_path=manifest_path,
        file_count=len(entries),
        total_bytes=total_bytes,
        elapsed_seconds=elapsed,
        algorithm=effective,
        entries=entries,
    )


async def compute_manifest(
    run_dir: Path,
    patterns: list[str],
    algo: str = "blake3",
) -> ManifestResult:
    """Compute a checksum manifest for files in run_dir matching patterns.

    Uses asyncio.to_thread to avoid blocking the event loop during file I/O.
    """
    return await asyncio.to_thread(_compute_manifest_sync, run_dir, patterns, algo)
