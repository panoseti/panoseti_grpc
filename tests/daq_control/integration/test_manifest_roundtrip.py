"""
Integration tests for GenerateManifest → GetManifest round-trip.

These tests spin up an in-process gRPC server (no Docker / hashpipe required)
on a dedicated port and verify the manifest generation and streaming paths.
"""

from __future__ import annotations

import asyncio
import hashlib
import multiprocessing
import socket
import time
from pathlib import Path

import pytest

from panoseti_grpc.daq_control.client import DaqControlClient
from panoseti_grpc.daq_control.server import serve

TEST_PORT = 50062
MODULE_ID = 42


def _run_server(port: int) -> None:
    asyncio.run(serve(grpc_port=port))


@pytest.fixture(scope="module")
def manifest_server(tmp_path_factory):
    """Start a single in-process server for the module; yield (client, data_root)."""
    data_root = tmp_path_factory.mktemp("manifest")
    proc = multiprocessing.Process(target=_run_server, args=[TEST_PORT], daemon=True)
    proc.start()

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", TEST_PORT), timeout=0.1):
                break
        except (OSError, ConnectionRefusedError):
            time.sleep(0.05)
    else:
        proc.terminate()
        raise TimeoutError(f"Server did not bind on port {TEST_PORT} within 10 s")

    client = DaqControlClient(host="localhost", port=TEST_PORT)
    yield client, data_root
    proc.terminate()
    proc.join(timeout=2)
    if proc.is_alive():
        proc.kill()


# Known file contents — deterministic so we can verify digests locally.
_PFF_FILES = {
    "start_2024-01-01T00:00:00Z.dp_ph256.bpp_2.module_42.seqno_0.pff": b"pff frame alpha " * 64,
    "start_2024-01-01T00:01:00Z.dp_ph256.bpp_2.module_42.seqno_1.pff": b"pff frame beta  " * 128,
    "start_2024-01-01T00:02:00Z.dp_ph256.bpp_2.module_42.seqno_2.pff": b"pff frame gamma " * 32,
}


def _make_manifest_run_dir(tmp_path, run_dir_name: str = "manifest_test.pffd"):
    """Create a fake run dir with known .pff files.

    GenerateManifestModel validates that ``data_dir / run_dir`` exists.
    The server additionally checks ``data_dir / module_{id} / run_dir``.
    """
    # Top-level run dir required by GenerateManifestModel validator
    top_run_dir = tmp_path / run_dir_name
    top_run_dir.mkdir(parents=True, exist_ok=True)

    # Per-module data directory
    module_run_dir = tmp_path / f"module_{MODULE_ID}" / run_dir_name
    module_run_dir.mkdir(parents=True)

    for filename, content in _PFF_FILES.items():
        (module_run_dir / filename).write_bytes(content)

    return module_run_dir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_generate_manifest_success(manifest_server):
    """GenerateManifest returns success=True, correct file_count and total_bytes."""
    client, data_root = manifest_server
    run_dir = "manifest_test.pffd"
    module_run_dir = _make_manifest_run_dir(data_root, run_dir)

    expected_total_bytes = sum(len(c) for c in _PFF_FILES.values())

    # Always pass algorithm explicitly — an empty string fails Pydantic's Literal validator.
    # The server falls back to sha256 when blake3/xxhash are unavailable.
    resp = client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "algorithm": "blake3",
            "include_patterns": ["*.pff"],
        }
    )

    assert resp["success"] is True
    assert resp["file_count"] == len(_PFF_FILES)
    # total_bytes is uint64 — MessageToDict may serialise as string
    assert int(resp["total_bytes"]) == expected_total_bytes
    assert resp["manifest_path"] != ""
    assert resp["algorithm"] in ("blake3", "xxh3_128", "sha256")


def test_generate_manifest_file_exists(manifest_server):
    """The manifest file reported by GenerateManifest actually exists on disk."""
    client, data_root = manifest_server
    run_dir = "manifest_file_exists.pffd"
    _make_manifest_run_dir(data_root, run_dir)

    resp = client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )

    assert resp["success"] is True
    manifest_path = Path(resp["manifest_path"])
    assert manifest_path.is_file(), f"Manifest file not found: {manifest_path}"
    assert manifest_path.stat().st_size > 0


def test_get_manifest_entry_count(manifest_server):
    """GetManifest streams exactly as many entries as files included in the manifest."""
    client, data_root = manifest_server
    run_dir = "manifest_count.pffd"
    _make_manifest_run_dir(data_root, run_dir)

    # Generate first
    gen_resp = client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )
    assert gen_resp["success"] is True

    # Stream entries
    entries = client.GetManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
        }
    )

    assert len(entries) == len(_PFF_FILES)


def test_get_manifest_sizes_match(manifest_server):
    """Each streamed ManifestEntry has size_bytes matching the actual file size."""
    client, data_root = manifest_server
    run_dir = "manifest_sizes.pffd"
    module_run_dir = _make_manifest_run_dir(data_root, run_dir)

    client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )

    entries = client.GetManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
        }
    )

    for entry in entries:
        rel_path = entry["relative_path"]
        actual_size = (module_run_dir / rel_path).stat().st_size
        # size_bytes is uint64 — MessageToDict serialises it as a string
        assert int(entry["size_bytes"]) == actual_size, (
            f"size mismatch for {rel_path}: got {entry['size_bytes']}, expected {actual_size}"
        )


def test_get_manifest_mtime_ns_positive(manifest_server):
    """Each streamed ManifestEntry has a positive mtime_ns."""
    client, data_root = manifest_server
    run_dir = "manifest_mtime.pffd"
    _make_manifest_run_dir(data_root, run_dir)

    client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )

    entries = client.GetManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
        }
    )

    for entry in entries:
        # mtime_ns is uint64 — MessageToDict serialises it as a string
        assert int(entry["mtime_ns"]) > 0, f"mtime_ns not positive for {entry['relative_path']}"


def test_get_manifest_digest_hex_is_hex(manifest_server):
    """Each digest_hex field is a valid lowercase hexadecimal string."""
    client, data_root = manifest_server
    run_dir = "manifest_digest_hex.pffd"
    _make_manifest_run_dir(data_root, run_dir)

    client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )

    entries = client.GetManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
        }
    )

    for entry in entries:
        digest = entry["digest_hex"]
        assert len(digest) >= 32, f"digest_hex too short ({len(digest)}) for {entry['relative_path']}"
        # Must be valid hex
        try:
            int(digest, 16)
        except ValueError:
            pytest.fail(f"digest_hex is not valid hex for {entry['relative_path']}: {digest!r}")


def test_get_manifest_digest_matches_sha256_when_algorithm_is_sha256(manifest_server):
    """If the server used sha256, locally recomputed digests must match streamed ones."""
    client, data_root = manifest_server
    run_dir = "manifest_digest_verify.pffd"
    module_run_dir = _make_manifest_run_dir(data_root, run_dir)

    gen_resp = client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )
    assert gen_resp["success"] is True

    algorithm = gen_resp["algorithm"]
    if algorithm != "sha256":
        pytest.skip(f"Server used {algorithm!r} (not sha256); digest cross-check skipped")

    entries = client.GetManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
        }
    )

    for entry in entries:
        rel_path = entry["relative_path"]
        file_bytes = (module_run_dir / rel_path).read_bytes()
        expected = hashlib.sha256(file_bytes).hexdigest()
        assert entry["digest_hex"] == expected, (
            f"Digest mismatch for {rel_path}: got {entry['digest_hex']}, expected {expected}"
        )


def test_roundtrip_full(manifest_server):
    """Full round-trip: GenerateManifest → GetManifest covers all expected files."""
    client, data_root = manifest_server
    run_dir = "manifest_roundtrip.pffd"
    module_run_dir = _make_manifest_run_dir(data_root, run_dir)

    gen_resp = client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )

    assert gen_resp["success"] is True
    assert gen_resp["file_count"] == len(_PFF_FILES)

    entries = client.GetManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
        }
    )

    assert len(entries) == len(_PFF_FILES)

    # Verify structural correctness for all entries
    seen_paths = set()
    for entry in entries:
        rel_path = entry["relative_path"]
        seen_paths.add(rel_path)

        actual_size = (module_run_dir / rel_path).stat().st_size
        # size_bytes and mtime_ns are uint64 — MessageToDict serialises them as strings
        assert int(entry["size_bytes"]) == actual_size
        assert int(entry["mtime_ns"]) > 0
        assert len(entry["digest_hex"]) > 0

    # All expected filenames must appear in the manifest
    expected_names = set(_PFF_FILES.keys())
    assert seen_paths == expected_names, f"Missing files in manifest: {expected_names - seen_paths}"


def test_manifest_file_format_four_columns(manifest_server):
    """The manifest file on disk uses 4-column two-space-delimited format."""
    client, data_root = manifest_server
    run_dir = "manifest_format.pffd"
    _make_manifest_run_dir(data_root, run_dir)

    resp = client.GenerateManifest(
        {
            "data_dir": str(data_root),
            "run_dir": run_dir,
            "module_id": MODULE_ID,
            "include_patterns": ["*.pff"],
        }
    )
    assert resp["success"] is True

    manifest_path = Path(resp["manifest_path"])
    assert manifest_path.is_file(), f"Manifest file not found: {manifest_path}"

    lines = manifest_path.read_text().strip().splitlines()
    assert len(lines) == len(_PFF_FILES)
    for line in lines:
        parts = line.split("  ")  # exactly two spaces
        assert len(parts) == 4, f"Expected 4 columns, got {len(parts)}: {line!r}"
        digest_hex, size_str, mtime_str, relpath = parts
        assert int(size_str) > 0, f"size must be positive: {size_str!r}"
        assert int(mtime_str) > 0, f"mtime must be positive: {mtime_str!r}"
        assert relpath.endswith(".pff"), f"relpath must end with .pff: {relpath!r}"
        # digest must be valid hex of reasonable length
        assert len(digest_hex) >= 32, f"digest_hex too short: {digest_hex!r}"
        int(digest_hex, 16)  # raises ValueError if not valid hex
