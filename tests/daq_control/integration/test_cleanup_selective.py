"""
Integration tests for CleanupData CLEANUP_SELECTIVE mode.

These tests spin up an in-process gRPC server (no Docker / hashpipe required)
on a dedicated port and exercise the selective-cleanup paths, including backward
compatibility with the default CLEANUP_FULL mode.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import socket
import time

import pytest

from panoseti_grpc.daq_control.client import DaqControlClient
from panoseti_grpc.daq_control.server import serve

TEST_PORT = 50061
MODULE_ID = 99


def _run_server(port: int) -> None:
    asyncio.run(serve(grpc_port=port))


@pytest.fixture
def cleanup_server(tmp_path):
    """Start a fresh in-process server per test; yield (client, tmp_path)."""
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
    yield client, tmp_path
    proc.terminate()
    proc.join(timeout=2)
    if proc.is_alive():
        proc.kill()


def _make_run_dir(tmp_path, run_dir_name: str = "test_run.pffd"):
    """Create a fake run directory structure with mixed file types.

    The CleanupDataModel validator requires:
      - ``data_dir / run_dir`` to exist (config-level run dir)
      - ``data_dir / module_{id}`` to exist (module dir)

    The actual data lives under ``data_dir / module_{id} / run_dir``.
    """
    # Top-level run dir (config files, validated by CleanupDataModel)
    top_run_dir = tmp_path / run_dir_name
    top_run_dir.mkdir(parents=True, exist_ok=True)

    # Per-module data directory
    module_run_dir = tmp_path / f"module_{MODULE_ID}" / run_dir_name
    module_run_dir.mkdir(parents=True)

    # Create some .pff files
    (module_run_dir / "data1.pff").write_bytes(b"fake pff data 1" * 100)
    (module_run_dir / "data2.pff").write_bytes(b"fake pff data 2" * 200)
    (module_run_dir / "data3.pff").write_bytes(b"fake pff data 3" * 50)

    # Create some non-pff files that should survive selective cleanup
    (module_run_dir / "config.json").write_text('{"run": "test"}')
    (module_run_dir / "hp_stdout.log").write_text("hashpipe started\n")
    (module_run_dir / "hp_stderr.log").write_text("some stderr output\n")

    return module_run_dir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_selective_deletes_pff_keeps_others(cleanup_server):
    """CLEANUP_SELECTIVE with delete_patterns=['*.pff'] removes pff files only."""
    client, tmp_path = cleanup_server
    run_dir = "test_run.pffd"
    module_run_dir = _make_run_dir(tmp_path, run_dir)

    resp = client.CleanupData(
        {
            "data_dir": str(tmp_path),
            "run_dir": run_dir,
            "module_id": [MODULE_ID],
            "mode": "CLEANUP_SELECTIVE",
            "delete_patterns": ["*.pff"],
        }
    )

    assert resp["success"] is True
    # freed_bytes is a uint64 — MessageToDict serialises it as a string in JSON
    assert int(resp["deleted_count"]) > 0
    assert int(resp["freed_bytes"]) > 0

    # pff files should be gone
    assert not (module_run_dir / "data1.pff").exists()
    assert not (module_run_dir / "data2.pff").exists()
    assert not (module_run_dir / "data3.pff").exists()

    # non-pff files should still be present
    assert (module_run_dir / "config.json").exists()
    assert (module_run_dir / "hp_stdout.log").exists()
    assert (module_run_dir / "hp_stderr.log").exists()


def test_selective_preserve_overrides_delete(cleanup_server):
    """When a file matches both delete_patterns and preserve_patterns, it is kept."""
    client, tmp_path = cleanup_server
    run_dir = "test_preserve.pffd"
    module_run_dir = _make_run_dir(tmp_path, run_dir)

    pff_size_before = sum(
        f.stat().st_size for f in module_run_dir.glob("*.pff")
    )
    assert pff_size_before > 0

    resp = client.CleanupData(
        {
            "data_dir": str(tmp_path),
            "run_dir": run_dir,
            "module_id": [MODULE_ID],
            "mode": "CLEANUP_SELECTIVE",
            "delete_patterns": ["*.pff"],
            "preserve_patterns": ["*.pff"],
        }
    )

    assert resp["success"] is True
    # Everything matched preserve → nothing deleted
    # freed_bytes is a uint64 — MessageToDict serialises it as a string in JSON
    assert int(resp["deleted_count"]) == 0
    assert int(resp["freed_bytes"]) == 0

    # All pff files should still be there
    assert (module_run_dir / "data1.pff").exists()
    assert (module_run_dir / "data2.pff").exists()
    assert (module_run_dir / "data3.pff").exists()


def test_selective_no_matching_files(cleanup_server):
    """CLEANUP_SELECTIVE with a pattern that matches nothing returns deleted_count=0, success=True."""
    client, tmp_path = cleanup_server
    run_dir = "test_nomatch.pffd"
    _make_run_dir(tmp_path, run_dir)

    resp = client.CleanupData(
        {
            "data_dir": str(tmp_path),
            "run_dir": run_dir,
            "module_id": [MODULE_ID],
            "mode": "CLEANUP_SELECTIVE",
            "delete_patterns": ["*.nonexistent"],
        }
    )

    assert resp["success"] is True
    # freed_bytes is a uint64 — MessageToDict serialises it as a string in JSON
    assert int(resp["deleted_count"]) == 0
    assert int(resp["freed_bytes"]) == 0


def test_full_cleanup_default_backward_compat(cleanup_server):
    """CleanupData with NO mode field defaults to CLEANUP_FULL and rmtrees the run dir."""
    client, tmp_path = cleanup_server
    run_dir = "test_full.pffd"
    module_run_dir = _make_run_dir(tmp_path, run_dir)

    assert module_run_dir.is_dir()

    # Call with NO mode field — should default to CLEANUP_FULL
    resp = client.CleanupData(
        {
            "data_dir": str(tmp_path),
            "run_dir": run_dir,
            "module_id": [MODULE_ID],
        }
    )

    assert resp["success"] is True
    # The entire module run directory should be gone
    assert not module_run_dir.exists()


def test_full_cleanup_already_gone(cleanup_server):
    """CLEANUP_FULL on a directory that was already removed returns success=False."""
    client, tmp_path = cleanup_server
    run_dir = "test_gone.pffd"
    module_run_dir = _make_run_dir(tmp_path, run_dir)

    # First cleanup
    resp = client.CleanupData(
        {
            "data_dir": str(tmp_path),
            "run_dir": run_dir,
            "module_id": [MODULE_ID],
        }
    )
    assert resp["success"] is True

    # Second cleanup — directories no longer exist, validation should fail
    resp2 = client.CleanupData(
        {
            "data_dir": str(tmp_path),
            "run_dir": run_dir,
            "module_id": [MODULE_ID],
        }
    )
    assert resp2["success"] is False
