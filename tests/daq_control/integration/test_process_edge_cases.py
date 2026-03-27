"""
Tests for process-management edge cases in the DAQ Control service:
crash detection via SIGKILL, stale-PID handling, log-file placement,
and disk-usage response completeness.
"""
import os
import signal
import time
import psutil
import pytest
from pathlib import Path

START_PARAMS = {
    "data_dir": "/app/data",
    "daq_ip_addr": "127.0.0.1",
    "bindhost": "lo",
    "max_file_size_mb": 10,
    "group_ph_frames": True,
    "run_dir": "edge_test.pffd",
    "obs": "ucb-lab",
    "module_id": [253],
}

STOP_PARAMS = {
    "data_dir": "/app/data",
    "run_dir": "edge_test.pffd",
}

STATUS_BASE = {
    "data_dir": "/app/data",
    "check_hashpipe_running": False,
    "check_disk_usage": False,
    "check_run_dirs": False,
}

CLEANUP_PARAMS = {
    "data_dir": "/app/data",
    "run_dir": "edge_test.pffd",
    "module_id": [253],
}


def _find_hashpipe_pid():
    """Return the PID of a running hashpipe process, or None."""
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            if proc.info["cmdline"] and any("hashpipe" in c for c in proc.info["cmdline"]):
                return proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None


def test_hashpipe_crash_detection(grpc_client):
    """
    After StartDaq, forcibly kill the hashpipe process with SIGKILL.
    StatusDaq must subsequently report hashpipe_running=False.
    """
    assert grpc_client.StartDaq(START_PARAMS) is True
    time.sleep(0.5)

    pid = _find_hashpipe_pid()
    assert pid is not None, "hashpipe process not found after StartDaq"

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # Already gone — that's fine for the test

    # Give the OS a moment to reap the zombie
    time.sleep(0.5)

    _, status = grpc_client.StatusDaq({**STATUS_BASE, "check_hashpipe_running": True})
    assert status["hashpipe_running"] is False, (
        "StatusDaq should detect that hashpipe was killed"
    )

    # Cleanup: StopDaq on a dead process should be idempotent
    grpc_client.StopDaq(STOP_PARAMS)
    time.sleep(0.2)
    try:
        grpc_client.CleanupData(CLEANUP_PARAMS)
    except Exception:
        pass


def test_stop_daq_with_stale_pid(grpc_client):
    """
    StopDaq when the cached PID is invalid (process already gone) must
    return success=True and not raise an exception.
    """
    # Make sure nothing is running (no-op stop)
    result = grpc_client.StopDaq(STOP_PARAMS)
    assert result is True, "StopDaq on a server with no cached PID must succeed"

    # Call it again immediately — doubly idempotent
    result2 = grpc_client.StopDaq(STOP_PARAMS)
    assert result2 is True


def test_log_files_written_to_correct_run_dir(grpc_client):
    """
    After StartDaq, hp_stdout.log and hp_stderr.log must exist under
    {data_dir}/{run_dir}/ — not in the parent data_dir or elsewhere.
    """
    assert grpc_client.StartDaq(START_PARAMS) is True
    time.sleep(1.5)  # give the background monitor task time to write initial output

    run_dir = Path(START_PARAMS["data_dir"]) / START_PARAMS["run_dir"]
    stdout_log = run_dir / "hp_stdout.log"
    stderr_log = run_dir / "hp_stderr.log"

    try:
        assert stdout_log.exists(), f"hp_stdout.log not found at {stdout_log}"
        assert stderr_log.exists(), f"hp_stderr.log not found at {stderr_log}"

        # Verify they are not accidentally placed in the parent data_dir
        parent_stdout = Path(START_PARAMS["data_dir"]) / "hp_stdout.log"
        assert not parent_stdout.exists(), "hp_stdout.log must not be in data_dir root"
    finally:
        grpc_client.StopDaq(STOP_PARAMS)
        time.sleep(0.5)
        try:
            grpc_client.CleanupData(CLEANUP_PARAMS)
        except Exception:
            pass


def test_disk_usage_keys_present(grpc_client):
    """
    StatusDaq with check_disk_usage=True must return a disk_usage struct
    containing total_disk_space, used_disk_space, and free_disk_space,
    all non-negative, with total >= used + free.
    """
    _, status = grpc_client.StatusDaq({**STATUS_BASE, "check_disk_usage": True})
    du = status.get("disk_usage", {})

    assert "total_disk_space" in du, "disk_usage must contain 'total_disk_space'"
    assert "used_disk_space" in du, "disk_usage must contain 'used_disk_space'"
    assert "free_disk_space" in du, "disk_usage must contain 'free_disk_space'"

    total = du["total_disk_space"]
    used = du["used_disk_space"]
    free = du["free_disk_space"]

    assert total > 0, "total_disk_space must be positive"
    assert used >= 0, "used_disk_space must be non-negative"
    assert free >= 0, "free_disk_space must be non-negative"
    assert total >= used + free, (
        f"total ({total}) must be >= used ({used}) + free ({free})"
    )
