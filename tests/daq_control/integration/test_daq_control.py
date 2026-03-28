import pytest
import time
import psutil
from pathlib import Path
from tests.daq_control.conftest import wait_for_file

# Parameters reused across tests
START_PARAMS = {
    "data_dir": "/app/data",
    "daq_ip_addr": "127.0.0.1",
    "bindhost": "lo",
    "max_file_size_mb": 10,
    "group_ph_frames": True,
    "run_dir": "test.pffd",
    "obs": "ucb-lab",
    "module_id": [250, 251],
}

STOP_PARAMS = {
    "data_dir": "/app/data",
    "run_dir": "test.pffd",
}

STATUS_PARAMS_BASE = {
    "data_dir": "/app/data",
    "check_hashpipe_running": False,
    "check_disk_usage": False,
    "check_run_dirs": False,
}

CLEANUP_PARAMS = {
    "data_dir": "/app/data",
    "run_dir": "test.pffd",
    "module_id": [250, 251],
}


def test_start_daq(grpc_client):
    """Verify StartDaq succeeds and hashpipe starts."""
    assert grpc_client.StartDaq(START_PARAMS) is True


def test_hashpipe_log_files_created(grpc_client):
    """After StartDaq, hp_stdout.log and hp_stderr.log should exist in run_dir."""
    run_dir = Path(START_PARAMS["data_dir"]) / START_PARAMS["run_dir"]
    assert wait_for_file(run_dir / "hp_stdout.log"), f"hp_stdout.log not created in {run_dir}"
    assert wait_for_file(run_dir / "hp_stderr.log"), f"hp_stderr.log not created in {run_dir}"


def test_start_daq_already_running(grpc_client):
    """StartDaq must fail when a hashpipe instance is already running."""
    with pytest.raises(ValueError):
        grpc_client.StartDaq(START_PARAMS)


def test_cleanup_data_while_running(grpc_client):
    """CleanupData must be rejected while hashpipe is running."""
    # with pytest.raises(ValueError):
    assert grpc_client.CleanupData(CLEANUP_PARAMS)['success'] is False


def test_status_daq_hashpipe_running(grpc_client):
    """StatusDaq reports hashpipe_running=True while hashpipe is up."""
    _, status = grpc_client.StatusDaq({**STATUS_PARAMS_BASE, "check_hashpipe_running": True})
    assert status["hashpipe_running"] is True


def test_status_daq_disk_usage(grpc_client):
    """StatusDaq returns non-negative disk usage values."""
    _, status = grpc_client.StatusDaq({**STATUS_PARAMS_BASE, "check_disk_usage": True})
    du = status["disk_usage"]
    assert du["total_disk_space"] > 0
    assert du["free_disk_space"] >= 0
    assert du["used_disk_space"] >= 0


def test_stop_daq(grpc_client):
    """StopDaq should successfully terminate the running hashpipe."""
    assert grpc_client.StopDaq(STOP_PARAMS) is True


def test_stop_daq_not_running(grpc_client):
    """StopDaq is a no-op (success) when hashpipe is already stopped."""
    assert grpc_client.StopDaq(STOP_PARAMS) is True


def test_status_daq_hashpipe_not_running(grpc_client):
    """After StopDaq, hashpipe_running should be False."""
    _, status = grpc_client.StatusDaq({**STATUS_PARAMS_BASE, "check_hashpipe_running": True})
    assert status["hashpipe_running"] is False


def test_status_daq_run_dirs(grpc_client):
    """The run directory created by StartDaq should appear in run_dirs."""
    _, status = grpc_client.StatusDaq({**STATUS_PARAMS_BASE, "check_run_dirs": True})
    run_dirs = status["run_dirs"]
    assert any("test.pffd" in d for d in run_dirs)


def test_cleanup_data(grpc_client):
    """CleanupData removes the run directory after hashpipe is stopped."""
    assert grpc_client.CleanupData(CLEANUP_PARAMS)['success'] is True
