"""
Tests for concurrent and sequential request handling in the DAQ Control service.
"""

import contextlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

START_PARAMS = {
    "data_dir": "/app/data",
    "daq_ip_addr": "127.0.0.1",
    "bindhost": "lo",
    "max_file_size_mb": 10,
    "group_ph_frames": True,
    "run_dir": "concurrent_test.pffd",
    "obs": "ucb-lab",
    "module_id": [252],
}

STOP_PARAMS = {
    "data_dir": "/app/data",
    "run_dir": "concurrent_test.pffd",
}

STATUS_PARAMS = {
    "data_dir": "/app/data",
    "check_hashpipe_running": True,
    "check_disk_usage": False,
    "check_run_dirs": False,
}

CLEANUP_PARAMS = {
    "data_dir": "/app/data",
    "run_dir": "concurrent_test.pffd",
    "module_id": [252],
}


def test_concurrent_start_daq_rejected(grpc_client: Any) -> None:
    """
    Two simultaneous StartDaq RPCs: the second must fail with success=False
    because hashpipe is already running (already-running guard).
    """
    # First start must succeed
    assert grpc_client.StartDaq(START_PARAMS) is True

    try:

        def _start(_: Any) -> None:
            try:
                return grpc_client.StartDaq(START_PARAMS)
            except (ValueError, Exception):
                return False

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(_start, i) for i in range(3)]
            results = [f.result() for f in as_completed(futures)]

        # At most one additional start can succeed; the rest must return False
        successes = [r for r in results if r is True]
        assert len(successes) == 0, (
            f"Concurrent StartDaq calls should all fail while hashpipe is already running; got {successes} successes"
        )
    finally:
        grpc_client.StopDaq(STOP_PARAMS)
        # Brief wait for process to fully terminate
        time.sleep(0.5)
        with contextlib.suppress(Exception):
            grpc_client.CleanupData(CLEANUP_PARAMS)


def test_stop_then_start_idempotent(grpc_client: Any) -> None:
    """
    StopDaq when nothing is running returns success=True.
    A subsequent StartDaq should succeed without error.
    """
    # Ensure nothing is running
    stop_result = grpc_client.StopDaq(STOP_PARAMS)
    assert stop_result is True, "StopDaq on idle server should succeed (idempotent)"

    # Now start
    assert grpc_client.StartDaq(START_PARAMS) is True

    # Verify it's running
    _, status = grpc_client.StatusDaq(STATUS_PARAMS)
    assert status["hashpipe_running"] is True

    # Clean up
    assert grpc_client.StopDaq(STOP_PARAMS) is True
    time.sleep(0.5)
    with contextlib.suppress(Exception):
        grpc_client.CleanupData(CLEANUP_PARAMS)


def test_cleanup_while_start_in_progress_fails(grpc_client: Any) -> None:
    """
    CleanupData while hashpipe is running must return False, indicating cleanup failed.
    This ensures the safety guard preventing data deletion during an active run is exercised.
    """
    assert grpc_client.StartDaq(START_PARAMS) is True
    time.sleep(0.3)  # give hashpipe a moment to fully start

    try:
        # with pytest.raises((ValueError, Exception)):
        assert grpc_client.CleanupData(CLEANUP_PARAMS)["success"] is False
    finally:
        grpc_client.StopDaq(STOP_PARAMS)
        time.sleep(0.5)
        with contextlib.suppress(Exception):
            grpc_client.CleanupData(CLEANUP_PARAMS)
