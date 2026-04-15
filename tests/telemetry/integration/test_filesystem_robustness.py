from typing import Any
import logging
import time

import pytest

from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient
from panoseti_grpc.telemetry.logger import get_logger

# Import helper to check Redis for the dual-destination test
from .test_logging_scenarios import wait_for_service_log


def test_filesystem_writing( tmp_path: Any) -> None:
    """
    FIXED: Now searches for the filename using .lower() to handle
    auto-lowercasing behavior of the logger factory.
    """
    log_dir = tmp_path / "app_logs"
    log_dir.mkdir()
    service_name = "FS_TEST"
    unique_name = f"{service_name}_{int(time.time())}"

    logger = get_logger(unique_name, log_dir=str(log_dir), grpc_enabled=False, console=False)

    logger.info("FS_TEST_MESSAGE")

    # Force flush and close to ensure data is on disk
    for h in logger.handlers:
        h.flush()
        if isinstance(h, logging.handlers.RotatingFileHandler):
            h.close()

    # Match the exact unique name pattern.
    found_files = list(log_dir.glob(f"{unique_name}.log"))

    assert len(found_files) > 0, (
        f"No log file found matching {unique_name.lower()}. Dir content: {list(log_dir.iterdir())}"
    )

    content = found_files[0].read_text()
    assert "FS_TEST_MESSAGE" in content


def test_filesystem_rotation( tmp_path: Any) -> None:
    """
    FIXED: Uses .lower() for glob matching.
    """
    log_dir = tmp_path / "rotate_logs"
    log_dir.mkdir()
    service_name = "ROTATE_TEST"
    unique_name = f"{service_name}_{int(time.time())}"

    logger = get_logger(unique_name, log_dir=str(log_dir), grpc_enabled=False)

    # Monkey-patch limits for test
    file_handler = next(h for h in logger.handlers if isinstance(h, logging.handlers.RotatingFileHandler))
    file_handler.maxBytes = 50
    file_handler.backupCount = 2

    logger.info("X" * 100)
    logger.info("Y" * 100)

    file_handler.flush()
    file_handler.close()

    files = list(log_dir.glob(f"{unique_name}.log*"))
    assert len(files) >= 2, f"Rotation failed. Found: {files}"


# --- NEW TEST 1: Log Level Filtering (Local) ---
def test_log_level_filtering( tmp_path: Any) -> None:
    """
    Verifies that low-priority logs (DEBUG) are suppressed when
    the logger is set to a higher level (INFO).
    """
    log_dir = tmp_path / "filter_logs"
    log_dir.mkdir()
    service_name = "FILTER_TEST"

    # Set level to INFO
    logger = get_logger(service_name, log_dir=str(log_dir), level="info", grpc_enabled=False)

    logger.debug("THIS_SHOULD_BE_IGNORED")
    logger.info("THIS_SHOULD_BE_SEEN")

    for h in logger.handlers:
        h.flush()

    # Find log file
    log_file = next(log_dir.glob(f"{service_name}.log"))
    content = log_file.read_text()

    assert "THIS_SHOULD_BE_SEEN" in content
    assert "THIS_SHOULD_BE_IGNORED" not in content


# --- NEW TEST 2: Dual Destination (Local + Distributed) ---
def test_dual_destination_logging( tmp_path: Any, redis_client: Any, grpc_client: Any) -> None:
    """
    Verifies that a single logger instance correctly dispatches data
    to BOTH the local filesystem AND the remote Redis server.
    """
    log_dir = tmp_path / "dual_logs"
    log_dir.mkdir()
    service_name = "DUAL_TEST"

    # Enable BOTH filesystem and gRPC
    logger = get_logger(
        service_name,
        log_dir=str(log_dir),
        grpc_enabled=True,  # Remote
        console=False,
    )

    msg_body = f"Dual-Test-{time.time()}"
    logger.info(msg_body)

    # 1. Check Filesystem
    for h in logger.handlers:
        h.flush()
    log_file = next(log_dir.glob(f"*{service_name}*.log"))
    assert msg_body in log_file.read_text(), "Message missing from local file"

    # 2. Check Remote (Redis)
    remote_data = wait_for_service_log(redis_client, service_name)
    assert remote_data is not None, "Message missing from Remote Telemetry"
    assert msg_body in remote_data["payload_json"], "Payload mismatch in Redis"


# --- ROBUSTNESS TESTS ---


def test_grpc_server_down_resilience( grpc_client: Any) -> None:
    """
    CRITICAL: If the Telemetry Server is offline, the client app MUST NOT crash.
    It should just drop logs or queue them (up to limit).
    """
    service_name = "SERVER_DOWN_TEST"

    # Point to a blackhole port where nothing is listening
    TelemetryClient(host="localhost", port=59999)

    logger = get_logger(service_name, grpc_enabled=True)

    try:
        logger.info("This should not crash the app")
        logger.info("Neither should this")
    except Exception as e:
        pytest.fail(f"Logger raised exception when server was down: {e}")


def test_queue_overflow_protection( grpc_client: Any) -> None:
    """
    If the worker is stuck (or server down) and queue fills up,
    the handler should drop logs rather than blocking the main thread.
    """
    service_name = "OVERFLOW_TEST"

    # Tiny queue size = 1
    handler = AsyncGrpcHandler(grpc_client, service_name, queue_size=1)

    # Mock the worker to do NOTHING (simulating a stuck thread)
    # We replace the _worker function with a no-op before starting
    handler._worker = lambda: time.sleep(1)

    logger = logging.getLogger(service_name)
    logger.addHandler(handler)

    # Fill the queue
    logger.info("Log 1")  # Goes into queue

    start_time = time.time()
    # This attempt should find a full queue.
    # Ideally, it drops the log and returns IMMEDIATELY.
    # If it blocks, this test will fail on timing.
    logger.info("Log 2 (Overflow)")
    duration = time.time() - start_time

    assert duration < 0.1, f"Logging blocked main thread for {duration}s on queue full!"

    handler.close()
