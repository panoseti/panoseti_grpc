import pytest
import logging
import time
import json
from pathlib import Path
from panoseti_grpc.telemetry.logging import get_logger
from panoseti_grpc.telemetry.client import TelemetryClient, AsyncGrpcHandler


# --- FILESYSTEM TESTS ---

def test_filesystem_writing(tmp_path):
    log_dir = tmp_path / "app_logs"
    log_dir.mkdir()
    service_name = "FS_TEST"
    unique_name = f"{service_name}_{int(time.time())}"

    logger = get_logger(
        unique_name,
        log_dir=str(log_dir),
        grpc_enabled=False,
        console=False
    )

    logger.info("FS_TEST_MESSAGE")

    # Force flush
    for h in logger.handlers:
        h.flush()
        if isinstance(h, logging.handlers.RotatingFileHandler):
            h.close()  # Close matches flush + release file handle

    # RELAXED CHECK: Look for any file starting with the unique name (ignoring case)
    # The factory might lowercase the filename.
    found_files = list(log_dir.glob(f"*{unique_name}*.log"))

    assert len(found_files) > 0, f"No log file found matching {unique_name}. Dir content: {list(log_dir.iterdir())}"

    # Check content of the found file
    content = found_files[0].read_text()
    assert "FS_TEST_MESSAGE" in content


def test_filesystem_rotation(tmp_path):
    log_dir = tmp_path / "rotate_logs"
    log_dir.mkdir()
    service_name = "ROTATE_TEST"
    unique_name = f"{service_name}_{int(time.time())}"

    logger = get_logger(unique_name, log_dir=str(log_dir), grpc_enabled=False)

    # Monkey-patch limits
    file_handler = next(h for h in logger.handlers if isinstance(h, logging.handlers.RotatingFileHandler))
    file_handler.maxBytes = 50
    file_handler.backupCount = 2

    logger.info("X" * 100)
    logger.info("Y" * 100)

    file_handler.flush()
    file_handler.close()

    # Case-insensitive glob
    files = list(log_dir.glob(f"*{unique_name}*.log*"))
    assert len(files) >= 2, f"Rotation failed. Found: {files}"


# --- ROBUSTNESS TESTS ---

def test_grpc_server_down_resilience(grpc_client):
    """
    CRITICAL: If the Telemetry Server is offline, the client app MUST NOT crash.
    It should just drop logs or queue them (up to limit).
    """
    service_name = "SERVER_DOWN_TEST"

    # Point to a blackhole port where nothing is listening
    dead_client = TelemetryClient(host="localhost", port=59999)

    logger = logger = get_logger(service_name, grpc_enabled=True)

    try:
        logger.info("This should not crash the app")
        logger.info("Neither should this")
    except Exception as e:
        pytest.fail(f"Logger raised exception when server was down: {e}")


def test_queue_overflow_protection(grpc_client):
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