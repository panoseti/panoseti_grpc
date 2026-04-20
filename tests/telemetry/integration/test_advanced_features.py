import logging
import time
from typing import Any

import pytest

from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient

# Import helpers
from panoseti_grpc.telemetry.logger import get_logger

from .test_logging_scenarios import LOG_KEY, wait_for_service_log


# --- HELPER FOR ROBUST WAITING ---
def wait_for_worker_logs(redis_client: Any, session_id: Any, min_count: Any = 1, timeout: Any = 15) -> None:
    """
    Polls Redis until the workers report in with the specific session ID.
    Replaces brittle time.sleep() calls.
    """
    start = time.time()
    while time.time() - start < timeout:
        logs = redis_client.lrange(LOG_KEY, -200, -1)
        count = sum(1 for log in logs if session_id in log)
        if count >= min_count:
            return True
        time.sleep(0.5)
    return False


# --- A. OMNI-CHANNEL LOGGING TEST ---


def test_triple_destination_logging(tmp_path: Any, capsys: Any, redis_client: Any, start_grpc_server: Any) -> None:
    service_name = "OMNI_TEST"
    log_dir = tmp_path / "omni_logs"
    log_dir.mkdir()

    logger = get_logger(service_name, log_dir=str(log_dir), console=True, grpc_enabled=True, level=logging.INFO)

    timestamp = str(time.time())
    unique_msg = f"Triple-Check-{timestamp}"

    logger.info(unique_msg)

    time.sleep(0.5)
    for h in logger.handlers:
        h.flush()

    # 1. Console
    captured = capsys.readouterr()
    combined_output = captured.out + captured.err
    assert unique_msg in combined_output, "Message missing from Console"

    # 2. Filesystem
    # FIX: Use exact service name (preserving case) for glob
    found_files = list(log_dir.glob(f"*{service_name}*.log"))
    assert len(found_files) > 0, f"Message missing from Filesystem. Dir: {list(log_dir.iterdir())}"
    assert unique_msg in found_files[0].read_text()

    # 3. Redis
    data = wait_for_service_log(redis_client, service_name)
    assert data is not None
    assert unique_msg in data["payload_json"]


# --- B. DISTRIBUTED WORKER CONTROL TESTS ---


def test_distributed_session_switching(redis_client: Any, start_grpc_server: Any) -> None:
    """
    Validates that the worker swarm can dynamically switch sessions.
    """
    # Phase 1: Session Alpha
    session_alpha = "SESSION_ALPHA"
    print(f"\n🔵 Starting {session_alpha}")
    redis_client.set("DISTRIBUTED_SESSION_ID", session_alpha)

    assert wait_for_worker_logs(redis_client, session_alpha), (
        f"Workers did not start for {session_alpha} within timeout"
    )

    # Phase 2: The Pause
    print("🔴 Pausing Session")
    redis_client.delete("DISTRIBUTED_SESSION_ID")

    # Wait for silence (heuristic)
    time.sleep(2)

    # Phase 3: Session Beta
    session_beta = "SESSION_BETA"
    print(f"🟢 Starting {session_beta}")
    redis_client.set("DISTRIBUTED_SESSION_ID", session_beta)

    # Wait for resume
    assert wait_for_worker_logs(redis_client, session_beta), f"Workers did not resume for {session_beta} within timeout"

    # Verify Separation
    logs_beta = redis_client.lrange(LOG_KEY, -200, -1)
    # Check the very last log is NOT Alpha
    last_log = logs_beta[-1]
    assert session_alpha not in last_log, "Old Session Alpha logs leaked into Beta window!"

    redis_client.delete("DISTRIBUTED_SESSION_ID")


# --- C. ROBUSTNESS TESTS ---


def test_queue_overflow_protection(redis_client: Any) -> None:
    service_name = "OVERFLOW_TEST"
    client = TelemetryClient(host="localhost", port=50051)

    handler = AsyncGrpcHandler(client, queue_size=2)

    logger = logging.getLogger(service_name)
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    # Mock worker to be slow
    handler._worker = lambda: time.sleep(1)

    start_time = time.time()
    try:
        for i in range(50):
            logger.info(f"Flood {i}")
    except Exception as e:
        pytest.fail(f"Logger crashed: {e}")

    duration = time.time() - start_time
    # Should be fast (non-blocking drop)
    assert duration < 2.0, "Logging blocked main thread!"


def test_logger_reconfiguration(tmp_path: Any, start_grpc_server: Any) -> None:
    """
    Validates dynamic reconfiguration and strict level filtering.
    """
    log_dir = tmp_path / "reconfig"
    log_dir.mkdir()
    service = "RECONFIG_TEST"

    # 1. Init as DEBUG
    logger1 = get_logger(service, level="DEBUG", log_dir=str(log_dir), grpc_enabled=True)
    logger1.debug("DebugMessage")

    # 2. Re-init as INFO
    # Note: get_logger(reset=True) is the default, so this CLEARS old handlers automatically.
    logger2 = get_logger(service, level="INFO", log_dir=str(log_dir), grpc_enabled=True)

    logger2.debug("ShouldNotAppear")
    logger2.info("InfoMessage")

    # 3. Re-init as CRITICAL (Highest filtering)
    logger3 = get_logger(service, level="CRITICAL", log_dir=str(log_dir), grpc_enabled=True)

    logger3.warning("WarningShouldNotAppear")
    logger3.error("ErrorShouldNotAppear")
    logger3.critical("CriticalMessage")

    # Flush & Close
    for h in logger3.handlers:
        h.flush()
        h.close()

    # Verify content
    # FIX: Use exact service name (preserving case) for glob
    files = list(log_dir.glob(f"*{service}*.log"))
    assert len(files) > 0, f"Log file missing. Found: {list(log_dir.iterdir())}"
    content = files[0].read_text()

    # Positive Assertions
    assert "DebugMessage" in content
    assert "InfoMessage" in content
    assert "CriticalMessage" in content

    # Negative Assertions (Filtering Checks)
    assert "ShouldNotAppear" not in content, "INFO logger failed to filter DEBUG msg"
    assert "WarningShouldNotAppear" not in content, "CRITICAL logger failed to filter WARNING msg"
    assert "ErrorShouldNotAppear" not in content, "CRITICAL logger failed to filter ERROR msg"
