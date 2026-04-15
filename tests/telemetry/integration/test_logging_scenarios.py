import json
import logging
import time
from unittest.mock import MagicMock

import pytest

from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient
from panoseti_grpc.telemetry.logger import get_logger

LOG_KEY = "logs:ingress"


def wait_for_service_log(redis_client, service_name, retries=20):
    """
    Polls Redis for the specific service log.
    Now looks at the TAIL of the list (most recent logs).
    """
    for _ in range(retries):
        # Fetch the LAST 200 logs (-200 to end)
        raw_logs = redis_client.lrange(LOG_KEY, -200, -1)

        # Iterate backwards (newest first) to find it faster
        for entry in reversed(raw_logs):
            try:
                data = json.loads(entry)
                if data.get("service_name", "").lower() == service_name.lower():
                    return data
            except (json.JSONDecodeError, TypeError):
                continue
        time.sleep(0.2)
    return None


def test_unserializable_payload_handling(redis_client, start_grpc_server):
    service_name = "BAD_DATA_TEST"
    logger = get_logger(service_name, grpc_enabled=True)

    # Clean up previous runs if any (Optional but helps debugging)
    # redis_client.delete(LOG_KEY)

    # A set {1, 2, 3} is not JSON serializable
    bad_payload = {"valid": 1, "invalid": {1, 2, 3}}

    logger.info(bad_payload)

    data = wait_for_service_log(redis_client, service_name)
    assert data is not None, f"Log for {service_name} failed to appear in the last 200 Redis entries."

    # The serializer typically converts non-serializable objects to string
    assert "invalid" in data["payload_json"]


def test_huge_payload_logging(redis_client):
    service_name = "HUGE_LOG_TEST"
    client = TelemetryClient(host="localhost", port=50051)

    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    # Larger queue to accept the burst
    logger.addHandler(AsyncGrpcHandler(client, service_name, queue_size=100))

    huge_msg = "X" * 5000
    logger.info(huge_msg)

    data = wait_for_service_log(redis_client, service_name)
    assert data is not None, "Huge log failed to appear in Redis."

    payload = json.loads(data["payload_json"])
    # Handle both wrapped dicts and raw strings
    content = payload.get("text", payload) if isinstance(payload, dict) else payload
    assert len(content) == 5000


def test_handler_survives_queue_overflow():
    """
    Verifies that the AsyncGrpcHandler swallows the queue.Full exception
    and protects the main application thread from crashing when under load.
    """
    # 1. Setup Client
    mock_client = MagicMock(spec=TelemetryClient)

    # 2. Setup Handler with a TINY queue (size=1)
    # This makes it instant to overflow.
    handler = AsyncGrpcHandler(mock_client, "CRASH_TEST", queue_size=1)

    # 3. Create dummy records
    # Note: We must populate process/threadName because client.py now expects them
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=10, msg="Spam", args=(), exc_info=None
    )
    record.process = 1234
    record.threadName = "MainThread"

    # 4. Fill the queue
    # We do NOT start the worker thread logic (or we assume it's slow).
    # Since queue_size=1, the first put works.
    handler.emit(record)
    assert handler.queue.full(), "Queue should be full after 1 item"

    # 5. CRITICAL STEP: Attempt to overflow
    success = False
    try:
        # This call should normally block or raise Full.
        # Your AsyncGrpcHandler uses queue.put_nowait() inside a try/except.
        handler.emit(record)
        success = True
    except Exception as e:
        pytest.fail(f"Handler crashed the app on overflow! Error: {e}")

    # 6. Assertions
    assert success is True
    assert handler.queue.full()

    # Clean up
    handler._stop_event.set()


def test_metadata_context_propagation(redis_client, start_grpc_server):
    """
    Verifies that rich Python metadata (function name, filename, line number)
    survives the gRPC serialization loop and arrives in Redis.
    """
    service_name = "META_TEST"
    logger = get_logger(service_name, grpc_enabled=True)

    def internal_function():
        logger.info("Inside Function")  # Line X

    internal_function()

    data = wait_for_service_log(redis_client, service_name)
    assert data is not None

    # In 'client.py', metadata is injected into the payload or top-level struct
    # We check the top-level 'LogSchema' fields first, then the payload fallback

    # Note: exact keys depend on your LogSchema definition in config.py
    # If they aren't top-level, they might be inside 'extra_fields' or 'payload_json'

    payload = data.get("payload_json", "")

    # We expect the function name to be present somewhere
    assert "internal_function" in str(data) or "internal_function" in payload, (
        f"Function name metadata lost. Data: {data}"
    )

    # We expect the filename to be present
    assert "test_logging_scenarios.py" in str(data) or "test_logging_scenarios.py" in payload, "Filename metadata lost."


# --- NEW TEST 4: Severity Level Mapping ---
def test_severity_level_propagation(redis_client, start_grpc_server):
    """
    Verifies that Python logging levels (WARNING, ERROR, CRITICAL)
    are correctly mapped to the Telemetry Protocol Enums in Redis.
    """
    service_name = "SEVERITY_TEST"
    logger = get_logger(service_name, grpc_enabled=True)

    # Log an error
    error_msg = "Critical Failure Simulation"
    logger.error(error_msg)

    data = wait_for_service_log(redis_client, service_name)
    assert data is not None

    # Check the 'severity' field in the Redis JSON
    # LogSeverity Enum: DEBUG=1, INFO=2, WARNING=3, ERROR=4, CRITICAL=5

    # Verify the message content
    assert error_msg in data["payload_json"]

    # Verify the severity level.
    # Depending on implementation, it might be an int (4) or string "ERROR"
    severity = data.get("severity")

    # Accept either Int(4) or String("ERROR")
    valid_severities = [4, "ERROR", "LogSeverity.ERROR"]
    assert severity in valid_severities, f"Expected severity ERROR (4), got {severity}"
