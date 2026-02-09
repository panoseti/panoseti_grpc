import pytest
import time
import json
import logging
from unittest.mock import MagicMock
from panoseti_grpc.telemetry.client import make_grpc_logger, TelemetryClient, AsyncGrpcHandler

LOG_KEY = "logs:ingress"


def test_unserializable_payload_handling(redis_client):
    """
    Scenario: A developer accidentally logs a non-JSON-serializable object.
    """
    service_name = "BAD_DATA_TEST"
    client = TelemetryClient(host="localhost", port=50051)
    logger = make_grpc_logger(service_name, grpc_client=client)

    # A set is not JSON serializable
    bad_payload = {"valid": 1, "invalid": {1, 2, 3}}

    logger.info(bad_payload)

    time.sleep(1.0)

    # Since Redis was flushed, this should be the ONLY item
    log_json = redis_client.lindex(LOG_KEY, 0)  # Index 0 = First item
    assert log_json is not None, "Log was not stored in Redis"

    data = json.loads(log_json)

    # Check that service name matches (verifies we aren't reading old junk)
    assert data["service_name"] == service_name.lower()

    stored_msg = data["payload_json"]

    # Since we wrapped the dict in str(), we look for the string repr
    assert "invalid" in stored_msg
    assert "{1, 2, 3}" in stored_msg


def test_huge_payload_logging(redis_client):
    """
    Scenario: Logging a large data dump (e.g. 500KB).
    """
    service_name = "HUGE_LOG_TEST"
    client = TelemetryClient(host="localhost", port=50051)

    # Manual setup to bypass RichHandler regex perf issues
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(AsyncGrpcHandler(client, service_name, queue_size=100))

    # 50KB is enough to prove the point without timing out Docker
    huge_msg = "X" * 50_000

    logger.info(huge_msg)

    time.sleep(1.0)

    log_json = redis_client.lindex(LOG_KEY, 0)
    assert log_json is not None, "Huge log not found"

    data = json.loads(log_json)
    payload = json.loads(data["payload_json"])
    assert len(payload["text"]) == 50_000


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
        name="test", level=logging.INFO, pathname=__file__, lineno=10,
        msg="Spam", args=(), exc_info=None
    )
    record.process = 1234
    record.threadName = "MainThread"

    # 4. Fill the queue
    # We do NOT start the worker thread logic (or we assume it's slow).
    # Since queue_size=1, the first put works.
    handler.emit(record)
    assert handler.queue.full(), "Queue should be full after 1 item"

    # 5. CRITICAL STEP: Attempt to overflow
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