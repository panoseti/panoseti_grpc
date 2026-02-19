import pytest
import time
import json
import logging
from unittest.mock import MagicMock
from panoseti_grpc.telemetry.client import make_grpc_logger, TelemetryClient, AsyncGrpcHandler

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


def test_unserializable_payload_handling(redis_client):
    service_name = "BAD_DATA_TEST"
    client = TelemetryClient(host="localhost", port=50051)
    logger = make_grpc_logger(service_name, grpc_client=client)

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