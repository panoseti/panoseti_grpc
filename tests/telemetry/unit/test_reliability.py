import pytest
import logging
import time
from unittest.mock import MagicMock
from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient


# -----------------------------------------------------------------------------
# 1. Custom Fixture that bypasses helper functions to GUARANTEE correct wiring
# -----------------------------------------------------------------------------
@pytest.fixture
def crash_proof_logger():
    """
    Creates a logger manually wired to a MagicMock client.
    This avoids any risk of make_grpc_logger overwriting our mock.
    """
    # 1. Create the Mock Client (No spec, to avoid introspection issues)
    mock_client = MagicMock()

    # 2. Setup the Future Mock
    # When client.send_log_future() is called, it MUST return this future_mock
    future_mock = MagicMock()
    mock_client.send_log_future.return_value = future_mock

    # 3. Create the Handler manually
    handler = AsyncGrpcHandler(mock_client, "CrashTestDummy", queue_size=10)

    # 4. Create an isolated logger
    logger = logging.getLogger("CrashTestDummy")
    logger.setLevel(logging.INFO)
    # Clear existing handlers to prevent duplicates during testing
    logger.handlers = []
    logger.addHandler(handler)

    return logger, mock_client


# -----------------------------------------------------------------------------
# 2. Test Cases
# -----------------------------------------------------------------------------

def test_handler_survives_unserializable_object(crash_proof_logger):
    """
    Scenario: User logs an object that crashes json.dumps().
    """
    logger, _ = crash_proof_logger

    class UnserializableObj:
        def __str__(self): return "I am problematic"

        def __repr__(self): return "I am problematic"
        # Removing to_json/dict to force serialization issues if not handled strings

    try:
        # Should not raise exception
        logger.info({"bad_data": UnserializableObj()})
    except Exception as e:
        pytest.fail(f"Logger crashed the application: {e}")

    # Give worker time to process
    time.sleep(0.1)
    # Passed if we didn't crash


def test_handler_survives_schema_violation(crash_proof_logger):
    """
    Scenario: The worker thread tries to send data, but the gRPC client raises an error.
    Expectation: The worker thread should stay alive.
    """
    logger, mock_client = crash_proof_logger

    # 1. Configure the Mock to crash on the FIRST call, succeed on SECOND
    mock_client.send_log_future.side_effect = [
        RuntimeError("Protobuf Schema Mismatch!"),  # 1st log crashes
        MagicMock()  # 2nd log returns a fresh future
    ]

    # 2. Send logs
    logger.info("POISON PILL")
    time.sleep(0.1)  # Wait for worker to catch exception and print error

    logger.info("HEALTHY LOG")
    time.sleep(0.1)  # Wait for worker to process success

    # 3. Verify the worker tried TWICE
    # If the thread died after the first error, this would be 1
    assert mock_client.send_log_future.call_count == 2, \
        f"Worker died! Call count is {mock_client.send_log_future.call_count}"


def test_handler_survives_queue_overflow(crash_proof_logger):
    """
    Scenario: Logging faster than the network can handle.
    Expectation: Drop logs, don't crash.
    """
    logger, mock_client = crash_proof_logger

    # Reset mock to ensure clean state
    mock_client.send_log_future.reset_mock()
    # Ensure it always returns a valid future (mock)
    mock_client.send_log_future.side_effect = None
    mock_client.send_log_future.return_value = MagicMock()

    # Fill queue (size 10) with 25 items
    for i in range(25):
        logger.info(f"Log {i}")

    # Wait for drain
    time.sleep(0.5)

    # We expect at least 10 calls (the queue size). 
    # Some might be dropped, some might process fast.
    assert mock_client.send_log_future.call_count >= 10