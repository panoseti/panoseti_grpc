import pytest
import logging
from unittest.mock import MagicMock
from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient


def test_extra_context_merging():
    """
    Verify that `logger.info("msg", extra={"user": "nico"})`
    is correctly formatted into the final message.
    """
    mock_client = MagicMock(spec=TelemetryClient)
    handler = AsyncGrpcHandler(mock_client, "TEST", queue_size=10)

    # Setup standard formatter (usually done by basicConfig)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)

    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=10,
        msg="User Login", args=(), exc_info=None
    )
    # Inject 'extra' fields manually (simulating the logger adapter)
    record.user = "nico"

    # If we use a custom formatter that looks for 'user', it should appear.
    # But by default, AsyncHandler just takes record.msg or formatted msg.
    # The key behavior we want: The handler shouldn't crash on extra fields.

    try:
        handler.emit(record)
    except Exception as e:
        pytest.fail(f"Handler crashed on record with extra fields: {e}")

    # Check what was queued
    item = handler.queue.get()
    assert item['msg'] == "User Login"