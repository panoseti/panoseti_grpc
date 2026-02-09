import pytest
import logging
import json
import time
from unittest.mock import MagicMock
from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient


class TestStructuredLogic:

    def test_automatic_string_wrapping(self):
        """
        Verify that if the user logs a plain string (not JSON),
        the handler wraps it in {"text": "..."} automatically.
        """
        mock_client = MagicMock(spec=TelemetryClient)
        handler = AsyncGrpcHandler(mock_client, "TEST", queue_size=10)

        # 1. Log a plain string
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname=__file__, lineno=10,
            msg="Hello World", args=(), exc_info=None
        )

        handler.emit(record)
        time.sleep(0.1)  # Let worker process

        # 2. Check Client Call
        mock_client.send_log_sync.assert_called_once()
        kwargs = mock_client.send_log_sync.call_args[1]

        # The message passed to gRPC should be a JSON string
        sent_json = kwargs['message']
        assert sent_json == '{"text": "Hello World"}'

    def test_context_capture(self):
        """
        Verify that file path and line number are correctly extracted
        from the Python LogRecord and passed to gRPC.
        """
        mock_client = MagicMock(spec=TelemetryClient)
        handler = AsyncGrpcHandler(mock_client, "TEST", queue_size=10)

        record = logging.LogRecord(
            name="test", level=logging.ERROR,
            pathname="/src/app/main.py",
            lineno=101,
            msg="Crash!", args=(), exc_info=None
        )

        handler.emit(record)
        time.sleep(0.1)

        kwargs = mock_client.send_log_sync.call_args[1]
        assert kwargs['file_path'] == "/src/app/main.py"
        assert kwargs['line_number'] == 101