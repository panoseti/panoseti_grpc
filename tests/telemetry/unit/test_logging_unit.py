import logging
import os
import threading
import time
from unittest.mock import MagicMock

from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient


class TestAsyncHandler:
    def test_non_blocking_behavior(self):
        """
        Ensures the handler drops logs instead of blocking when queue is full.
        """
        mock_client = MagicMock(spec=TelemetryClient)

        # FIXED: Make the worker 'stuck' so the queue stays full for our assertion
        mock_client.send_log_future.side_effect = lambda *args, **kwargs: time.sleep(0.5)

        # Create Handler with TINY queue
        handler = AsyncGrpcHandler(mock_client, "TEST_SERVICE", queue_size=1)

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname=__file__, lineno=10, msg="Test Message", args=(), exc_info=None
        )
        record.process = os.getpid()
        record.threadName = threading.current_thread().name

        # 1. Fill the queue (size=1)
        handler.emit(record)
        # Give worker a split second to pick it up and get stuck in the sleep
        time.sleep(0.05)

        # Now fill it again (this one sits in the queue)
        handler.emit(record)
        assert handler.queue.full()

        # 2. Emit AGAIN (Queue is full, worker is sleeping)
        start_time = time.time()
        handler.emit(record)
        duration = time.time() - start_time

        # Assert it was instant (non-blocking)
        assert duration < 0.01

        # Clean up
        handler._stop_event.set()

    def test_worker_payload_construction(self):
        """
        Verifies the worker thread correctly constructs the gRPC call.
        """
        mock_client = MagicMock(spec=TelemetryClient)
        handler = AsyncGrpcHandler(mock_client, "TEST_SERVICE", queue_size=10)

        # FIXED: Use simple string to avoid JSON-in-JSON escaping confusion in tests.
        # The worker wraps plain strings in {"text": ...}
        plain_msg = "Simple Text"

        test_item = {
            "msg": plain_msg,
            "level": 3,
            "timestamp": 1234567890.0,
            "file_path": "/tmp/test.py",
            "line_number": 42,
            "function_name": "test_func",
            # NEW REQUIRED FIELDS
            "process": 1234,
            "thread": "TestWorkerThread",
        }
        handler.queue.put(test_item)

        # Allow worker to process
        time.sleep(0.1)

        mock_client.send_log_future.assert_called_once()

        args, kwargs = mock_client.send_log_future.call_args
        assert kwargs["service"] == "TEST_SERVICE"
        assert kwargs["severity"] == 3

        # Ensure the message is present (it might be wrapped in JSON)
        assert plain_msg in kwargs["message"]

        handler._stop_event.set()
