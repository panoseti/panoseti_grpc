import pytest
import logging
import asyncio
import sys
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

# Update this import to match where you placed logging.py
# If it is in src/panoseti_grpc/telemetry/logging.py:
from panoseti_grpc.telemetry.logging import get_logger, monitor_subprocess, PanosetiLogFactory
from panoseti_grpc.telemetry.client import AsyncGrpcHandler


# Reset the singleton between tests
@pytest.fixture(autouse=True)
def reset_log_factory():
    PanosetiLogFactory._shared_grpc_client = None
    PanosetiLogFactory._loggers = {}
    yield


def test_file_logger_creation_and_writing(tmp_path):
    """Verify logger creates file and writes to it."""
    log_dir = tmp_path / "logs"
    name = "FileTest"
    logger = get_logger(
       name,
        log_dir=str(log_dir),
        grpc_enabled=False,
        console=False
    )

    logger.info("Hello File System")

    # Check file exists
    expected_file = log_dir / f"{name}.log"
    assert expected_file.exists()

    # Check content
    content = expected_file.read_text()
    assert "Hello File System" in content
    assert "INFO" in content


def test_console_logger_output(capsys):
    """Verify logger writes to stdout."""
    logger = get_logger(
        "ConsoleTest",
        log_dir=None,
        grpc_enabled=False,
        console=True
    )

    logger.warning("Watch out!")

    # Capture stdout/stderr
    captured = capsys.readouterr()

    # FIX: Use .err and .out instead of .stderr and .stdout
    # RichHandler usually writes logs to stderr
    output = captured.err + captured.out
    assert "Watch out!" in output


def test_grpc_logger_metadata_capture():
    """Verify that function name and line number are captured and sent to gRPC."""

    # Mock the TelemetryClient
    mock_client = MagicMock()
    mock_future = MagicMock()
    mock_client.send_log_future.return_value = mock_future

    # Inject mock into Factory directly
    PanosetiLogFactory._shared_grpc_client = mock_client

    logger = get_logger("GrpcTest", log_dir=None, grpc_enabled=True, console=False)

    # Define a nested function to test function name capture
    def inner_function():
        logger.error("Error inside inner")

    inner_function()

    # Allow trivial sleep for queue drain
    import time
    time.sleep(0.1)

    # Check calls
    assert mock_client.send_log_future.called

    # Get arguments passed to send_log_future
    _, kwargs = mock_client.send_log_future.call_args

    assert kwargs['service'] == "GrpcTest"
    assert kwargs['function_name'] == "inner_function"

    # FIX: Don't assert exact filename "test_logging_system.py",
    # instead check if the actual filename (test_logger.py) is in the path.
    current_file = os.path.basename(__file__)
    assert current_file in kwargs['file_path']
    assert kwargs['severity'] == 4  # ERROR


@pytest.mark.asyncio
async def test_subprocess_stream_capture(capsys):
    """Verify capturing stdout from a C-like subprocess."""

    logger = get_logger("StreamTest", log_dir=None, grpc_enabled=False, console=True)

    # Create a simple echo process
    script_cmd = "import sys; print('StdOut Msg'); print('StdErr Msg', file=sys.stderr)"

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", script_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    await monitor_subprocess(proc, logger)
    await proc.wait()

    captured = capsys.readouterr()

    # FIX: Use .err and .out
    output = captured.err + captured.out
    assert "StdOut Msg" in output
    assert "StdErr Msg" in output


def test_factory_singleton_behavior():
    """Verify we don't create multiple gRPC clients."""
    with patch("panoseti_grpc.telemetry.client.TelemetryClient") as MockClient:
        # 1. First call should create a client
        get_logger("ServiceA", grpc_enabled=True)

        # 2. Second call (same host/port) should reuse it
        get_logger("ServiceB", grpc_enabled=True)

        # Should only initialize the client ONCE
        assert MockClient.call_count == 1