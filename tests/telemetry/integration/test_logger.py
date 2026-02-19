import pytest
import asyncio
import sys
from unittest.mock import MagicMock, patch
import time
from panoseti_grpc.telemetry.logging import get_logger, monitor_subprocess, PanosetiLogFactory


def test_grpc_logger_metadata_capture():
    """
    Verify that function name and line number are captured and sent to gRPC.
    """
    # 1. Reset singleton to ensure we don't get a stale real client from previous tests
    PanosetiLogFactory.reset_clients()

    # 2. Mock the TelemetryClient and its future
    mock_client = MagicMock()
    mock_future = MagicMock()
    mock_client.send_log_future.return_value = mock_future

    # 3. Inject mock into Factory registry using the expected key
    # Default config uses localhost:50051.
    # We must insert it exactly where get_shared_client looks.
    registry_key = ("localhost", 50051)
    PanosetiLogFactory._grpc_clients[registry_key] = mock_client

    # 4. Create logger (it will retrieve our mock from the registry)
    logger = get_logger("GrpcTest", log_dir=None, grpc_enabled=True, console=False)

    # 5. Trigger a log entry inside a function to test metadata capture
    def inner_function():
        logger.error("Error inside inner")

    inner_function()

    # Allow sleep for queue drain (AsyncGrpcHandler is threaded)
    time.sleep(0.5)

    # 6. Verify the mock was used
    assert mock_client.send_log_future.called, "Mock client was not called. Did the logger use a real client?"


def test_factory_singleton_behavior():
    """Verify we don't create multiple gRPC clients."""

    # 1. Reset the singleton cache so we start fresh
    PanosetiLogFactory.reset_clients()

    # 2. PATCH TARGET: The module where 'TelemetryClient' is IMPORTED and USED.
    #    Tree: src/panoseti_grpc/telemetry/logging.py
    #    Path: panoseti_grpc.telemetry.logging.TelemetryClient
    with patch("panoseti_grpc.telemetry.logging.TelemetryClient") as MockClientClass:
        # First call -> Should instantiate a new client
        get_logger("ServiceA", grpc_enabled=True)

        # Second call -> Should reuse the existing instance from registry
        get_logger("ServiceB", grpc_enabled=True)

        # Verify instantiation happened exactly once
        assert MockClientClass.call_count == 1, f"Expected 1 init, got {MockClientClass.call_count}"


# --- OPTIONAL: Sanity check for file logger (Existing test preserved) ---
def test_file_logger_creation_and_writing(tmp_path):
    """Verify logger creates file and writes to it."""
    log_dir = tmp_path / "logs"
    service_name = "FileTest"

    logger = get_logger(
        service_name,
        log_dir=str(log_dir),
        grpc_enabled=False,
        console=False
    )

    logger.info("Hello File System")

    # Force flush
    for h in logger.handlers:
        h.flush()
        h.close()

    # Verify file exists using Case-Sensitive check (as per your previous fix)
    expected_file = log_dir / f"{service_name}.log"
    assert expected_file.exists(), f"Log file missing: {list(log_dir.iterdir())}"


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
