import pytest
import asyncio
import sys
from unittest.mock import MagicMock, patch
import time
from panoseti_grpc.telemetry.logger import (
    get_logger,
    monitor_subprocess,
    PanosetiLogFactory,
    _stream_reader
)

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
    #    Path: panoseti_grpc.telemetry.logger.TelemetryClient
    with patch("panoseti_grpc.telemetry.logger.TelemetryClient") as MockClientClass:
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

    # Use .err and .out
    output = captured.err + captured.out
    assert "StdOut Msg" in output
    assert "StdErr Msg" in output


@pytest.mark.asyncio
async def test_stream_reader_invalid_utf8():
    """
    Failure Mode: A C-program or subprocess outputs binary garbage.
    Expected: The reader uses errors='replace' and does not crash, logging the replacement character.
    """
    mock_stream = asyncio.StreamReader()
    # Feed valid text, followed by invalid UTF-8 bytes (\xff\xfe), followed by valid text
    mock_stream.feed_data(b"valid line 1\n")
    mock_stream.feed_data(b"bad bytes \xff\xfe here\n")
    mock_stream.feed_data(b"valid line 2\n")
    mock_stream.feed_eof()

    logged_messages = []

    def mock_logger_method(msg):
        logged_messages.append(msg)

    await _stream_reader(mock_stream, mock_logger_method)

    assert len(logged_messages) == 3
    assert logged_messages[0] == "valid line 1"
    # \ufffd is the standard Unicode replacement character
    assert "bad bytes \ufffd\ufffd here" in logged_messages[1]
    assert logged_messages[2] == "valid line 2"


@pytest.mark.asyncio
async def test_stream_reader_skips_whitespace_lines():
    """
    Expected Use Case: Subprocess prints blank lines or trailing spaces.
    Expected: The reader strips whitespace and drops completely empty messages to avoid log spam.
    """
    mock_stream = asyncio.StreamReader()
    mock_stream.feed_data(b"data1\n")
    mock_stream.feed_data(b"\n")  # Empty line
    mock_stream.feed_data(b"   \n")  # Spaces only
    mock_stream.feed_data(b"\t\t\n")  # Tabs only
    mock_stream.feed_data(b"data2   \n")  # Trailing spaces
    mock_stream.feed_eof()

    logged_messages = []

    def mock_logger_method(msg):
        logged_messages.append(msg)

    await _stream_reader(mock_stream, mock_logger_method)

    # Out of 5 lines, 3 should be skipped. "data2   " should be stripped to "data2"
    assert len(logged_messages) == 2
    assert logged_messages[0] == "data1"
    assert logged_messages[1] == "data2"


@pytest.mark.asyncio
async def test_monitor_subprocess_missing_pipes():
    """
    Failure Mode: The subprocess was created without stdout=PIPE or stderr=PIPE.
    Expected: monitor_subprocess detects this, logs a warning, and returns safely without raising an AttributeError.
    """
    logger_mock = MagicMock()

    # Create a process but intentionally FORGET to set stdout/stderr to asyncio.subprocess.PIPE
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "print('hello')"
    )

    # Monitor it
    await monitor_subprocess(proc, logger_mock)
    await proc.wait()

    # Verify the warning was logged
    logger_mock.warning.assert_called_once()
    assert "without piped streams" in logger_mock.warning.call_args[0][0]

    # It should exit gracefully without trying to read None streams
    logger_mock.info.assert_not_called()
    logger_mock.error.assert_not_called()


@pytest.mark.asyncio
async def test_monitor_subprocess_large_output_concurrency():
    """
    Expected Use Case: High volume of data spewing from both stdout and stderr simultaneously.
    Expected: monitor_subprocess captures all lines asynchronously without deadlocking.
    """
    logger_mock = MagicMock()

    # A script that blasts 1000 lines to stdout and 1000 lines to stderr
    script_cmd = """
import sys
for i in range(1000):
    print(f"OUT {i}")
    print(f"ERR {i}", file=sys.stderr)
"""

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", script_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    await monitor_subprocess(proc, logger_mock)
    await proc.wait()

    # Verify all 1000 info and 1000 error calls were made
    assert logger_mock.info.call_count == 1000
    assert logger_mock.error.call_count == 1000

    # Sample check the first and last
    logger_mock.info.assert_any_call("OUT 0")
    logger_mock.info.assert_any_call("OUT 999")
    logger_mock.error.assert_any_call("ERR 0")
    logger_mock.error.assert_any_call("ERR 999")


@pytest.mark.asyncio
async def test_logger_triple_dispatch(tmp_path, capsys):
    """
    Expected Use Case: A worker node needs to log to Console, File, and Headnode.
    Expected: monitor_subprocess captures stdout, and the logger routes the exact
              message to all 3 configured handlers without dropping data.
    """
    # 1. Isolate and Mock the gRPC Client
    PanosetiLogFactory.reset_clients()
    mock_client = MagicMock()
    mock_future = MagicMock()
    mock_client.send_log_future.return_value = mock_future
    PanosetiLogFactory._grpc_clients[("localhost", 50051)] = mock_client

    # 2. Initialize Logger with ALL destinations enabled
    logger = get_logger(
        "TripleThreatWorker",
        log_dir=str(tmp_path),  # Temp directory provided by pytest
        grpc_enabled=True,
        console=True
    )

    # 3. Create a subprocess that emits a specific traceable sequence
    unique_msg = "CRITICAL_CORE_BREACH_DETECTED_1337"
    script = f"print('{unique_msg}')"

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-u", "-c", script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # 4. Monitor and wait
    await monitor_subprocess(proc, logger)
    await proc.wait()

    # Allow a brief moment for asynchronous/threaded handlers (like File/gRPC queues) to flush
    time.sleep(0.5)

    # --- VERIFICATION PHASE ---

    # A. Verify Console (RichHandler usually writes to stderr)
    captured = capsys.readouterr()
    console_output = captured.out + captured.err
    assert unique_msg in console_output, "Message missing from Console Output"

    # B. Verify File System (RotatingFileHandler)
    log_file = tmp_path / "TripleThreatWorker.log"
    assert log_file.exists(), f"Log file was not created in {tmp_path}"
    file_content = log_file.read_text()
    assert unique_msg in file_content, "Message missing from File Output"

    # C. Verify gRPC Telemetry
    assert mock_client.send_log_future.called, "gRPC client was never invoked"

    # Robustly check ALL arguments (positional and keyword) across ALL recorded calls
    all_calls_str = str(mock_client.send_log_future.mock_calls)

    assert unique_msg in all_calls_str, \
        f"Message missing from gRPC payload. Recorded calls: {all_calls_str}"
