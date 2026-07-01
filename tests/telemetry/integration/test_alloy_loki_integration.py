import json
import os
import time
import urllib.parse
import urllib.request
from typing import Any

from panoseti_grpc.telemetry.logger import get_logger

LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")


def wait_for_loki_log(loki_url: str, service_name: str, retries: int = 60, delay: float = 1.0) -> dict[str, Any] | None:
    """
    Polls the Loki HTTP API for logs belonging to a specific service.
    Returns the parsed JSON dictionary of the most recent log line if found, else None.
    Alloy scrapes files and pushes to Loki asynchronously, so we wait up to ~20s by default.
    """
    # Loki LogQL query format: {service="service_name"}
    query = f'{{service="{service_name}"}}'
    encoded_query = urllib.parse.urlencode({"query": query})
    url = f"{loki_url}/loki/api/v1/query_range?{encoded_query}"

    for i in range(retries):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=2) as response:
                print(f"[{service_name} poll {i}] status: {response.status}")
                if response.status == 200:
                    body = response.read().decode("utf-8")
                    data = json.loads(body)
                    results = data.get("data", {}).get("result", [])
                    print(f"[{service_name} poll {i}] results len: {len(results)}")
                    if results:
                        # Find the most recent log line across all returned streams
                        # values are typically [["<nanosecond_timestamp>", "<log_line_string>"], ...]
                        all_values = []
                        for stream_data in results:
                            all_values.extend(stream_data.get("values", []))

                        print(f"[{service_name} poll {i}] all_values len: {len(all_values)}")
                        if all_values:
                            # Sort by timestamp (index 0) descending
                            all_values.sort(key=lambda x: x[0], reverse=True)
                            latest_log_line = all_values[0][1]
                            print(f"[{service_name} poll {i}] parsed successfully!")
                            return json.loads(latest_log_line)
        except Exception as e:
            # Loki might not be fully up or network error, ignore and retry
            print(f"[{service_name} poll {i}] Exception: {e}")

        time.sleep(delay)
    return None


def test_loki_basic_logging(tmp_path: Any) -> None:
    """Verifies that a basic log reaches Loki via the JSONL -> Alloy pipeline."""
    service_name = f"LOKI_BASIC_TEST_{int(time.time() * 1000)}"
    logger = get_logger(service_name, log_dir="/var/log/panoseti", grpc_enabled=False, per_host=True)

    unique_msg = f"Basic Loki Pipeline Test - {time.time()}"
    logger.info(unique_msg)

    # Flush handlers to ensure file is written immediately
    for h in logger.handlers:
        h.flush()

    data = wait_for_loki_log(LOKI_URL, service_name)
    assert data is not None, f"Log for {service_name} failed to appear in Loki."
    assert data["service"] == service_name
    assert unique_msg in data["message"]
    assert data["level"] == "INFO"


def test_loki_unserializable_payload_handling(tmp_path: Any) -> None:
    """Verifies that non-serializable objects (like sets) don't crash the JSONL logger and are successfully ingested."""
    service_name = f"LOKI_BAD_DATA_TEST_{int(time.time() * 1000)}"
    logger = get_logger(service_name, log_dir="/var/log/panoseti", grpc_enabled=False, per_host=True)

    # A set {1, 2, 3} is not standard JSON serializable
    bad_payload = {"valid": 1, "invalid": {1, 2, 3}}
    logger.info(bad_payload)

    for h in logger.handlers:
        h.flush()

    data = wait_for_loki_log(LOKI_URL, service_name)
    assert data is not None, f"Log for {service_name} failed to appear in Loki."

    # The JsonlFormatter's json.dumps uses `default=str` which converts the set to a string
    assert "invalid" in data["message"]
    assert "1" in data["message"] and "2" in data["message"] and "3" in data["message"]


def test_loki_huge_payload_logging(tmp_path: Any) -> None:
    """Verifies that very large string payloads reach Loki intact without being truncated by the pipeline."""
    service_name = f"LOKI_HUGE_LOG_TEST_{int(time.time() * 1000)}"
    logger = get_logger(service_name, log_dir="/var/log/panoseti", grpc_enabled=False, per_host=True)

    huge_msg = "X" * 5000
    logger.info(huge_msg)

    for h in logger.handlers:
        h.flush()

    data = wait_for_loki_log(LOKI_URL, service_name)
    assert data is not None, "Huge log failed to appear in Loki."
    assert len(data["message"]) == 5000


def test_loki_metadata_context_propagation(tmp_path: Any) -> None:
    """Verifies that rich context (like process, thread, or custom extra fields) propagates to Loki."""
    service_name = f"LOKI_META_TEST_{int(time.time() * 1000)}"
    logger = get_logger(service_name, log_dir="/var/log/panoseti", grpc_enabled=False, per_host=True)

    def internal_function() -> None:
        # Pass a custom extra field "run_id" which Alloy extracts as a label
        logger.info("Inside Function", extra={"run_id": "test_run_999", "custom_field": "hello"})

    internal_function()

    for h in logger.handlers:
        h.flush()

    data = wait_for_loki_log(LOKI_URL, service_name)
    assert data is not None

    # The custom extra fields should be merged into the JSON
    assert data.get("run_id") == "test_run_999"
    assert data.get("custom_field") == "hello"

    # Core system metadata should be captured
    assert "pid" in data
    assert "hostname" in data
    assert "thread" in data


def test_loki_severity_level_propagation(tmp_path: Any) -> None:
    """Verifies that Python logging levels (WARNING, ERROR, CRITICAL) are correctly preserved in Loki."""
    service_name = f"LOKI_SEVERITY_TEST_{int(time.time() * 1000)}"
    logger = get_logger(service_name, log_dir="/var/log/panoseti", grpc_enabled=False, per_host=True)

    error_msg = "Critical Failure Simulation"
    logger.error(error_msg)

    for h in logger.handlers:
        h.flush()

    data = wait_for_loki_log(LOKI_URL, service_name)
    assert data is not None

    assert error_msg in data["message"]
    # The 'level' field should be extracted correctly as 'ERROR'
    assert data.get("level") == "ERROR"
