import pytest
import time
import json
import logging
import panoseti_grpc.telemetry.logging as client_module

LOG_KEY = "logs:ingress"

def test_git_metadata_flow(redis_client, start_grpc_server):
    """
    Verifies that GIT_COMMIT is attached to logs.
    We manually force the client module's cached variable to 'deadbeef'.
    """
    # 1. Force the internal cached variable to our test value
    # This bypasses the need to mock get_sw_info() or reload the module
    original_commit = getattr(client_module, 'CACHED_COMMIT', 'unknown')
    client_module.CACHED_COMMIT = 'deadbeef'

    try:
        # 2. Create client (it reads client_module.CACHED_COMMIT)
        # client = client_module.TelemetryClient(host="localhost", port=50051)

        logger = client_module.get_logger(
            "GIT_TEST",
            grpc_enabled=True,
            level=logging.INFO
        )

        logger.info("Testing Git Info")
        time.sleep(1.0)

        # 3. Verify
        found = False
        logs = redis_client.lrange("logs:ingress", -10, -1)
        for log in logs:
            data = json.loads(log)
            if data.get("service_name") == "git_test":
                assert data.get('git_commit') == 'deadbeef'
                found = True
                break

        assert found, f"Could not find git_test log. Last logs: {logs}"

    finally:
        # 4. Restore original state to avoid polluting other tests
        client_module.CACHED_COMMIT = original_commit

