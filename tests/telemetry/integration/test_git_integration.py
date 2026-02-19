import pytest
import time
import json
import logging
from panoseti_grpc.telemetry.logging import get_logger

LOG_KEY = "logs:ingress"

from unittest.mock import patch
import panoseti_grpc.telemetry.client as client_module

def test_git_metadata_flow(redis_client):
    # Patch the CONSTANT in the client module
    with patch.object(client_module, 'CACHED_COMMIT', 'deadbeef'):
        # Force a new client/logger creation to pick up any dynamic usage
        # (Though AsyncGrpcHandler might read the constant at init)
        logger = get_logger("GIT_TEST", grpc_enabled=True)
        logger.info("Testing Git Info")

        # 2. Create client (it reads client_module.CACHED_COMMIT)
        # client = client_module.TelemetryClient(host="localhost", port=50051)

        logger = get_logger(
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


