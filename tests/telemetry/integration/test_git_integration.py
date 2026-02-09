import pytest
import time
import json
import logging
from unittest.mock import patch
from panoseti_grpc.telemetry.client import make_grpc_logger, TelemetryClient

# We need to reload client to force it to pick up the mocked git info
import importlib
import panoseti_grpc.telemetry.client as client_module

LOG_KEY = "logs:ingress"


def test_git_metadata_flow(redis_client, start_grpc_server):
    """
    Verifies that GIT_COMMIT and GIT_BRANCH are correctly attached
    to the gRPC message and stored in Redis.
    """
    # 1. Mock the module-level constants in client.py
    # Since they are calculated at import time, we patch them directly on the module
    with patch.object(client_module, 'CACHED_COMMIT', 'deadbeef'), \
            patch.object(client_module, 'CACHED_BRANCH', 'feature-x'):
        logger_name = "GIT_TEST"
        # Use port 50051 (assuming fixture server is running there)
        client = TelemetryClient(host="localhost", port=50051)

        # Manually create logger using the patched module
        logger = client_module.make_grpc_logger(
            logger_name,
            grpc_client=client,
            level=logging.INFO
        )

        logger.info("Testing Git Info")

        time.sleep(1.0)

        # 2. Verify in Redis
        log_json = redis_client.lindex(LOG_KEY, -1)
        assert log_json is not None

        data = json.loads(log_json)

        assert data.get('git_commit') == 'deadbeef'
        assert data.get('git_branch') == 'feature-x'