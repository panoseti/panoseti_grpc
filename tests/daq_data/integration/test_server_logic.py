import logging

import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient
from panoseti_grpc.grpc_utils.exceptions import FailedPreconditionError

pytestmark = pytest.mark.asyncio


async def test_stream_fails_if_not_initialized(default_server_process):
    """
    Verify that StreamImages fails with FAILED_PRECONDITION if InitHpIo has not been called.
    """
    async with AioDaqDataClient(
        default_server_process["host"],
        default_server_process["port"],
        log_level=logging.DEBUG,
    ) as client:
        with pytest.raises(FailedPreconditionError):
            async for _ in client.stream_images(
                stream_movie_data=True,
                stream_pulse_height_data=False,
                update_interval_seconds=1.0,
                timeout=10.0,
            ):
                break


async def test_status_rpc_initialization_flow(default_server_process):
    """
    Verify that Status correctly reports initialization state before and after InitHpIo.
    """
    async with AioDaqDataClient(
        default_server_process["host"],
        default_server_process["port"],
        log_level=logging.DEBUG,
    ) as client:
        # 1. Check status on uninitialized server
        status = await client.status()
        assert status is not None
        assert status.hp_io_initialized is False
        assert "not initialized" in status.message.lower()

        # 2. Initialize simulation
        success = await client.init_sim()
        assert success is True

        # 3. Check status again
        status = await client.status()
        assert status is not None
        assert status.hp_io_initialized is True
        assert "initialized and valid" in status.message.lower()
