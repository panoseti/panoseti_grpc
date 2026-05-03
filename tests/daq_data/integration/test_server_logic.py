import logging

import grpc
import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio


async def test_stream_fails_if_not_initialized(default_server_process):
    """
    Verify that StreamImages fails with FAILED_PRECONDITION if InitHpIo has not been called.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    stop_event = default_server_process["stop_event"]
    async with AioDaqDataClient(
        daq_config, network_config=None, log_level=logging.DEBUG, stop_event=stop_event
    ) as client:
        with pytest.raises(grpc.aio.AioRpcError) as e:
            stream = await client.stream_images(
                hosts=None,
                stream_movie_data=True,
                stream_pulse_height_data=False,  # Can be false
                update_interval_seconds=1.0,  # Must be provided
                timeout_sec=10.0,
            )
            await stream.__anext__()
        assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION


async def test_status_rpc_initialization_flow(default_server_process):
    """
    Verify that Status correctly reports initialization state before and after InitHpIo.
    """
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process["ip_addr"]}]}
    stop_event = default_server_process["stop_event"]
    async with AioDaqDataClient(
        daq_config, network_config=None, log_level=logging.DEBUG, stop_event=stop_event
    ) as client:
        host = default_server_process["ip_addr"]

        # 1. Check status on uninitialized server
        status = await client.status(host)
        assert status is not None
        assert status.hp_io_initialized is False
        assert "not initialized" in status.message.lower()

        # 2. Initialize simulation
        success = await client.init_sim(host)
        assert success is True

        # 3. Check status again
        status = await client.status(host)
        assert status is not None
        assert status.hp_io_initialized is True
        assert "initialized and valid" in status.message.lower()
