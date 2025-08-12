import pytest
import asyncio
from daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio

@pytest.mark.parametrize(
    'sim_server_process',
    [
        'rpc_sim_server_config',
        'uds_sim_server_config',
        'filesystem_pipe_sim_server_config',
        'filesystem_poll_sim_server_config'
    ],
    indirect=True
)
async def test_simulation_modes(sim_server_process):
    """
    Tests that both RPC and UDS simulation modes can be initialized and stream data.
    """
    daq_config = {"daq_nodes": [{"ip_addr": sim_server_process}]}
    async with AioDaqDataClient(daq_config, network_config=None) as client:
        # 1. Initialize the server in simulation mode.
        success = await client.init_sim(hosts=None, timeout=10.0)
        assert success is True, "init_sim should succeed for all simulation modes"

        # 2. Request a data stream to confirm the data path is alive.
        stream = await asyncio.wait_for(client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.01,
            timeout=10.0,
        ), timeout=10.0)

        # 3. Receive and validate a few images.
        MIN_IMAGES_RECEIVED = 10
        received_images = 0
        async for image in stream:
            received_images += 1
            if received_images >= MIN_IMAGES_RECEIVED:
                break
        assert received_images >= MIN_IMAGES_RECEIVED, "Should receive images from the simulation"