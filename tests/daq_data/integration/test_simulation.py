import pytest

from panoseti_grpc.daq_data.client import AioDaqDataClient

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("sim_server_process", ["uds_sim_server_config"], indirect=True)
async def test_simulation_modes(sim_server_process):
    """
    Tests that the UDS simulation mode can be initialized and stream data.
    """
    async with AioDaqDataClient(sim_server_process["host"], sim_server_process["port"]) as client:
        success = await client.init_sim(timeout=10.0)
        assert success is True, "init_sim should succeed for all simulation modes"

        MIN_IMAGES_RECEIVED = 10
        received_images = 0
        async for _image in client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.01,
            timeout=10.0,
        ):
            received_images += 1
            if received_images >= MIN_IMAGES_RECEIVED:
                break
        assert received_images >= MIN_IMAGES_RECEIVED, "Should receive images from the simulation"
