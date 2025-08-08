import pytest
import asyncio
from daq_data.daq_data_pb2 import PanoImage
from daq_data.client import AioDaqDataClient
import grpc

pytestmark = pytest.mark.asyncio


async def test_async_ping(async_client):
    """Test the Ping RPC with the async client."""
    host = list(async_client.daq_nodes.keys())[0]
    assert await async_client.ping(host) is True


async def test_async_stream_images(async_client):
    """Test the full data streaming workflow: init -> stream -> receive."""
    assert await async_client.init_sim(hosts=None) is True
    stream = await async_client.stream_images(
        hosts=None, stream_movie_data=True, stream_pulse_height_data=True,
        update_interval_seconds=0.05,
    )
    received_images = 0
    async for image in stream:
        assert isinstance(image, dict)
        received_images += 1
        if received_images >= 2:
            break
    assert received_images >= 2


async def test_async_stream_stops_with_event(default_server_process):
    """Verify that the stream can be gracefully shut down with a stop_event."""
    stop_event = asyncio.Event()
    daq_config = {"daq_nodes": [{"ip_addr": default_server_process}]}
    # The AioDaqDataClient's stop_event must be passed during initialization
    async with AioDaqDataClient(daq_config, network_config=None, stop_event=stop_event) as client:
        assert await client.init_sim(hosts=None)
        stream = await client.stream_images(
            hosts=None, stream_movie_data=True, stream_pulse_height_data=True,
            update_interval_seconds=0.05,
        )

        async def stop_streamer():
            await asyncio.sleep(0.2)
            stop_event.set()

        stopper_task = asyncio.create_task(stop_streamer())
        images_received = 0
        async for _ in stream:
            images_received += 1

        await stopper_task
        assert images_received > 0


async def test_async_initialization(async_client):
    """Test the InitHpIo RPC in simulation mode with the async client."""
    success = await async_client.init_sim(hosts=None)
    assert success is True, "init_sim should succeed"

