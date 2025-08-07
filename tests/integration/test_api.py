import pytest
import asyncio
from daq_data.daq_data_pb2 import PanoImage

pytestmark = pytest.mark.asyncio


async def test_ping(server_and_client):
    """Test the Ping RPC to ensure the server is responsive."""
    client = server_and_client
    host = client.daq_nodes.keys().__iter__().__next__()
    assert await client.ping(host) is True, "Ping should return True for a running server"


async def test_initialization(server_and_client):
    """Test the InitHpIo RPC in simulation mode."""
    client = server_and_client

    # Use the client's init_sim convenience method
    success = await client.init_sim(hosts=None)
    assert success is True, "init_sim should succeed"


async def test_stream_images(server_and_client):
    """Test the full data streaming workflow: init -> stream -> receive."""
    client = server_and_client

    # 1. Initialize the server in simulation mode. The test server is configured
    #    for "rpc" mode, so this will start the simulation data flow.
    assert await client.init_sim(hosts=None) is True

    # 2. Request a data stream
    stream = await client.stream_images(
        hosts=None,
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=0.1,
    )

    # 3. Receive and validate a few images
    received_images = 0
    image_types_seen = set()
    async for image in stream:
        assert isinstance(image, dict)
        assert 'type' in image
        assert 'header' in image
        assert 'image_array' in image

        image_types_seen.add(image['type'])
        received_images += 1

        if received_images >= 4:  # Get a few movie and ph images
            break

    assert received_images >= 4
    assert 'MOVIE' in image_types_seen
    assert 'PULSE_HEIGHT' in image_types_seen


async def test_stream_stops_with_event(server_and_client):
    """Verify that the stream can be gracefully shut down with a stop_event."""
    client = server_and_client
    stop_event = asyncio.Event()

    # Re-initialize the client with a stop_event
    client._stop_event = stop_event

    assert await client.init_sim(hosts=None)

    stream = await client.stream_images(
        hosts=None,
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=0.05,
    )

    # Let the stream run for a moment, then signal it to stop
    async def stop_streamer():
        await asyncio.sleep(0.2)
        stop_event.set()

    stopper_task = asyncio.create_task(stop_streamer())

    # This loop should exit gracefully when the event is set
    images_received = 0
    async for _ in stream:
        images_received += 1

    await stopper_task
    assert images_received > 0, "Should have received at least one image before stopping"
