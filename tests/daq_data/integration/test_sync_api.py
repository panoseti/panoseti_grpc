import asyncio
import pytest
from daq_data.daq_data_pb2 import PanoImage
from daq_data.client import DaqDataClient

pytestmark = pytest.mark.asyncio

async def test_sync_ping(sync_client: DaqDataClient):
    """Test the Ping RPC with the sync client."""
    # Run the synchronous get_valid_daq_hosts in a separate thread to avoid blocking the event loop
    valid_hosts = await asyncio.to_thread(sync_client.get_valid_daq_hosts)
    assert len(valid_hosts) == 1, f"Expected 1 valid host, but found {len(valid_hosts)}"

    # Test the ping method directly on the valid host
    host = valid_hosts[0]
    ping_success = await asyncio.to_thread(sync_client.ping, host)
    assert ping_success is True, "Ping should work for a running server"


async def test_sync_initialization(sync_client: DaqDataClient):
    """Test the InitHpIo RPC in simulation mode with the sync client."""
    num_valid_hosts = len(await asyncio.to_thread(sync_client.get_valid_daq_hosts))
    assert num_valid_hosts == 1, f"Exactly one DAQ node is expected. Got {num_valid_hosts=}"

    # Run the blocking init_sim call in a thread
    success = await asyncio.to_thread(sync_client.init_sim, hosts=None)
    assert success is True, "init_sim should succeed"


async def test_sync_stream_images(sync_client: DaqDataClient):
    """Test the full synchronous data streaming workflow."""
    num_valid_hosts = len(await asyncio.to_thread(sync_client.get_valid_daq_hosts))
    assert num_valid_hosts == 1, f"Exactly one DAQ node is expected. Got {num_valid_hosts=}"

    # 1. Initialize for simulation.
    assert await asyncio.to_thread(sync_client.init_sim, hosts=None) is True

    # 2. The stream returns a blocking generator. The entire iteration loop must run in a thread.
    def stream_and_validate_data():
        """Synchronous function to get and validate data from the stream."""
        image_stream = sync_client.stream_images(
            hosts=None,
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
        )

        received_images = 0
        image_types_seen = set()
        for image in image_stream:
            assert isinstance(image, dict)
            assert 'type' in image and image['type'] in ('MOVIE', 'PULSE_HEIGHT')
            image_types_seen.add(image['type'])
            received_images += 1
            if received_images >= 4:
                break
        return received_images, image_types_seen

    # 3. Run the synchronous loop in a thread and get the results.
    received_count, types_seen = await asyncio.wait_for(
        asyncio.to_thread(stream_and_validate_data),
        timeout=5.0
    )

    assert received_count >= 4, "Should receive at least 4 images"
    assert 'MOVIE' in types_seen, "Should have received at least one MOVIE image"
    assert 'PULSE_HEIGHT' in types_seen, "Should have received at least one PULSE_HEIGHT image"