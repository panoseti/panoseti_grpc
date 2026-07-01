import asyncio

import pytest

from panoseti_grpc.daq_data.client import DaqDataClient, hp_io_config_simulate

pytestmark = pytest.mark.asyncio


async def test_sync_ping(sync_client: DaqDataClient):
    """Test the ping method with the sync client."""
    ping_success = await asyncio.to_thread(sync_client.ping)
    assert ping_success is True, "Ping should work for a running server"


async def test_sync_initialization(sync_client: DaqDataClient):
    """Test the InitHpIo RPC in simulation mode with the sync client."""
    success = await asyncio.to_thread(sync_client.init_hp_io, hp_io_config_simulate)
    assert success is True, "init_hp_io should succeed"


async def test_sync_stream_images(sync_client: DaqDataClient):
    """Test the full synchronous data streaming workflow."""
    assert await asyncio.to_thread(sync_client.init_hp_io, hp_io_config_simulate) is True

    def stream_and_validate_data() -> tuple[int, set[str]]:
        """Synchronous function to get and validate data from the stream."""
        image_stream = sync_client.stream_images(
            stream_movie_data=True,
            stream_pulse_height_data=True,
            update_interval_seconds=0.1,
        )

        received_images = 0
        image_types_seen = set()
        for image in image_stream:
            assert isinstance(image, dict)
            assert "type" in image and image["type"] in ("MOVIE", "PULSE_HEIGHT")
            image_types_seen.add(image["type"])
            received_images += 1
            if received_images >= 4:
                break
        return received_images, image_types_seen

    received_count, types_seen = await asyncio.wait_for(asyncio.to_thread(stream_and_validate_data), timeout=5.0)

    assert received_count >= 4, "Should receive at least 4 images"
    assert "MOVIE" in types_seen, "Should have received at least one MOVIE image"
    assert "PULSE_HEIGHT" in types_seen, "Should have received at least one PULSE_HEIGHT image"
