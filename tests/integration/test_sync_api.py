import pytest
from daq_data.daq_data_pb2 import PanoImage
from daq_data.client import DaqDataClient


def test_sync_ping(sync_client):
    """Test the Ping RPC with the sync client."""
    for host in sync_client.get_valid_daq_hosts():
        assert sync_client.ping(host) is True, "Ping should work for a running server"


def test_sync_initialization(sync_client):
    """Test the InitHpIo RPC in simulation mode with the sync client."""
    num_valid_hosts = len(sync_client.get_valid_daq_hosts())
    assert num_valid_hosts == 1, f"Exactly one DAQ node is expected. Got {num_valid_hosts=}"
    assert sync_client.init_sim(hosts=None) is True, "init_sim should succeed"


def test_sync_stream_images(sync_client):
    """Test the full synchronous data streaming workflow."""
    num_valid_hosts = len(sync_client.get_valid_daq_hosts())
    assert num_valid_hosts == 1, f"Exactly one DAQ node is expected. Got {num_valid_hosts=}"
    # 1. Initialize for simulation.
    assert sync_client.init_sim(hosts=None) is True

    # 2. Get the streaming generator.
    stream = sync_client.stream_images(
        hosts=None,
        stream_movie_data=True,
        stream_pulse_height_data=True,
        update_interval_seconds=0.1,
    )

    # 3. Receive and validate images.
    received_images = 0
    image_types_seen = set()
    for image in stream:
        assert isinstance(image, dict)
        assert 'type' in image and image['type'] in ('MOVIE', 'PULSE_HEIGHT')
        image_types_seen.add(image['type'])
        received_images += 1
        if received_images >= 4:
            break

    assert received_images >= 4
    assert 'MOVIE' in image_types_seen
    assert 'PULSE_HEIGHT' in image_types_seen
