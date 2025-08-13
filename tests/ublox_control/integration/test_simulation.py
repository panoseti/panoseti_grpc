# tests/test_simulation.py

import pytest
import pytest_asyncio
import asyncio
import json
import base64
from pyubx2 import UBXReader


# This assumes you have run save_raw_ubx.py and have ubx_packets.jsonl
@pytest.fixture(scope="module")
def raw_ubx_packets():
    """Loads the captured raw UBX packet data."""
    packets = []
    try:
        with open("ubx_packets.jsonl", "r") as f:
            for line in f:
                packets.append(json.loads(line))
    except FileNotFoundError:
        pytest.skip("ubx_packets.jsonl not found. Run save_raw_ubx.py first.")
    return packets


@pytest.mark.asyncio
async def test_inject_and_capture(sim_servicer, raw_ubx_packets):
    """
    Tests the server's caching and streaming logic by injecting data directly
    into the servicer's cache and then reading it back via CaptureUblox.
    """
    # 1. Inject data into the servicer's cache
    injected_packets = {}
    async with sim_servicer._cache_lock:
        for packet_data in raw_ubx_packets:
            raw_bytes = base64.b64decode(packet_data["payload_b64"])
            # The UBXReader needs a stream-like object
            from io import BytesIO
            stream = BytesIO(raw_bytes)
            ubr = UBXReader(stream)
            _, parsed = ubr.read()

            if parsed:
                # The server caches the parsed UBXMessage object
                sim_servicer._packet_cache[parsed.identity] = parsed
                injected_packets[parsed.identity] = parsed

    assert len(sim_servicer._packet_cache) > 0, "Injection failed, cache is empty."

    # 2. Simulate a client connecting and capturing data
    # We will call the method directly instead of via RPC for this unit test
    request = ublox_control_pb2.CaptureUbloxRequest(patterns=[".*"])

    # Mock the context to prevent errors
    class MockContext:
        def peer(self):
            return "sim_client"

        async def abort(self, code, details):
            raise Exception(details)

        def cancelled(self):
            return False

    context = MockContext()

    # The servicer's io_task must appear to be running
    sim_servicer._io_task = asyncio.create_task(asyncio.sleep(3600))

    # 3. Read from the stream and verify
    received_packets = {}
    async for response in sim_servicer.CaptureUblox(request, context):
        received_packets[response.name] = response.payload
        # The initial broadcast should empty the cache for the client
        if len(received_packets) == len(injected_packets):
            break

    assert len(received_packets) == len(injected_packets), "Did not receive all injected packets."

    # Verify that the payload of received packets matches the injected ones
    for identity, parsed_msg in injected_packets.items():
        assert identity in received_packets
        # Serialize the injected message to compare its raw form with what was received
        assert received_packets[identity] == parsed_msg.serialize()

    sim_servicer._io_task.cancel()  # clean up task
