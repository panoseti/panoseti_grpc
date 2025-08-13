# tests/test_integration.py

import pytest
import pytest_asyncio
import grpc
import copy
from ublox_control import ublox_control_pb2, ublox_control_pb2_grpc
from ublox_control.resources import default_f9t_cfg
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

TIMEOUT = 10.0


@pytest.mark.asyncio
async def test_initf9t_error_no_device(live_server):
    """
    Tests that InitF9t fails gracefully if the device path is missing.
    """
    address = live_server["address"]
    async with grpc.aio.insecure_channel(address) as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        # Create a config with the 'device' key removed
        bad_config = copy.deepcopy(default_f9t_cfg)
        chip_config = bad_config['f9t_chips'][0]
        del chip_config['device']
        bad_config.update(chip_config)
        del bad_config['f9t_chips']

        request = ublox_control_pb2.InitF9tRequest(
            f9t_config=ParseDict(bad_config, Struct())
        )

        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.InitF9t(request, timeout=TIMEOUT)

        assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "Device path not specified" in e.value.details()


@pytest.mark.asyncio
async def test_initf9t_error_invalid_device(live_server):
    """
    Tests that InitF9t fails if the device path does not exist.
    """
    address = live_server["address"]
    async with grpc.aio.insecure_channel(address) as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        # Create a config with a non-existent device path
        bad_config = copy.deepcopy(default_f9t_cfg)
        chip_config = bad_config['f9t_chips'][0]
        chip_config['device'] = "/dev/nonexistentdevice12345"  # Should not exist
        bad_config.update(chip_config)
        del bad_config['f9t_chips']

        request = ublox_control_pb2.InitF9tRequest(
            f9t_config=ParseDict(bad_config, Struct())
        )

        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.InitF9t(request, timeout=TIMEOUT)

        assert e.value.code() == grpc.StatusCode.UNAVAILABLE
        assert "Could not connect to device" in e.value.details()


@pytest.mark.asyncio
async def test_initf9t_error_uid_mismatch(live_server):
    """
    Tests that InitF9t fails if the detected UID doesn't match the client's.

    NOTE: This test requires a virtual serial port setup to mock the hardware
    response. For this example, we'll assume the server's get_f9t_unique_id
    could be patched, but in a real scenario, `socat` would be used.
    Since patching a running server process is complex, this test outlines
    the principle. A full implementation would require more setup.
    """
    # This is a placeholder for a more complex test.
    # The server would need to be started with a mocked `get_f9t_unique_id`
    # or connected to a virtual serial port controlled by the test.

    # Principle of the test:
    # 1. Start server connected to a virtual serial port.
    # 2. Test sends a fake UBX-SEC-UNIQID response with UID "AAAAAAAAAA" to the port.
    # 3. Client sends InitF9t request with expected UID "BBBBBBBBBB".
    # 4. Assert server returns INVALID_ARGUMENT with a UID mismatch error.
    pytest.skip("Full UID mismatch test requires advanced setup (e.g., socat).")

