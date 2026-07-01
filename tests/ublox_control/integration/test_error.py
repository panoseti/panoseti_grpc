# tests/test_integration.py

import copy

import grpc
import pytest
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.generated import ublox_control_pb2, ublox_control_pb2_grpc
from panoseti_grpc.ublox_control.resources import default_f9t_cfg

pytestmark = pytest.mark.skip(reason="ublox_control deprecated — tests preserved for the removal PR")

TIMEOUT = 5.0


@pytest.mark.asyncio
async def test_initf9t_error_no_device(live_server):
    """
    Tests that InitF9t fails gracefully if the device path is missing.
    """
    address = live_server["address"]
    async with grpc.aio.insecure_channel(address) as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        bad_config = copy.deepcopy(default_f9t_cfg)
        chip_config = bad_config["f9t_chips"][0]
        del chip_config["device"]
        bad_config.update(chip_config)
        del bad_config["f9t_chips"]

        request = ublox_control_pb2.InitF9tRequest(f9t_config=ParseDict(bad_config, Struct()))

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

        bad_config = copy.deepcopy(default_f9t_cfg)
        chip_config = bad_config["f9t_chips"][0]
        chip_config["device"] = "/dev/nonexistentdevice12345"
        bad_config.update(chip_config)
        del bad_config["f9t_chips"]

        request = ublox_control_pb2.InitF9tRequest(f9t_config=ParseDict(bad_config, Struct()))

        with pytest.raises(grpc.aio.AioRpcError) as e:
            await stub.InitF9t(request, timeout=TIMEOUT)

        # The server will hang trying to access the invalid device,
        # causing the client to time out.
        assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
