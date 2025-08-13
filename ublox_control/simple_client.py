import asyncio
import json
import grpc
import copy
from ublox_control import ublox_control_pb2, ublox_control_pb2_grpc
from ublox_control.resources import make_rich_logger, default_f9t_cfg
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct


async def run():
    logger = make_rich_logger("UbloxControlClient")
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        # 1. Initialize the F9T
        for f9t_chip in default_f9t_cfg['f9t_chips']:
            # Remove f9t_chips and update dict with config for just f9t_chip
            f9t_config = copy.deepcopy(default_f9t_cfg)
            del f9t_config['f9t_chips']
            f9t_config.update(f9t_chip)

            init_request = ublox_control_pb2.InitF9tRequest(
                f9t_config = ParseDict(f9t_config, Struct()),
                force_init=True
            )
            # logger.info(f"Sending InitF9t request: {init_request}")
            try:
                init_response = await stub.InitF9t(init_request)
                logger.info(f"InitF9t response: {init_response.message}")
                break
            except grpc.aio.AioRpcError as e:
                logger.error(f"InitF9t failed: {e.details()}")
                return -1

        # 2. Capture Ublox data
        capture_request = ublox_control_pb2.CaptureUbloxRequest()
        try:
            async for response in stub.CaptureUblox(capture_request):
                logger.debug(f"Received data: {response.name}")
        except grpc.aio.AioRpcError as e:
            logger.error(f"CaptureUblox stream failed: {e.details()}")
            raise e


if __name__ == '__main__':
    asyncio.run(run())
