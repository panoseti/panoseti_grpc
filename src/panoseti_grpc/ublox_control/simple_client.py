import asyncio
import json
import logging

import grpc
import copy
import signal
from google.protobuf.json_format import ParseDict, MessageToDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.generated import ublox_control_pb2, ublox_control_pb2_grpc
from .resources import make_rich_logger, default_f9t_cfg


async def run():
    logger = make_rich_logger("UbloxControlClient", level=logging.DEBUG)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Shutdown signal received.")
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

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
        capture_request = ublox_control_pb2.CaptureUbloxRequest(
            patterns=[".*"]
        )
        logger.info(f"Sending CaptureUblox request: {capture_request}")
        try:
            async for response in stub.CaptureUblox(capture_request):
                if stop_event.is_set():
                    logger.info("CaptureUblox stream cancelled.")
                    return -1
                parsed_data = MessageToDict(
                    response,
                    always_print_fields_with_no_presence=True,
                    preserving_proto_field_name=True,
                )
                logger.info(f"Received data: {response.name}: {parsed_data}")
        except grpc.aio.AioRpcError as e:
            logger.error(f"CaptureUblox stream failed: {e.details()}")
            raise e


if __name__ == '__main__':

    asyncio.run(run())
