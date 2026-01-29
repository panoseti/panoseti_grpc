import asyncio
import json
import logging
import grpc
import copy
import signal
import base64
from panoseti_grpc.generated import ublox_control_pb2, ublox_control_pb2_grpc
from .resources import make_rich_logger, default_f9t_cfg
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct


async def run():
    """
    Connects to the UbloxControl service, captures raw UBX message payloads,
    and saves them to a file for later use in simulations.
    """
    logger = make_rich_logger("UbloxDataSaver", level=logging.INFO)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Shutdown signal received.")
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    output_file = "ubx_packets.jsonl"
    logger.info(f"Will save raw packet data to {output_file}")

    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        # 1. Find the first available F9T chip config and initialize
        f9t_chip_config = None
        if default_f9t_cfg.get('f9t_chips'):
            f9t_chip_config = default_f9t_cfg['f9t_chips'][0]

        if not f9t_chip_config:
            logger.error("No F9T chip configurations found in f9t_config.json")
            return

        f9t_config = copy.deepcopy(default_f9t_cfg)
        del f9t_config['f9t_chips']
        f9t_config.update(f9t_chip_config)

        init_request = ublox_control_pb2.InitF9tRequest(
            f9t_config=ParseDict(f9t_config, Struct()),
            force_init=True
        )

        try:
            init_response = await stub.InitF9t(init_request)
            logger.info(f"InitF9t successful: {init_response.message}")
        except grpc.aio.AioRpcError as e:
            logger.error(f"InitF9t failed: {e.details()}")
            return

        # 2. Capture Ublox data and save the raw payload
        capture_request = ublox_control_pb2.CaptureUbloxRequest(patterns=[".*"])

        try:
            with open(output_file, "w") as f:
                async for response in stub.CaptureUblox(capture_request):
                    if stop_event.is_set():
                        logger.info("Capture stream cancelled by user.")
                        break

                    if response.payload:
                        # The server's cache key is the message identity (e.g., "TIM-TP")
                        packet_identity = response.name
                        # The raw bytes are in the payload field. Encode as base64 for JSON.
                        payload_b64 = base64.b64encode(response.payload).decode('ascii')

                        record = {
                            "identity": packet_identity,
                            "payload_b64": payload_b64
                        }
                        f.write(json.dumps(record) + "\n")
                        logger.info(f"Saved packet: {packet_identity}")

        except grpc.aio.AioRpcError as e:
            logger.error(f"CaptureUblox stream failed: {e.details()}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    asyncio.run(run())
