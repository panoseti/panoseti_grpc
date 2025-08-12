#!/usr/bin/env python3
"""
The Python implementation of a gRPC UbloxControl server.

Requires the following to function correctly:
    1. A POSIX-compliant operating system.
    2. A valid connection to a ZED-F9T u-blox chip.
    3. All Python packages specified in requirements.txt.
"""
import asyncio
import logging
import signal
import json
from typing import Optional
from pathlib import Path
import grpc
from grpc_reflection.v1alpha import reflection
from ublox_control import (
    ublox_control_pb2,
    ublox_control_pb2_grpc,
)
from ublox_control.managers import F9tManager, ClientManager, F9tIoManager
from ublox_control.resources import CFG_DIR, make_rich_logger


class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """
    Provides async methods that implement the UbloxControl service.
    """

    def __init__(self, f9t_manager: F9tManager, client_manager: ClientManager, logger: logging.Logger):
        self.f9t_manager = f9t_manager
        self.client_manager = client_manager
        self.logger = logger

    async def InitF9t(self, request, context):
        """Handles InitF9t by acquiring an async writer lock."""
        self.logger.info(f"New InitF9t RPC from {context.peer()}")
        async with self.client_manager.get_writer_access(context):
            return await self.f9t_manager.initialize_f9t(request, context)

    async def CaptureUblox(self, request, context):
        """Handles CaptureUblox by acquiring an async reader lock."""
        self.logger.info(f"New CaptureUblox RPC from {context.peer()}")
        if not self.f9t_manager.is_running():
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "F9T is not initialized.")

        async with self.client_manager.get_reader_access(context) as reader_queue:
            async for response in self.f9t_manager.stream_data(request, context, reader_queue):
                yield response

async def serve(server_cfg, shutdown_event=Optional[asyncio.Event], in_main_thread: bool = True):
    """Initializes managers and starts the async gRPC server."""
    logger = make_rich_logger(__name__, level=logging.DEBUG)


    # 1. Initialize Managers
    io_manager = F9tIoManager(logger)
    client_manager = ClientManager(logger, max_readers=server_cfg['max_workers'])
    f9t_manager = F9tManager(io_manager, client_manager, server_cfg, logger)

    # Define a signal handler to set the shutdown event
    async def _signal_handler(*_):
        logger.info("Shutdown signal received, initiating graceful shutdown.")
        await shutdown_event.set()

    # Attach signal handlers only if running in the main thread
    if in_main_thread:
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except RuntimeError as e:
                logger.warning(f"Could not set signal handler for {sig}: {e}. "
                               f"This is expected if not in the main thread.")
    else:
        assert shutdown_event is not None, "shutdown_event must be provided if not running in the main thread."


    # 2. Create and start async gRPC server
    server = grpc.aio.server()
    ublox_control_pb2_grpc.add_UbloxControlServicer_to_server(
        UbloxControlServicer(f9t_manager, client_manager, logger), server
    )

    SERVICE_NAMES = (
        ublox_control_pb2.DESCRIPTOR.services_by_name["UbloxControl"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:50051")
    await server.start()
    logger.info("UbloxControl async server started. Press CTRL+C to stop.")

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down server.")
        await server.stop(grace=1)
    finally:
        # Ensure the I/O task is cleaned up
        await io_manager.stop()


if __name__ == "__main__":
    try:
        with open(CFG_DIR / "ublox_control_server_config.json", "r") as f:
                server_config = json.load(f)
        # asyncio.run will wait for the serve() coroutine to complete
        asyncio.run(serve(server_config))
    except KeyboardInterrupt:
        pass
