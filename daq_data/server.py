#!/usr/bin/env python3
"""
The Python implementation of a gRPC DaqData server.

Requires following to function correctly:
    1. All Python packages specified in requirements.txt.
    2. A connection to a panoseti module (for real data streaming).
"""

import os
import asyncio
import logging
import json
import time
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

# --- gRPC imports ---
import grpc
from grpc_reflection.v1alpha import reflection
from google.protobuf.json_format import MessageToDict
from google.protobuf.empty_pb2 import Empty

# --- Protoc-generated imports ---
from . import daq_data_pb2, daq_data_pb2_grpc
from .daq_data_pb2 import InitHpIoResponse, StreamImagesResponse

# --- DAQ Data Utils ---
from .resources import make_rich_logger, CFG_DIR, is_daq_active
from .testing import is_os_posix
from .managers import ClientManager, HpIoTaskManager


class DaqDataServicer(daq_data_pb2_grpc.DaqDataServicer):
    """
    Provides implementations for DaqData RPCs by orchestrating manager classes.
    """

    def __init__(self, server_cfg):
        self.logger = make_rich_logger("daq_data.server", level=logging.INFO)
        test_result, msg = is_os_posix()
        assert test_result, msg

        self.server_cfg = server_cfg

        # 1. Compose the manager classes to delegate responsibilities
        self.client_manager = ClientManager(self.logger, server_cfg)
        self.task_manager = HpIoTaskManager(
            self.logger,
            server_cfg,
            self.client_manager.reader_states
        )

        self.initial_task_started = asyncio.Event()

    async def start_initial_task(self):
        """Starts the initial hp_io task if configured to do so."""
        if self.server_cfg.get("init_from_default", False):
            self.logger.info(f"Creating initial hp_io task from default config.")
            try:
                with open(CFG_DIR / self.server_cfg["default_hp_io_config_file"], "r") as f:
                    hp_io_cfg = json.load(f)
                await self.task_manager.start(hp_io_cfg)
            except Exception as e:
                self.logger.error(f"Failed to start initial hp_io task: {e}", exc_info=True)
        self.initial_task_started.set()

    async def shutdown(self):
        """Gracefully shuts down the server by delegating to the managers."""
        self.logger.info("Shutdown initiated. Stopping all tasks.")
        self.client_manager.signal_shutdown()
        await self.client_manager.cancel_all_readers()
        await self.task_manager.stop()
        self.logger.info("All server tasks and managers stopped.")

    async def StreamImages(self, request, context) -> AsyncIterator[StreamImagesResponse]:
        """Forward PanoImages to the client. [reader]"""
        peer = urllib.parse.unquote(context.peer())
        self.logger.info(f"New StreamImages rpc from {peer}: "
                         f"{MessageToDict(request, preserving_proto_field_name=True)}")

        if not request.stream_movie_data and not request.stream_pulse_height_data:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "At least one stream flag must be True.")

        # Use the ClientManager to safely acquire a reader slot and handle all preconditions
        async with self.client_manager.get_reader_access(context, self.task_manager) as reader_state:
            # At this point, a reader slot is allocated and the server is in a valid state for streaming.

            # Configure the reader's stream based on the request
            reader_state.config['stream_movie_data'] = request.stream_movie_data
            reader_state.config['stream_pulse_height_data'] = request.stream_pulse_height_data
            reader_state.config['module_ids'] = list(request.module_ids)

            # Set update interval, respecting server limits
            req_interval = request.update_interval_seconds
            hp_io_interval = self.task_manager.hp_io_cfg.get('update_interval_seconds', 1.0)
            reader_state.config['update_interval_seconds'] = max(req_interval, hp_io_interval)

            self.logger.info(
                f"Stream configured for {peer} with interval {reader_state.config['update_interval_seconds']}s")

            # Main streaming loop
            while not (context.cancelled() or reader_state.cancel_reader_event.is_set() or reader_state.shutdown_event.is_set()):
                try:
                    # Wait for an image from the HpIoManager's broadcast
                    pano_image = await asyncio.wait_for(
                        reader_state.queue.get(),
                        timeout=self.server_cfg['reader_timeout']
                    )

                    if pano_image == "shutdown":
                        self.logger.info(f"Shutdown signal received in queue for {peer}. Ending stream.")
                        break

                    yield StreamImagesResponse(pano_image=pano_image)
                    reader_state.dequeue_timeouts = 0  # Reset on success

                except asyncio.TimeoutError:
                    reader_state.dequeue_timeouts += 1
                    if reader_state.dequeue_timeouts >= self.server_cfg['max_reader_dequeue_timeouts']:
                        self.logger.warning(f"Client {peer} timed out waiting for data. Ending stream.")
                        await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Client timed out.")
                    continue
                except Exception as e:
                    self.logger.error(f"Error in stream loop for {peer}: {e}", exc_info=True)
                    break

            self.logger.info(f"Stream ended for {peer}.")
            if not context.cancelled():
                if reader_state.cancel_reader_event.is_set():
                    await context.abort(grpc.StatusCode.CANCELLED, f"cancel_reader_event set for {peer}."
                                                                   f"A writer has likely forced a reconfiguration of hp_io")
                elif reader_state.shutdown_event.is_set():
                    await context.abort(grpc.StatusCode.CANCELLED, f"shutdown_event set for {peer}.")

    async def InitHpIo(self, request, context) -> InitHpIoResponse:
        """Initialize or re-initialize the hp_io task. [writer]"""
        peer = urllib.parse.unquote(context.peer())
        self.logger.info(f"New InitHpIo rpc from {peer}: "
                         f"{MessageToDict(request, preserving_proto_field_name=True)}")

        # --- Request Validation ---
        if not request.simulate_daq:
            if not os.path.exists(request.data_dir):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"data_dir '{request.data_dir}' does not exist.")
            if not await is_daq_active(simulate_daq=False):
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Real DAQ software is not active.")

        if request.update_interval_seconds < self.server_cfg['min_hp_io_update_interval_seconds']:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "update_interval_seconds is below server minimum.")

        # Use ClientManager to safely acquire exclusive writer access
        async with self.client_manager.get_writer_access(context, self.task_manager, force=request.force):
            self.logger.info("Acquired writer lock. Proceeding with hp_io task re-initialization.")

            last_valid_config = self.task_manager.hp_io_cfg.copy()

            hp_io_cfg = {
                "data_dir": request.data_dir,
                "simulate_daq": request.simulate_daq,
                "update_interval_seconds": request.update_interval_seconds,
                "module_ids": list(request.module_ids),
            }

            # Delegate starting the new task to the HpIoTaskManager
            success = await self.task_manager.start(hp_io_cfg)

            if success:
                self.logger.info("InitHpIo transaction succeeded: new hp_io task is valid.")
            else:
                self.logger.warning("Failed to start new hp_io task.")
                # Optional: Attempt to restore the last known good configuration
                if last_valid_config:
                    self.logger.info("Attempting to restore previous hp_io configuration.")
                    if not await self.task_manager.start(last_valid_config):
                        self.logger.error("Failed to restore previous hp_io configuration. Server is now idle.")

            return InitHpIoResponse(success=success)

    async def Ping(self, request, context):
        """Returns Empty to verify client-server connection."""
        self.logger.info(f"Ping rpc from {urllib.parse.unquote(context.peer())}")
        return Empty()


async def serve(server_cfg):
    """Create and run the gRPC server."""
    server = grpc.aio.server()
    daq_data_servicer = DaqDataServicer(server_cfg)
    daq_data_pb2_grpc.add_DaqDataServicer_to_server(daq_data_servicer, server)

    SERVICE_NAMES = (
        daq_data_pb2.DESCRIPTOR.services_by_name["DaqData"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)

    logger = logging.getLogger("daq_data.server")
    logger.info(f"Server starting, listening on {listen_addr}")

    try:
        await server.start()
        # Start the initial background task after the server has started
        await daq_data_servicer.start_initial_task()
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("'Ctrl+C' received, initiating shutdown.")
    finally:
        grace = server_cfg.get("shutdown_grace_period", 5)
        await daq_data_servicer.shutdown()
        await server.stop(grace)
        logger.info("Server shut down gracefully.")


if __name__ == "__main__":
    try:
        with open(CFG_DIR / "daq_data_server_config.json", "r") as f:
            server_config = json.load(f)
        asyncio.run(serve(server_config))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("Exiting server process.")
