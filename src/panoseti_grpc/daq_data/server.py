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
from typing import AsyncIterator, List, Dict, Optional
import signal

# gRPC imports
import grpc
from grpc_reflection.v1alpha import reflection
from google.protobuf.json_format import MessageToDict
from google.protobuf.empty_pb2 import Empty

# Protoc-generated imports
from panoseti_grpc.generated import daq_data_pb2, daq_data_pb2_grpc
from panoseti_grpc.generated.daq_data_pb2 import InitHpIoResponse, StreamImagesResponse, PanoImage
from tests.ublox_control.conftest import server_config

# Package imports
from .resources import make_rich_logger, CFG_DIR, is_daq_active, load_package_json, daq_data_anchor_package
from .testing import is_os_posix
from .managers import ClientManager, HpIoTaskManager
from .state import ReaderState, CachedPanoImage
from .hp_io_manager import HpIoManager


class DaqDataServicer(daq_data_pb2_grpc.DaqDataServicer):
    """ Provides implementations for DaqData RPCs by orchestrating manager classes. """

    def __init__(self, server_cfg, logging_level=logging.DEBUG):
        self.logger = make_rich_logger("daq_data.server", level=logging_level)
        test_result, msg = is_os_posix()
        assert test_result, msg
        self.server_cfg = server_cfg
        self.client_manager = ClientManager(self.logger, server_cfg)
        self.task_manager = HpIoTaskManager(self.logger, server_cfg, self.client_manager.reader_states)

    async def start_initial_task(self):
        """Starts the initial hp_io task if configured to do so."""
        if self.server_cfg.get("init_from_default", False):
            self.logger.info("Creating initial hp_io task from default config.")
            try:
                with open(CFG_DIR / self.server_cfg["default_hp_io_config_file"], "r") as f:
                    hp_io_cfg = json.load(f)
                await self.task_manager.start(hp_io_cfg)
            except Exception as e:
                self.logger.error(f"Failed to start initial hp_io task: {e}", exc_info=True)

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
        self.logger.info(f"New StreamImages rpc from '{peer}': {MessageToDict(request, True, True)}")
        if not request.stream_movie_data and not request.stream_pulse_height_data:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "At least one stream flag must be True.")

        async with self.client_manager.get_reader_access(context, self.task_manager) as reader_state:
            # Configure the reader's stream based on the request
            hp_io_update_interval_seconds = self.task_manager.hp_io_cfg.get('update_interval_seconds', self.server_cfg['min_hp_io_update_interval_seconds'])
            reader_state.config.update({
                "stream_movie_data": request.stream_movie_data,
                "stream_pulse_height_data": request.stream_pulse_height_data,
                "module_ids": list(request.module_ids),
                "update_interval_seconds": max(request.update_interval_seconds, hp_io_update_interval_seconds)
            })
            self.logger.info(f"Stream configured for ({reader_state.uid}) with interval {reader_state.config['update_interval_seconds']}s")

            # Main streaming loop
            while not any([context.cancelled(), reader_state.cancel_reader_event.is_set(), reader_state.shutdown_event.is_set()]):
                try:
                    now = time.monotonic()
                    interval = reader_state.config['update_interval_seconds']
                    
                    # Check if it's time to send an update to this client
                    delta_t = now - reader_state.last_update_t
                    if delta_t >= interval:
                        fresh_images = self._get_fresh_images_for_client(reader_state)
                        
                        if fresh_images:
                            for image in fresh_images:
                                yield StreamImagesResponse(pano_image=image)
                            reader_state.last_update_t = now
                    await asyncio.sleep(interval)  # Sleep until the next update interval
                    reader_state.dequeue_timeouts = 0  # Reset on success
                except asyncio.TimeoutError:
                    reader_state.dequeue_timeouts += 1
                    if reader_state.dequeue_timeouts >= self.server_cfg['max_reader_dequeue_timeouts']:
                        self.logger.warning(f"Client ({reader_state.uid}) from '{peer}' timed out waiting for data. Ending stream.")
                        await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Client timed out.")
                    self.logger.warning(f"Client '{peer}' timed out waiting for data. {reader_state.dequeue_timeouts} timeouts so far")
                    continue
                except Exception as e:
                    self.logger.error(f"Error in stream loop for ({reader_state.uid}) from '{peer}': {e}", exc_info=True)
                    break

            self.logger.info(f"Stream ended for ({reader_state.uid}) from '{peer}'.")
            if not context.cancelled():
                if reader_state.shutdown_event.is_set():
                    await context.abort(grpc.StatusCode.CANCELLED, f"server shutdown_event set for ({reader_state.uid}) from '{peer}'.")
                elif reader_state.cancel_reader_event.is_set():
                    await context.abort(grpc.StatusCode.CANCELLED, f"cancel_reader_event set for ({reader_state.uid}) from '{peer}'."
                                                                   f"A writer has likely forced a reconfiguration of hp_io")

    def _get_fresh_images_for_client(self, rs: ReaderState) -> List[PanoImage]:
        """ Checks the cache for images that are newer than what the client has already seen.  """
        images_to_send = []
        if not self.task_manager.hp_io_manager:
            return images_to_send
            
        cache = self.task_manager.hp_io_manager.latest_data_cache
        subscribed_mids = set(rs.config['module_ids'])

        for mid, data in cache.items():
            if subscribed_mids and mid not in subscribed_mids:
                continue

            # Check for fresh movie data
            if rs.config['stream_movie_data']:
                cached_movie: CachedPanoImage = data.get('movie')
                if cached_movie and cached_movie.frame_id > rs.last_sent_movie_id:
                    images_to_send.append(cached_movie.pano_image)
                    rs.last_sent_movie_id = cached_movie.frame_id
            
            # Check for fresh pulse-height data
            if rs.config['stream_pulse_height_data']:
                cached_ph: CachedPanoImage = data.get('ph')
                if cached_ph and cached_ph.frame_id > rs.last_sent_ph_id:
                    images_to_send.append(cached_ph.pano_image)
                    rs.last_sent_ph_id = cached_ph.frame_id
        
        return images_to_send

    async def InitHpIo(self, request, context) -> InitHpIoResponse:
        """Initialize or re-initialize the hp_io task. [writer]"""
        peer = urllib.parse.unquote(context.peer())
        self.logger.info(f"New InitHpIo rpc from {peer}: "
                         f"{MessageToDict(request, True, True)}")

        # Request validation
        if not request.simulate_daq:
            if not os.path.exists(request.data_dir):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"data_dir '{request.data_dir}' does not exist.")
            if not await is_daq_active(simulate_daq=False):
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Real DAQ software is not active.")

        if request.update_interval_seconds < self.server_cfg['min_hp_io_update_interval_seconds']:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "update_interval_seconds is below server minimum.")

        async with self.client_manager.get_writer_access(context, force=request.force) as uid:
            self.logger.info(f"({uid}) acquired writer lock. Initializing hp_io task.")

            last_valid_config = self.task_manager.hp_io_cfg.copy()

            # Filter hp_io_fields from the request
            # hp_io_cfg = MessageToDict(request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)
            hp_io_cfg = {
                "data_dir": request.data_dir,
                "simulate_daq": request.simulate_daq,
                "update_interval_seconds": request.update_interval_seconds,
                "module_ids": list(request.module_ids),
            }
            self.logger.debug(f"Received hp_io configuration: {hp_io_cfg}")

            # Delegate starting the new task to the HpIoTaskManager
            success = await self.task_manager.start(hp_io_cfg)

            if success:
                self.logger.info(f"InitHpIo transaction ({uid}) succeeded: new hp_io task is valid.")
            else:
                self.logger.warning(f"({uid}) failed to start new hp_io task.")
                # Optional: Attempt to restore the last known good configuration
                if last_valid_config:
                    self.logger.info("Attempting to restore previous hp_io configuration.")
                    if not await self.task_manager.start(last_valid_config):
                        self.logger.error("Failed to restore previous hp_io configuration. Server is now idle.")

            return InitHpIoResponse(success=success)

    async def Ping(self, request, context):
        """Returns Empty to verify client-server connection."""
        self.logger.info(f"Ping rpc from '{urllib.parse.unquote(context.peer())}'")
        return Empty()

    async def UploadImages(self, request_iterator, context) -> Empty:
        """Accepts a stream of PanoImages and forwards them to the HpIoManager. [writer-like]"""
        peer = urllib.parse.unquote(context.peer())
        self.logger.info(f"New UploadImages rpc from {peer}")

        # Check if the core IO task is running and able to process images.
        do_daq_simulation = True #self.task_manager.hp_io_cfg.get('simulate_daq', False)
        if not do_daq_simulation and not self.task_manager.is_valid():
            emsg = f"UploadImages: hp_io task is not running. Please initialize the server first."
            self.logger.warning(emsg)
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)

        async with self.client_manager.get_uploader_access(context) as uid:
            image_count = 0
            try:
                async for request in request_iterator:
                    if not request.HasField("pano_image"):
                        self.logger.warning(f"Received empty UploadImageRequest for ({uid}) from '{peer}'")
                        continue
                    try:
                        # Use non-blocking put to avoid holding up the RPC if the system is overloaded.
                        # self.logger.debug(f"Received image from {peer}.")
                        self.task_manager.hp_io_manager.data_queue.put_nowait(request.pano_image)
                        image_count += 1
                    except asyncio.QueueFull:
                        self.logger.error(f"Upload queue is full. Aborting stream for client ({uid}) from '{peer}'.")
                        await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Server upload queue is full.")
                    if context.cancelled() or self.client_manager.shutdown_event.is_set():
                        self.logger.info(f"Stream ended for ({uid}) from '{peer}'.")
                        if not context.cancelled():
                            if self.client_manager.shutdown_event.is_set():
                                await context.abort(grpc.StatusCode.CANCELLED, f"server shutdown_event set for ({uid}) from '{peer}'.")
                            elif self.client_manager.cancel_readers_event.is_set():
                                await context.abort(grpc.StatusCode.CANCELLED, f"cancel_reader_event set for ({uid}) from '{peer}'."
                                                                               f"A writer has likely forced a reconfiguration of hp_io")
                        await context.abort(grpc.StatusCode.CANCELLED, f"client ({uid}) from '{peer}' cancelled stream.")
                self.logger.info(f"Successfully processed {image_count} uploaded images for ({uid}) from '{peer}'.")
            except grpc.aio.AioRpcError as e:
                self.logger.error(f"Error during UploadImages stream for {peer}: {e.details()}")
                raise e
            return Empty()


async def serve(server_cfg, shutdown_event=None, in_main_thread: bool = True):
    """Create and run the gRPC server."""
    logger = logging.getLogger("daq_data.server")

    # Define a signal handler to set the shutdown event
    def _signal_handler(*_):
        logger.info("Shutdown signal received, initiating graceful shutdown.")
        shutdown_event.set()

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

    server = grpc.aio.server()
    servicer = DaqDataServicer(server_cfg)
    daq_data_pb2_grpc.add_DaqDataServicer_to_server(servicer, server)

    SERVICE_NAMES = (
        daq_data_pb2.DESCRIPTOR.services_by_name["DaqData"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    # Add regular socket
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logger.info(f"Server starting, listening on '{listen_addr}'")

    # Add a Unix Domain Socket listener for local inter-process communication
    uds_listen_addr = server_cfg.get("unix_domain_socket", None)
    if uds_listen_addr:
        server.add_insecure_port(uds_listen_addr)
        logger.info(f"Server also listening on '{uds_listen_addr}'")

    # Start the server and initial tasks
    await server.start()
    initial_task = asyncio.create_task(servicer.start_initial_task())

    # shutdown sequence:
    # 0. wait for the shutdown event to be set
    logger.info("Server started.")
    await shutdown_event.wait()
    logger.info("Shutting down...")
    # 1. Stop the application-level managers first.
    await servicer.shutdown()
    # 2. Stop the gRPC server to prevent new connections.
    grace = server_cfg.get("shutdown_grace_period", 5)
    await server.stop(grace)
    # 3. Ensure the initial task is complete.
    await initial_task
    logger.info("Server shut down gracefully.")

if __name__ == "__main__":
    try:
        server_config = load_package_json(daq_data_anchor_package,CFG_DIR / "daq_data_server_config.json" )
        # with open(CFG_DIR / "daq_data_server_config.json", "r") as f:
        #     server_config = json.load(f)
        # asyncio.run will wait for the serve() coroutine to complete
        asyncio.run(serve(server_config))
    except (KeyboardInterrupt, asyncio.CancelledError):
        # This will now only be triggered if Ctrl+C is hit during initial setup
        print("\nServer startup interrupted.")
    finally:
        print("Exiting server process.")