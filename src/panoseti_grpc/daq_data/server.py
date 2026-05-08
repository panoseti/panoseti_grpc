#!/usr/bin/env python3
"""
The Python implementation of a gRPC DaqData server.

Requires following to function correctly:
    1. All Python packages specified in requirements.txt.
    2. A connection to a panoseti module (for real data streaming).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
import urllib.parse
from collections.abc import AsyncIterator

# gRPC imports
import grpc
from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import MessageToDict
from grpc_reflection.v1alpha import reflection

# Protoc-generated imports
from panoseti_grpc.generated import daq_data_pb2, daq_data_pb2_grpc
from panoseti_grpc.generated.daq_data_pb2 import (
    InitHpIoResponse,
    PanoImage,
    StatusResponse,
    StreamImagesResponse,
)

# Package imports
from panoseti_grpc.telemetry.logger import get_logger

from .config import DaqDataServerConfig
from .managers import ClientManager, HpIoTaskManager
from .resources import CFG_DIR, daq_data_anchor_package, is_daq_active, load_package_json
from .state import CachedPanoImage, ReaderState
from .testing import is_os_posix


class DaqDataServicer(daq_data_pb2_grpc.DaqDataServicer):
    """Provides implementations for DaqData RPCs by orchestrating manager classes."""

    def __init__(self, server_cfg: DaqDataServerConfig, logging_level: int = logging.DEBUG) -> None:
        self.logger = get_logger(
            "daq_data.server",
            level=logging_level,
            log_dir=server_cfg.log_dir,
            grpc_enabled=server_cfg.grpc_logging,
        )
        test_result, msg = is_os_posix()
        assert test_result, msg
        self.server_cfg = server_cfg
        self.client_manager = ClientManager(self.logger, server_cfg)
        self.task_manager = HpIoTaskManager(self.logger, server_cfg, self.client_manager.reader_states)

    async def start_initial_task(self) -> None:
        """Starts the initial hp_io task if configured to do so."""
        if self.server_cfg.init_from_default:
            self.logger.info("Creating initial hp_io task from default config.")
            try:
                import anyio

                async with await anyio.open_file(CFG_DIR / self.server_cfg.default_hp_io_config_file) as f:
                    hp_io_cfg = json.loads(await f.read())
                await self.task_manager.start(hp_io_cfg)
            except Exception as e:
                self.logger.error(f"Failed to start initial hp_io task: {e}", exc_info=True)

    async def shutdown(self) -> None:
        """Gracefully shuts down the server by delegating to the managers."""
        self.logger.info("Shutdown initiated. Stopping all tasks.")
        self.client_manager.signal_shutdown()
        await self.client_manager.cancel_all_readers()
        await self.task_manager.stop()
        self.logger.info("All server tasks and managers stopped.")

    async def StreamImages(
        self, request: daq_data_pb2.StreamImagesRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[StreamImagesResponse]:
        """Forward PanoImages to the client. [reader]"""
        peer = urllib.parse.unquote(context.peer())
        self.logger.info(f"New StreamImages rpc from '{peer}': {MessageToDict(request, True, True)}")
        if not request.stream_movie_data and not request.stream_pulse_height_data:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "At least one stream flag must be True.")

        async with self.client_manager.get_reader_access(context, self.task_manager) as reader_state:
            # Configure the reader's stream based on the request
            hp_io_update_interval_seconds = self.task_manager.hp_io_cfg.get(
                "update_interval_seconds", self.server_cfg.min_hp_io_update_interval_seconds
            )
            reader_state.config.update(
                {
                    "stream_movie_data": request.stream_movie_data,
                    "stream_pulse_height_data": request.stream_pulse_height_data,
                    "module_ids": list[int](request.module_ids),
                    "update_interval_seconds": max(request.update_interval_seconds, hp_io_update_interval_seconds),
                }
            )
            self.logger.info(
                f"Stream configured for ({reader_state.uid}) with interval "
                f"{reader_state.config['update_interval_seconds']}s"
            )

            # Track wall-clock time of last fresh data delivery for idle detection
            last_data_t = time.monotonic()

            # Main streaming loop
            while not context.cancelled():
                cre = reader_state.cancel_reader_event
                if cre and cre.is_set():
                    break
                se = reader_state.shutdown_event
                if se and se.is_set():
                    break

                try:
                    now = time.monotonic()
                    interval = float(reader_state.config["update_interval_seconds"])

                    # Check if it's time to send an update to this client
                    fresh_images: list[PanoImage] = []
                    delta_t = now - reader_state.last_update_t
                    if delta_t >= interval:
                        fresh_images = self._get_fresh_images_for_client(reader_state)
                        if fresh_images:
                            for image in fresh_images:
                                yield StreamImagesResponse(pano_image=image)
                            reader_state.last_update_t = now
                            last_data_t = now

                    # Time-based idle detection: abort if no data has arrived for reader_timeout seconds
                    if now - last_data_t >= self.server_cfg.reader_timeout:
                        self.logger.warning(
                            f"Client ({reader_state.uid}) from '{peer}' received no data for "
                            f"{now - last_data_t:.1f}s. Ending stream."
                        )
                        await context.abort(
                            grpc.StatusCode.DEADLINE_EXCEEDED, "No data received within timeout window."
                        )
                        return

                    # Sleep for the remainder of the interval, accounting for processing time
                    elapsed = time.monotonic() - now
                    await asyncio.sleep(max(0.0, interval - elapsed))
                except Exception as e:
                    self.logger.error(
                        f"Error in stream loop for ({reader_state.uid}) from '{peer}': {e}", exc_info=True
                    )
                    break

            self.logger.info(f"Stream ended for ({reader_state.uid}) from '{peer}'.")
            if not context.cancelled():
                se = reader_state.shutdown_event
                if se and se.is_set():
                    await context.abort(
                        grpc.StatusCode.CANCELLED, f"server shutdown_event set for ({reader_state.uid}) from '{peer}'."
                    )
                cre = reader_state.cancel_reader_event
                if cre and cre.is_set():
                    await context.abort(
                        grpc.StatusCode.CANCELLED,
                        f"cancel_reader_event set for ({reader_state.uid}) from '{peer}'."
                        f"A writer has likely forced a reconfiguration of hp_io",
                    )

    def _get_fresh_images_for_client(self, rs: ReaderState) -> list[PanoImage]:
        """Checks the cache for images newer than what the client last received."""
        if not self.task_manager.hp_io_manager:
            return []

        cache = self.task_manager.hp_io_manager.latest_data_cache
        subscribed = set(rs.config["module_ids"])
        # Only iterate subscribed modules when a whitelist is given (avoids scanning all modules)
        module_ids = subscribed if subscribed else list(cache.keys())

        images: list[PanoImage] = []
        for mid in module_ids:
            data = cache.get(mid)  # .get() avoids triggering defaultdict factory
            if data is None:
                continue
            if rs.config["stream_movie_data"]:
                cached_movie: CachedPanoImage | None = data.get("movie")
                if cached_movie and cached_movie.frame_id > rs.last_sent_movie_id:
                    images.append(cached_movie.pano_image)
                    rs.last_sent_movie_id = cached_movie.frame_id
            if rs.config["stream_pulse_height_data"]:
                cached_ph: CachedPanoImage | None = data.get("ph")
                if cached_ph and cached_ph.frame_id > rs.last_sent_ph_id:
                    images.append(cached_ph.pano_image)
                    rs.last_sent_ph_id = cached_ph.frame_id
        return images

    async def InitHpIo(
        self, request: daq_data_pb2.InitHpIoRequest, context: grpc.aio.ServicerContext
    ) -> InitHpIoResponse:
        """Initialize or re-initialize the hp_io task. [writer]"""
        peer = urllib.parse.unquote(context.peer())
        self.logger.info(f"New InitHpIo rpc from {peer}: {MessageToDict(request, True, True)}")

        # Request validation
        if not request.simulate_daq:
            if not await asyncio.to_thread(os.path.exists, request.data_dir):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"data_dir '{request.data_dir}' does not exist.")
            if not await is_daq_active(simulate_daq=False):
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Real DAQ software is not active.")

        if request.update_interval_seconds < self.server_cfg.min_hp_io_update_interval_seconds:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "update_interval_seconds is below server minimum.")

        async with self.client_manager.get_writer_access(context, force=request.force) as uid:
            self.logger.info(f"({uid}) acquired writer lock. Initializing hp_io task.")

            last_valid_config = self.task_manager.hp_io_cfg.copy()

            # Filter hp_io_fields from the request
            # hp_io_cfg = MessageToDict(
            #     request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
            # )
            hp_io_cfg = {
                "data_dir": request.data_dir,
                "simulate_daq": request.simulate_daq,
                "update_interval_seconds": request.update_interval_seconds,
                "module_ids": list[int](request.module_ids),
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

    async def Status(self, request: Empty, context: grpc.aio.ServicerContext) -> StatusResponse:
        """Returns the status of the DaqData service."""
        is_initialized = self.task_manager.is_valid(verbose=False)
        message = "hp_io task is initialized and valid." if is_initialized else "hp_io task is not initialized."
        return StatusResponse(hp_io_initialized=is_initialized, message=message)

    async def Ping(self, request: Empty, context: grpc.aio.ServicerContext) -> Empty:
        """Returns Empty to verify client-server connection."""
        self.logger.info(f"Ping rpc from '{urllib.parse.unquote(context.peer())}'")
        return Empty()

    async def UploadImages(
        self, request_iterator: AsyncIterator[daq_data_pb2.UploadImageRequest], context: grpc.aio.ServicerContext
    ) -> Empty:
        """Placeholder for UploadImages RPC (Not yet implemented)."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "UploadImages is not yet implemented.")
        return Empty()


async def serve(
    server_cfg: DaqDataServerConfig,
    shutdown_event: asyncio.Event | None = None,
    in_main_thread: bool = True,
) -> None:
    """Create and run the gRPC server."""
    logger = logging.getLogger("daq_data.server")
    # server_cfg is already a DaqDataServerConfig here due to unified server or pydantic validation in main
    if isinstance(server_cfg, dict):
        server_cfg = DaqDataServerConfig.model_validate(server_cfg)

    # Define a signal handler to set the shutdown event
    def _signal_handler() -> None:
        logger.info("Shutdown signal received, initiating graceful shutdown.")
        if shutdown_event:
            shutdown_event.set()

    # Attach signal handlers only if running in the main thread
    if in_main_thread:
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except RuntimeError as e:
                logger.warning(
                    f"Could not set signal handler for {sig}: {e}. This is expected if not in the main thread."
                )
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
    if server_cfg.unix_domain_socket:
        server.add_insecure_port(server_cfg.unix_domain_socket)
        logger.info(f"Server also listening on '{server_cfg.unix_domain_socket}'")

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
    grace = server_cfg.shutdown_grace_period
    await server.stop(grace)
    # 3. Ensure the initial task is complete.
    await initial_task
    logger.info("Server shut down gracefully.")


def main() -> None:
    """Console script entry point (``panoseti-daq-data``)."""
    try:
        raw_cfg = load_package_json(daq_data_anchor_package, CFG_DIR / "daq_data_server_config.json")
        server_config = DaqDataServerConfig.model_validate(raw_cfg)
        asyncio.run(serve(server_config))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nServer startup interrupted.")
    finally:
        print("Exiting server process.")


if __name__ == "__main__":
    main()
