"""
Orchestrates UDS data acquisition and broadcasting for PANOSETI DAQ.

Hashpipe sends data to the server via Unix Domain Sockets (UDS). The server
acts as a UDS server, accepting connections from Hashpipe for each data product.
Incoming frames are assigned monotonically increasing frame IDs and cached
in a shared dict so that any number of gRPC streaming clients can poll for
fresh frames at their own rate.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from panoseti_grpc.generated.daq_data_pb2 import PanoImage

from .config import DaqDataServerConfig
from .data_sources import UdsDataSource
from .resources import get_dp_name_from_props
from .state import CachedPanoImage, ModuleState, ReaderState


class HpIoManager:
    """Orchestrates data acquisition from UDS sources and broadcasts to clients."""

    def __init__(
        self,
        server_cfg: DaqDataServerConfig,
        hp_io_cfg: dict[str, Any],
        reader_states: list[ReaderState],
        stop_event: asyncio.Event,
        valid: asyncio.Event,
        active_data_products_queue: asyncio.Queue[set[str]],
        logger: logging.Logger,
    ) -> None:
        self.server_cfg = server_cfg
        self.hp_io_cfg = hp_io_cfg
        self.reader_states = reader_states
        self.stop_event = stop_event
        self.valid = valid
        self.active_data_products_queue = active_data_products_queue
        self.logger = logger
        self.processing_loop_timeout = 0.75

        self.data_queue: asyncio.Queue[PanoImage] = asyncio.Queue(maxsize=500)
        self.data_sources: list[UdsDataSource] = []

        self.modules: dict[int, ModuleState] = {}
        self.latest_data_cache: dict[int, dict[str, CachedPanoImage | None]] = defaultdict(
            lambda: {"ph": None, "movie": None}
        )
        self._frame_id_counter = 0

        self._configure_data_sources()

    def _configure_data_sources(self) -> None:
        """Instantiates UDS data sources based on server configuration."""
        uds_cfg = self.server_cfg.acquisition_methods.uds
        self.logger.info(f"Configuring data sources with UDS enabled={uds_cfg.enabled}.")

        if uds_cfg.enabled:
            self.logger.info("Configuring UDS data sources (Server Mode).")
            for dp_name in uds_cfg.data_products:
                source_cfg = {
                    "dp_name": dp_name,
                    "socket_path_template": uds_cfg.socket_path_template,
                    "read_timeout": uds_cfg.read_timeout,
                }
                self.logger.info(f"Creating UDS server for data product '{dp_name}'")
                self.data_sources.append(UdsDataSource(source_cfg, self.logger, self.data_queue, self.stop_event))
        self.logger.info(f"Configured {len(self.data_sources)} data sources: {self.data_sources}")

    async def run(self) -> None:
        """Main entry point: starts data sources and the processing loop."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()

        if not self.data_sources:
            self.logger.error("No data acquisition sources configured. HpIoManager cannot run.")
            return

        # Outer try catches asyncio.TimeoutError raised from the TaskGroup body (startup
        # timeout).  Cannot mix except / except* in the same try block, so they are split.
        try:
            try:
                async with asyncio.TaskGroup() as tg:
                    for source in self.data_sources:
                        tg.create_task(source.run())
                    tg.create_task(self._processing_loop())

                    # Wait for all data sources to signal they are ready.
                    self.logger.info("Waiting for all data sources to become ready.")
                    async with asyncio.timeout(10.0):
                        await asyncio.gather(*(s.ready_event.wait() for s in self.data_sources))
                    self.logger.info("All data sources have reported ready.")

                    await self._update_active_data_products()
                    self.valid.set()
                    self.logger.info("HpIoManager task started and is valid.")
                    # TaskGroup now waits for all tasks to finish.
            except* asyncio.CancelledError:
                pass  # Normal shutdown path
            except* Exception as eg:
                for exc in eg.exceptions:
                    self.logger.error(f"HpIoManager task error: {exc}", exc_info=exc)
        except TimeoutError:
            self.logger.error("Timeout waiting for all data sources to become ready. HpIoManager will not be valid.")
        finally:
            self.valid.clear()
            self.logger.info("HpIoManager task exited.")

    async def _processing_loop(self) -> None:
        """Assigns a unique frame_id to each incoming image before caching."""
        self.logger.info("Starting processing loop.")
        while not self.stop_event.is_set():
            try:
                async with asyncio.timeout(self.processing_loop_timeout):
                    pano_image = await self.data_queue.get()

                await self._discover_module_from_image(pano_image)

                self._frame_id_counter += 1
                cached_image = CachedPanoImage(frame_id=self._frame_id_counter, pano_image=pano_image)
                self._cache_pano_image(cached_image)

            except asyncio.CancelledError:
                break
            except TimeoutError:
                continue
        self.logger.info("Processing loop finished.")

    def _cache_pano_image(self, cached_image: CachedPanoImage) -> None:
        """Caches the received CachedPanoImage, overwriting the previous one. Synchronous — no awaits needed."""
        pano_image = cached_image.pano_image
        cache_key = "ph" if pano_image.type == PanoImage.Type.PULSE_HEIGHT else "movie"
        self.latest_data_cache[pano_image.module_id][cache_key] = cached_image

    async def _discover_module_from_image(self, pano_image: PanoImage) -> None:
        """Discovers a new module or data product from a received image."""
        module_id = pano_image.module_id
        if module_id not in self.modules:
            self.logger.info(f"Discovered new module {module_id} via data stream.")
            self.modules[module_id] = ModuleState(module_id, self.logger)

        module = self.modules[module_id]
        try:
            dp_name = get_dp_name_from_props(pano_image.type, list[int](pano_image.shape), pano_image.bytes_per_pixel)
            if dp_name not in module.dp_configs:
                module.add_dp(dp_name)
                await self._update_active_data_products()
        except ValueError as e:
            self.logger.warning(f"Could not identify data product from image for module {module_id}: {e}")

    async def _update_active_data_products(self) -> None:
        active_dps = set().union(*(m.dp_configs.keys() for m in self.modules.values()))
        await self.active_data_products_queue.put(active_dps)
