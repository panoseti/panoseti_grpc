"""
Orchestrates UDS data acquisition and broadcasting for PANOSETI DAQ.

Hashpipe sends data to the server via Unix Domain Sockets (UDS). The server
acts as a UDS server, accepting connections from Hashpipe for each data product.
Incoming frames are assigned monotonically increasing frame IDs and cached
in a shared dict so that any number of gRPC streaming clients can poll for
fresh frames at their own rate.
"""
import asyncio
import logging
from typing import Dict, List, Optional
from collections import defaultdict

from panoseti_grpc.generated.daq_data_pb2 import PanoImage

from .resources import get_dp_name_from_props
from .state import ReaderState, DataProductState, CachedPanoImage, ModuleState
from .data_sources import UdsDataSource


class HpIoManager:
    """Orchestrates data acquisition from UDS sources and broadcasts to clients."""

    def __init__(self, server_config: Dict, reader_states: List[ReaderState], stop_event: asyncio.Event,
                 valid: asyncio.Event, active_data_products_queue: asyncio.Queue, logger: logging.Logger):
        self.server_config = server_config
        self.reader_states = reader_states
        self.stop_event = stop_event
        self.valid = valid
        self.active_data_products_queue = active_data_products_queue
        self.logger = logger
        self.processing_loop_timeout = 0.75

        self.data_queue = asyncio.Queue(maxsize=500)
        self.data_sources = []

        self.modules: Dict[int, ModuleState] = {}
        self.latest_data_cache: Dict[int, Dict[str, Optional[CachedPanoImage]]] = defaultdict(
            lambda: {'ph': None, 'movie': None}
        )
        self._frame_id_counter = 0

        self._configure_data_sources()

    def _configure_data_sources(self):
        """Instantiates UDS data sources based on server configuration."""
        acq_config = self.server_config.get("acquisition_methods", {})
        self.logger.info(f"Configuring data sources: {acq_config}")

        uds_cfg = acq_config.get("uds", {})
        if uds_cfg.get("enabled"):
            self.logger.info("Configuring UDS data sources (Server Mode).")
            socket_template = uds_cfg.get("socket_path_template")
            if not socket_template:
                self.logger.error("UDS is enabled, but 'socket_path_template' is not defined.")
            else:
                data_products = uds_cfg.get("data_products", [])
                for dp_name in data_products:
                    source_cfg = {
                        "dp_name": dp_name,
                        "socket_path_template": socket_template,
                        "read_timeout": uds_cfg.get("read_timeout", 60.0),
                    }
                    self.logger.info(f"Creating UDS server for data product '{dp_name}'")
                    self.data_sources.append(
                        UdsDataSource(source_cfg, self.logger, self.data_queue, self.stop_event)
                    )
        self.logger.info(f"Configured {len(self.data_sources)} data sources: {self.data_sources}")

    async def run(self):
        """Main entry point: starts data sources and the processing loop."""
        self.logger.info("HpIoManager task starting.")
        self.valid.clear()

        if not self.data_sources:
            self.logger.error("No data acquisition sources configured. HpIoManager cannot run.")
            return

        source_tasks = [asyncio.create_task(source.run()) for source in self.data_sources]
        processing_task = asyncio.create_task(self._processing_loop())

        # Wait for all data sources to signal they are ready.
        try:
            self.logger.info("Waiting for all data sources to become ready.")
            all_sources_ready = asyncio.gather(*(s.ready_event.wait() for s in self.data_sources))
            await asyncio.wait_for(all_sources_ready, timeout=10.0)
            self.logger.info("All data sources have reported ready.")
        except asyncio.TimeoutError:
            self.logger.error(
                "Timeout waiting for all data sources to become ready. HpIoManager will not be valid.")
            for task in source_tasks + [processing_task]:
                if not task.done():
                    task.cancel()
            return  # Exit without setting self.valid

        await self._update_active_data_products()
        self.valid.set()
        self.logger.info("HpIoManager task started and is valid.")

        try:
            await asyncio.gather(processing_task, *source_tasks)
        except Exception as e:
            self.logger.error(f"HpIoManager run error: {e}", exc_info=True)
        finally:
            self.valid.clear()
            self.logger.info("HpIoManager task exited.")

    async def _processing_loop(self):
        """Assigns a unique frame_id to each incoming image before caching."""
        self.logger.info("Starting processing loop.")
        while not self.stop_event.is_set():
            try:
                pano_image = await asyncio.wait_for(self.data_queue.get(), timeout=self.processing_loop_timeout)

                await self._discover_module_from_image(pano_image)

                self._frame_id_counter += 1
                cached_image = CachedPanoImage(
                    frame_id=self._frame_id_counter,
                    pano_image=pano_image
                )
                await self._cache_pano_image(cached_image)

                self.data_queue.task_done()
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                continue
        self.logger.info("Processing loop finished.")

    async def _cache_pano_image(self, cached_image: CachedPanoImage):
        """Caches the received CachedPanoImage, overwriting the previous one."""
        pano_image = cached_image.pano_image
        is_ph = (pano_image.type == PanoImage.Type.PULSE_HEIGHT)
        cache_key = 'ph' if is_ph else 'movie'
        self.latest_data_cache[pano_image.module_id][cache_key] = cached_image

    async def _discover_module_from_image(self, pano_image: PanoImage):
        """Discovers a new module or data product from a received image."""
        module_id = pano_image.module_id
        if module_id not in self.modules:
            self.logger.info(f"Discovered new module {module_id} via data stream.")
            self.modules[module_id] = ModuleState(module_id, self.logger)

        module = self.modules[module_id]
        try:
            dp_name = get_dp_name_from_props(pano_image.type, list(pano_image.shape), pano_image.bytes_per_pixel)
            if dp_name not in module.dp_configs:
                module.add_dp(dp_name)
                await self._update_active_data_products()
        except ValueError as e:
            self.logger.warning(f"Could not identify data product from image for module {module_id}: {e}")

    async def _update_active_data_products(self):
        active_dps = set().union(*(m.dp_configs.keys() for m in self.modules.values()))
        await self.active_data_products_queue.put(active_dps)
