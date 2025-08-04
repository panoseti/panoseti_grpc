"""
Manages the lifecycle of DAQ simulation tasks for the DaqData server.
Supports both filesystem-based and RPC-based simulation modes.
"""
from pathlib import Path
import aiofiles
import json
import asyncio
import os
import time
import logging
from typing import Dict, Any, AsyncGenerator
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from .resources import get_dp_config, get_daq_active_file, get_sim_pff_path, is_daq_active_sync
from .client import AioDaqDataClient
from .state import DataProductConfig
from .daq_data_pb2 import PanoImage, UploadImageRequest
from panoseti_util import pff


class SimulationManager:
    """Manages DAQ simulation tasks."""

    def __init__(self, server_cfg: dict, logger: logging.Logger):
        self.server_cfg = server_cfg
        self.logger = logger
        self.sim_task: asyncio.Task or None = None
        self._sim_stop_event = asyncio.Event()

    async def start(self, sim_cfg: dict) -> bool:
        """Starts the appropriate simulation task based on the configuration."""
        await self.stop()  # Ensure any previous task is stopped
        self._sim_stop_event.clear()

        mode = sim_cfg.get("simulation_mode", "filesystem")
        self.logger.info(f"Starting simulation in '{mode}' mode.")

        if mode == "filesystem":
            self.sim_task = asyncio.create_task(self._run_filesystem_sim(sim_cfg))
        elif mode == "rpc":
            self.sim_task = asyncio.create_task(self._run_rpc_sim(sim_cfg))
        else:
            self.logger.error(f"Unknown simulation mode: {mode}")
            return False

        # A short delay to allow the simulation to initialize before proceeding
        await asyncio.sleep(0.1)
        return True

    async def stop(self):
        """Stops the active simulation task."""
        if not self.sim_task or self.sim_task.done():
            return

        self.logger.info("Stopping simulation task...")
        self._sim_stop_event.set()
        try:
            await asyncio.wait_for(self.sim_task, timeout=5.0)
        except asyncio.TimeoutError:
            self.logger.warning("Simulation task did not stop gracefully within timeout. Cancelling.")
            self.sim_task.cancel()
        except Exception as e:
            self.logger.error(f"Exception while stopping simulation task: {e}", exc_info=True)
        self.sim_task = None
        self.logger.info("Simulation task stopped.")

    def data_flow_valid(self) -> bool:
        """Checks if the simulation task is running and considered valid."""
        if self.sim_task and not self.sim_task.done() and not self._sim_stop_event.is_set():
            return True
        return False

    async def _run_filesystem_sim(self, sim_cfg: dict):
        """
        Simulates the DAQ by writing PFF files to disk asynchronously.
        This is an asyncio-native version of the original simulation thread.
        """
        self.logger.info("Filesystem simulation thread started.")
        simulated_files = []
        daq_active_files = []
        active_pff_handles = {}

        try:
            # Setup: Create daq-active files
            for mid in sim_cfg['sim_module_ids']:
                active_file = get_daq_active_file(sim_cfg, mid)
                daq_active_files.append(active_file)
                async with aiofiles.open(active_file, "w") as f:
                    await f.write("1")

            # Get data product configurations
            dp_cfg = get_dp_config([sim_cfg['movie_type'], sim_cfg['ph_type']])
            movie_pff_path = get_sim_pff_path(sim_cfg, sim_cfg['real_module_id'], 0, False, False)
            ph_pff_path = get_sim_pff_path(sim_cfg, 3, 0, True, False)  # Using module 3 for PH as before

            with open(movie_pff_path, "rb") as movie_src, open(ph_pff_path, "rb") as ph_src:
                movie_fs, movie_nframes, _, _ = await asyncio.to_thread(pff.img_info, movie_src,
                                                                        dp_cfg[sim_cfg['movie_type']].bytes_per_image)
                ph_fs, ph_nframes, _, _ = await asyncio.to_thread(pff.img_info, ph_src,
                                                                  dp_cfg[sim_cfg['ph_type']].bytes_per_image)
                await asyncio.to_thread(movie_src.seek, 0)
                await asyncio.to_thread(ph_src.seek, 0)

                fnum = 0
                while not self._sim_stop_event.is_set():
                    # Read frames (blocking I/O in a separate thread)
                    movie_frame_data = movie_src.read(movie_fs)
                    ph_frame_data = ph_src.read(ph_fs)

                    if not movie_frame_data or not ph_frame_data:
                        self.logger.warning("Simulation source file reached EOF, looping.")
                        await asyncio.to_thread(movie_src.seek, 0)
                        await asyncio.to_thread(ph_src.seek, 0)
                        continue

                    # Logic to create new files every 'frames_per_pff'
                    seqno = fnum // sim_cfg['frames_per_pff']
                    if fnum % sim_cfg['frames_per_pff'] == 0:
                        # Close old handles
                        for mid in active_pff_handles:
                            if 'movie' in active_pff_handles[mid]: await asyncio.to_thread(
                                active_pff_handles[mid]['movie'].close)
                            if 'ph' in active_pff_handles[mid]: await asyncio.to_thread(
                                active_pff_handles[mid]['ph'].close)
                        active_pff_handles.clear()

                        # Open new files
                        for mid in sim_cfg['sim_module_ids']:
                            movie_dest_path = get_sim_pff_path(sim_cfg, mid, seqno, False, True)
                            ph_dest_path = get_sim_pff_path(sim_cfg, mid, seqno, True, True)
                            simulated_files.extend([movie_dest_path, ph_dest_path])
                            active_pff_handles[mid] = {
                                'movie': await asyncio.to_thread(open, movie_dest_path, 'ab'),
                                'ph': await asyncio.to_thread(open, ph_dest_path, 'ab')
                            }

                    # Write to simulated files
                    for mid in sim_cfg['sim_module_ids']:
                        # self.logger.debug(f"Writing movie frame {movie_frame_data}")
                        # self.logger.debug(f"Writing ph frame {ph_frame_data}")
                        active_pff_handles[mid]['movie'].write(movie_frame_data)
                        active_pff_handles[mid]['ph'].write(ph_frame_data)
                        active_pff_handles[mid]['movie'].flush()
                        active_pff_handles[mid]['ph'].flush()

                    fnum += 1
                    await asyncio.sleep(self.server_cfg['min_hp_io_update_interval_seconds'])

        except Exception as e:
            self.logger.error(f"Filesystem simulation failed: {e}", exc_info=True)
        finally:
            self.logger.info("Cleaning up filesystem simulation...")
            # Close all open file handles
            for mid in active_pff_handles:
                if 'movie' in active_pff_handles[mid]: await asyncio.to_thread(active_pff_handles[mid]['movie'].close)
                if 'ph' in active_pff_handles[mid]: await asyncio.to_thread(active_pff_handles[mid]['ph'].close)
            # Delete all created files
            for fpath in daq_active_files + list(set(simulated_files)):
                try:
                    if await asyncio.to_thread(os.path.exists, fpath):
                        # self.logger.critical(f'want to remove {fpath}')
                        await asyncio.to_thread(os.unlink, fpath)
                except Exception as e:
                    self.logger.warning(f"Failed to clean up sim file {fpath}: {e}")
            self.logger.info("Filesystem simulation finished cleanup.")

    async def _run_rpc_sim(self, sim_cfg: dict):
        """
        Simulates the DAQ by sending data via the UploadImages RPC.
        """
        self.logger.info("RPC simulation thread started.")

        def _blocking_read(dp_config: DataProductConfig, frame_idx: int) -> tuple[str, bytes]:
            dp_config.f.seek(frame_idx * dp_config.frame_size)
            header_str = pff.read_json(dp_config.f)
            img = pff.read_image(dp_config.f, dp_config.image_shape[0], dp_config.bytes_per_pixel)
            if header_str and img is not None:
                return json.loads(header_str), img
            return None, None

        daq_active_files = []
        dp_cfg = get_dp_config([sim_cfg['movie_type'], sim_cfg['ph_type']])
        try:
            # We need a client to talk to our own server.
            # Using a minimal dummy daq config because the client will connect to localhost via UDS.
            assert 'unix_domain_socket' in self.server_cfg, "RPC simulation requires a Unix Domain Socket."
            uds_listen_addr = self.server_cfg['unix_domain_socket']
            uds_path = Path(uds_listen_addr.split(':')[-1])
            self.logger.info(f"RPC simulation connecting to server... on {uds_listen_addr}")
            assert os.path.exists(uds_path), f"Unix Domain Socket path at {os.path.abspath(uds_path)} does not exist."
            daq_config_uds = {
                'daq_nodes': [
                    {
                        'ip_addr': uds_listen_addr,
                    }
                ]
            }
            async with AioDaqDataClient(daq_config=daq_config_uds, network_config=None) as client:
                ping_success = await client.ping(uds_listen_addr)
                async def image_generator() -> AsyncGenerator[PanoImage, None]:
                    """Generator that reads from files and yields PanoImage objects."""
                    movie_pff_path = get_sim_pff_path(sim_cfg, sim_cfg['real_module_id'], 0, False, False)
                    ph_pff_path = get_sim_pff_path(sim_cfg, 3, 0, True, False)

                    with open(movie_pff_path, "rb") as movie_src, open(ph_pff_path, "rb") as ph_src:
                        dp_cfg[sim_cfg['ph_type']].f = ph_src
                        dp_cfg[sim_cfg['ph_type']].frame_size = await asyncio.to_thread(pff.img_frame_size, ph_src,
                                                                dp_cfg[sim_cfg['ph_type']].bytes_per_image)
                        dp_cfg[sim_cfg['movie_type']].f = movie_src
                        dp_cfg[sim_cfg['movie_type']].frame_size = await asyncio.to_thread(pff.img_frame_size, movie_src,
                                                                dp_cfg[sim_cfg['movie_type']].bytes_per_image)
                        fnum = 0
                        while not self._sim_stop_event.is_set():
                            # Read frames
                            for dp in dp_cfg.values():
                                header, img_data = await asyncio.to_thread(_blocking_read, dp, fnum)

                                if not img_data:
                                    await asyncio.to_thread(movie_src.seek, 0)
                                    continue

                                for mid in sim_cfg['sim_module_ids']:
                                    yield PanoImage(
                                        type=dp.pano_image_type,
                                        header=ParseDict(header, Struct()),
                                        image_array=img_data,
                                        shape=dp.image_shape,
                                        bytes_per_pixel=dp.bytes_per_pixel,
                                        file=dp.f.name,
                                        frame_number=fnum,
                                        module_id=mid,
                                    )
                            fnum += 1
                            await asyncio.sleep(self.server_cfg['min_hp_io_update_interval_seconds'])

                # Setup: Create daq-active files
                for mid in sim_cfg['sim_module_ids']:
                    active_file = get_daq_active_file(sim_cfg, mid)
                    os.makedirs(os.path.dirname(active_file), exist_ok=True)
                    daq_active_files.append(active_file)
                    async with aiofiles.open(active_file, "w") as f:
                        await f.write("1")

                # The target address uses a Unix Domain Socket for efficient local IPC
                await client.upload_images(hosts=[], image_iterator=image_generator())

        except Exception as e:
            if not self._sim_stop_event.is_set():
                self.logger.error(f"RPC simulation failed: {e}", exc_info=True)
            raise e
        finally:
            # Delete all created files
            for dp in dp_cfg.values():
                if dp.f:
                    dp.f.close()
                    dp.f = None
            for fpath in daq_active_files:
                try:
                    if await asyncio.to_thread(os.path.exists, fpath):
                        # self.logger.critical(f'want to remove {fpath}')
                        await asyncio.to_thread(os.unlink, fpath)
                except Exception as e:
                    self.logger.warning(f"Failed to clean up sim file {fpath}: {e}")
            self.logger.info("RPC simulation exited.")

