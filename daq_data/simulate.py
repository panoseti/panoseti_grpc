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
import stat
import logging
from typing import Dict, Any, AsyncGenerator, List
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
        self.sim_created_files = []
        self.read_status_pipe_name = server_cfg['read_status_pipe_name']

    async def start(self, sim_cfg: dict) -> bool:
        await self.stop()
        self._sim_stop_event.clear()
        self.sim_created_files = []
        mode = sim_cfg.get("simulation_mode", "filesystem")
        self.logger.info(f"Starting simulation in '{mode}' mode.")

        if mode == "filesystem":
            if not await self._setup_initial_filesystem_sim(sim_cfg):
                self.logger.error("Failed to complete initial filesystem setup.")
                return False
            self.sim_task = asyncio.create_task(self._run_filesystem_sim(sim_cfg))
        elif mode == "rpc":
            self.sim_task = asyncio.create_task(self._run_rpc_sim(sim_cfg))
        elif mode == "uds":
            if "uds_input_config" not in self.server_cfg or not self.server_cfg["uds_input_config"].get("enabled"):
                self.logger.error(
                    "UDS simulation mode selected, but 'uds_input_config' is not enabled in server config.")
                return False
            self.sim_task = asyncio.create_task(self._run_uds_sim(sim_cfg))
        else:
            self.logger.error(f"Unknown simulation mode: {mode}")
            return False

        await asyncio.sleep(0.1) # yield control and wait for simulation task to start
        return True

    async def stop(self):
        if self.sim_task and not self.sim_task.done():
            self.logger.info("Stopping simulation task...")
            self._sim_stop_event.set()
            try:
                await asyncio.wait_for(self.sim_task, timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("Simulation task did not stop gracefully. Cancelling.")
                self.sim_task.cancel()
            except Exception as e:
                self.logger.error(f"Exception while stopping simulation task: {e}", exc_info=True)

        for fpath in self.sim_created_files:
            try:
                if os.path.exists(fpath):
                    if stat.S_ISFIFO(os.stat(fpath).st_mode):
                        os.unlink(fpath)
                    elif not os.path.isdir(fpath):
                        os.unlink(fpath)
            except Exception as e:
                self.logger.warning(f"Failed to clean up sim resource {fpath}: {e}")
        self.sim_task = None

    def data_flow_valid(self) -> bool:
        return self.sim_task and not self.sim_task.done()

    async def _setup_initial_filesystem_sim(self, sim_cfg: dict) -> bool:
        self.logger.info("Setting up initial files for filesystem simulation.")
        try:
            dp_configs = get_dp_config([sim_cfg['movie_type'], sim_cfg['ph_type']])
            movie_pff_path = get_sim_pff_path(sim_cfg, sim_cfg['real_module_id'], 0, is_ph=False, is_simulated=False)
            ph_pff_path = get_sim_pff_path(sim_cfg, 3, 0, is_ph=True, is_simulated=False)

            with open(movie_pff_path, "rb") as movie_src, open(ph_pff_path, "rb") as ph_src:
                movie_fs, _, _, _ = pff.img_info(movie_src, dp_configs[sim_cfg['movie_type']].bytes_per_image)
                movie_src.seek(0, os.SEEK_SET)
                first_movie_frame = movie_src.read(movie_fs)

                ph_fs, _, _, _ = pff.img_info(ph_src, dp_configs[sim_cfg['ph_type']].bytes_per_image)
                ph_src.seek(0, os.SEEK_SET)
                first_ph_frame = ph_src.read(ph_fs)

            for mid in sim_cfg['sim_module_ids']:
                sim_run_dir = Path(sim_cfg['files']['data_dir']) / sim_cfg['files']['sim_run_dir_template'].format(
                    module_id=mid)
                os.makedirs(sim_run_dir, exist_ok=True)

                active_file = get_daq_active_file(sim_cfg, mid)
                async with aiofiles.open(active_file, "w") as f:
                    await f.write("1")
                self.sim_created_files.append(active_file)

                pipe_path = sim_run_dir / self.read_status_pipe_name
                if not os.path.exists(pipe_path):
                    os.mkfifo(pipe_path)
                self.sim_created_files.append(str(pipe_path))

                movie_dest_path = get_sim_pff_path(sim_cfg, mid, 0, False, True)
                async with aiofiles.open(movie_dest_path, 'wb') as f:
                    await f.write(first_movie_frame)
                self.sim_created_files.append(movie_dest_path)

                ph_dest_path = get_sim_pff_path(sim_cfg, mid, 0, True, True)
                async with aiofiles.open(ph_dest_path, 'wb') as f:
                    await f.write(first_ph_frame)
                self.sim_created_files.append(ph_dest_path)

            return True
        except Exception as e:
            self.logger.error(f"Failed to set up initial simulation environment: {e}", exc_info=True)
            return False

    async def _run_filesystem_sim(self, sim_cfg: dict):
        self.logger.info("Filesystem simulation task started.")
        pipe_fds = {}
        simulated_pff_handles = {}

        try:
            frames_per_pff = sim_cfg.get('frames_per_pff', 1000)
            self.logger.info(f"Simulating with up to {frames_per_pff} frames per PFF file.")

            # Load all source frames into memory
            real_module_id = sim_cfg['real_module_id']
            dp_configs = get_dp_config([sim_cfg['movie_type'], sim_cfg['ph_type']])

            movie_frames: List[bytes] = []
            movie_pff_path = get_sim_pff_path(sim_cfg, real_module_id, 0, is_ph=False, is_simulated=False)
            with open(movie_pff_path, "rb") as movie_src:
                movie_fs, nframes, _, _ = pff.img_info(movie_src, dp_configs[sim_cfg['movie_type']].bytes_per_image)
                movie_src.seek(0, os.SEEK_SET)
                for i in range(nframes):
                    movie_frames.append(movie_src.read(movie_fs))

            ph_frames: List[bytes] = []
            ph_pff_path = get_sim_pff_path(sim_cfg, real_module_id, 0, is_ph=True, is_simulated=False)
            with open(ph_pff_path, "rb") as ph_src:
                ph_fs, nframes, _, _ = pff.img_info(ph_src, dp_configs[sim_cfg['ph_type']].bytes_per_image)
                ph_src.seek(0, os.SEEK_SET)
                for i in range(nframes):
                    ph_frames.append(ph_src.read(ph_fs))

            self.logger.info(
                f"Loaded {len(movie_frames)} movie and {len(ph_frames)} PH frames into memory for simulation.")

            # State tracking for each simulated module
            module_states = {
                mid: {'seqno': 0, 'frames_written': 1}
                for mid in sim_cfg['sim_module_ids']
            }

            # Open initial file handles for seqno=0 files in append mode
            for mid in sim_cfg['sim_module_ids']:
                movie_path = get_sim_pff_path(sim_cfg, mid, 0, is_ph=False, is_simulated=True)
                ph_path = get_sim_pff_path(sim_cfg, mid, 0, is_ph=True, is_simulated=True)
                simulated_pff_handles[mid] = {
                    'movie_f': open(movie_path, 'ab'),
                    'ph_f': open(ph_path, 'ab')
                }

            fnum = 1  # Start from the second frame since setup wrote the first

            while not self._sim_stop_event.is_set():
                if fnum >= len(movie_frames) or fnum >= len(ph_frames):
                    self.logger.info("Simulation source file reached EOF, looping.")
                    fnum = 0
                    continue

                movie_frame_data = movie_frames[fnum]
                ph_frame_data = ph_frames[fnum]

                for mid in sim_cfg['sim_module_ids']:
                    state = module_states[mid]

                    if state['frames_written'] >= frames_per_pff:
                        self.logger.info(
                            f"Module {mid}: Reached {frames_per_pff} frames. Rolling over to new PFF file.")

                        simulated_pff_handles[mid]['movie_f'].close()
                        simulated_pff_handles[mid]['ph_f'].close()

                        state['seqno'] += 1
                        state['frames_written'] = 0

                        new_movie_path = get_sim_pff_path(sim_cfg, mid, state['seqno'], is_ph=False, is_simulated=True)
                        new_ph_path = get_sim_pff_path(sim_cfg, mid, state['seqno'], is_ph=True, is_simulated=True)

                        simulated_pff_handles[mid]['movie_f'] = open(new_movie_path, 'wb')
                        simulated_pff_handles[mid]['ph_f'] = open(new_ph_path, 'wb')

                        self.sim_created_files.append(new_movie_path)
                        self.sim_created_files.append(new_ph_path)

                        self.logger.debug(f"Module {mid}: Created new files for seqno {state['seqno']}:")
                        self.logger.debug(f"  - {new_movie_path}")
                        self.logger.debug(f"  - {new_ph_path}")

                    if mid not in pipe_fds:
                        try:
                            sim_run_dir = Path(sim_cfg['files']['data_dir']) / sim_cfg['files'][
                                'sim_run_dir_template'].format(module_id=mid)
                            pipe_path = sim_run_dir / self.read_status_pipe_name
                            pipe_fds[mid] = os.open(pipe_path, os.O_WRONLY | os.O_NONBLOCK)
                        except OSError:
                            await asyncio.sleep(0.1)
                            continue

                    simulated_pff_handles[mid]['movie_f'].write(movie_frame_data)
                    simulated_pff_handles[mid]['movie_f'].flush()
                    os.write(pipe_fds[mid], f"{sim_cfg['movie_type']:<6}".encode())

                    simulated_pff_handles[mid]['ph_f'].write(ph_frame_data)
                    simulated_pff_handles[mid]['ph_f'].flush()
                    os.write(pipe_fds[mid], f"{sim_cfg['ph_type']:<6}".encode())

                    state['frames_written'] += 1

                fnum += 1
                await asyncio.sleep(self.server_cfg.get('min_hp_io_update_interval_seconds', 0.1))

        except Exception as e:
            if not isinstance(e, (BrokenPipeError, asyncio.CancelledError)):
                self.logger.error(f"Filesystem simulation failed: {e}", exc_info=True)
        finally:
            self.logger.info("Closing simulation file and pipe handles...")
            for mid, handle_dict in simulated_pff_handles.items():
                if 'movie_f' in handle_dict and not handle_dict['movie_f'].closed:
                    handle_dict['movie_f'].close()
                if 'ph_f' in handle_dict and not handle_dict['ph_f'].closed:
                    handle_dict['ph_f'].close()
            for fd in pipe_fds.values():
                os.close(fd)
            self.logger.info("Filesystem simulation task finished.")


    async def _run_rpc_sim(self, sim_cfg: dict):
        """
        Simulates the DAQ by sending data via the UploadImages RPC.
        """
        self.logger.info("RPC simulation thread started.")

        daq_active_files = []
        try:
            # We need a client to talk to our own server.
            # Using a minimal dummy daq config because the client will connect to localhost via UDS.
            assert 'unix_domain_socket' in self.server_cfg, "RPC simulation requires a Unix Domain Socket."
            uds_listen_addr = self.server_cfg['unix_domain_socket']
            uds_path = Path(uds_listen_addr.split(':')[-1])
            self.logger.info(f"RPC simulation connecting to server... on {uds_listen_addr}")
            assert os.path.exists(uds_path), f"Unix Domain Socket path at {os.path.abspath(uds_path)} does not exist."
            daq_config_uds = {

                'daq_nodes': [{

                    'ip_addr': uds_listen_addr,

                }]}
            async with AioDaqDataClient(daq_config=daq_config_uds, network_config=None) as client:
                def _blocking_read(dp_config: DataProductConfig, frame_idx: int) -> tuple[str, bytes]:
                    dp_config.f.seek(frame_idx * dp_config.frame_size)
                    header_str = pff.read_json(dp_config.f)
                    img = pff.read_image(dp_config.f, dp_config.image_shape[0], dp_config.bytes_per_pixel)
                    if header_str and img is not None:
                        return json.loads(header_str), img
                    return None, None

                ping_success = await client.ping(uds_listen_addr)
                assert ping_success, "UploadImage RPC simulation ping failed."
                async def simulate_upload_image_generator() -> AsyncGenerator[PanoImage, None]:
                    """Generator that reads from files and yields PanoImage objects."""
                    movie_pff_path = get_sim_pff_path(sim_cfg, sim_cfg['real_module_id'], 0, False, False)
                    ph_pff_path = get_sim_pff_path(sim_cfg, 3, 0, True, False)

                    with open(movie_pff_path, "rb") as movie_src, open(ph_pff_path, "rb") as ph_src:
                        dp_cfg = get_dp_config([sim_cfg['movie_type'], sim_cfg['ph_type']])

                        dp_cfg[sim_cfg['ph_type']].f = ph_src
                        dp_cfg[sim_cfg['ph_type']].frame_size = await asyncio.to_thread(pff.img_frame_size, ph_src,
                                                                dp_cfg[sim_cfg['ph_type']].bytes_per_image)
                        num_ph = os.path.getsize(ph_pff_path) // dp_cfg[sim_cfg['ph_type']].frame_size
                        dp_cfg[sim_cfg['movie_type']].f = movie_src
                        dp_cfg[sim_cfg['movie_type']].frame_size = await asyncio.to_thread(pff.img_frame_size, movie_src,
                                                                dp_cfg[sim_cfg['movie_type']].bytes_per_image)
                        num_movie = os.path.getsize(movie_pff_path) // dp_cfg[sim_cfg['movie_type']].frame_size
                        fnum = 0
                        while not self._sim_stop_event.is_set():
                            if fnum >= num_ph or fnum >= num_movie:
                                for dp in dp_cfg.values():
                                    self.logger.debug(f"Simulation source file for {dp.name=} reached EOF, looping.")
                                    await asyncio.to_thread(dp.f.seek, 0)
                                fnum = 0
                            # Read frames
                            for dp in dp_cfg.values():
                                header, img_data = await asyncio.to_thread(_blocking_read, dp, fnum)

                                if not img_data:
                                    self.logger.debug("Simulation source file reached EOF, looping.")
                                    await asyncio.to_thread(dp.f.seek, 0)
                                    continue

                                pano_image = PanoImage(
                                    type=dp.pano_image_type,
                                    header=ParseDict(header, Struct()),
                                    image_array=img_data,
                                    shape=dp.image_shape,
                                    bytes_per_pixel=dp.bytes_per_pixel,
                                    file=dp.f.name,
                                    frame_number=fnum,
                                )

                                for mid in sim_cfg['sim_module_ids']:
                                    pano_image.module_id = mid
                                    yield pano_image
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
                await client.upload_images(hosts=[], image_iterator=simulate_upload_image_generator())

        except Exception as e:
            if not self._sim_stop_event.is_set():
                self.logger.error(f"RPC simulation failed: {e}", exc_info=True)
            raise e
        finally:
            for fpath in daq_active_files:
                try:
                    if await asyncio.to_thread(os.path.exists, fpath):
                        # self.logger.critical(f'want to remove {fpath}')
                        await asyncio.to_thread(os.unlink, fpath)
                except Exception as e:
                    self.logger.warning(f"Failed to clean up sim file {fpath}: {e}")
            self.logger.info("RPC simulation exited.")


    async def _run_uds_sim(self, sim_cfg: dict):
        """
        Simulates the DAQ by writing PFF frame data directly to the Unix Domain Sockets
        that the HpIoManager is configured to listen on.
        """
        self.logger.info("UDS simulation task started.")
        connections = {}
        source_frames = {}
        frame_counters = {}

        try:
            # 1. Get data products to simulate from the main server config
            uds_config = self.server_cfg.get("uds_input_config", {})
            data_products = uds_config.get("data_products", [])
            if not data_products:
                self.logger.warning(
                    "UDS simulation running, but no data_products are configured in 'uds_input_config'. Exiting.")
                return

            self.logger.info(f"UDS simulation will stream data for: {data_products}")

            # 2. Load all source frames into memory for each data product type
            # Note: We use the single movie_type and ph_type files as generic sources.
            # A more advanced setup could map each DP to a specific file in the config.
            real_module_id = sim_cfg['real_module_id']

            # Load movie source
            dp_configs = get_dp_config([sim_cfg['movie_type']])
            movie_dp_config = dp_configs[sim_cfg['movie_type']]
            movie_pff_path = get_sim_pff_path(sim_cfg, real_module_id, 0, is_ph=False, is_simulated=False)
            movie_source_data = []
            with open(movie_pff_path, "rb") as f:
                frame_size, nframes, _, _ = pff.img_info(f, movie_dp_config.bytes_per_image)
                f.seek(0, os.SEEK_SET)
                for _ in range(nframes):
                    movie_source_data.append(f.read(frame_size))

            # Load pulse-height source
            dp_configs = get_dp_config([sim_cfg['ph_type']])
            ph_dp_config = dp_configs[sim_cfg['ph_type']]
            ph_pff_path = get_sim_pff_path(sim_cfg, real_module_id, 0, is_ph=True, is_simulated=False)
            ph_source_data = []
            with open(ph_pff_path, "rb") as f:
                frame_size, nframes, _, _ = pff.img_info(f, ph_dp_config.bytes_per_image)
                f.seek(0, os.SEEK_SET)
                for _ in range(nframes):
                    ph_source_data.append(f.read(frame_size))

            self.logger.info(
                f"Loaded {len(movie_source_data)} movie and {len(ph_source_data)} PH source frames for UDS sim.")

            for dp_name in data_products:
                connections[dp_name] = None
                frame_counters[dp_name] = 0
                if 'ph' in dp_name:
                    source_frames[dp_name] = ph_source_data
                else:
                    source_frames[dp_name] = movie_source_data

                if not source_frames[dp_name]:
                    self.logger.warning(f"No source frames loaded for {dp_name}. It will not be simulated.")

            # 3. Main simulation loop
            while not self._sim_stop_event.is_set():
                for dp_name in data_products:
                    if not source_frames[dp_name]:
                        continue  # Skip if no data for this product

                    socket_path = f"/tmp/hashpipe_grpc_{dp_name}.sock"

                    # Attempt to connect if not already connected
                    if connections.get(dp_name) is None:
                        try:
                            _, writer = await asyncio.open_unix_connection(socket_path)
                            connections[dp_name] = writer
                            self.logger.info(f"UDS sim: Connected to {socket_path}")
                        except (ConnectionRefusedError, FileNotFoundError):
                            self.logger.debug(f"UDS sim: Could not connect to {socket_path}. Will retry.")
                            continue  # Try again on the next loop iteration

                    # Write data to the socket
                    writer = connections[dp_name]
                    frame_idx = frame_counters[dp_name]
                    frame_data = source_frames[dp_name][frame_idx]

                    try:
                        writer.write(frame_data)
                        await writer.drain()

                        # Increment frame counter, looping back to 0 at EOF
                        frame_counters[dp_name] = (frame_idx + 1) % len(source_frames[dp_name])

                    except (BrokenPipeError, ConnectionResetError) as e:
                        self.logger.warning(
                            f"UDS sim: Connection to {socket_path} lost: {e}. Will attempt to reconnect.")
                        if not writer.is_closing():
                            writer.close()
                            await writer.wait_closed()
                        connections[dp_name] = None

                # Control the simulation speed
                await asyncio.sleep(self.server_cfg.get('min_hp_io_update_interval_seconds', 0.1))

        except asyncio.CancelledError:
            self.logger.info("UDS simulation task was cancelled.")
        except Exception as e:
            if not self._sim_stop_event.is_set():
                self.logger.error(f"UDS simulation failed: {e}", exc_info=True)
        finally:
            self.logger.info("Closing all UDS simulation connections...")
            for dp_name, writer in connections.items():
                if writer and not writer.is_closing():
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception as e:
                        self.logger.warning(f"Error closing writer for {dp_name}: {e}")
            self.logger.info("UDS simulation task finished.")

