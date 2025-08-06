"""
Defines abstract and concrete data source classes for the HpIoManager.
Each class is responsible for one method of acquiring PANOSETI data.
"""
import abc
import asyncio
import logging
import os
import stat
from io import BytesIO
from json import loads
from pathlib import Path
import select
from typing import Dict, Optional

from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct
from watchfiles import awatch

from .daq_data_pb2 import PanoImage
from .state import ModuleState, get_dp_config  # Forward reference for typing
from .resources import _parse_dp_name, _parse_seqno
from panoseti_util import pff

class BaseDataSource(abc.ABC):
    """Abstract base class for a data acquisition source."""

    def __init__(self, config: dict, logger: logging.Logger, data_queue: asyncio.Queue, stop_event: asyncio.Event):
        self.config = config
        self.logger = logger
        self.data_queue = data_queue
        self.stop_event = stop_event

    @abc.abstractmethod
    async def run(self):
        """The main entry point to start watching for and producing data."""
        pass


class UdsDataSource(BaseDataSource):
    """Acquires data from a Unix Domain Socket."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dp_name = self.config['dp_name']
        self.module_id = self.config['module_id']
        self.socket_path = f"/tmp/hashpipe_grpc_{self.dp_name}.sock"
        self.dp_config = get_dp_config([self.dp_name])[self.dp_name]
        self.server: Optional[asyncio.AbstractServer] = None

    async def run(self):
        self.logger.info(f"Starting UDS receiver for {self.dp_name} on {self.socket_path}")
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        try:
            self.server = await asyncio.start_unix_server(self._handle_client, path=self.socket_path)
            await self.stop_event.wait()
        except Exception as e:
            self.logger.error(f"UDS receiver for {self.dp_name} failed: {e}", exc_info=True)
        finally:
            if self.server:
                self.server.close()
                await self.server.wait_closed()
            if os.path.exists(self.socket_path):
                try:
                    os.unlink(self.socket_path)
                except OSError as e:
                    self.logger.warning(f"Could not unlink UDS socket {self.socket_path}: {e}")
            self.logger.info(f"UDS receiver for {self.dp_name} has stopped.")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.logger.info(f"New connection on {self.dp_name} socket.")
        frame_count = 0
        try:
            while not self.stop_event.is_set():
                # Read the JSON header to determine its size
                header_buf = BytesIO()
                while True:
                    char = await reader.read(1)
                    if not char: raise asyncio.IncompleteReadError(b"Socket closed while reading header", None)
                    header_buf.write(char)
                    # A double newline signifies the end of the JSON header
                    if header_buf.getvalue().endswith(b'\n\n'):
                        break
                header_buf.seek(0)
                header_str = header_buf.read().decode().strip()
                header = loads(header_str)

                # Read the image data
                img_data_buf = await reader.readexactly(self.dp_config.bytes_per_image + 1)
                if img_data_buf[0:1] != b'*':
                    self.logger.warning(f"Invalid image start character on {self.dp_name} stream.")
                    continue
                img_array = pff.read_image(BytesIO(img_data_buf), self.dp_config.image_shape[0], self.dp_config.bytes_per_pixel)

                pano_image = PanoImage(
                    type=self.dp_config.pano_image_type,
                    header=ParseDict(header, Struct()),
                    image_array=img_array,
                    shape=self.dp_config.image_shape,
                    bytes_per_pixel=self.dp_config.bytes_per_pixel,
                    file=f"uds_{self.dp_name}",
                    frame_number=frame_count,
                    module_id=self.module_id,
                )
                await self.data_queue.put(pano_image)
                frame_count += 1
        except (asyncio.IncompleteReadError, ConnectionResetError):
            self.logger.info(f"Client disconnected from {self.dp_name} socket.")
        except asyncio.CancelledError:
            self.logger.info(f"Client handler for {self.dp_name} was cancelled.")
        finally:
            writer.close()
            await writer.wait_closed()


class FilesystemDataSource(BaseDataSource):
    """Base class for filesystem-based data sources. Needs access to manager state."""

    def __init__(self, manager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manager = manager  # Reference to HpIoManager instance

    async def _process_file_change(self, filepath: Path):
        """Shared logic to process a detected file change and enqueue a PanoImage."""
        if not filepath.name.endswith('.pff'): return

        match = self.manager.module_id_re.search(str(filepath))
        if not match: return
        module_id = int(match.group(1))

        dp_name = _parse_dp_name(filepath.name)
        if not dp_name: return

        # Discover module/DP if not already known
        if module_id not in self.manager.modules:
            if not await self.manager.discover_new_module(module_id): return
        module = self.manager.modules[module_id]
        if dp_name not in module.dp_configs:
            if not await module.add_dp_from_fs(dp_name): return
        
        dp_config = module.dp_configs[dp_name]

        # Fetch latest frame and enqueue
        header, img, frame_idx = await self.manager.fetch_latest_frame_from_file(filepath, dp_config)
        if header and img is not None:
            pano_image = PanoImage(
                type=dp_config.pano_image_type,
                header=ParseDict(header, Struct()),
                image_array=img,
                shape=dp_config.image_shape,
                bytes_per_pixel=dp_config.bytes_per_pixel,
                file=filepath.name,
                frame_number=frame_idx,
                module_id=module.module_id,
            )
            await self.data_queue.put(pano_image)


class PollWatcherDataSource(FilesystemDataSource):
    """Acquires data by polling the filesystem for changes using watchfiles."""

    async def run(self):
        data_dir = self.manager.data_dir
        if not await asyncio.to_thread(os.path.isdir, data_dir):
            self.logger.warning(f"Data directory {data_dir} does not exist. Polling watcher will not start.")
            return

        self.logger.info("Starting filesystem polling watcher.")
        update_ms = int(self.manager.update_interval_seconds * 1000)
        async for changes in awatch(data_dir, stop_event=self.stop_event, recursive=True,
                                    force_polling=True, poll_delay_ms=update_ms):
            for _, filepath_str in changes:
                await self._process_file_change(Path(filepath_str))


class PipeWatcherDataSource(FilesystemDataSource):
    """Acquires data by listening to a named pipe for signals."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipe_fds_to_close = []
        self._pipe_readers_installed = False

    async def run(self):
        self.logger.info("Starting pipe-based watcher.")
        loop = asyncio.get_running_loop()
        pipes_to_watch: Dict[int, ModuleState] = {}
        
        while not self.stop_event.is_set():
            self.logger.debug(f"self.manager.modules[module_id]: {self.manager.modules}")
            for mid, module in self.manager.modules.items():
                if module.run_path:
                    pipe_path = module.run_path / self.manager.read_status_pipe_name
                    if await asyncio.to_thread(pipe_path.exists) and \
                    stat.S_ISFIFO((await asyncio.to_thread(os.stat, pipe_path)).st_mode):
                        try:
                            fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)
                            pipes_to_watch[fd] = module
                            self.pipe_fds_to_close.append(fd)
                            self.logger.debug(f"Found pipe at {pipe_path}")
                        except Exception as e:
                            self.logger.error(f"Failed to open pipe for module {mid}: {e}")

            if not pipes_to_watch:
                self.logger.warning("No valid pipes found to watch. Waiting for new pipes...")
                # Run this code to discover new modules and pipes dynamically
                await asyncio.sleep(1)
                async for changes in awatch(self.manager.data_dir, stop_event=self.stop_event, recursive=True,
                                            force_polling=True, poll_delay_ms=1000):
                    for _, filepath_str in changes:
                        await self._process_file_change(Path(filepath_str))
                    if self.manager.modules:
                        break
            else:
                break

        data_ready = asyncio.Event()
        def _pipe_readable_callback():
            data_ready.set()

        for fd in pipes_to_watch.keys():
            loop.add_reader(fd, _pipe_readable_callback)
        self._pipe_readers_installed = True

        try:
            while not self.stop_event.is_set():
                await asyncio.wait_for(data_ready.wait(), timeout=1.0)
                
                readable_fds, _, _ = select.select(list(pipes_to_watch.keys()), [], [], 0)
                for fd in readable_fds:
                    module = pipes_to_watch[fd]
                    try:
                        while True: # Read all messages from the pipe
                            msg = os.read(fd, 10)
                            if not msg: break
                            dp_name = msg.strip().decode()
                            await self._process_newest_file_for_dp(module, dp_name)
                    except BlockingIOError:
                        continue # Done reading from this pipe for now
                    except Exception as e:
                        self.logger.error(f"Error reading from pipe for module {module.module_id}: {e}")

                data_ready.clear()
        except asyncio.TimeoutError:
            pass # Normal behavior, loop will continue
        except asyncio.CancelledError:
            self.logger.info("Pipe watcher task cancelled.")
        finally:
            if self._pipe_readers_installed:
                for fd in self.pipe_fds_to_close:
                    loop.remove_reader(fd)
            for fd in self.pipe_fds_to_close:
                os.close(fd)
            self.logger.info("Pipe watcher task finished.")

    async def _process_newest_file_for_dp(self, module, dp_name: str):
        """Find the latest .pff file for a specific data product and process it."""
        if not module.run_path: return
        try:
            # Find all files for this specific data product
            pff_files = list(module.run_path.glob(f'*.dp_{dp_name}.*.pff'))
            if not pff_files:
                self.logger.warning(f"Pipe signal for {dp_name} on module {module.module_id}, but no matching files found.")
                return

            # Find the one with the highest sequence number
            newest_pff_file = max(pff_files, key=lambda p: _parse_seqno(p.name) or -1)
            await self._process_file_change(newest_pff_file)
        except Exception as e:
            self.logger.error(f"Error finding newest file for {dp_name} on module {module.module_id}: {e}", exc_info=True)