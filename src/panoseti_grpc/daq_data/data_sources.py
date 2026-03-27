"""
Defines data source classes for the HpIoManager.
Only the Unix Domain Socket (UDS) data source is supported.
"""
import abc
import asyncio
import logging
import os
import stat
from io import BytesIO
from json import loads
from typing import Optional
import socket
import struct

from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct

from panoseti_grpc.generated.daq_data_pb2 import PanoImage
from panoseti_grpc.panoseti_util import pff

from .state import get_dp_config


class BaseDataSource(abc.ABC):
    """Abstract base class for a data acquisition source."""
    def __init__(self, config: dict, logger: logging.Logger, data_queue: asyncio.Queue, stop_event: asyncio.Event):
        self.config = config
        self.logger = logger
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.ready_event = asyncio.Event()

    @abc.abstractmethod
    async def run(self):
        """The main entry point to start watching for and producing data."""
        pass


class UdsDataSource(BaseDataSource):
    """Acquires data from a Unix Domain Socket. Acts as the UDS SERVER."""
    SOCKET_BUFFER_SIZE = 2048 * 100

    def __init__(self, config: dict, logger: logging.Logger, data_queue: asyncio.Queue, stop_event: asyncio.Event):
        super().__init__(config, logger, data_queue, stop_event)
        self.dp_name = self.config['dp_name']

        socket_path_template = self.config.get('socket_path_template')
        if not socket_path_template:
            raise ValueError("UdsDataSource requires a 'socket_path_template'")

        self.socket_path = socket_path_template.format(dp_name=self.dp_name)
        self.dp_config = get_dp_config([self.dp_name])[self.dp_name]
        self.server: Optional[asyncio.AbstractServer] = None
        self.read_timeout = config.get('read_timeout', 10.0)
        self.client_handler_tasks = set()

    async def _client_connection_wrapper(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Wraps the client handler to track its task lifecycle."""
        task = asyncio.create_task(self._handle_client(reader, writer))
        self.client_handler_tasks.add(task)

        def on_task_done(t):
            self.client_handler_tasks.discard(t)
            client_info = writer.get_extra_info('peername')
            self.logger.info(f"Client handler task for {client_info} on {self.socket_path} has finished.")

        task.add_done_callback(on_task_done)

    async def run(self):
        """Creates, binds, and listens on the UDS socket file."""
        self.logger.info(f"Starting UDS receiver for '{self.dp_name}' on {self.socket_path}")

        try:
            # Check if a file exists at the socket path and if it's actually a socket.
            if os.path.exists(self.socket_path):
                s = os.stat(self.socket_path)
                if stat.S_ISSOCK(s.st_mode):
                    self.logger.warning(f"Removing stale socket file: {self.socket_path}")
                    os.unlink(self.socket_path)
                else:
                    self.logger.error(
                        f"A non-socket file exists at the socket path {self.socket_path}. "
                        "Manual intervention required."
                    )
                    return  # Prevent the server from starting
        except OSError as e:
            self.logger.error(
                f"Error removing stale socket file {self.socket_path}: {e}. "
                "UDS receiver cannot start."
            )
            return  # Abort if cleanup fails

        server_sock = None
        try:
            server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

            # Set receive buffer size to detect disconnects faster.
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.SOCKET_BUFFER_SIZE)
            self.logger.info(f"Set UDS receive buffer size to {self.SOCKET_BUFFER_SIZE} for {self.socket_path}")

            # SO_LINGER: immediately signal a connection reset to the client's OS on close.
            linger_struct = struct.pack('ii', 1, 0)
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger_struct)
            self.logger.info(f"Set SO_LINGER option for {self.socket_path} to force RST on close.")

            server_sock.bind(self.socket_path)
            os.chmod(self.socket_path, 0o777)
            server_sock.listen(1)  # Hashpipe should be the only process connecting.
            server_sock.setblocking(False)

            self.server = await asyncio.start_unix_server(self._client_connection_wrapper, sock=server_sock)
            self.ready_event.set()
            await self.stop_event.wait()

        except OSError as e:
            self.logger.error(f"UDS receiver for {self.socket_path} failed to bind or start: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"UDS receiver for {self.socket_path} failed unexpectedly: {e}", exc_info=True)
        finally:
            self.logger.info(f"Shutting down UDS receiver for {self.socket_path}...")

            if self.client_handler_tasks:
                self.logger.warning(
                    f"Cancelling {len(self.client_handler_tasks)} outstanding client handler tasks for {self.socket_path}."
                )
                tasks_to_cancel = list(self.client_handler_tasks)
                for task in tasks_to_cancel:
                    task.cancel()
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

            if self.server:
                self.server.close()
                await self.server.wait_closed()

            if server_sock:
                server_sock.close()

            if os.path.exists(self.socket_path):
                try:
                    os.unlink(self.socket_path)
                except OSError:
                    pass
            self.ready_event.clear()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handles a client connection, parsing [module_id][PFF frame] messages."""
        client_info = writer.get_extra_info('peername')
        self.logger.info(f"New client connection on {self.socket_path} from {client_info}")
        frame_count = 0
        header_size = None  # Discover from first frame

        try:
            while not self.stop_event.is_set():
                # 1. Read the 2-byte module ID prefix with a timeout
                module_id_bytes = await asyncio.wait_for(reader.readexactly(2), self.read_timeout)
                module_id = int.from_bytes(module_id_bytes, 'big')

                # 2. Discover or read the fixed-size PFF frame with a timeout
                if header_size is None:
                    header_with_sep = await asyncio.wait_for(reader.readuntil(b'\n\n'), self.read_timeout)
                    header_size = len(header_with_sep)
                    self.logger.info(f"Discovered header size of {header_size} bytes for {self.socket_path}")
                else:
                    header_with_sep = await asyncio.wait_for(reader.readexactly(header_size), self.read_timeout)

                # 3. Read the image data with a timeout
                # '1 +' is needed to account for the '*' prefix
                img_data_size = 1 + self.dp_config.bytes_per_image
                img_data = await asyncio.wait_for(reader.readexactly(img_data_size), self.read_timeout)

                # 4. Parse and process the frame
                json_bytes = header_with_sep[:-2]  # Strip the '\n\n' separator
                header = loads(json_bytes.decode())
                img_array = pff.read_image(BytesIO(img_data), self.dp_config.image_shape[0],
                                           self.dp_config.bytes_per_pixel)

                pano_image = PanoImage(
                    type=self.dp_config.pano_image_type,
                    header=ParseDict(header, Struct()),
                    image_array=img_array,
                    shape=self.dp_config.image_shape,
                    bytes_per_pixel=self.dp_config.bytes_per_pixel,
                    file=f"uds_{self.dp_name}",
                    frame_number=frame_count,
                    module_id=module_id,
                )
                await self.data_queue.put(pano_image)
                frame_count += 1
        except (asyncio.IncompleteReadError, ConnectionResetError):
            self.logger.info(f"Client {client_info} disconnected from {self.socket_path}.")
        except asyncio.CancelledError:
            self.logger.info(f"Client handler for {self.socket_path} was cancelled.")
        finally:
            writer.close()
            await writer.wait_closed()
