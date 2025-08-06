import asyncio
import os
import logging
import json
from io import BytesIO

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

from .daq_data_pb2 import PanoImage
from .resources import get_dp_config
from panoseti_util import pff


class UdsReceiver:
    """Manages reading PFF data from a single Unix Domain Socket."""

    def __init__(self, dp_name: str, module_id: int, upload_queue: asyncio.Queue, logger: logging.Logger):
        self.dp_name = dp_name
        self.module_id = module_id
        self.upload_queue = upload_queue
        self.logger = logger

        # Derive socket path from data product name
        self.socket_path = f"/tmp/hashpipe_grpc_{dp_name}.sock"

        # Get data product configuration
        self.dp_config = get_dp_config([dp_name])[dp_name]

        self.server: asyncio.AbstractServer = None
        self._stop_event = asyncio.Event()

    async def run(self):
        """Starts the UDS server and listens for connections."""
        self.logger.info(f"Starting UDS receiver for {self.dp_name} on {self.socket_path}")

        # Clean up old socket file if it exists
        # if os.path.exists(self.socket_path):
        #     try:
        #         os.unlink(self.socket_path)
        #     except OSError as e:
        #         self.logger.error(f"Error removing existing socket file {self.socket_path}: {e}")
        #         return

        try:
            self.server = await asyncio.start_unix_server(self._handle_client, path=self.socket_path)
            self.logger.info(f"UDS server for {self.dp_name} is listening on {self.socket_path}")
            await self._stop_event.wait()
        except Exception as e:
            self.logger.error(f"UDS receiver for {self.dp_name} failed: {e}", exc_info=True)
        finally:
            if self.server:
                self.server.close()
                await self.server.wait_closed()
            # if os.path.exists(self.socket_path):
            #     os.unlink(self.socket_path)
            self.logger.info(f"UDS receiver for {self.dp_name} has stopped.")

    async def stop(self):
        """Signals the receiver to stop."""
        self.logger.info(f"Stopping UDS receiver for {self.dp_name}.")
        self._stop_event.set()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Callback to handle a new client connection."""
        peer = writer.get_extra_info('peername')
        self.logger.info(f"New connection on {self.dp_name} socket from {peer or 'unknown'}")
        frame_count = 0

# Create a file-like object for pff.read_json
# This is a workaround as pff library expects a file object
class StreamReaderWrapper:
    def __init__(self, stream_reader):
        self._reader = stream_reader
        self._buffer = b''

    async def read(self, n=-1):
        if n == -1:
            return await self._reader.read()

        while len(self._buffer) < n:
            chunk = await self._reader.read(n - len(self._buffer))
            if not chunk:
                raise asyncio.IncompleteReadError(self._buffer, n)
            self._buffer += chunk

        data = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return data

        def readinto(self, b):
            # Placeholder for compatibility if needed
            return 0

        try:
            while not self._stop_event.is_set():
                # The pff functions are blocking, so run them in a thread
                def _blocking_pff_read(data_chunk):
                    f = BytesIO(data_chunk)
                    header_str = pff.read_json(f)
                    header_size = f.tell()
                    return json.loads(header_str) if header_str else None, header_size

                # First, read a chunk to parse the header
                # A reasonable guess for max header size
                initial_chunk = await reader.read(4096)
                if not initial_chunk:
                    break

                header, header_size = await asyncio.to_thread(_blocking_pff_read, initial_chunk)

                if not header:
                    self.logger.warning(f"Could not parse JSON header on {self.dp_name} stream.")
                    continue

                # The rest of the initial chunk is the start of the image
                img_buffer = initial_chunk[header_size:]

                # Read the remaining bytes for the image
                bytes_to_read = self.dp_config.bytes_per_image - len(img_buffer)
                if bytes_to_read > 0:
                    img_buffer += await reader.readexactly(bytes_to_read)

                pano_image = PanoImage(
                    type=self.dp_config.pano_image_type,
                    header=ParseDict(header, Struct()),
                    image_array=img_buffer,
                    shape=self.dp_config.image_shape,
                    bytes_per_pixel=self.dp_config.bytes_per_pixel,
                    file=f"uds_{self.dp_name}",
                    frame_number=frame_count,
                    module_id=self.module_id,
                )

                try:
                    self.upload_queue.put_nowait(pano_image)
                    frame_count += 1
                except asyncio.QueueFull:
                    self.logger.warning(f"Upload queue is full. Dropping frame from UDS {self.dp_name}.")

        except asyncio.IncompleteReadError:
            self.logger.info(f"Client disconnected from {self.dp_name} socket.")
        except asyncio.CancelledError:
            self.logger.info(f"Client handler for {self.dp_name} was cancelled.")
        except Exception as e:
            self.logger.error(f"Error in UDS client handler for {self.dp_name}: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()
            self.logger.info(f"Connection closed on {self.dp_name} socket.")
