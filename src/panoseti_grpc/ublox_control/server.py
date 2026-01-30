#!/usr/bin/env python3
"""
The Python implementation of a gRPC UbloxControl server.

Requires the following to function correctly:
    1. A POSIX-compliant operating system.
    2. A valid connection to a ZED-F9T u-blox chip.
    3. All Python packages specified in requirements.txt.
"""
import os
import stat
import asyncio
import logging
import signal
import re
import uuid
import json
from typing import Dict, Any, Optional
import copy

from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, TXN_NONE
from serial import Serial

import grpc
from grpc_reflection.v1alpha import reflection
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from panoseti_grpc.generated import ublox_control_pb2, ublox_control_pb2_grpc
from .initialize.conf_gnss import (
    _to_cfg_items,
    _layers_mask,
    send_cfg_valset_grouped,
    detect_model,
    build_tmode_fixed_from_json,
    poll_cfg,
    _fmt_val,
    DTYPE_BY_ID,
    get_f9t_unique_id
)
from .resources import make_rich_logger, CFG_DIR, ubx_to_dict, load_package_json


class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """
    Provides async methods that implement the UbloxControl service.
    """

    F9T_MODEL_PREFIX = 'ZED-F9T'

    def __init__(self, server_cfg: Dict[str, Any], logger: logging.Logger):
        self.server_cfg = server_cfg
        self.logger = logger
        self._f9t_cfg: Dict[str, Any] = {}
        self._serial: Optional[Serial] = None
        self._io_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._packet_cache: Dict[str, UBXMessage] = {}
        self._cache_lock = asyncio.Lock()
        self._client_queues: list[asyncio.Queue] = []
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

    async def _open_serial(self, device: str, baud: int, context, timeout=0.5):
        """Opens the serial port."""
        try:
            if not device:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Device path not specified in config.")
            if not isinstance(device, str):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Device path must be a string.")
            if not os.path.exists(device):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Device path {device} does not exist.")
            if not stat.S_ISCHR(os.stat(device).st_mode):
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                    f"Device path {device} is not a character device as expected for a ZED-F9T device file:"
                                    f"{os.stat(device).st_mode=}")
            self._serial = Serial(device, baudrate=baud, timeout=timeout)
            self.logger.info(f"Opened serial port {device} at {baud} baud.")
            self._serial.reset_input_buffer()
            self._serial.reset_output_buffer()
            return True
        except Exception as e:
            self.logger.error(f"Failed to open serial port {device}: {e}")
            return False

    async def _close_serial(self):
        """Closes the serial port."""
        if self._serial and self._serial.is_open:
            self._serial.close()
            self.logger.info("Closed serial port.")

    async def InitF9t(self, request, context):
        """
        Handles InitF9t by configuring the F9T device. Replicates the logic
        from conf_gnss.py's main function for robust configuration and verification.
        """
        self.logger.info(f"New InitF9t RPC from {context.peer()}")
        num_active_clients = len(self._client_queues)
        if not request.force_init and num_active_clients > 0 and self._io_task and not self._io_task.done():
            emsg = (f"Cannot initiate F9T while {num_active_clients} clients are connected. "
                    f"Use force_init=True to force initialization and cancel all active clients.")
            self.logger.warning(emsg)
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, emsg)

        # Stop any active io_task
        await self.stop()
        self._stop_event.clear()

        # Start processing request
        client_f9t_cfg = MessageToDict(
            request.f9t_config,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True
        )

        # Validate the provided F9T device file
        device = client_f9t_cfg.get("device")
        if not await self._open_serial(device, client_f9t_cfg.get("baud", 115200), context):
            device_abs_path = os.path.abspath(device)
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Could not connect to device {device_abs_path}.")

        try:
            # 0. Detect model
            model = await asyncio.to_thread(detect_model, self._serial)
            self.logger.info(f"Detected model: {model}")
            if not model.startswith(self.F9T_MODEL_PREFIX):
                await self._close_serial()
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                    f"Model {model} does not match expected prefix {self.F9T_MODEL_PREFIX}.")
            # 1. Detect F9T uid
            uid = await asyncio.to_thread(get_f9t_unique_id, self._serial)
            if not uid:
                await self._close_serial()
                await context.abort(grpc.StatusCode.INTERNAL, "Could not detect F9T UID.")
            if uid != client_f9t_cfg.get("f9t_uid"):
                emsg = f"Detected F9T UID {uid} does not match client UID {client_f9t_cfg.get('f9t_uid')}."
                self.logger.error(emsg)
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, emsg)
            else:
                self.logger.info(f"Detected F9T UID='{uid}' matches client UID.")

            # 2. Get base configuration items
            cfg_entries = client_f9t_cfg.get("cfg_key_settings", [])
            cfg_items = _to_cfg_items(cfg_entries)

            # 3. Add ZED-F9T specific TMODE settings if applicable
            position_settings = client_f9t_cfg.get("position")
            if model.startswith(self.F9T_MODEL_PREFIX) and position_settings:
                self.logger.info("ZED-F9T detected with position settings. Applying TMODE configuration.")
                tmode_pairs = build_tmode_fixed_from_json(
                    position_settings,
                    position_settings.get("acc_m", 0.05)
                )
                tmode_items = _to_cfg_items([{"key": k, "value": v} for k, v in tmode_pairs])
                cfg_items.extend(tmode_items)
                self.logger.debug(f"Added {len(tmode_items)} TMODE configuration items.")

            # 4. Prepare and send configuration
            layers = client_f9t_cfg.get("apply_to_layers", ["RAM"])
            layers_mask = _layers_mask(layers)
            self.logger.info(f"Sending {len(cfg_items)} configuration items to device.")
            acks = await asyncio.to_thread(send_cfg_valset_grouped, self._serial, cfg_items, layers_mask, verbose=True)
            if not all(acks):
                raise RuntimeError("One or more configuration messages were NAKed.")
            self.logger.info("Configuration sent and acknowledged.")

            # 5. Verify configuration by polling the device
            verify_layer = client_f9t_cfg.get("verify_layer", "RAM").upper()
            self.logger.info(f"Verifying configuration on layer: {verify_layer}")
            key_ids = [it["id"] for it in cfg_items]

            reported_cfg = await asyncio.to_thread(poll_cfg, self._serial, key_ids, verify_layer)

            failures = []
            for item in cfg_items:
                kid = item["id"]
                want = item["value"]
                got = reported_cfg.get(kid)
                if got != want:
                    dtype = DTYPE_BY_ID.get(kid)
                    failure_detail = (
                        f"Key: {item['name']}, "
                        f"Wanted: {_fmt_val(want, dtype)}, "
                        f"Got: {_fmt_val(got, dtype)}"
                    )
                    failures.append(failure_detail)

            if failures:
                error_message = (f"Configuration verification failed for {len(failures)}"
                                 f" items on layer {verify_layer}:\n") + "\n".join(failures)
                self.logger.error(error_message)
                raise RuntimeError(error_message)

            self.logger.info("All settings applied and verified successfully.")
            self._f9t_cfg = client_f9t_cfg  # commit transaction

            # 6. Start the I/O reader loop
            if not self._io_task or self._io_task.done():
                self._main_loop = asyncio.get_running_loop()
                self._io_task = asyncio.create_task(self._reader_loop())

            return ublox_control_pb2.InitF9tResponse(
                init_status=ublox_control_pb2.InitF9tResponse.InitStatus.SUCCESS,
                message="F9T initialized, configured, and verified successfully.",
                f9t_config=ParseDict(self._f9t_cfg, Struct()),
            )

        except Exception as e:
            self.logger.error(f"Error during InitF9t: {e}", exc_info=True)
            await self._close_serial()
            await context.abort(grpc.StatusCode.INTERNAL, f"Initialization failed: {e}")

    def _reader_loop_sync(self):
        """The synchronous part of the reader loop that runs in a separate thread."""
        if not self._main_loop:
            self.logger.error("Reader loop cannot start: main event loop is not available.")
            return
        elif not self._serial:
            self.logger.error("Reader loop cannot start: serial port is not available.")
            return
        ubr = UBXReader(self._serial, protfilter=UBX_PROTOCOL)
        self.logger.info("Starting reader loop.")
        while not self._stop_event.is_set():
            try:
                raw, parsed = ubr.read()
                if parsed:
                    self.logger.debug(f"Received packet: {parsed.identity} ")
                    asyncio.run_coroutine_threadsafe(self._distribute_packet(parsed), self._main_loop)
            except Exception as e:
                # Log non-critical errors without stopping the loop
                if self._serial and self._serial.is_open:
                    self.logger.warning(f"Error in reader loop: {e}", exc_info=False)
                else:  # Stop if serial is closed
                    self.logger.error(f"Serial port closed, stopping reader loop.")
                    break
        self.logger.info("Reader loop stopped.")

    async def _reader_loop(self):
        """Asynchronous wrapper for the reader loop."""
        await asyncio.to_thread(self._reader_loop_sync)

    async def _distribute_packet(self, parsed: UBXMessage):
        """Caches and distributes a parsed packet."""
        async with self._cache_lock:
            self.logger.debug(f"Caching packet: {parsed.identity}")
            self._packet_cache[parsed.identity] = parsed
            for q in self._client_queues:
                try:
                    q.put_nowait(parsed)
                except asyncio.QueueFull:
                    self.logger.warning("Client queue full; dropping packet.")

    async def CaptureUblox(self, request, context):
        """
        Handles CaptureUblox by streaming data to a client.
        1. Broadcasts the current cache state for packets matching client's regex patterns.
        2. Streams new packets in real-time that match the patterns.
        """
        peer = context.peer()
        client_uid = uuid.uuid4()
        self.logger.info(f"New CaptureUblox RPC {client_uid=} from {peer} with patterns: {request.patterns}")

        if not self.is_running():
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "F9T is not initialized or I/O is not running.")

        patterns = [re.compile(p) for p in request.patterns] if request.patterns else [re.compile(".*")]

        q = asyncio.Queue(maxsize=self.server_cfg.get("max_read_queue_size", 200))
        self._client_queues.append(q)
        self.logger.info(f"Client from {peer} subscribed. Total clients: {len(self._client_queues)}")

        try:
            def _create_capture_ublox_response(_packet_name, _parsed_data):
                # unpack parsed UBXMessage class into a dictionary, then serialize
                parsed_data_dict = ubx_to_dict(_parsed_data)
                parsed_data_struct = Struct()
                ParseDict(parsed_data_dict, parsed_data_struct)

                # get timestamp
                timestamp = Timestamp()
                timestamp.GetCurrentTime()
                return ublox_control_pb2.CaptureUbloxResponse(
                    type=ublox_control_pb2.CaptureUbloxResponse.Type.DATA,
                    name=_packet_name,
                    parsed_data=parsed_data_struct,
                    payload=_parsed_data.serialize(),
                    pkt_unix_timestamp=timestamp
                )
            # Broadcast initial cache state
            async with self._cache_lock:
                self.logger.info(f"Broadcasting initial cache to {peer} for matching patterns.")
                for packet_name, parsed_data in self._packet_cache.items():
                    if any(p.match(packet_name) for p in patterns):
                        yield _create_capture_ublox_response(packet_name, parsed_data)

            # Stream new packets
            while not context.cancelled() and self.is_running():
                try:
                    parsed_data = await asyncio.wait_for(q.get(), timeout=1.0)
                    packet_name = parsed_data.identity
                    if any(p.match(packet_name) for p in patterns):
                        yield _create_capture_ublox_response(packet_name, parsed_data)

                except asyncio.TimeoutError:
                    continue
        except grpc.aio.AioRpcError as e:
            self.logger.info(f"Client stream from {peer} ended: {e.details()}")
        except Exception as e:
            self.logger.error(f"Error during CaptureUblox stream for {peer}: {e}", exc_info=True)
        finally:
            if q in self._client_queues:
                self._client_queues.remove(q)
            self.logger.info(f"Client from {peer} unsubscribed. Total clients: {len(self._client_queues)}")

    def is_running(self):
        return self._io_task and not self._io_task.done()

    async def stop(self):
        """Gracefully shuts down the servicer's background tasks."""
        self.logger.info("Initiating graceful shutdown.")

        # Cancel the main I/O task if it exists and is running
        if self._io_task and not self._io_task.done():
            self._io_task.cancel()
            try:
                # Wait for the task to acknowledge cancellation
                await asyncio.wait_for(self._io_task, timeout=2.0)
            except asyncio.CancelledError:
                # This is the expected outcome of cancellation
                self.logger.debug("I/O task successfully cancelled.")
            except asyncio.TimeoutError:
                self.logger.warning("Timeout waiting for I/O task to stop.")

        # Close the serial port if it's open
        if self._serial and self._serial.is_open:
            await self._close_serial()


async def serve(_stop_event=None, in_main_thread=True):
    """Initializes and starts the async gRPC server."""
    logger = make_rich_logger(__name__, level=logging.DEBUG)
    try:
        server_config = load_package_json("panoseti_grpc", CFG_DIR / "ublox_control_server_config.json")
        # with open(, "r") as f:
        #     server_config = json.load(f)
    except FileNotFoundError:
        logger.error("Server config file not found. Exiting.")
        return

    if _stop_event is None:
        _stop_event = asyncio.Event()

    server = grpc.aio.server()
    servicer = UbloxControlServicer(server_config, logger)
    ublox_control_pb2_grpc.add_UbloxControlServicer_to_server(servicer, server)

    SERVICE_NAMES = (
        ublox_control_pb2.DESCRIPTOR.services_by_name["UbloxControl"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:50051")
    await server.start()
    logger.info("UbloxControl async server started. Press CTRL+C to stop.")

    # Set up signal handling for graceful shutdown
    loop = asyncio.get_running_loop()

    def _signal_handler(*_):
        logger.info("Shutdown signal received.")
        if not _stop_event.is_set():
            loop.create_task(shutdown())

    async def shutdown():
        _stop_event.set()
        await servicer.stop()
        await server.stop(grace=server_config.get("shutdown_grace_period", 1.0))
        logger.info("Server shut down gracefully.")

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info("Main serve task cancelled.")
    finally:
        if not _stop_event.is_set():
            await shutdown()


if __name__ == "__main__":
    try:
        stop_event = asyncio.Event()
        asyncio.run(serve(stop_event, in_main_thread=True))
    except KeyboardInterrupt:
        pass
