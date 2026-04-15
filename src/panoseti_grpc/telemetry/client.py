from __future__ import annotations

import json
import logging
import os
import queue
import socket
import threading
from queue import Full, Queue
from typing import Any

import grpc
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from rich.logging import RichHandler

from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc
from panoseti_grpc.telemetry.resources import get_sw_info

# Default to "headnode" (Hosts file or DNS name)
DEFAULT_HEADNODE = "localhost"
DEFAULT_GRPC_PORT = 50051

# Cache hostname and Git info to avoid system calls on every log
HOSTNAME = socket.gethostname()
PID = os.getpid()

_RAW_SW_INFO = get_sw_info()
# Normalize the data for Protobuf (Strings only, no dicts/Nones)
if isinstance(_RAW_SW_INFO, dict):
    CACHED_COMMIT = _RAW_SW_INFO.get("commit", "unknown")
    CACHED_BRANCH = _RAW_SW_INFO.get("branch", "unknown")
else:
    CACHED_COMMIT = "unknown"
    CACHED_BRANCH = "unknown"


class TelemetryClient:
    """
    Client for the PANOSETI Telemetry Service.
    Supports both Strict (Production) and Flexible (Experimental) logging.
    """

    def __init__(self, host: str | None = None, port: int | None = None) -> None:
        """
        Initialize the client connection to the Headnode Telemetry Service server.
        Args:
            host: HEADNOE Hostname or IP address
            port: HEADNOE gRPC Port number
        """
        self.host = host or os.getenv("HEADNODE_IP", DEFAULT_HEADNODE)
        self.grpc_port = port or int(os.getenv("HEADNODE_GRPC_PORT", DEFAULT_GRPC_PORT))
        # --- Define Retry Policy (JSON) ---
        # service_config = {
        #     "methodConfig": [
        #         {
        #             # Apply this policy to the 'Log' method in the 'Telemetry' service
        #             "name": [{"service": "panoseti.telemetry.Telemetry", "method": "Log"}],
        #             "retryPolicy": {
        #                 "maxAttempts": 2,
        #                 "initialBackoff": "0.1s",
        #                 "maxBackoff": "5s",
        #                 "backoffMultiplier": 2,
        #                 "retryableStatusCodes": ["UNAVAILABLE", "UNKNOWN", "DEADLINE_EXCEEDED"]
        #             }
        #         }
        #     ]
        # }
        #
        # # # --- Create Channel with Options ---
        options = [
            # Inject the JSON config
            # ("grpc.service_config", json.dumps(service_config)),
            # Keepalive: Detect broken connections proactively (every 30s)
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 10000),
        ]

        self.target = f"{self.host}:{self.grpc_port}"
        self.channel = grpc.insecure_channel(self.target, options=options)
        self.channel.subscribe(self._on_channel_state_change)
        self.stub = telemetry_pb2_grpc.TelemetryStub(self.channel)

    def _on_channel_state_change(self, connectivity: grpc.ChannelConnectivity) -> None:
        # This runs in a background thread whenever connection state changes
        if connectivity == grpc.ChannelConnectivity.TRANSIENT_FAILURE:
            print(f"⚠️ Telemetry Connection Lost to [{self.target}] - Retrying...")
        elif connectivity == grpc.ChannelConnectivity.READY:
            print(f"✅ Telemetry Connection Active / Restored to [{self.target}]")

    def _get_timestamp(self) -> Timestamp:
        ts = Timestamp()
        ts.GetCurrentTime()
        return ts

    def _send(self, request: telemetry_pb2.StatusRequest) -> None:
        try:
            resp = self.stub.ReportStatus(request)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}") from e

    def log_flexible(self, device_type: str, device_id: str, data: dict[str, Any]) -> None:
        """
        Experimental Mode Logging.

        Use this for R&D, debugging, or prototyping new sensors.
        NOTE: Data logged via this method is subject to TTL (e.g. 24h) and
        will be automatically deleted from the server.
        """
        s = Struct()
        s.update(data)

        req = telemetry_pb2.StatusRequest(
            device_type=device_type, device_id=device_id, timestamp=self._get_timestamp(), flexible=s
        )
        self._send(req)

    def log_test(self, device_id: str, iteration: int, value: float, message: str, active: bool) -> None:
        """
        Strict Mode: Test Payload.
        Used for CI/CD pipeline health checks.
        """
        payload = telemetry_pb2.TestPayload(iteration=iteration, value=value, message=message, active=active)

        req = telemetry_pb2.StatusRequest(
            device_type="test", device_id=device_id, timestamp=self._get_timestamp(), test=payload
        )
        self._send(req)

    def log_strict(self, device_type: str, device_id: str, data: dict[str, Any]) -> None:
        """
        Production Mode Logging.

        Dispatches dictionary to specific Protobuf message types.
        Data logged here is PERMANENT (TTL=0) and STRICTLY VALIDATED.

        Args:
            device_type: "gnss", "dew", etc.
            device_id: Unique hardware identifier.
            data: Dictionary matching the schema defined in config.py.
        """
        req = telemetry_pb2.StatusRequest(device_type=device_type, device_id=device_id, timestamp=self._get_timestamp())

        match device_type:
            case "gnss":
                gnss_payload = telemetry_pb2.GnssPayload()
                ParseDict(data, gnss_payload)
                req.gnss.CopyFrom(gnss_payload)
            case "dew":
                dew_payload = telemetry_pb2.DewPayload()
                ParseDict(data, dew_payload)
                req.dew.CopyFrom(dew_payload)
            case _:
                raise ValueError(
                    f"Unsupported strict device_type: '{device_type}'. "
                    f"Check telemetry_config.toml or use log_flexible() for R&D."
                )

        self._send(req)

    def send_log_future(
        self,
        service: str,
        severity: int,
        message: str,
        timestamp: float | None = None,
        file_path: str = "",
        line_number: int = 0,
        function_name: str = "",
        process_id: int = 0,
        thread_name: str = "",
    ) -> grpc.Future:
        ts = Timestamp()
        if timestamp:
            ts.FromSeconds(int(timestamp))
        else:
            ts.GetCurrentTime()

        req = telemetry_pb2.LogMessage(
            host=HOSTNAME,
            service_name=service,
            timestamp=ts,
            severity=severity,
            file_path=file_path,
            line_number=line_number,
            function_name=function_name,
            process_id=process_id if process_id else PID,
            thread_name=thread_name,
            git_commit=CACHED_COMMIT,
            git_branch=CACHED_BRANCH,
            payload_json=str(message),
        )

        return self.stub.Log.future(req, timeout=10.0, wait_for_ready=True)


class AsyncGrpcHandler(logging.Handler):
    def __init__(self, grpc_client: TelemetryClient | None, service_name: str, queue_size: int = 1000) -> None:
        super().__init__()
        self.grpc_client = grpc_client
        self.service_name = service_name
        self.queue: Queue[dict[str, Any]] = Queue(maxsize=queue_size)

        if grpc_client is not None:
            # Start the background worker
            self._stop_event = threading.Event()
            self.worker = threading.Thread(target=self._worker, daemon=True, name="LogShipper")
            self.worker.start()
        else:
            print("LogShipper is disabled because grpc_client is not available.")

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            # Python's LogRecord already captures these!

            severity = int(record.levelno / 10)
            if severity < 1:
                severity = 1
            if severity > 5:
                severity = 5

            payload = {
                "msg": msg,
                "level": severity,
                "timestamp": record.created,
                "file_path": record.pathname,
                "line_number": record.lineno,
                "function_name": record.funcName,
                "process": record.process,  # PID
                "thread": record.threadName,  # Thread Name
            }
            self.queue.put_nowait(payload)
        except Full:
            pass  # Dropping logs is better than crashing observations
        except Exception:
            self.handleError(record)

    def _worker(self) -> None:
        while hasattr(self, "_stop_event") and not self._stop_event.is_set():
            try:
                # 1. Get from queue (Blocking wait for new logs)
                payload = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if self.grpc_client is None:
                    continue

                # Unwrap and Enrich JSON
                raw_msg = payload["msg"]
                final_json_str = ""

                # Check if the message is already JSON
                is_json = False
                if isinstance(raw_msg, str) and raw_msg.strip().startswith("{"):
                    try:
                        # Validate it's actually JSON
                        json_obj = json.loads(raw_msg)
                        # Inject Metadata into the JSON payload itself for easier querying in Grafana
                        json_obj["_meta"] = {"pid": payload["process"], "thread": payload["thread"]}
                        final_json_str = json.dumps(json_obj)
                        is_json = True
                    except json.JSONDecodeError:
                        pass

                if not is_json:
                    # Wrap text and add metadata
                    final_json_str = json.dumps(
                        {"text": str(raw_msg), "_meta": {"pid": payload["process"], "thread": payload["thread"]}}
                    )

                future = self.grpc_client.send_log_future(
                    service=self.service_name,
                    severity=payload["level"],
                    message=final_json_str,
                    timestamp=payload["timestamp"],
                    file_path=payload["file_path"],
                    line_number=payload["line_number"],
                    function_name=payload["function_name"],
                )
                # 3. Attach a callback to handle success/failure in the background
                # This ensures we don't block THIS thread waiting for the result.
                future.add_done_callback(self._on_rpc_done)

            except Exception as e:
                # Only happens if local object creation fails
                print(f"Local Grpc Submit Error: {e}")

            self.queue.task_done()

    def _on_rpc_done(self, future: grpc.Future) -> None:
        """
        Called by gRPC background thread when the request finishes (or times out).
        """
        try:
            future.result()  # Will raise exception if RPC failed
        except grpc.RpcError:
            # Handle specific errors if needed, or just suppress typical "server down" noise
            # status_code = e.code()
            # if status_code != grpc.StatusCode.CANCELLED:
            #     print(f"{status_code}: {e.details()}")
            pass

    def close(self) -> None:
        """
        Stops the worker thread.
        NOTE: We generally do NOT close the self.client here because it might
        be shared by other loggers. The Client's channel cleans up on process exit.
        """
        if hasattr(self, "_stop_event"):
            self._stop_event.set()
        if hasattr(self, "worker") and self.worker.is_alive():
            self.worker.join(timeout=1.0)
        super().close()


def make_grpc_logger(
    service_name: str,
    grpc_client: TelemetryClient | None = None,
    queue_size: int = 1000,
    level: int = logging.INFO,
    attach_to_root: bool = False,
    add_console_handler: bool = False,
) -> logging.Logger:
    """
    Configures the root logger to send data to:
    1. The Console (Rich pretty print)
    2. The Telemetry Service (Async gRPC)

    Args:
        service_name (str): The service name
        grpc_client (TelemetryClient, optional): The gRPC client
        queue_size (int, optional): The queue size
        level (int, optional): The logging level
        attach_to_root (bool, optional): If True, adds the gRPC handler to the ROOT logger.
                           This captures logs from ALL libraries and other modules.
                           If False, only captures logs for 'service_name'.
        add_console_handler (bool, optional): If True, adds a rich console logger.

    Important: define the following environment variables to enable this logger to auto create the grpc connection.
        - HEADNODE_IP
        - HEADNODE_GRPC_PORT

    Usage:
        import logging
        from client import setup_panoseti_logging

        logger = setup_panoseti_logging("Quabo_Control")
        logger.info("System Ready")
    """
    # 0. Setup targets (Root or Isolated)
    if attach_to_root:
        target_logger = logging.getLogger()  # Root
    else:
        target_logger = logging.getLogger(service_name)
        target_logger.propagate = False
    target_logger.setLevel(level)

    # 1. Connect to the Telemetry Service
    if grpc_client is None:
        try:
            grpc_client = TelemetryClient()
        except Exception as e:
            logging.exception(e)

    # 2. Create the gRPC Handler (Always do this)
    grpc_handler = AsyncGrpcHandler(grpc_client, service_name, queue_size=queue_size)

    # 3. Attach gRPC Handler (Idempotent check)
    if not any(isinstance(h, AsyncGrpcHandler) for h in target_logger.handlers):
        target_logger.addHandler(grpc_handler)

    # 4. Attach Console Handler (ONLY if requested)
    if add_console_handler:
        # Check for existing RichHandler to avoid duplicates
        if not any(isinstance(h, RichHandler) for h in target_logger.handlers):
            console = RichHandler(rich_tracebacks=True, markup=True)
            target_logger.addHandler(console)

    return logging.getLogger(service_name)
