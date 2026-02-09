import os
import json
import logging
import queue
import threading
import socket
from queue import Queue, Full
from rich.logging import RichHandler
import traceback

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict

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
    CACHED_COMMIT = _RAW_SW_INFO.get('commit', 'unknown')
    CACHED_BRANCH = _RAW_SW_INFO.get('branch', 'unknown')
else:
    CACHED_COMMIT = "unknown"
    CACHED_BRANCH = "unknown"



class TelemetryClient:
    """
    Client for the PANOSETI Telemetry Service.
    Supports both Strict (Production) and Flexible (Experimental) logging.
    """

    def __init__(self, host=None, port=None):
        """
        Initialize the client connection to the Headnode Telemetry Service server.
        Args:
            host: HEADNOE Hostname or IP address
            port: HEADNOE gRPC Port number
        """
        self.host = host or os.getenv("HEADNODE_IP", DEFAULT_HEADNODE)
        self.grpc_port = port or int(os.getenv("HEADNODE_GRPC_PORT", DEFAULT_GRPC_PORT))
        self.channel = grpc.insecure_channel(f'{self.host}:{self.grpc_port}')
        self.stub = telemetry_pb2_grpc.TelemetryStub(self.channel)

    def _get_timestamp(self):
        ts = Timestamp()
        ts.GetCurrentTime()
        return ts

    def _send(self, request):
        try:
            resp = self.stub.ReportStatus(request)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}")

    def log_flexible(self, device_type: str, device_id: str, data: dict):
        """
        Experimental Mode Logging.

        Use this for R&D, debugging, or prototyping new sensors.
        NOTE: Data logged via this method is subject to TTL (e.g. 24h) and
        will be automatically deleted from the server.
        """
        s = Struct()
        s.update(data)

        req = telemetry_pb2.StatusRequest(
            device_type=device_type,
            device_id=device_id,
            timestamp=self._get_timestamp(),
            flexible=s
        )
        self._send(req)

    def log_test(self, device_id: str, iteration: int, value: float, message: str, active: bool):
        """
        Strict Mode: Test Payload.
        Used for CI/CD pipeline health checks.
        """
        payload = telemetry_pb2.TestPayload(
            iteration=iteration,
            value=value,
            message=message,
            active=active
        )

        req = telemetry_pb2.StatusRequest(
            device_type="test",
            device_id=device_id,
            timestamp=self._get_timestamp(),
            test=payload
        )
        self._send(req)

    def log_strict(self, device_type: str, device_id: str, data: dict):
        """
        Production Mode Logging.

        Dispatches dictionary to specific Protobuf message types.
        Data logged here is PERMANENT (TTL=0) and STRICTLY VALIDATED.

        Args:
            device_type: "gnss", "dew", etc.
            device_id: Unique hardware identifier.
            data: Dictionary matching the schema defined in config.py.
        """
        req = telemetry_pb2.StatusRequest(
            device_type=device_type,
            device_id=device_id,
            timestamp=self._get_timestamp()
        )

        if device_type == "gnss":
            payload = telemetry_pb2.GnssPayload()
            ParseDict(data, payload)
            req.gnss.CopyFrom(payload)

        elif device_type == "dew":
            payload = telemetry_pb2.DewPayload()
            ParseDict(data, payload)
            req.dew.CopyFrom(payload)

        else:
            raise ValueError(
                f"Unsupported strict device_type: '{device_type}'. "
                f"Check telemetry_config.toml or use log_flexible() for R&D."
            )

        self._send(req)

    def send_log_sync(self, service, severity, message, timestamp=None,
                      file_path="", line_number=0, function_name="",
                      thread_name=""):
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
            process_id=PID,
            thread_name=thread_name,
            git_commit=CACHED_COMMIT,
            git_branch=CACHED_BRANCH,
            payload_json=str(message),
        )

        self.stub.Log(req, timeout=1.0)


class AsyncGrpcHandler(logging.Handler):
    def __init__(self, grpc_client, service_name, queue_size=1000):
        super().__init__()
        self.grpc_client = grpc_client
        self.service_name = service_name
        self.queue = Queue(maxsize=queue_size)

        # Start the background worker
        self._stop_event = threading.Event()
        self.worker = threading.Thread(target=self._worker, daemon=True, name="LogShipper")
        self.worker.start()

    def emit(self, record):
        try:
            msg = self.format(record)

            # --- METADATA ENRICHMENT ---
            # Python's LogRecord already captures these!

            severity = int(record.levelno / 10)
            if severity < 1: severity = 1
            if severity > 5: severity = 5

            payload = {
                'msg': msg,
                'level': severity,
                'timestamp': record.created,
                'file_path': record.pathname,
                'line_number': record.lineno,
                'function_name': record.funcName,
                'process': record.process,  # PID
                'thread': record.threadName  # Thread Name
            }
            self.queue.put_nowait(payload)
        except Full:
            pass  # Dropping logs is better than crashing observations
        except Exception:
            self.handleError(record)

    def _worker(self):
        while not self._stop_event.is_set():
            try:
                payload = self.queue.get(timeout=0.5)

                # Unwrap and Enrich JSON
                raw_msg = payload['msg']
                final_json_str = ""

                # Check if the message is already JSON
                is_json = False
                if isinstance(raw_msg, str) and raw_msg.strip().startswith('{'):
                    try:
                        # Validate it's actually JSON
                        json_obj = json.loads(raw_msg)
                        # Inject Metadata into the JSON payload itself for easier querying in Grafana
                        json_obj['_meta'] = {
                            "pid": payload['process'],
                            "thread": payload['thread']
                        }
                        final_json_str = json.dumps(json_obj)
                        is_json = True
                    except json.JSONDecodeError:
                        pass

                if not is_json:
                    # Wrap text and add metadata
                    final_json_str = json.dumps({
                        "text": str(raw_msg),
                        "_meta": {
                            "pid": payload['process'],
                            "thread": payload['thread']
                        }
                    })

                self.grpc_client.send_log_sync(
                    service=self.service_name,
                    severity=payload['level'],
                    message=final_json_str,
                    timestamp=payload['timestamp'],
                    file_path=payload['file_path'],
                    line_number=payload['line_number'],
                    function_name=payload['function_name']
                )
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                # Use sys.stderr to avoid recursive logging loops if we used logger
                import sys
                print(f"Log Worker Error: {e}", file=sys.stderr)

def make_grpc_logger(
        service_name: str,
        grpc_client: TelemetryClient = None,
        queue_size: int = 1000,
        level: int = logging.INFO,
        attach_to_root: bool = False,
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

    Important: define the following environment variables to enable this logger to auto create the grpc connection.
        - HEADNODE_IP
        - HEADNODE_GRPC_PORT

    Usage:
        import logging
        from client import setup_panoseti_logging

        logger = setup_panoseti_logging("Quabo_Control")
        logger.info("System Ready")
    """
    if grpc_client is None:
        grpc_client = TelemetryClient()

        # 1. Create the Handler
    grpc_handler = AsyncGrpcHandler(grpc_client, service_name)

    # 2. Rich Console Handler (Visuals)
    # We allow this to exist alongside file handlers
    console = RichHandler(rich_tracebacks=True, markup=True)

    if attach_to_root:
        # Get Root Logger
        root = logging.getLogger()
        root.setLevel(level)

        # Add our handlers without removing existing FileHandlers
        # Check if we already added them to avoid duplicates
        if not any(isinstance(h, AsyncGrpcHandler) for h in root.handlers):
            root.addHandler(grpc_handler)
        if not any(isinstance(h, RichHandler) for h in root.handlers):
            root.addHandler(console)

        # Return the specific service logger, but it will propagate up to root
        return logging.getLogger(service_name)
    else:
        # Isolated Logger
        logger = logging.getLogger(service_name)
        logger.setLevel(level)
        logger.addHandler(grpc_handler)
        logger.addHandler(console)
        logger.propagate = False  # Do not bubble up to avoid double logging if root has handlers
        return logger