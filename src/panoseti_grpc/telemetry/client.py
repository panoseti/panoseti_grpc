import os
import json
import logging
import queue
import threading
from queue import Queue, Full
from rich.logging import RichHandler
import traceback

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict
from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc

# Default to "headnode" (Hosts file or DNS name)
DEFAULT_HEADNODE = "headnode.local"
DEFAULT_GRPC_PORT = 50051

class TelemetryClient:
    """
    Client for the PANOSETI Telemetry Service.
    Supports both Strict (Production) and Flexible (Experimental) logging.
    """

    def __init__(self, host=None, grpc_port=None):
        self.host = host or os.getenv("HEADNODE_IP", DEFAULT_HEADNODE)
        self.grpc_port = grpc_port or int(os.getenv("HEADNODE_GRPC_PORT", DEFAULT_GRPC_PORT))

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
                      file_path="", line_number=0, function_name=""):
        """Synchronous wrapper for sending a log (runs in worker thread)."""
        ts = Timestamp()
        if timestamp:
            ts.FromSeconds(int(timestamp))
        else:
            ts.GetCurrentTime()

        req = telemetry_pb2.LogMessage(
            host=self.host,  # Client host, usually localhost or container ID
            service_name=service,
            timestamp=ts,
            severity=severity,
            file_path=file_path,
            line_number=line_number,
            function_name=function_name,
            payload_json=message  # Simple string message for now
        )
        # We use a short timeout. If server is busy, drop the log.
        self.stub.Log(req, timeout=1)


class AsyncGrpcHandler(logging.Handler):
    def __init__(self, grpc_client, service_name, queue_size=1000):
        super().__init__()
        self.grpc_client = grpc_client
        self.service_name = service_name
        self.queue = Queue(maxsize=queue_size)

        # Start the background worker
        self._stop_event = threading.Event()
        self.worker = threading.Thread(target=self._worker, daemon=True)
        self.worker.start()

    def emit(self, record):
        try:
            # 1. Format the message (apply formatter if exists)
            msg = self.format(record)

            # 2. Construct Payload
            # We map Python LogLevels to gRPC Severity (1=DEBUG, 2=INFO...)
            # Python: DEBUG=10, INFO=20... -> (level // 10) roughly works
            severity = int(record.levelno / 10)
            if severity < 1: severity = 1
            if severity > 5: severity = 5

            payload = {
                'msg': msg,
                'level': severity,
                'timestamp': record.created,
                'file_path': record.pathname,
                'line_number': record.lineno,
                'function_name': record.funcName
            }

            # 3. Non-blocking Put
            self.queue.put_nowait(payload)
        except Full:
            # Fail silently to avoid crashing app
            print(f"AsyncGrpcHandler Buffer Full! Dropping log: {record.getMessage()}")
        except Exception:
            self.handleError(record)

    def _worker(self):
        while not self._stop_event.is_set():
            try:
                payload = self.queue.get(timeout=0.5)

                # --- NEW: JSON SERIALIZATION ---
                # The Server expects 'payload_json' to be a valid JSON string.
                # We wrap the message in a JSON string.
                if isinstance(payload['msg'], (dict, list)):
                    json_payload = json.dumps(payload['msg'])
                else:
                    # Even simple strings must be valid JSON values (quoted)
                    # or wrapped in a structure. Let's wrap it for safety.
                    json_payload = json.dumps({"text": str(payload['msg'])})

                self.grpc_client.send_log_sync(
                    service=self.service_name,
                    severity=payload['level'],
                    message=json_payload,  # <--- PASS THE JSON STRING
                    timestamp=payload['timestamp'],
                    file_path=payload['file_path'],
                    line_number=payload['line_number'],
                    function_name=payload['function_name']
                )
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                # If network fails, we just print locally and move on
                # Ideally, we might backoff, but for logs, we prefer dropping over blocking
                print(f"Log Worker Error: {e}")

def make_grpc_logger(
        name: str,
        headnode_ip: str = None,
        grpc_port: int = None,
        queue_size: int = 1000,
        level: int = logging.INFO,
) -> logging.Logger:
    """
    Creates a configured logger using RichHandler and gRPC for log aggregation.
    """
    grpc_client = TelemetryClient(headnode_ip, grpc_port)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(rich_tracebacks=True, markup=True),
            AsyncGrpcHandler(grpc_client, service_name=name, queue_size=queue_size),
        ]
    )
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger