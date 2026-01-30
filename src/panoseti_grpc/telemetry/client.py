import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict
from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc


class TelemetryClient:
    def __init__(self, host="localhost", port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
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
        R&D Mode: Wraps data in the 'flexible' Struct field.
        """
        s = Struct()
        s.update(data)

        req = telemetry_pb2.StatusRequest(
            device_type=device_type,
            device_id=device_id,
            timestamp=self._get_timestamp(),
            flexible=s  # Correct field for oneof payload
        )
        self._send(req)

    def log_test(self, device_id: str, iteration: int, value: float, message: str, active: bool):
        """
        Test Mode: Wraps data in the 'TestPayload' message.
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

    def log(self, device_type: str, device_id: str, data: dict):
        """
        Strict Mode: Dispatches dictionary to specific Protobuf message types.
        """
        req = telemetry_pb2.StatusRequest(
            device_type=device_type,
            device_id=device_id,
            timestamp=self._get_timestamp()
        )

        if device_type == "gnss":
            # Automatically converts dict (including nested 'extra_data') into GnssPayload
            payload = telemetry_pb2.GnssPayload()
            ParseDict(data, payload)
            req.gnss.CopyFrom(payload)

        elif device_type == "dew":
            payload = telemetry_pb2.DewPayload()
            ParseDict(data, payload)
            req.dew.CopyFrom(payload)

        else:
            raise ValueError(
                f"Unsupported strict device_type: '{device_type}'. Use log_flexible() for unstructured data.")

        self._send(req)