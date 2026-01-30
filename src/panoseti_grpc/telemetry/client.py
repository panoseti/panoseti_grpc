import grpc
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc


class TelemetryClient:
    def __init__(self, host="localhost", port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = telemetry_pb2_grpc.TelemetryStub(self.channel)

    def log(self, device_type: str, device_id: str, data: dict):
        """
        Logs data to the telemetry server.
        Raises exception if server rejects data (e.g. validation error).
        """
        # Convert dict to Struct
        s = Struct()
        s.update(data)

        # Timestamp
        ts = Timestamp()
        ts.GetCurrentTime()

        req = telemetry_pb2.StatusRequest(
            device_type=device_type,
            device_id=device_id,
            timestamp=ts,
            data=s
        )

        try:
            resp = self.stub.ReportStatus(req)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}")