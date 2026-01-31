import grpc
import logging
import asyncio
from google.protobuf.json_format import MessageToDict
from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc
from .config import TelemetryConfig, ValidationError

logger = logging.getLogger("telemetry.server")


class TelemetryServicer(telemetry_pb2_grpc.TelemetryServicer):
    def __init__(self, config_path, redis_client):
        self.config = TelemetryConfig.load(config_path)
        self.redis = redis_client

    def _proto_to_dict(self, message):
        """Helper to safely convert Proto to Dict for Pydantic."""
        return MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True
        )

    async def ReportStatus(self, request, context):
        try:
            # 1. Determine Payload Source
            if request.HasField("gnss"):
                raw_data = self._proto_to_dict(request.gnss)
            elif request.HasField("dew"):
                raw_data = self._proto_to_dict(request.dew)
            elif request.HasField("test"):
                raw_data = self._proto_to_dict(request.test)
            elif request.HasField("flexible"):
                raw_data = self._proto_to_dict(request.flexible)
            else:
                return telemetry_pb2.StatusResponse(success=False, message="No payload provided")

            # 2. Validation & Config Lookup
            try:
                redis_key = self.config.get_redis_key(request.device_type, request.device_id)
                validated_data = self.config.validate_and_flatten(request.device_type, raw_data)
            except (ValueError, ValidationError) as e:
                return telemetry_pb2.StatusResponse(success=False, message=str(e))

            # 3. Add Timestamp
            validated_data['Computer_UTC'] = request.timestamp.ToDatetime().timestamp()

            # 4. Write to Redis
            # Cast all values to strings for Redis
            redis_data = {k: str(v) for k, v in validated_data.items()}

            # Using asyncio.to_thread for the blocking Redis IO
            await asyncio.to_thread(self.redis.hset, redis_key, mapping=redis_data)

            return telemetry_pb2.StatusResponse(success=True)

        except Exception as e:
            logger.exception("Internal Server Error")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))


async def serve(config_path, redis_host='localhost', port=50051):
    server = grpc.aio.server()
    import redis
    # Using decode_responses=True ensures we get strings back from Redis
    r = redis.Redis(host=redis_host, port=6379, decode_responses=True)

    telemetry_pb2_grpc.add_TelemetryServicer_to_server(
        TelemetryServicer(config_path, r), server
    )
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    await server.wait_for_termination()