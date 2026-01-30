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

    async def ReportStatus(self, request, context):
        try:
            # 1. Validation: Check if device is registered
            try:
                redis_key = self.config.get_redis_key(request.device_type, request.device_id)
            except ValueError as e:
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))

            # 2. Validation: Check Data Integrity (Pydantic)
            raw_data = MessageToDict(request.data)
            try:
                validated_data = self.config.validate_payload(request.device_type, raw_data)
            except ValidationError as e:
                error_msg = f"Schema Validation Failed: {e.errors()}"
                logger.warning(f"Bad data from {request.device_id}: {error_msg}")
                # Return success=False so client script crashes/logs explicitly
                return telemetry_pb2.StatusResponse(success=False, message=error_msg)

            # 3. Write to Redis (Async)
            # Inject timestamp
            validated_data['Computer_UTC'] = request.timestamp.ToDatetime().timestamp()

            # Use asyncio.to_thread for the blocking redis call
            # (Or use aioredis if you want pure async)
            await asyncio.to_thread(self.redis.hset, redis_key, mapping={k: str(v) for k, v in validated_data.items()})

            return telemetry_pb2.StatusResponse(success=True)

        except Exception as e:
            logger.error(f"Server error: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, "Internal Server Error")


async def serve(config_path, redis_host='localhost'):
    server = grpc.aio.server()
    # Assuming redis connection is created here
    import redis
    r = redis.Redis(host=redis_host, decode_responses=True)

    telemetry_pb2_grpc.add_TelemetryServicer_to_server(
        TelemetryServicer(config_path, r), server
    )
    server.add_insecure_port('[::]:50051')
    await server.start()
    await server.wait_for_termination()