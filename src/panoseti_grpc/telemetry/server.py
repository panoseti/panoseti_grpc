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
            # 1. Determine Payload Source
            if request.HasField("gnss"):
                raw_data = MessageToDict(request.gnss)
            elif request.HasField("dew"):
                raw_data = MessageToDict(request.dew)
            elif request.HasField("test"):
                raw_data = MessageToDict(request.test)
            elif request.HasField("flexible"):
                raw_data = MessageToDict(request.flexible)
            else:
                return telemetry_pb2.StatusResponse(success=False, message="No payload provided")

            # 2. Validation & Config Lookup
            try:
                redis_key = self.config.get_redis_key(request.device_type, request.device_id)
                validated_data = self.config.validate_and_flatten(request.device_type, raw_data)
            except (ValueError, ValidationError) as e:
                # Differentiate user error from server error
                return telemetry_pb2.StatusResponse(success=False, message=str(e))

            # 3. Add Timestamp
            validated_data['Computer_UTC'] = request.timestamp.ToDatetime().timestamp()

            # 4. Redis Write (Ensure unambigous types for storeInfluxDB)
            # We cast bools to int (0/1) or specific strings because standard Python
            # bool stringification ("True") can be ambiguous if not handled perfectly.
            redis_data = {}
            for k, v in validated_data.items():
                if isinstance(v, bool):
                    redis_data[k] = 1 if v else 0
                else:
                    redis_data[k] = str(v)

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