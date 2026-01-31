#!/usr/bin/env python3
"""
The Python implementation of the PANOSETI Telemetry gRPC Server.
Features:
- Validated Strict Logging (Pydantic)
- Flexible JSON Logging (Redis Hash)
- High-Performance Redis Caching
- Graceful Shutdown & Signal Handling
"""

import asyncio
import signal
import os
import grpc
from pathlib import Path

# gRPC Imports
from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc
from google.protobuf.json_format import MessageToDict

# Local Imports
from .config import TelemetryConfig, ValidationError
from .resources import make_rich_logger, get_config_path

# Create the main logger
logger = make_rich_logger("telemetry_server")


class TelemetryServicer(telemetry_pb2_grpc.TelemetryServicer):
    """
    Implements the Telemetry gRPC service.
    Handles data validation, flattening, and storage into Redis.
    """

    def __init__(self, config_path: Path, redis_client):
        self.config_path = config_path
        self.redis = redis_client
        self._load_config()
        logger.info(f"TelemetryServicer initialized with config: [bold cyan]{self.config_path}[/]")

    def _load_config(self):
        """Loads or reloads the configuration."""
        try:
            self.config = TelemetryConfig.load(str(self.config_path))
        except Exception as e:
            logger.critical(f"Failed to load configuration: {e}")
            raise

    async def shutdown(self):
        """Cleanup resources before server stop."""
        logger.info("Closing Redis connection...")
        await self.redis.aclose()
        logger.info("Redis connection closed.")

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
                msg = "No known payload field provided in request."
                logger.warning(f"Invalid Request: {msg}")
                return telemetry_pb2.StatusResponse(success=False, message=msg)

            # 2. Validation & Config Lookup
            # We defer to the loaded TelemetryConfig for business logic
            try:
                redis_key = self.config.get_redis_key(request.device_type, request.device_id)
                validated_data = self.config.validate_and_flatten(request.device_type, raw_data)
            except (ValueError, ValidationError) as e:
                logger.warning(f"Validation Error for {request.device_type}: {e}")
                return telemetry_pb2.StatusResponse(success=False, message=str(e))

            # 3. Add Timestamp (Server Receipt Time)
            validated_data['Computer_UTC'] = request.timestamp.ToDatetime().timestamp()

            # 4. Write to Redis
            # Cast all values to strings to ensure Redis compatibility
            redis_data = {k: str(v) for k, v in validated_data.items()}

            # Use await directly (Redis-py 4.2+ supports async natively)
            # If using synchronous redis, wrap in asyncio.to_thread
            await self.redis.hset(redis_key, mapping=redis_data)

            logger.debug(f"Stored data for [green]{redis_key}[/]")
            return telemetry_pb2.StatusResponse(success=True)

        except Exception as e:
            logger.exception("Internal Server Error processing ReportStatus")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))


async def serve(redis_host='localhost', redis_port=6379, grpc_port=50051, uds_path=None):
    """
    Main entry point for running the server.
    """
    # 1. Setup Redis (Async)
    import redis.asyncio as redis
    logger.info(f"Connecting to Redis at [bold]{redis_host}:{redis_port}[/]...")

    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        await r.ping()  # Fail fast if Redis is down
        logger.info("Connected to Redis.")
    except Exception as e:
        logger.critical(f"Could not connect to Redis: {e}")
        return

    # 2. Setup gRPC Server
    server = grpc.aio.server()
    config_file = get_config_path()
    servicer = TelemetryServicer(config_file, r)

    telemetry_pb2_grpc.add_TelemetryServicer_to_server(servicer, server)

    # 3. Bind Ports (TCP and Optional UDS)
    # Standard TCP Port
    server.add_insecure_port(f'[::]:{grpc_port}')
    logger.info(f"gRPC Server listening on TCP port [bold]{grpc_port}[/]")

    # Unix Domain Socket (for local speed)
    if uds_path:
        if os.path.exists(uds_path):
            os.unlink(uds_path)  # cleanup stale socket
        server.add_insecure_port(f'unix://{uds_path}')
        logger.info(f"gRPC Server listening on UDS [bold]{uds_path}[/]")

    # 4. Graceful Shutdown Setup
    shutdown_event = asyncio.Event()

    def _handle_signal(*args):
        logger.info("Signal received. Initiating shutdown...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _handle_signal)
    loop.add_signal_handler(signal.SIGTERM, _handle_signal)

    # 5. Start Serving
    await server.start()
    logger.info("Server started. Press Ctrl+C to stop.")

    # Wait until a signal is received
    await shutdown_event.wait()

    # 6. Shutdown Sequence
    logger.info("Stopping gRPC server (allowing 5s grace period)...")
    await server.stop(5)

    logger.info("Cleaning up servicer resources...")
    await servicer.shutdown()

    logger.info("Goodbye.")


if __name__ == "__main__":
    # Environment variables are a good way to configure dockerized apps
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

    try:
        asyncio.run(serve(redis_host=REDIS_HOST, grpc_port=GRPC_PORT))
    except KeyboardInterrupt:
        # This catch is usually redundant due to signal handlers, 
        # but good as a final fallback.
        pass