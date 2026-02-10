#!/usr/bin/env python3
"""
The Python implementation of the PANOSETI Telemetry gRPC Server.
Features:
- Validated Strict Logging (Production)
- Flexible JSON Logging (Experimental with TTL)
- High-Performance Redis Caching
- Graceful Shutdown & Signal Handling
- Descriptive Error Reporting
"""

import time
import asyncio
import signal
import os
import grpc
from pathlib import Path

# gRPC Imports
from panoseti_grpc.generated import telemetry_pb2, telemetry_pb2_grpc
from google.protobuf.json_format import MessageToDict

# Local Imports
from .config import TelemetryConfig, LogSchema
from .resources import make_rich_logger, get_config_path

# Create the main logger
logger = make_rich_logger("telemetry_server")

# --- Configuration ---
BATCH_SIZE = 100         # Max items to send in one Redis command
BATCH_INTERVAL = 0.5     # Max time to wait before flushing (seconds)
LOG_REDIS_KEY = "logs:ingress"

class RedisBatcher:
    """
    Accumulates logs in memory and flushes them to Redis in bulk.
    This turns N network calls into 1.
    """

    def __init__(self, redis_client):
        self.redis = redis_client
        self.queue = asyncio.Queue()
        self._shutdown = False
        self._worker_task = asyncio.create_task(self._worker())

    async def add(self, log_json: str):
        """Adds a serialized log to the batch queue."""
        await self.queue.put(log_json)

    async def _worker(self):
        """Background loop that flushes the queue to Redis."""
        while not self._shutdown or not self.queue.empty():
            try:
                # Wait for the first item
                logs = [await self.queue.get()]

                # Drain the rest of the queue up to BATCH_SIZE (instant grab)
                # This grabs everything currently available in RAM
                while not self.queue.empty() and len(logs) < BATCH_SIZE:
                    logs.append(self.queue.get_nowait())

                # Bulk Push to Redis (One Network Call)
                if logs:
                    await self.redis.rpush(LOG_REDIS_KEY, *logs)

                    # Mark tasks as done for the queue
                    for _ in logs:
                        self.queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Redis Batcher Error: {e}")
                # Backoff slightly on error to prevent CPU spin
                await asyncio.sleep(1)

    async def shutdown(self):
        """Gracefully flushes remaining logs and stops worker."""
        self._shutdown = True
        # Wait for queue to empty
        if not self.queue.empty():
            logger.info(f"Flushing {self.queue.qsize()} remaining logs...")
            await self.queue.join()
        self._worker_task.cancel()
        try:
            await self._worker_task
        except asyncio.CancelledError:
            pass

class TelemetryServicer(telemetry_pb2_grpc.TelemetryServicer):
    """
    Implements the Telemetry gRPC service.
    Handles data validation, flattening, and storage into Redis.
    """

    def __init__(self, config_path: Path, redis_client):
        self.config_path = config_path
        self.redis = redis_client
        self._load_config()
        # Initialize the Batcher
        self.batcher = RedisBatcher(redis_client)
        logger.info(f"TelemetryServicer initialized with config: [bold cyan]{self.config_path}[/]")
        logger.info(f"[bold green]Telemetry Server Online[/]", extra={"markup": True})

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

    async def Log(self, request, context) -> telemetry_pb2.StatusResponse:
        """
        High-throughput endpoint for Logging.
        Validates schema and queues for Redis batch writing.
        """
        try:
            # 1. Extract Data
            # We map protobuf fields to our Pydantic Schema
            log_data = {
                "host": request.host,
                "service_name": request.service_name,
                "timestamp": request.timestamp.seconds + request.timestamp.nanos / 1e9,
                "severity": request.severity,
                "file_path": request.file_path,
                "line_number": request.line_number,
                "function_name": request.function_name,
                "process_id": request.process_id if request.process_id != 0 else None,
                "thread_name": request.thread_name,
                "git_commit": request.git_commit,
                "git_branch": request.git_branch,
                "payload_json": request.payload_json
            }

            # 2. Validate (Pydantic)
            # This is the CPU-bound part. Pydantic is fast, but it happens inline.
            validated = LogSchema(**log_data)

            # 3. Serialize & Queue (Async)
            # model_dump_json is faster than json.dumps(model_dump())
            json_str = validated.model_dump_json()

            # Fire and Forget (Wait only for memory queue insertion, not Redis write)
            await self.batcher.add(json_str)

            return telemetry_pb2.StatusResponse(success=True, message="Queued")

        except Exception as e:
            # We log the error locally so the server admin sees it
            logger.warning(f"Log Ingest Rejected: {e}")
            return telemetry_pb2.StatusResponse(success=False, message=str(e))


    async def ReportStatus(self, request, context):
        start_time = time.perf_counter()

        # Metadata for logging
        device_type = "unknown"
        device_id = "unknown"
        payload_source = "unknown"
        payload_size = request.ByteSize()

        try:
            # 1. Determine Payload Source & Type
            if request.HasField("gnss"):
                payload_source = "gnss"
                raw_data = self._proto_to_dict(request.gnss)
            elif request.HasField("dew"):
                payload_source = "dew"
                raw_data = self._proto_to_dict(request.dew)
            elif request.HasField("test"):
                payload_source = "test"
                raw_data = self._proto_to_dict(request.test)
            elif request.HasField("flexible"):
                payload_source = "flexible"
                raw_data = self._proto_to_dict(request.flexible)
            else:
                msg = "Invalid Request: No payload field provided (gnss, dew, flexible, etc)."
                logger.warning(msg)
                return telemetry_pb2.StatusResponse(success=False, message=msg)

            # Update identifiers
            device_id = request.device_id or raw_data.get("device_id", "N/A")
            if request.device_type:
                device_type = request.device_type

            # --- DIAGNOSTICS & WARNINGS ---
            # Check for configuration mismatches before processing
            if device_type not in self.config.devices:
                # Warning for unregistered devices (Sandbox Flow)
                logger.warning(
                    f"[bold yellow]Unregistered Type:[/bold yellow] '{device_type}' not found in TOML. "
                    f"Routing to SANDBOX (TTL=1h). Check `telemetry_config.toml`.",
                    extra={"markup": True}
                )
            else:
                # Check for Mode vs Payload Mismatches
                mode = self.config.devices[device_type].mode
                if mode == "production" and payload_source == "flexible":
                    logger.warning(
                        f"[bold orange3]Protocol Mismatch:[/bold orange3] Production device '{device_type}' "
                        f"sent via 'log_flexible'. Schema will be STRICTLY enforced.",
                        extra={"markup": True}
                    )

            # 2. Validation & Config Lookup
            try:
                redis_key = self.config.get_redis_key(device_type, device_id)
                validated_data = self.config.validate_and_flatten(device_type, raw_data)
            except (ValueError, Exception) as e:
                err_str = str(e)

                # Make Pydantic errors human-readable
                if "Field required" in err_str:
                    friendly_msg = f"Missing Required Fields for '{device_type}'. {err_str}"
                elif "Input should be" in err_str:
                    friendly_msg = f"Invalid Data Types for '{device_type}'. {err_str}"
                elif "Schema Violation" in err_str:
                    friendly_msg = f"Strict Schema Violation for '{device_type}': {err_str}"
                else:
                    friendly_msg = f"Validation Error: {err_str}"

                logger.error(f"[bold red]REJECTED:[/bold red] {friendly_msg} (ID: {device_id})", extra={"markup": True})
                return telemetry_pb2.StatusResponse(success=False, message=friendly_msg)

            # 3. Add Timestamp (Server Receipt Time)
            validated_data['Computer_UTC'] = time.time()

            # 4. Write to Redis (Async)
            # Cast all values to strings to ensure Redis compatibility
            redis_data = {k: str(v) for k, v in validated_data.items()}

            async with self.redis.pipeline() as pipe:
                pipe.hset(redis_key, mapping=redis_data)

                # 5. LIFETIME MANAGEMENT
                ttl = self.config.get_ttl(device_type)
                if ttl > 0:
                    pipe.expire(redis_key, ttl)
                else:
                    pipe.persist(redis_key)

                await pipe.execute()

            # Observability
            duration_ms = (time.perf_counter() - start_time) * 1000

            # Debug Log (Silent unless verbose)
            logger.debug(
                f"Stored [bold cyan]{device_type}[/] for [yellow]{device_id}[/] "
                f"({payload_size}b) in {duration_ms:.2f}ms [dim](TTL: {ttl}s)[/]",
                extra={"markup": True}
            )

            return telemetry_pb2.StatusResponse(success=True)

        except Exception as e:
            logger.exception("Internal Server Error processing ReportStatus")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))


async def serve(redis_host='localhost', redis_port=6379, port=50051, uds_path=None, config_path=None):
    """
    Main entry point for running the server.
    Args:
        config_path (str/Path): Explicit path to telemetry_config.toml.
                                If None, falls back to library default.
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

    # LOGIC CHANGE: specific path takes precedence over internal default
    if config_path:
        final_config_path = Path(config_path)
        if not final_config_path.exists():
            logger.warning(f"Provided config {final_config_path} not found. Falling back to default.")
            final_config_path = get_config_path()  # Internal fallback
    else:
        final_config_path = get_config_path()  # Internal fallback

    # Inject the path into the Servicer
    servicer = TelemetryServicer(final_config_path, r)

    telemetry_pb2_grpc.add_TelemetryServicer_to_server(servicer, server)

    # 3. Bind Ports (TCP and Optional UDS)
    server.add_insecure_port(f'[::]:{port}')
    logger.info(f"gRPC Server listening on TCP port [bold]{port}[/]")

    if uds_path:
        if os.path.exists(uds_path):
            os.unlink(uds_path)
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

    await shutdown_event.wait()

    # 6. Shutdown Sequence
    logger.info("Stopping gRPC server (allowing 5s grace period)...")
    await server.stop(5)

    logger.info("Cleaning up servicer resources...")
    await servicer.shutdown()

    logger.info("Goodbye.")


if __name__ == "__main__":
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

    try:
        asyncio.run(serve(redis_host=REDIS_HOST, port=GRPC_PORT))
    except KeyboardInterrupt:
        pass