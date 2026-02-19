"""
PANOSETI Unified Logging Module.

This module provides a thread-safe factory for creating loggers that can dispatch
logs to three destinations simultaneously:
1. Console (via Rich)
2. Filesystem (Rotating File Handler)
3. Telemetry Service (via gRPC)

Usage:
    from panoseti_grpc.logging import get_logger

    # Simple
    logger = get_logger("DAQ_Controller")
    logger.info("Starting up...")

    # Advanced
    logger = get_logger(
        "Lookout",
        console=True,
        log_dir="/var/log/panoseti",
        grpc_enabled=True
    )
"""
import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import Optional, Dict, Union
from logging.handlers import RotatingFileHandler

from pydantic import BaseModel, Field, field_validator
from rich.logging import RichHandler

# Import your existing client components
from panoseti_grpc.telemetry.client import TelemetryClient, AsyncGrpcHandler


# --- Configuration Models ---

class GrpcLogConfig(BaseModel):
    """Configuration for remote gRPC logging."""
    enabled: bool = True
    host: str = Field(default_factory=lambda: os.getenv("HEADNODE_IP", "localhost"))
    port: int = Field(default_factory=lambda: int(os.getenv("HEADNODE_GRPC_PORT", 50051)))
    fail_fast: bool = False  # If True, raises ConnectionError on startup if server is down.


class FileLogConfig(BaseModel):
    """Configuration for local filesystem logging."""
    enabled: bool = True
    directory: Path = Path("/var/log/panoseti")
    rotation_size_mb: int = 50
    backup_count: int = 5

    @field_validator("directory")
    @classmethod
    def validate_directory(cls, v: Path) -> Path:
        """Ensures the log directory exists or falls back to a temporary location."""
        try:
            v.mkdir(parents=True, exist_ok=True)
            # Test write permission
            test_file = v / ".perm_test"
            test_file.touch()
            test_file.unlink()
        except (PermissionError, OSError) as e:
            import tempfile
            fallback = Path(tempfile.gettempdir()) / "panoseti_logs"
            fallback.mkdir(parents=True, exist_ok=True)
            print(f"⚠️ Log directory '{v}' is not writable ({e}). Falling back to '{fallback}'", file=sys.stderr)
            return fallback
        return v


class LoggerConfig(BaseModel):
    """Master configuration for a specific logger instance."""
    service_name: str
    level: str = "INFO"
    console: bool = True
    file: FileLogConfig = Field(default_factory=FileLogConfig)
    grpc: GrpcLogConfig = Field(default_factory=GrpcLogConfig)


# --- Singleton Factory ---

class PanosetiLogFactory:
    """
    Manages logger instances and the shared gRPC connection.
    Implements the Singleton pattern for the gRPC client to prevent resource exhaustion.
    """
    _shared_grpc_client: Optional[TelemetryClient] = None
    _loggers: Dict[str, logging.Logger] = {}

    @classmethod
    def _get_grpc_client(cls, config: GrpcLogConfig) -> Optional[TelemetryClient]:
        """Lazy-loads the shared gRPC client."""
        if not config.enabled:
            return None

        if cls._shared_grpc_client is None:
            try:
                cls._shared_grpc_client = TelemetryClient(
                    host=config.host,
                    port=config.port
                )
                if config.fail_fast:
                    cls._shared_grpc_client.check_connection(timeout=1.0)
            except Exception as e:
                print(f"⚠️ Telemetry Service Unreachable: {e}. gRPC logging disabled for this process.",
                      file=sys.stderr)
                # We return None so the app continues running without remote logging
                return None

        return cls._shared_grpc_client

    @classmethod
    def configure_logger(cls, config: LoggerConfig) -> logging.Logger:
        """Configures and returns a standard Python Logger with attached handlers."""

        # 1. Return existing if configured
        if config.service_name in cls._loggers:
            return cls._loggers[config.service_name]

        logger = logging.getLogger(config.service_name)
        logger.setLevel(getattr(logging, config.level.upper()))
        logger.propagate = False  # Prevent double logging to root
        logger.handlers = []  # Reset handlers to ensure clean state

        # 2. Console Handler (Rich)
        if config.console:
            console_handler = RichHandler(
                rich_tracebacks=True,
                markup=True,
                show_path=False  # We handle path in metadata
            )
            console_handler.setLevel(logger.level)
            logger.addHandler(console_handler)

        # 3. File Handler (Rotation)
        if config.file.enabled:
            safe_name = config.service_name.lower().replace(" ", "_").replace(".", "_")
            log_path = config.file.directory / f"{safe_name}.log"

            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=config.file.rotation_size_mb * 1024 * 1024,
                backupCount=config.file.backup_count,
                encoding="utf-8"
            )
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # 4. gRPC Handler (Async)
        client = cls._get_grpc_client(config.grpc)
        if client:
            grpc_handler = AsyncGrpcHandler(
                grpc_client=client,
                service_name=config.service_name
            )
            grpc_handler.setLevel(logger.level)
            logger.addHandler(grpc_handler)

        cls._loggers[config.service_name] = logger
        return logger


# --- Public API ---

def get_logger(
        service_name: str,
        level: str = "INFO",
        console: bool = True,
        log_dir: Optional[str] = "/var/log/panoseti",
        grpc_enabled: bool = True
) -> logging.Logger:
    """
    Main entry point for obtaining a configured PANOSETI logger.

    Args:
        service_name: Unique identifier for the service (e.g. "Quabo_Ctrl").
        level: Logging level ("DEBUG", "INFO", "WARNING", "ERROR").
        console: Whether to print logs to stdout via Rich.
        log_dir: Directory for local log files. Set None to disable file logging.
        grpc_enabled: Whether to send logs to the Telemetry Service.

    Returns:
        A logging.Logger instance ready to use.
    """
    # Build configuration object dynamically
    file_config = FileLogConfig(enabled=False)
    if log_dir:
        file_config = FileLogConfig(enabled=True, directory=Path(log_dir))

    grpc_config = GrpcLogConfig(enabled=grpc_enabled)

    config = LoggerConfig(
        service_name=service_name,
        level=level,
        console=console,
        file=file_config,
        grpc=grpc_config
    )

    return PanosetiLogFactory.configure_logger(config)


# --- Subprocess Utilities ---

async def _stream_reader(stream: asyncio.StreamReader, logger_method):
    """Internal utility to bridge a stream to a logger method."""
    while True:
        line = await stream.readline()
        if not line:
            break
        # Replace errors to prevent crashes on binary garbage output
        message = line.decode('utf-8', errors='replace').strip()
        if message:
            logger_method(message)


async def monitor_subprocess(process: asyncio.subprocess.Process, logger: logging.Logger):
    """
    Attaches a logger to a running asyncio subprocess's stdout/stderr.

    Args:
        process: The process object returned by asyncio.create_subprocess_exec
        logger: The logger instance to pipe output to.
    """
    if process.stdout is None or process.stderr is None:
        logger.warning("Subprocess created without piped streams (stdout/stderr). Cannot capture logs.")
        return

    await asyncio.gather(
        _stream_reader(process.stdout, logger.info),
        _stream_reader(process.stderr, logger.error)
    )