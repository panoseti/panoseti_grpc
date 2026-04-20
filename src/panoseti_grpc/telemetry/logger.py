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

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import os
import sys
import threading
from collections.abc import Callable
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator
from rich.logging import RichHandler

# Import your existing client components
from panoseti_grpc.telemetry.client import AsyncGrpcHandler, TelemetryClient

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
    max_bytes: int = 10 * 1024 * 1024  # 10 MB
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
    service_name: str
    level: int | str = logging.INFO
    console: bool = True
    file: FileLogConfig = Field(default_factory=lambda: FileLogConfig(enabled=False))
    grpc: GrpcLogConfig = Field(default_factory=GrpcLogConfig)

    @field_validator("level")
    @classmethod
    def normalize_level(cls, v: Any) -> int:
        """Allows user to pass 'DEBUG', 'debug', or 10."""
        if isinstance(v, str):
            v = v.upper()
            # Check if it's a known level name
            level = logging.getLevelName(v)
            if isinstance(level, int):
                return level
            # Fallback for edge cases
            return int(getattr(logging, v, logging.INFO))
        return int(v)


# --- Singleton Factory ---


class PanosetiLogFactory:
    """
    Factory for creating loggers with SHARED gRPC resources.
    """

    # Singleton Registry: Maps (host, port) -> TelemetryClient
    import typing

    _grpc_clients: typing.ClassVar[dict[tuple[str, int], TelemetryClient]] = {}
    _grpc_handlers: typing.ClassVar[dict[tuple[str, int], AsyncGrpcHandler]] = {}
    _lock: threading.RLock = threading.RLock()

    @classmethod
    def get_shared_client(cls, host: str, port: int) -> TelemetryClient:
        """
        Returns an existing TelemetryClient for the given host/port,
        or creates a new one if it doesn't exist. Thread-safe.
        """
        key = (host, port)
        with cls._lock:
            if key not in cls._grpc_clients:
                # Lazy initialization of the connection
                cls._grpc_clients[key] = TelemetryClient(host=host, port=port)
            return cls._grpc_clients[key]

    @classmethod
    def get_shared_handler(cls, host: str, port: int) -> AsyncGrpcHandler:
        """
        Returns a shared AsyncGrpcHandler for the given host/port.
        """
        key = (host, port)
        with cls._lock:
            if key not in cls._grpc_handlers:
                client = cls.get_shared_client(host, port)
                cls._grpc_handlers[key] = AsyncGrpcHandler(client)
            return cls._grpc_handlers[key]

    @classmethod
    def reset_clients(cls) -> None:
        """For testing purposes: Clear cached clients and handlers."""
        with cls._lock:
            for h in cls._grpc_handlers.values():
                h.close()
            cls._grpc_handlers.clear()
            cls._grpc_clients.clear()

    @staticmethod
    def configure_logger(cfg: LoggerConfig, reset_handlers: bool = True) -> logging.Logger:
        logger = logging.getLogger(cfg.service_name)
        logger.setLevel(cfg.level)

        if reset_handlers and logger.handlers:
            for h in list(logger.handlers):
                h.close()
                logger.removeHandler(h)

        # 1. Console
        if cfg.console and not any(isinstance(h, RichHandler) for h in logger.handlers):
            console = RichHandler(rich_tracebacks=True, markup=False, show_path=False)
            console.setLevel(cfg.level)
            logger.addHandler(console)

        # 2. Filesystem
        if cfg.file.enabled:
            cfg.file.directory.mkdir(parents=True, exist_ok=True)
            log_path = cfg.file.directory / f"{cfg.service_name}.log"

            if not any(
                isinstance(h, RotatingFileHandler) and h.baseFilename == str(log_path.resolve())
                for h in logger.handlers
            ):
                fh = RotatingFileHandler(log_path, maxBytes=cfg.file.max_bytes, backupCount=cfg.file.backup_count)
                fh.setLevel(cfg.level)
                fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
                logger.addHandler(fh)

        # 3. gRPC (SHARED RESOURCE)
        if cfg.grpc.enabled and not any(isinstance(h, AsyncGrpcHandler) for h in logger.handlers):
            try:
                # RETRIEVE SHARED HANDLER
                grpc_handler = PanosetiLogFactory.get_shared_handler(cfg.grpc.host, cfg.grpc.port)
                logger.addHandler(grpc_handler)
            except Exception as e:
                if cfg.grpc.fail_fast:
                    raise ConnectionError(f"Failed to init Telemetry: {e}") from e
                sys.stderr.write(f"Warning: Telemetry unavailable: {e}\n")

        return logger


# --- Public API ---


def get_logger(
    service_name: str,
    level: int | str = logging.INFO,
    console: bool = True,
    log_dir: str | Path | None = None,
    grpc_enabled: bool = True,
    reset: bool = True,
) -> logging.Logger:

    """
    Get or create a configured logger.

    Args:
        service_name: Unique name for the service (e.g. 'DAQ_Writer').
        level: Logging level (e.g. logging.INFO, 'DEBUG').
        console: Whether to print to stdout/stderr.
        log_dir: Path to write log files. If None, file logging is disabled.
        grpc_enabled: Whether to send logs to the Telemetry Server.
        reset: If True (default), clears existing handlers on this logger
               to apply the new configuration cleanly.
    """
    file_config = FileLogConfig(enabled=False)
    if log_dir:
        log_dir_path = Path(log_dir) if isinstance(log_dir, str) else log_dir
        file_config = FileLogConfig(enabled=True, directory=log_dir_path)

    grpc_config = GrpcLogConfig(enabled=grpc_enabled)

    config = LoggerConfig(service_name=service_name, level=level, console=console, file=file_config, grpc=grpc_config)

    return PanosetiLogFactory.configure_logger(config, reset_handlers=reset)


# --- Subprocess Utilities ---


async def _stream_reader(stream: asyncio.StreamReader, logger_method: Callable[[str], None]) -> None:
    """Internal utility to bridge a stream to a logger method."""
    while True:
        line = await stream.readline()
        if not line:
            break
        # Replace errors to prevent crashes on binary garbage output
        message = line.decode("utf-8", errors="replace").strip()
        if message:
            logger_method(message)


async def monitor_subprocess(process: asyncio.subprocess.Process, logger: logging.Logger) -> None:
    """
    Attaches a logger to a running asyncio subprocess's stdout/stderr.

    Args:
        process: The process object returned by asyncio.create_subprocess_exec
        logger: The logger instance to pipe output to.
    """
    if process.stdout is None or process.stderr is None:
        logger.warning("Subprocess created without piped streams (stdout/stderr). Cannot capture logs.")
        return

    await asyncio.gather(_stream_reader(process.stdout, logger.info), _stream_reader(process.stderr, logger.error))
