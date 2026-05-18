"""
PANOSETI Unified Logging Module.

This module provides a thread-safe factory for creating loggers that can dispatch
logs to four destinations simultaneously:
1. Console (via Rich)
2. Filesystem — plain text ``.log`` (human-readable)
3. Filesystem — structured JSONL ``.jsonl`` (picked up by Grafana Alloy → Loki)
4. Telemetry Service (via gRPC, shadow path during Alloy migration)

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
import json
import logging
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

# --- JSONL Formatter ---


class JsonlFormatter(logging.Formatter):
    """Single-line JSON formatter for Grafana Alloy ingestion.

    Each log record is serialised to one JSON object per line with the fields
    that Alloy's ``loki.process`` stage extracts as labels:
    ``service``, ``level``, ``git_commit``, ``run_id``, ``hostname``, ``pid``,
    ``thread``.  Any ``extra`` dict fields passed to the logger call are merged
    into the top-level object so they are queryable via LogQL.
    """

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service = service_name
        self._hostname = os.getenv("HOSTNAME", os.uname().nodename)

    # Standard LogRecord attributes that should not be emitted as extra fields.
    _STDLIB_KEYS: frozenset[str] = frozenset(
        {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "taskName",
            "message",
            "asctime",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        obj: dict[str, Any] = {
            "timestamp": self.formatTime(record, datefmt=None),
            "service": self._service,
            "level": record.levelname,
            "message": record.getMessage(),
            "hostname": self._hostname,
            "pid": record.process,
            "thread": record.threadName,
        }
        # Merge any extra fields injected by the caller (git_commit, run_id, …)
        for key, val in record.__dict__.items():
            if key not in self._STDLIB_KEYS and not key.startswith("_"):
                obj.setdefault(key, val)
        if record.exc_info:
            obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(obj, default=str)


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
    jsonl_enabled: bool = True  # write structured JSONL for Grafana Alloy ingestion

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
            print(f"Warning: Log directory '{v}' is not writable ({e}). Falling back to '{fallback}'", file=sys.stderr)
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

        # Idempotency: skip if already configured with our standard handlers
        if not reset_handlers and any(isinstance(h, RichHandler) for h in logger.handlers):
            return logger

        logger.setLevel(cfg.level)
        # Prevent propagation to root logger to avoid double logging in production.
        # However, allow it in test environments so pytest's log_cli can capture it.
        if "pytest" in sys.modules or os.getenv("PYTEST_CURRENT_TEST"):
            logger.propagate = True
        else:
            logger.propagate = False

        if reset_handlers and logger.handlers:
            for h in list(logger.handlers):
                h.close()
                logger.removeHandler(h)

        # 1. Console
        if cfg.console and not any(isinstance(h, RichHandler) for h in logger.handlers):
            console = RichHandler(rich_tracebacks=True, markup=False, show_path=False)
            console.setLevel(cfg.level)
            # Add service tag to console output
            console.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
            logger.addHandler(console)

        # 2. Filesystem — plain text .log (human-readable)
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

        # 2b. Filesystem — structured JSONL .jsonl (Grafana Alloy → Loki)
        if cfg.file.enabled and cfg.file.jsonl_enabled:
            cfg.file.directory.mkdir(parents=True, exist_ok=True)
            jsonl_path = cfg.file.directory / f"{cfg.service_name}.jsonl"

            if not any(
                isinstance(h, RotatingFileHandler) and h.baseFilename == str(jsonl_path.resolve())
                for h in logger.handlers
            ):
                jfh = RotatingFileHandler(jsonl_path, maxBytes=cfg.file.max_bytes, backupCount=cfg.file.backup_count)
                jfh.setLevel(cfg.level)
                jfh.setFormatter(JsonlFormatter(cfg.service_name))
                logger.addHandler(jfh)

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
    jsonl_enabled: bool = True,
    reset: bool = True,
    per_host: bool = True,
) -> logging.Logger:
    """Get or create a configured logger with up to four output paths.

    When *log_dir* is set the logger writes to two files in that directory:
    - ``{service_name}.log`` — plain text, human-readable.
    - ``{service_name}.jsonl`` — one JSON object per line, consumed by
      Grafana Alloy and shipped to Loki (shadow path during Alloy migration).

    Args:
        service_name: Unique name for the service (e.g. ``'daq_control'``).
        level: Logging level (e.g. ``logging.INFO``, ``'DEBUG'``).
        console: Whether to emit rich-formatted output to stdout/stderr.
        log_dir: Directory for ``.log`` and ``.jsonl`` files.  File logging is
            disabled when ``None``.
        grpc_enabled: Whether to forward logs to the Telemetry gRPC service
            (shadow path running alongside Alloy during the migration window).
        jsonl_enabled: Whether to write the structured JSONL file for Alloy.
            Defaults to ``True`` when *log_dir* is provided.
        reset: Clear existing handlers before applying this configuration.
        per_host: When ``True`` (default), appends ``socket.gethostname()`` as
            a subdirectory under *log_dir* so Grafana Alloy can glob
            ``/var/log/panoseti/*/*.jsonl`` and still label logs by host.
            Pass ``False`` for run-scoped log dirs (e.g. hashpipe stdout/stderr)
            where the caller controls the exact target directory.
    """
    import socket

    file_config = FileLogConfig(enabled=False)
    if log_dir:
        log_dir_path = Path(log_dir) if isinstance(log_dir, str) else log_dir
        if per_host:
            log_dir_path = log_dir_path / (socket.gethostname() or os.getenv("HOSTNAME", "unknown"))
        file_config = FileLogConfig(enabled=True, directory=log_dir_path, jsonl_enabled=jsonl_enabled)

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
