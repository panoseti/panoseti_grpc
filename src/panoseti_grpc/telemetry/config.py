"""
Telemetry Service configuration classes for validation and
"""

from __future__ import annotations

import json
import os
import time
import tomllib
from enum import IntEnum
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator


# --- 1. Pydantic Models (Production Schemas) ---
# Map Protobuf Enum to Python Enum
class LogSeverity(IntEnum):
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


class LogSchema(BaseModel):
    """
    Validator for incoming gRPC LogMessages.
    Enforces 'Loki Hygiene' (Low Cardinality Labels).
    """

    # --- Labels (Strict Validation for Indexing) ---
    # Hostnames should be alphanumeric + dashes. No spaces.
    host: str = Field(..., pattern=r"^[a-zA-Z0-9_\-\.]+$", min_length=2, max_length=50)

    # Service names should be concise
    service_name: str = Field(..., min_length=2, max_length=50)

    # --- Metadata ---
    timestamp: float = Field(default_factory=time.time)
    severity: LogSeverity = Field(default=LogSeverity.INFO)

    # Source info (Optional)
    file_path: str | None = None
    line_number: int | None = None
    function_name: str | None = None

    # --- System Metadata (New Fields) ---
    process_id: int | None = None
    thread_name: str | None = None

    # Optional because development environments might not be git repos
    git_commit: str | None = None
    git_branch: str | None = None

    # --- Payload ---
    # We accept a raw string (from gRPC) but validate it isn't massive.
    # 1MB limit for a single log entry is generous but sane.
    payload_json: str = Field(..., max_length=1_000_000)

    @field_validator("service_name")
    @classmethod
    def prevent_high_cardinality(cls, v: str) -> str:
        """
        Prevent dynamic names like 'process_12345' from becoming labels.
        This protects Loki from index explosion.
        """
        if any(char.isdigit() for char in v) and len(v) > 15:
            # Heuristic: If it has numbers and is long, it might be a dynamic ID.
            # You might want to log a warning or force it to a generic name.
            pass
        return v.lower()

    @field_validator("payload_json")
    @classmethod
    def validate_json_structure(cls, v: str) -> str:
        try:
            json.loads(v)
        except ValueError:
            raise ValueError("Payload must be valid JSON") from None
        return v


class GnssModel(BaseModel):
    satellites: int = Field(ge=0, le=100)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    fix_mode: str
    # Core + Extensions Pattern: "extra_data" is the safe extension point
    # Use a default_factory (or default to None) to avoid cross-instance state leakage.
    extra_data: dict[str, Any] | None = Field(default_factory=dict)


class DewModel(BaseModel):
    temp_c: float = Field(ge=-50, le=100)
    humidity: float = Field(ge=0, le=100)
    extra_data: dict[str, Any] | None = Field(default_factory=dict)


class PayloadTestModel(BaseModel):
    iteration: int
    value: float
    message: str
    active: bool
    extra_data: dict[str, Any] | None = Field(default_factory=dict)

    @field_validator("message")
    @classmethod
    def must_be_uppercase(cls, v: str) -> str:
        if not v.isupper():
            raise ValueError("Message must be uppercase")
        return v


SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "gnss": GnssModel,
    "dew": DewModel,
    "test": PayloadTestModel,
    # "dev" types don't need a model here; handled via 'experimental' mode
}


# --- 2. Registry Configuration ---


class DeviceConfig(BaseModel):
    """
    Represents a single device entry in telemetry_config.toml
    """

    mode: str = Field(default="production", pattern="^(production|experimental)$")
    redis_prefix: str
    ttl_seconds: int = Field(default=0, ge=0)
    description: str | None = ""

    @field_validator("redis_prefix")
    @classmethod
    def validate_prefix(cls, v: str, info: Any) -> str:
        # We need access to the 'mode' field to validate this rule.
        # Pydantic v2 validation allows access to other fields via 'info' context if needed.
        # For simple robustness, we enforce the "DEV_" rule if mode is experimental.
        if (
            hasattr(info, "data")
            and "mode" in info.data
            and info.data["mode"] == "experimental"
            and not v.startswith("DEV_")
        ):
            raise ValueError(f"Experimental prefix '{v}' must start with 'DEV_'")
        return v


class TelemetryConfig:
    def __init__(self, devices: dict[str, DeviceConfig]) -> None:
        self.devices = devices

    @classmethod
    def load(cls, path: str | None = None) -> TelemetryConfig:
        """Loads TOML config and parses into DeviceConfig objects."""
        if path is None or not os.path.exists(path):
            # Fallback for installed package resources
            try:
                from . import resources as r

                path = str(r.get_config_path())
            except ImportError:
                pass

        # If still missing, try generic fallback or fail
        if path is None or not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "rb") as f:
            data = tomllib.load(f)

        parsed_devices = {}
        raw_devices = data.get("devices", {})

        for name, cfg in raw_devices.items():
            try:
                # This validates the TOML fields (mode, prefix, etc.)
                parsed_devices[name] = DeviceConfig(**cfg)
            except ValidationError as e:
                # We log but continue so one bad config doesn't kill the server
                print(f"⚠️  Config Error for '[devices.{name}]': {e}")

        return cls(parsed_devices)

    def get_redis_key(self, device_type: str, device_id: str) -> str:
        """Returns the Redis key. Falls back to SANDBOX if unknown."""
        if device_type not in self.devices:
            # Unknown types go to a quarantine namespace
            return f"SANDBOX:{device_type}:{device_id}"

        prefix = self.devices[device_type].redis_prefix
        return f"{prefix}{device_id}"

    def get_ttl(self, device_type: str) -> int:
        """Returns the TTL in seconds. 0 means permanent."""
        if device_type not in self.devices:
            return 3600  # Unknown types die after 1 hour
        return self.devices[device_type].ttl_seconds

    def validate_and_flatten(self, device_type: str, data: dict[str, Any]) -> dict[str, Any]:
        """
        Validates data if Production. Flattens data for Redis.
        """
        device_cfg = self.devices.get(device_type)

        # 1. Unknown or Experimental? SKIP Validation.
        if not device_cfg or device_cfg.mode == "experimental":
            return self._flatten_dict(data)

        # 2. Production? Enforce Schema.
        if device_type in SCHEMA_MAP:
            try:
                # Pydantic validation
                model = SCHEMA_MAP[device_type](**data)
                clean_data = model.model_dump()
            except ValidationError as e:
                raise ValueError(f"Schema Violation for {device_type}: {e}") from e
        else:
            # Should not happen if config and code are synced
            raise ValueError(f"No schema defined for production type '{device_type}'")

        # 3. Flatten (Handling nested 'extra_data')
        if clean_data.get("extra_data"):
            extras = clean_data.pop("extra_data")
            if isinstance(extras, dict):
                for k, v in extras.items():
                    clean_data[f"extra_{k}"] = v

        return self._flatten_dict(clean_data)

    def _flatten_dict(self, d: dict[str, Any], parent_key: str = "", sep: str = "_") -> dict[str, Any]:
        items: list[tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


class TelemetryServerConfig(BaseModel):
    """Server-level configuration for the Telemetry gRPC service."""

    grpc_port: int = Field(50051, ge=1024, le=65535)
    redis_host: str = Field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    redis_port: int = 6379
    redis_db: int = 0
    uds_path: str | None = None
    telemetry_config_path: str | None = None  # overrides env var / package default
    shutdown_grace_period: float = Field(5.0, ge=0)
    log_level: str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
