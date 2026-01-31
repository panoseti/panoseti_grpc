import tomli
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Dict, Type, Optional, Any
from importlib import resources
import os


# --- 1. Pydantic Models (Production Schemas) ---

class GnssModel(BaseModel):
    satellites: int = Field(ge=0, le=100)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    fix_mode: str
    # Core + Extensions Pattern: "extra_data" is the safe extension point
    extra_data: Optional[Dict[str, Any]] = {}


class DewModel(BaseModel):
    temp_c: float = Field(ge=-50, le=100)
    humidity: float = Field(ge=0, le=100)
    extra_data: Optional[Dict[str, Any]] = {}


class PayloadTestModel(BaseModel):
    iteration: int
    value: float
    message: str
    active: bool

    @field_validator('message')
    def must_be_uppercase(cls, v):
        if not v.isupper():
            raise ValueError('Message must be uppercase')
        return v


SCHEMA_MAP: Dict[str, Type[BaseModel]] = {
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
    description: Optional[str] = ""

    @field_validator('redis_prefix')
    def validate_prefix(cls, v, info):
        # We need access to the 'mode' field to validate this rule.
        # Pydantic v2 validation allows access to other fields via 'info' context if needed,
        # but simple cross-field validation is often easier in the model_validator or by check logic.
        # For simple robustness, we enforce the "DEV_" rule if mode is experimental.
        if 'mode' in info.data and info.data['mode'] == 'experimental':
            if not v.startswith("DEV_"):
                raise ValueError(f"Experimental prefix '{v}' must start with 'DEV_'")
        return v


class TelemetryConfig:
    def __init__(self, devices: Dict[str, DeviceConfig]):
        self.devices = devices

    @classmethod
    def load(cls, path: str):
        """Loads TOML config and parses into DeviceConfig objects."""
        if not os.path.exists(path):
            # Fallback for installed package resources
            try:
                from . import resources as r
                path = r.get_config_path()
            except ImportError:
                pass

        # If still missing, try generic fallback or fail
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "rb") as f:
            data = tomli.load(f)

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

    def validate_and_flatten(self, device_type: str, data: dict) -> dict:
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
                raise ValueError(f"Schema Violation for {device_type}: {e}")
        else:
            # Should not happen if config and code are synced
            raise ValueError(f"No schema defined for production type '{device_type}'")

        # 3. Flatten (Handling nested 'extra_data')
        if 'extra_data' in clean_data and clean_data['extra_data']:
            extras = clean_data.pop('extra_data')
            for k, v in extras.items():
                clean_data[f"extra_{k}"] = v

        return self._flatten_dict(clean_data)

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)