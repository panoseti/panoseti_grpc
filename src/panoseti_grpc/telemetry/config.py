import tomli
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Dict, Type, Optional, Any
from importlib import resources
import os


# --- 1. Pydantic Models ---

class GnssModel(BaseModel):
    satellites: int = Field(ge=0, le=100)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    fix_mode: str
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
    "generic": dict
}


# --- 2. Registry Configuration ---

class DeviceConfig(BaseModel):
    type: str
    redis_prefix: str
    description: Optional[str] = ""


class TelemetryConfig(BaseModel):
    devices: Dict[str, DeviceConfig]

    @classmethod
    def load(cls, path="telemetry_config.toml"):
        """
        Loads configuration from a local file or falls back to package resources.
        """
        config_dict = None

        # 1. Try Local File (Primary)
        # We check existence explicitly to avoid race conditions with open()
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    config_dict = tomli.load(f)
            except Exception as e:
                print(f"Error reading local config '{path}': {e}")

        # 2. Try Package Resource (Fallback)
        if config_dict is None:
            print(f"Config '{path}' not found locally, checking package resources...")
            try:
                # FIX: Correct package name 'panoseti_grpc.telemetry'
                # resources.path is a context manager that yields a pathlib.Path
                with resources.path("panoseti_grpc.telemetry", path) as p:
                    if p.exists():
                        with open(p, "rb") as f:
                            config_dict = tomli.load(f)
            except (ModuleNotFoundError, FileNotFoundError, TypeError) as e:
                print(f"Package resource lookup failed: {e}")

        # 3. Final Check
        if config_dict is None:
            raise FileNotFoundError(
                f"Could not load '{path}' from local dir or package 'panoseti_grpc.telemetry'"
            )

        return cls(**config_dict)

    def get_redis_key(self, device_type: str, device_id: str) -> str:
        """Validates ID existence and returns formatted Redis Key."""
        # 1. Check if type exists
        if device_type not in self.devices:
            raise ValueError(f"Unknown device type: {device_type}")

        # 2. Whitelist Check:
        # In this implementation, we allow any ID if the TYPE is registered.
        # To strictly whitelist IDs, we would need a list of allowed IDs in DeviceConfig.
        # For now, we rely on the prefix to format the key.
        prefix = self.devices[device_type].redis_prefix
        return f"{prefix}{device_id}"

    def validate_and_flatten(self, device_type: str, data: dict) -> dict:
        """
        Validates data against schema and flattens 'extra_data' for Redis.
        """
        # 1. Validate
        if device_type in SCHEMA_MAP and SCHEMA_MAP[device_type] is not dict:
            model = SCHEMA_MAP[device_type](**data)
            clean_data = model.model_dump()
        else:
            clean_data = data

        # 2. Flatten 'extra_data' if present (Redis doesn't like nested dicts)
        if 'extra_data' in clean_data and clean_data['extra_data']:
            extras = clean_data.pop('extra_data')
            for k, v in extras.items():
                clean_data[f"extra_{k}"] = v

        return clean_data