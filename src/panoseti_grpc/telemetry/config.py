import toml
from pydantic import BaseModel, ValidationError, Field
from typing import Dict, Type, Optional


# --- 1. Define Data Schemas ---
class GnssModel(BaseModel):
    satellites: int = Field(ge=0, le=32)
    lat: float
    lon: float
    fix_mode: str


class WeatherModel(BaseModel):
    temperature_c: float
    humidity: float = Field(ge=0, le=100)
    pressure_mbar: float


# Registry of schemas
SCHEMA_MAP: Dict[str, Type[BaseModel]] = {
    "gps": GnssModel,
    "weather": WeatherModel,
    "generic": dict  # Fallback for untyped
}


# --- 2. Define Device Registry ---
# This matches your storeInfluxDB regex logic but explicitly
class DeviceConfig(BaseModel):
    type: str
    redis_prefix: str  # e.g., "UBLOX_ZED-F9T_"
    description: Optional[str] = ""


class TelemetryConfig(BaseModel):
    devices: Dict[str, DeviceConfig]

    @classmethod
    def load(cls, path="telemetry_config.toml"):
        with open(path, "r") as f:
            return cls(**toml.load(f))

    def get_redis_key(self, device_type: str, device_id: str) -> str:
        """Constructs the Redis key: PREFIX + ID"""
        if device_type not in self.devices:
            raise ValueError(f"Unknown device type: {device_type}")
        prefix = self.devices[device_type].redis_prefix
        return f"{prefix}{device_id}"

    def validate_payload(self, device_type: str, data: dict) -> dict:
        """Validates dictionary against Pydantic schema."""
        if device_type not in SCHEMA_MAP:
            return data  # Or raise error if strict

        # Pydantic validation magic
        model = SCHEMA_MAP[device_type](**data)
        return model.model_dump()