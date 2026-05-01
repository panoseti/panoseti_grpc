"""
Configuration models for the DaqData v2 service.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class DataProductConfig(BaseModel):
    shape: tuple[int, int]
    bpp: int
    is_ph: bool = Field(default=False)


class DaqDataV2ServerConfig(BaseModel):
    enabled: bool = False
    log_level: str = "INFO"


DATA_PRODUCTS: dict[str, DataProductConfig] = {
    "img8": DataProductConfig(shape=(32, 32), bpp=1, is_ph=False),
    "img16": DataProductConfig(shape=(32, 32), bpp=2, is_ph=False),
    "ph256": DataProductConfig(shape=(16, 16), bpp=2, is_ph=True),
    "ph1024": DataProductConfig(shape=(32, 32), bpp=2, is_ph=True),
}
