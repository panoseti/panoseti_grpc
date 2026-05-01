"""
Configuration models for the DaqData v2 service.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class DataProductConfig(BaseModel):
    shape: tuple[int, int]
    bpp: int
    is_ph: bool = Field(default=False)


class DaqDataV2ServerConfig(BaseModel):
    enabled: bool = False
    log_level: str = "INFO"

    # New fields for unified server integration
    mode: Literal["aggregator", "forwarder"] = "aggregator"
    headnode_target: str = "headnode:50051"
    socket_path_template: str = "/tmp/hashpipe_grpc.dp_{dp_name}.sock"
    data_products: list[str] = ["img16", "ph256"]


DATA_PRODUCTS: dict[str, DataProductConfig] = {
    "img8": DataProductConfig(shape=(32, 32), bpp=1, is_ph=False),
    "img16": DataProductConfig(shape=(32, 32), bpp=2, is_ph=False),
    "ph256": DataProductConfig(shape=(16, 16), bpp=2, is_ph=True),
    "ph1024": DataProductConfig(shape=(32, 32), bpp=2, is_ph=True),
}
