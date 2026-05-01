"""
Configuration models for the DaqData v2 service.
"""

from __future__ import annotations
from pydantic import BaseModel, Field

class DaqDataV2ServerConfig(BaseModel):
    enabled: bool = False
    log_level: str = "INFO"
