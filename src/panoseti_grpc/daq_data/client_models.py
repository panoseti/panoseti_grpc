"""
Pydantic models for DaqData client-side validation.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

Uint8 = Annotated[int, Field(ge=0, le=255)]


class InitHpIoParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    update_interval_seconds: float = Field(1.0, gt=0)
    force: bool = False
    simulate_daq: bool = False
    module_ids: list[int] = Field(default_factory=list)


class StreamImagesParameters(BaseModel):
    stream_movie_data: bool = True
    stream_pulse_height_data: bool = True
    update_interval_seconds: float = Field(1.0, gt=0)
    module_ids: list[int] = Field(default_factory=list)
