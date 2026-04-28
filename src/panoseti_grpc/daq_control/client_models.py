"""
Pydantic models for DaqControl client-side validation.
These models validate the parameters passed to the DaqControlClient methods
without performing server-side filesystem checks.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, IPvAnyAddress

Uint8 = Annotated[int, Field(ge=0, le=255)]


class StartDaqParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    daq_ip_addr: IPvAnyAddress
    bindhost: str = Field(..., min_length=1, max_length=16)
    max_file_size_mb: float = Field(ge=0, le=99999)
    group_ph_frames: bool
    run_dir: str = Field(..., min_length=1)
    obs: str = Field(..., min_length=1, max_length=16)
    module_id: list[Uint8] = Field(...)
    force: bool = False


class StopDaqParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = ""


class StatusDaqParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    check_hashpipe_running: bool = True
    check_disk_usage: bool = True
    check_run_dirs: bool = True


class CleanupMode(StrEnum):
    CLEANUP_FULL = "CLEANUP_FULL"
    CLEANUP_SELECTIVE = "CLEANUP_SELECTIVE"


class CleanupDataParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = Field(..., min_length=1)
    module_id: list[Uint8] = Field(...)
    force: bool = False
    mode: CleanupMode = CleanupMode.CLEANUP_FULL
    delete_patterns: list[str] = []
    preserve_patterns: list[str] = []
    manifest_digest: bytes = b""
    dry_run: bool = False


class GenerateManifestParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = Field(..., min_length=1)
    module_id: list[Uint8] = Field(...)
    algorithm: Literal["blake3", "xxh3_128"] = "blake3"
    include_patterns: list[str] = Field(default=["*.pff"], min_length=1)


class GetManifestParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = Field(..., min_length=1)
    module_id: list[Uint8] = Field(...)


class GetTransferStatusParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = ""


class GetManifestDigestParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = Field(..., min_length=1)
    module_id: list[Uint8] = Field(...)


class RetryFailedTransferParameters(BaseModel):
    data_dir: str = Field(..., min_length=1)
    run_dir: str = Field(..., min_length=1)
    module_id: list[Uint8] = Field(...)
    file_path: str = Field(..., min_length=1)
