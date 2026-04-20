"""
Daq Control Service configuration classes for validation
"""

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, DirectoryPath, Field, IPvAnyAddress, model_validator


class DaqControlServerConfig(BaseModel):
    """Server-level configuration for the DaqControl gRPC service."""

    grpc_port: int = Field(50051, ge=1024, le=65535)
    log_dir: str = "/var/log/panoseti"
    grpc_logging: bool = True
    shutdown_grace_period: float = Field(5.0, ge=0)
    log_level: str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")


Uint8 = Annotated[int, Field(ge=0, le=255)]


class StartDaqModel(BaseModel):
    data_dir: Path = Field(...)
    daq_ip_addr: IPvAnyAddress
    bindhost: str = Field(..., min_length=1, max_length=16)
    max_file_size_mb: float = Field(ge=1, le=99999)
    group_ph_frames: bool
    run_dir: str = Field(..., min_length=1)
    obs: str = Field(..., min_length=1, max_length=16)
    module_id: list[Uint8] = Field(...)
    force: bool = False

    @model_validator(mode="after")
    def create_run_dir(self) -> StartDaqModel:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        full_path = self.data_dir / self.run_dir
        full_path.mkdir(parents=True, exist_ok=True)
        return self


class StopDaqModel(BaseModel):
    data_dir: DirectoryPath = Field(...)
    run_dir: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def check_run_dir(self) -> StopDaqModel:
        full_path = self.data_dir / self.run_dir
        if not full_path.is_dir():
            raise ValueError("{full_path} not exist.")
        return self


class StatusDaqModel(BaseModel):
    data_dir: DirectoryPath = Field(...)
    check_hashpipe_running: bool = Field(...)
    check_disk_usage: bool = Field(...)
    check_run_dirs: bool = Field(...)


class CleanupMode(StrEnum):
    CLEANUP_FULL = "CLEANUP_FULL"
    CLEANUP_SELECTIVE = "CLEANUP_SELECTIVE"


class CleanupDataModel(BaseModel):
    data_dir: DirectoryPath = Field(...)
    run_dir: str = Field(..., min_length=1)
    module_id: list[Uint8] = Field(...)
    force: bool = False
    mode: CleanupMode = CleanupMode.CLEANUP_FULL
    delete_patterns: list[str] = []
    preserve_patterns: list[str] = []

    @model_validator(mode="after")
    def check_run_dir(self) -> CleanupDataModel:
        full_path = self.data_dir / self.run_dir
        if not full_path.is_dir():
            raise ValueError(f"'{full_path}' not exist.")
        return self

    @model_validator(mode="after")
    def check_module_id(self) -> CleanupDataModel:
        for mid in self.module_id:
            full_path = self.data_dir / f"module_{mid}"
            if not full_path.is_dir():
                raise ValueError(f"'{full_path}' not exist.")
        return self

    @model_validator(mode="after")
    def check_selective_requires_patterns(self) -> CleanupDataModel:
        if self.mode == CleanupMode.CLEANUP_SELECTIVE and not self.delete_patterns:
            raise ValueError("CLEANUP_SELECTIVE requires at least one delete_pattern")
        return self


class GenerateManifestModel(BaseModel):
    data_dir: DirectoryPath
    run_dir: str = Field(..., min_length=1)
    module_id: Uint8
    algorithm: Literal["blake3", "xxh3_128"] = "blake3"
    include_patterns: list[str] = Field(default=["*.pff"], min_length=1)

    @model_validator(mode="after")
    def check_run_dir(self) -> GenerateManifestModel:
        full_path = self.data_dir / self.run_dir
        if not full_path.is_dir():
            raise ValueError(f"'{full_path}' does not exist")
        return self
