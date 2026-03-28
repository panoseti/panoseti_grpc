"""
Daq Control Service configuration classes for validation
"""
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ValidationError,
    IPvAnyAddress,
    DirectoryPath
)
from typing import Annotated
from pathlib import Path

Uint8 = Annotated[int, Field(ge=0, le=255)]

class StartDaqModel(BaseModel):
    data_dir: Path = Field(...)
    daq_ip_addr: IPvAnyAddress
    bindhost: str = Field(..., min_length=1, max_length=16)
    max_file_size_mb: float = Field(ge=1, le=99999)
    group_ph_frames: bool
    run_dir: Path = Field(...)
    obs: str = Field(..., min_length=1, max_length=16)
    module_id: list[Uint8] = Field(...)
    
    @model_validator(mode='after')
    def create_run_dir(self) -> 'StartDaqModel':
        self.data_dir.mkdir(parents=True, exist_ok=True)
        full_path = self.data_dir / self.run_dir
        full_path.mkdir(parents=True, exist_ok=True)
        return self

class StopDaqModel(BaseModel):
    data_dir: DirectoryPath = Field(...)
    run_dir: Path = Field(...)

    @model_validator(mode='after')
    def check_run_dir(self) -> 'StopDaqModel':
        full_path = self.data_dir / self.run_dir
        if not full_path.is_dir():
            raise ValueError('{full_path} not exist.')
        return self
    
class StatusDaqModel(BaseModel):
    data_dir: DirectoryPath = Field(...)
    check_hashpipe_running: bool = Field(...)
    check_disk_usage: bool = Field(...)
    check_run_dirs: bool = Field(...)

class CleanupDataModel(BaseModel):
    data_dir: DirectoryPath = Field(...)
    run_dir: Path = Field(...)
    module_id: list[Uint8] = Field(...)

    @model_validator(mode='after')
    def check_run_dir(self) -> 'CleanupDataModel':
        full_path = self.data_dir / self.run_dir
        if not full_path.is_dir():
            raise ValueError('{full_path} not exist.')
        return self




