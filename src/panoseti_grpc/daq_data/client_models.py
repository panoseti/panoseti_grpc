"""
Pydantic models for DaqData client-side validation.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    IPvAnyAddress,
    field_validator,
    model_validator,
)


class BaseStrictModel(BaseModel):
    """Disallows extra fields to catch typos in configuration keys."""

    model_config = ConfigDict(extra="forbid")


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


class PortForwarding(BaseStrictModel):
    """Networking metadata for port-forwarded devices (Gateways)."""

    status: bool = Field(False)
    gw_ip: str | IPvAnyAddress
    reboot_port: list[int | None] | None = Field(None)
    cmd_port: list[int | None] | None = Field(None)
    port: int | None = None  # SSH forwarded port (legacy)
    grpc_port: int = Field(50051, ge=1, le=65535)  # gRPC forwarded port


class DaqNode(BaseModel):
    """Configuration for a single remote Data Acquisition (DAQ) node."""

    model_config = ConfigDict(extra="allow")
    username: str | None = None
    data_dir: str | None = None
    ip_addr: str | IPvAnyAddress
    module_ids: list[int] = Field(default_factory=list)
    bindhost: str | None = Field("0.0.0.0")
    port_forwarding: PortForwarding | None = None
    modules: list[Any] = Field(default_factory=list)

    @field_validator("module_ids", mode="before")
    def validate_module_range(cls, v: Any) -> list[int]:
        if isinstance(v, list):
            # Module ids must be non-negative
            res = [int(x) for x in v]
            if not all(mid >= 0 for mid in res):
                raise ValueError(f"Invalid module IDs ({v}): Module ids must be non-negative")
            elif len(set(res)) != len(res):
                raise ValueError(
                    f"Invalid module IDs ({v}): Module IDs must be unique if provided as a list of integers"
                )
            else:
                return res
        elif isinstance(v, str):
            if re.match(r"^\d+\-\d+$", v):
                start, end = map(int, v.split("-"))
                if start > end:
                    raise ValueError(f"Start module ID ({start}) must be <= End module ID ({end})")
                return list(range(start, end + 1))
            elif re.match(r"^(\d+)(, ?\d+)*$", v):
                module_ids = list(map(int, v.split(",")))
                if len(module_ids) != len(set(module_ids)):
                    raise ValueError("module_ids in list format must be unique")
                return module_ids
            elif re.match(r"^\[\d+\]$", v):
                return [int(v[1:-1])]
            elif v.isdigit():
                return [int(v)]
            else:
                raise ValueError(
                    "module_ids must be in the format 'start-end' (e.g., '0-127') "
                    "OR '<module_id_A>, <module_id_B>, ..., <module_id_N>'"
                )
        elif isinstance(v, int):
            return [v]
        else:
            raise ValueError(f"Unexpected type for 'module_ids': '{type(v)=}'")


class DaqConfig(BaseStrictModel):
    """DAQ node networking and storage configuration (daq_config.json)."""

    comment: str | None = None
    head_node_data_dir: str | None = None
    head_node_ip_addr: str | IPvAnyAddress | None = None
    head_node_container: bool | None = Field(False)
    daq_node_module_limit: int | None = Field(
        4, description="Maximum number of modules per DAQ node (structural limit)"
    )
    daq_nodes: list[DaqNode]

    @model_validator(mode="after")
    def check_head_node_data_dir_match(self) -> DaqConfig:
        # If the head node and the DAQ node are the same machine, data_dir must NOT match.
        if self.head_node_ip_addr is None or self.head_node_data_dir is None:
            return self
        head_ip = str(self.head_node_ip_addr)
        for node in self.daq_nodes:
            if str(node.ip_addr) == head_ip and node.data_dir == self.head_node_data_dir:
                raise ValueError(
                    f"DAQ Node IP ({node.ip_addr}) matches head node, but "
                    f"data_dir ({node.data_dir}) is identical to head_node_data_dir. "
                    "They MUST be different directories to prevent the transfer cleanup step from deleting the data!"
                )
        return self


class NetworkModule(BaseStrictModel):
    """Network-level mapping for a physical module."""

    ip_addr: str | IPvAnyAddress
    port_forwarding: PortForwarding


class NetworkDaqNode(BaseStrictModel):
    """Network-level mapping for a DAQ node."""

    ip_addr: str | IPvAnyAddress
    # Direct-connection gRPC port -- used when port_forwarding.status is
    # False (or absent), i.e. this node is reached directly at ip_addr
    # rather than through a forwarded gateway. Distinct from
    # port_forwarding.grpc_port, which only applies when status is True.
    grpc_port: int = Field(50051, ge=1, le=65535)
    port_forwarding: PortForwarding


class NetworkHeadnode(BaseStrictModel):
    """Network-level configuration for the head node itself."""

    grpc_port: int = Field(50051, ge=1, le=65535)


class NetworkConfig(BaseStrictModel):
    """Global network routing and port-forwarding map (network_config.json)."""

    headnode: NetworkHeadnode = Field(default_factory=NetworkHeadnode)
    modules: list[NetworkModule] = Field(default_factory=list)
    daq_nodes: list[NetworkDaqNode] = Field(default_factory=list)


class DaqNodeRuntime(BaseModel):
    """Internal runtime state for a single DAQ node."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    node: DaqNode
    channel: Any | None = None
    stub: Any | None = None
    connection_target: str
