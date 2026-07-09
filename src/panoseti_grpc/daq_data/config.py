"""
Pydantic configuration models for the DaqData gRPC service.

Validated at server startup via DaqDataServerConfig.model_validate(raw_dict).
A ValidationError at startup is a clear, actionable signal that daq_data_server_config.json
has a bad value, rather than a KeyError buried in a call stack mid-observation.
"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class UdsAcquisitionConfig(BaseModel):
    """Configuration for the UDS data source (one socket per data product)."""

    enabled: bool = True
    data_products: list[str] = ["img8", "img16", "ph256", "ph1024"]
    socket_path_template: str = "/tmp/hashpipe_grpc.dp_{dp_name}.sock"
    read_timeout: float = Field(
        60.0, gt=0, description="Seconds to wait for data before closing an idle Hashpipe connection"
    )

    @field_validator("socket_path_template")
    @classmethod
    def must_have_dp_name_placeholder(cls, v: str) -> str:
        if "{dp_name}" not in v:
            raise ValueError("socket_path_template must contain the '{dp_name}' placeholder")
        return v

    @field_validator("data_products")
    @classmethod
    def validate_data_products(cls, v: list[str]) -> list[str]:
        from .state import DataProduct

        for dp in v:
            try:
                DataProduct(dp)
            except ValueError:
                raise ValueError(
                    f"Unknown data product '{dp}'. Valid values: {[d.value for d in DataProduct]}"
                ) from None
        return v


class AcquisitionMethodsConfig(BaseModel):
    uds: UdsAcquisitionConfig = Field(default_factory=lambda: UdsAcquisitionConfig(read_timeout=60.0))


class SimSourceDataConfig(BaseModel):
    real_module_id: int
    movie_pff_path: str
    ph_pff_path: str


class UdsSimStrategyConfig(BaseModel):
    sim_module_ids: list[int] = Field(default_factory=lambda: [224])
    data_products: list[str] = ["ph256", "img16"]
    frame_limit: int = Field(-1, description="Max frames to send; -1 means unlimited")


class SimulateDaqConfig(BaseModel):
    simulation_mode: str = "uds"
    sim_module_ids: list[int]
    movie_type: str = "img16"
    ph_type: str = "ph256"
    source_data: SimSourceDataConfig
    strategies: dict[str, UdsSimStrategyConfig]


class DaqDataGatewayConfig(BaseModel):
    """Settings used when ``DaqDataServerConfig.role == "gateway"``."""

    daq_config_path: str | None = Field(None, description="Path to daq_config.json listing edge DAQ nodes.")
    network_config_path: str | None = Field(
        None, description="Optional path to network_config.json for port forwarding."
    )
    # Default sourced from DAQNODE_GRPC_PORT (mirrors PanosetiServerConfig.port's
    # GRPC_PORT default_factory) rather than a bare 50051 literal, so a
    # fleet-wide port change (env var only, no TOML edit) automatically keeps
    # the gateway's fan-in pointed at the right port on every edge node that
    # doesn't have an explicit per-node port_forwarding.grpc_port override in
    # network_config.json (aggregator.py's per-node override still wins there).
    edge_port: int = Field(
        default_factory=lambda: int(os.getenv("DAQNODE_GRPC_PORT", "50051")),
        ge=1,
        le=65535,
        description="gRPC port on each edge node without an explicit port_forwarding override.",
    )


class DaqDataServerConfig(BaseModel):
    """Top-level server configuration. Loaded from daq_data_server_config.json at startup."""

    role: Literal["edge", "gateway"] = "edge"
    gateway: DaqDataGatewayConfig = Field(default_factory=DaqDataGatewayConfig)

    init_from_default: bool = False
    default_hp_io_config_file: str = "hp_io_config_simulate.json"
    unix_domain_socket: str | None = None
    max_concurrent_rpcs: int = Field(100, ge=1)
    max_read_queue_size: int = Field(50, ge=1)
    min_hp_io_update_interval_seconds: float = Field(0.001, gt=0)
    max_client_update_interval_seconds: float = Field(60.0, gt=0)
    max_reader_enqueue_timeouts: int = Field(2, ge=1)
    max_reader_dequeue_timeouts: int = Field(3, ge=1)
    reader_timeout: float = Field(5.0, gt=0)
    shutdown_grace_period: float = Field(5.0, ge=0)
    hp_io_stop_timeout: float = Field(5.0, gt=0)
    valid_data_products: list[str] = ["img8", "img16", "ph256", "ph1024"]
    acquisition_methods: AcquisitionMethodsConfig = Field(default_factory=lambda: AcquisitionMethodsConfig())
    simulate_daq_cfg: SimulateDaqConfig | None = None
    # Logging — passed to get_logger()
    log_dir: str | None = None
    grpc_logging: bool = True
