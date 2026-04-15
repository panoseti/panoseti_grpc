"""Unit tests for Pydantic config models and DataProduct enum."""

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_data.config import (
    DaqDataServerConfig,
    UdsAcquisitionConfig,
)
from panoseti_grpc.daq_data.state import DataProduct

# ---------------------------------------------------------------------------
# DataProduct enum
# ---------------------------------------------------------------------------


class TestDataProductEnum:
    def test_valid_values_parse(self) -> None:
        assert DataProduct("img16") == DataProduct.IMG16
        assert DataProduct("img8") == DataProduct.IMG8
        assert DataProduct("ph256") == DataProduct.PH256
        assert DataProduct("ph1024") == DataProduct.PH1024

    def test_string_equality(self) -> None:
        assert DataProduct.IMG16 == "img16"
        assert DataProduct.PH256 == "ph256"

    def test_invalid_value_raises(self) -> None:
        with pytest.raises(ValueError):
            DataProduct("bad_product")

    def test_image_shape(self) -> None:
        assert DataProduct.IMG16.image_shape == (32, 32)
        assert DataProduct.IMG8.image_shape == (32, 32)
        assert DataProduct.PH256.image_shape == (16, 16)
        assert DataProduct.PH1024.image_shape == (32, 32)

    def test_bytes_per_pixel(self) -> None:
        assert DataProduct.IMG16.bytes_per_pixel == 2
        assert DataProduct.IMG8.bytes_per_pixel == 1
        assert DataProduct.PH256.bytes_per_pixel == 2
        assert DataProduct.PH1024.bytes_per_pixel == 2

    def test_is_ph(self) -> None:
        assert DataProduct.PH256.is_ph is True
        assert DataProduct.PH1024.is_ph is True
        assert DataProduct.IMG16.is_ph is False
        assert DataProduct.IMG8.is_ph is False

    def test_bytes_per_image(self) -> None:
        assert DataProduct.IMG16.bytes_per_image == 32 * 32 * 2
        assert DataProduct.IMG8.bytes_per_image == 32 * 32 * 1
        assert DataProduct.PH256.bytes_per_image == 16 * 16 * 2
        assert DataProduct.PH1024.bytes_per_image == 32 * 32 * 2

    def test_pano_image_type(self) -> None:
        from panoseti_grpc.generated.daq_data_pb2 import PanoImage

        assert DataProduct.PH256.pano_image_type == PanoImage.Type.PULSE_HEIGHT
        assert DataProduct.IMG16.pano_image_type == PanoImage.Type.MOVIE


# ---------------------------------------------------------------------------
# UdsAcquisitionConfig
# ---------------------------------------------------------------------------


class TestUdsAcquisitionConfig:
    def test_defaults_are_valid(self) -> None:
        cfg = UdsAcquisitionConfig()
        assert cfg.enabled is True
        assert "{dp_name}" in cfg.socket_path_template
        assert cfg.read_timeout > 0

    def test_socket_template_without_placeholder_raises(self) -> None:
        with pytest.raises(ValidationError, match="dp_name"):
            UdsAcquisitionConfig(socket_path_template="/tmp/no_placeholder.sock")

    def test_unknown_data_product_raises(self) -> None:
        with pytest.raises(ValidationError, match="Unknown data product"):
            UdsAcquisitionConfig(data_products=["img16", "bad_product"])

    def test_all_valid_data_products_accepted(self) -> None:
        cfg = UdsAcquisitionConfig(data_products=["img8", "img16", "ph256", "ph1024"])
        assert len(cfg.data_products) == 4

    def test_read_timeout_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            UdsAcquisitionConfig(read_timeout=0.0)

        with pytest.raises(ValidationError):
            UdsAcquisitionConfig(read_timeout=-1.0)


# ---------------------------------------------------------------------------
# DaqDataServerConfig
# ---------------------------------------------------------------------------


class TestDaqDataServerConfig:
    def test_defaults_parse_without_arguments(self) -> None:
        cfg = DaqDataServerConfig()
        assert cfg.max_concurrent_rpcs == 100
        assert cfg.grpc_logging is True
        assert cfg.unix_domain_socket is None
        assert cfg.simulate_daq_cfg is None

    def test_max_concurrent_rpcs_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            DaqDataServerConfig(max_concurrent_rpcs=0)

        with pytest.raises(ValidationError):
            DaqDataServerConfig(max_concurrent_rpcs=-1)

    def test_min_hp_io_update_interval_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            DaqDataServerConfig(min_hp_io_update_interval_seconds=0.0)

    def test_shutdown_grace_period_can_be_zero(self) -> None:
        cfg = DaqDataServerConfig(shutdown_grace_period=0.0)
        assert cfg.shutdown_grace_period == 0.0

    def test_reader_timeout_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            DaqDataServerConfig(reader_timeout=0.0)

    def test_model_validate_from_dict(self) -> None:
        raw = {
            "max_concurrent_rpcs": 10,
            "min_hp_io_update_interval_seconds": 0.05,
            "reader_timeout": 3.0,
        }
        cfg = DaqDataServerConfig.model_validate(raw)
        assert cfg.max_concurrent_rpcs == 10
        assert cfg.reader_timeout == 3.0

    def test_acquisition_methods_default(self) -> None:
        cfg = DaqDataServerConfig()
        assert cfg.acquisition_methods.uds.enabled is True
        assert cfg.acquisition_methods.uds.read_timeout > 0

    def test_simulate_daq_cfg_optional(self) -> None:
        cfg = DaqDataServerConfig(simulate_daq_cfg=None)
        assert cfg.simulate_daq_cfg is None

    def test_full_config_with_simulate_daq(self) -> None:
        raw = {
            "simulate_daq_cfg": {
                "sim_module_ids": [224, 225],
                "source_data": {
                    "real_module_id": 1,
                    "movie_pff_path": "some/movie.pff",
                    "ph_pff_path": "some/ph.pff",
                },
                "strategies": {"uds": {"data_products": ["img16", "ph256"]}},
            }
        }
        cfg = DaqDataServerConfig.model_validate(raw)
        assert cfg.simulate_daq_cfg is not None
        assert cfg.simulate_daq_cfg.sim_module_ids == [224, 225]
        assert cfg.simulate_daq_cfg.strategies["uds"].data_products == ["img16", "ph256"]

    def test_log_dir_optional(self) -> None:
        cfg = DaqDataServerConfig(log_dir=None)
        assert cfg.log_dir is None

        cfg2 = DaqDataServerConfig(log_dir="/tmp/logs")
        assert cfg2.log_dir == "/tmp/logs"

    def test_grpc_logging_default_true(self) -> None:
        """gRPC logging defaults to True; set to False to suppress telemetry noise."""
        cfg = DaqDataServerConfig()
        assert cfg.grpc_logging is True
        cfg_no_logging = DaqDataServerConfig(grpc_logging=False)
        assert cfg_no_logging.grpc_logging is False
