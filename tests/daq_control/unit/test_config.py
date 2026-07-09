"""
Unit tests for daq_control/config.py Pydantic validation models.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_control.config import (
    CleanupDataModel,
    StartDaqModel,
    StatusDaqModel,
    StopDaqModel,
)

# ---------------------------------------------------------------------------
# StartDaqModel
# ---------------------------------------------------------------------------


class TestStartDaqModel:
    def test_valid(self, tmp_path: Any) -> None:
        m = StartDaqModel(
            data_dir=str(tmp_path),
            daq_ip_addr="192.168.1.1",
            bindhost="eth0",
            max_file_size_mb=100,
            group_ph_frames=True,
            run_dir="run001.pffd",
            obs="test-obs",
            module_id=[0, 128, 255],
        )
        assert m.obs == "test-obs"
        assert m.module_id == [0, 128, 255]
        # Opt-in recovery flag must default to False -- clearing semaphores
        # is a manual action, not something StartDaq does on every call.
        assert m.force_clean_semaphores is False

    def test_force_clean_semaphores_explicit_true(self, tmp_path: Any) -> None:
        m = StartDaqModel(
            data_dir=str(tmp_path),
            daq_ip_addr="192.168.1.1",
            bindhost="eth0",
            max_file_size_mb=100,
            group_ph_frames=True,
            run_dir="run001.pffd",
            obs="test-obs",
            module_id=[0],
            force_clean_semaphores=True,
        )
        assert m.force_clean_semaphores is True

    def test_model_validator_creates_directories(self, tmp_path: Any) -> None:
        """model_validator should mkdir data_dir/run_dir."""
        run_dir = "myrun.pffd"
        StartDaqModel(
            data_dir=str(tmp_path),
            daq_ip_addr="10.0.0.1",
            bindhost="lo",
            max_file_size_mb=10,
            group_ph_frames=False,
            run_dir=run_dir,
            obs="obs1",
            module_id=[1],
        )
        assert (tmp_path / run_dir).is_dir()

    def test_invalid_ip(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="not-an-ip",
                bindhost="eth0",
                max_file_size_mb=10,
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[1],
            )

    def test_bindhost_too_long(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="a" * 17,  # max_length=16
                max_file_size_mb=10,
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[1],
            )

    def test_bindhost_empty(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="",
                max_file_size_mb=10,
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[1],
            )

    def test_max_file_size_too_small(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="lo",
                max_file_size_mb=-1,  # ge=0
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[1],
            )

    def test_max_file_size_too_large(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="lo",
                max_file_size_mb=100000,  # le=99999
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[1],
            )

    def test_module_id_overflow(self, tmp_path: Any) -> None:
        """module_id values must be 0-255 (Uint8)."""
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="lo",
                max_file_size_mb=10,
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[256],
            )

    def test_module_id_negative(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="lo",
                max_file_size_mb=10,
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="obs",
                module_id=[-1],
            )

    def test_obs_too_long(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            StartDaqModel(
                data_dir=str(tmp_path),
                daq_ip_addr="127.0.0.1",
                bindhost="lo",
                max_file_size_mb=10,
                group_ph_frames=False,
                run_dir="run.pffd",
                obs="o" * 17,  # max_length=16
                module_id=[1],
            )


# ---------------------------------------------------------------------------
# StopDaqModel
# ---------------------------------------------------------------------------


class TestStopDaqModel:
    def test_valid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        m = StopDaqModel(data_dir=str(tmp_path), run_dir="run.pffd")
        assert m.data_dir == tmp_path


# ---------------------------------------------------------------------------
# StatusDaqModel
# ---------------------------------------------------------------------------


class TestStatusDaqModel:
    def test_valid(self, tmp_path: Any) -> None:
        m = StatusDaqModel(
            data_dir=str(tmp_path),
            check_hashpipe_running=True,
            check_disk_usage=False,
            check_run_dirs=True,
        )
        assert m.check_hashpipe_running is True
        assert m.check_disk_usage is False

    def test_all_flags_false(self, tmp_path: Any) -> None:
        m = StatusDaqModel(
            data_dir=str(tmp_path),
            check_hashpipe_running=False,
            check_disk_usage=False,
            check_run_dirs=False,
        )
        assert m.check_disk_usage is False


# ---------------------------------------------------------------------------
# CleanupDataModel
# ---------------------------------------------------------------------------


class TestCleanupDataModel:
    def test_valid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        MODULE_IDS = [10, 20, 40, 70]
        module_dirs = [tmp_path / f"module_{mid}" for mid in MODULE_IDS]
        for module_dir in module_dirs:
            module_dir.mkdir(parents=True, exist_ok=True)
        m = CleanupDataModel(data_dir=str(tmp_path), run_dir="run.pffd", module_id=MODULE_IDS)
        assert m.module_id == MODULE_IDS

    def test_module_id_overflow(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        with pytest.raises(ValidationError):
            CleanupDataModel(data_dir=str(tmp_path), run_dir="run.pffd", module_id=[256])
