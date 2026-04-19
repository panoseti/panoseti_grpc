"""
Red tests: assert Pydantic validation for the extended CleanupDataModel.
These tests FAIL until CleanupDataModel gains mode/delete_patterns/preserve_patterns fields.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from panoseti_grpc.daq_control.config import CleanupDataModel


class TestCleanupDataModelBackwardCompat:
    def test_no_mode_defaults_to_full(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        (tmp_path / "module_10").mkdir()
        m = CleanupDataModel(data_dir=str(tmp_path), run_dir="run.pffd", module_id=[10])
        assert m.mode == "CLEANUP_FULL"

    def test_cleanup_full_without_delete_patterns_valid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        (tmp_path / "module_10").mkdir()
        m = CleanupDataModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=[10],
            mode="CLEANUP_FULL",
        )
        assert m.mode == "CLEANUP_FULL"


class TestCleanupDataModelSelective:
    def test_selective_with_delete_patterns_valid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        (tmp_path / "module_10").mkdir()
        m = CleanupDataModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=[10],
            mode="CLEANUP_SELECTIVE",
            delete_patterns=["*.pff"],
        )
        assert m.mode == "CLEANUP_SELECTIVE"
        assert m.delete_patterns == ["*.pff"]

    def test_selective_with_empty_delete_patterns_invalid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        (tmp_path / "module_10").mkdir()
        with pytest.raises(ValidationError):
            CleanupDataModel(
                data_dir=str(tmp_path),
                run_dir="run.pffd",
                module_id=[10],
                mode="CLEANUP_SELECTIVE",
                delete_patterns=[],
            )

    def test_selective_without_delete_patterns_invalid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        (tmp_path / "module_10").mkdir()
        with pytest.raises(ValidationError):
            CleanupDataModel(
                data_dir=str(tmp_path),
                run_dir="run.pffd",
                module_id=[10],
                mode="CLEANUP_SELECTIVE",
            )

    def test_selective_with_preserve_patterns_valid(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        (tmp_path / "module_10").mkdir()
        m = CleanupDataModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=[10],
            mode="CLEANUP_SELECTIVE",
            delete_patterns=["*.pff"],
            preserve_patterns=["manifest.json"],
        )
        assert m.preserve_patterns == ["manifest.json"]
