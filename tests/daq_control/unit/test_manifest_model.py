"""
Red tests: assert Pydantic validation for the new GenerateManifestModel.
These tests FAIL until GenerateManifestModel is added to daq_control/config.py.
"""

from typing import Any

import pytest
from pydantic import ValidationError

try:
    from panoseti_grpc.daq_control.config import GenerateManifestModel
except ImportError:
    pytest.fail("GenerateManifestModel not yet implemented in daq_control/config.py — run Phase 1 implementation first")


class TestGenerateManifestModelDefaults:
    def test_default_algorithm_is_blake3(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        m = GenerateManifestModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=10,
        )
        assert m.algorithm == "blake3"

    def test_default_include_patterns_is_pff_glob(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        m = GenerateManifestModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=10,
        )
        assert m.include_patterns == ["*.pff"]


class TestGenerateManifestModelValid:
    def test_explicit_blake3(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        m = GenerateManifestModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=10,
            algorithm="blake3",
            include_patterns=["*.pff"],
        )
        assert m.algorithm == "blake3"
        assert m.include_patterns == ["*.pff"]

    def test_explicit_xxh3_128(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        m = GenerateManifestModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=10,
            algorithm="xxh3_128",
            include_patterns=["*.pff"],
        )
        assert m.algorithm == "xxh3_128"

    def test_multiple_include_patterns(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        m = GenerateManifestModel(
            data_dir=str(tmp_path),
            run_dir="run.pffd",
            module_id=10,
            include_patterns=["*.pff", "*.log"],
        )
        assert m.include_patterns == ["*.pff", "*.log"]


class TestGenerateManifestModelInvalid:
    def test_invalid_algorithm_raises(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        with pytest.raises(ValidationError):
            GenerateManifestModel(
                data_dir=str(tmp_path),
                run_dir="run.pffd",
                module_id=10,
                algorithm="md5",
            )

    def test_sha256_algorithm_raises(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        with pytest.raises(ValidationError):
            GenerateManifestModel(
                data_dir=str(tmp_path),
                run_dir="run.pffd",
                module_id=10,
                algorithm="sha256",
            )

    def test_empty_include_patterns_raises(self, tmp_path: Any) -> None:
        run_dir = tmp_path / "run.pffd"
        run_dir.mkdir()
        with pytest.raises(ValidationError):
            GenerateManifestModel(
                data_dir=str(tmp_path),
                run_dir="run.pffd",
                module_id=10,
                include_patterns=[],
            )

    def test_run_dir_not_exist_raises(self, tmp_path: Any) -> None:
        with pytest.raises(ValidationError):
            GenerateManifestModel(
                data_dir=str(tmp_path),
                run_dir="missing.pffd",
                module_id=10,
            )
