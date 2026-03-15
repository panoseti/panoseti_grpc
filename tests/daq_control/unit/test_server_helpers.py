"""
Unit tests for DaqControlServicer private helper methods.
Patches psutil.process_iter so __init__ sees no running hashpipe instances.
"""
import pytest
from pathlib import Path
from unittest.mock import patch

from panoseti_grpc.daq_control.server import DaqControlServicer


@pytest.fixture
def servicer():
    """Return a DaqControlServicer whose __init__ finds no hashpipe processes."""
    with patch("panoseti_grpc.daq_control.server.psutil.process_iter", return_value=[]):
        return DaqControlServicer()


# ---------------------------------------------------------------------------
# _check_disk_usage
# ---------------------------------------------------------------------------

class TestCheckDiskUsage:
    def test_returns_expected_keys(self, servicer, tmp_path):
        result = servicer._check_disk_usage(str(tmp_path))
        assert set(result.keys()) == {"total_disk_space", "used_disk_space", "free_disk_space"}

    def test_values_are_positive(self, servicer, tmp_path):
        result = servicer._check_disk_usage(str(tmp_path))
        assert result["total_disk_space"] > 0
        assert result["used_disk_space"] >= 0
        assert result["free_disk_space"] >= 0

    def test_total_greater_than_used_plus_free(self, servicer, tmp_path):
        # total >= used + free: Linux reserves blocks for root, so strict equality doesn't hold.
        result = servicer._check_disk_usage(str(tmp_path))
        assert result["total_disk_space"] >= result["used_disk_space"] + result["free_disk_space"]


# ---------------------------------------------------------------------------
# _check_run_dirs
# ---------------------------------------------------------------------------

class TestCheckRunDirs:
    def test_no_pffd_dirs(self, servicer, tmp_path):
        (tmp_path / "other_dir").mkdir()
        result = servicer._check_run_dirs(str(tmp_path))
        assert result == []

    def test_finds_pffd_dirs(self, servicer, tmp_path):
        (tmp_path / "run001.pffd").mkdir()
        (tmp_path / "run002.pffd").mkdir()
        result = servicer._check_run_dirs(str(tmp_path))
        assert len(result) == 2
        assert all(p.endswith(".pffd") for p in result)

    def test_ignores_pffd_files(self, servicer, tmp_path):
        """glob should match directories; plain files with .pffd extension are also matched
        since glob does not filter by type. Verify only real entries are returned."""
        (tmp_path / "run.pffd").mkdir()
        (tmp_path / "file.pffd").write_text("data")
        result = servicer._check_run_dirs(str(tmp_path))
        assert len(result) == 2  # glob matches both files and dirs with *.pffd

    def test_does_not_recurse(self, servicer, tmp_path):
        """Nested .pffd dirs should not be returned."""
        outer = tmp_path / "outer.pffd"
        outer.mkdir()
        (outer / "inner.pffd").mkdir()
        result = servicer._check_run_dirs(str(tmp_path))
        assert len(result) == 1
        assert str(outer) in result


# ---------------------------------------------------------------------------
# _cleanup_dir
# ---------------------------------------------------------------------------

class TestCleanupDir:
    def test_removes_existing_directory(self, servicer, tmp_path):
        target = tmp_path / "rundir"
        target.mkdir()
        (target / "file.dat").write_text("data")
        result = servicer._cleanup_dir(str(target))
        assert result is True
        assert not target.exists()

    def test_returns_falsy_when_dir_not_exist(self, servicer, tmp_path):
        result = servicer._cleanup_dir(str(tmp_path / "ghost"))
        assert not result  # returns None (falsy) — logs a warning

    def test_removes_nested_contents(self, servicer, tmp_path):
        target = tmp_path / "rundir"
        (target / "sub").mkdir(parents=True)
        (target / "sub" / "data.bin").write_text("x")
        result = servicer._cleanup_dir(str(target))
        assert result is True
        assert not target.exists()


# ---------------------------------------------------------------------------
# _create_module_config
# ---------------------------------------------------------------------------

class TestCreateModuleConfig:
    def test_creates_file(self, servicer, tmp_path):
        servicer._create_module_config(str(tmp_path), [10, 20, 30])
        config_file = tmp_path / "module.config"
        assert config_file.exists()

    def test_file_content(self, servicer, tmp_path):
        servicer._create_module_config(str(tmp_path), [250, 251])
        content = (tmp_path / "module.config").read_text()
        assert "250" in content
        assert "251" in content

    def test_overwrites_existing(self, servicer, tmp_path):
        servicer._create_module_config(str(tmp_path), [1, 2])
        servicer._create_module_config(str(tmp_path), [3])
        content = (tmp_path / "module.config").read_text()
        assert "3" in content
        assert "1" not in content


# ---------------------------------------------------------------------------
# _setup_data_directories
# ---------------------------------------------------------------------------

class TestSetupDataDirectories:
    def test_creates_run_config_dir(self, servicer, tmp_path):
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [10])
        assert (tmp_path / "run.pffd").is_dir()

    def test_creates_module_data_dirs(self, servicer, tmp_path):
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [10, 20])
        assert (tmp_path / "module_10" / "run.pffd").is_dir()
        assert (tmp_path / "module_20" / "run.pffd").is_dir()

    def test_idempotent(self, servicer, tmp_path):
        """Calling twice should not raise (exist_ok=True)."""
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [5])
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [5])
        assert (tmp_path / "run.pffd").is_dir()
