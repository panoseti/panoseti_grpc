"""
Unit tests for DaqControlServicer private helper methods and module-level async utilities.
Patches psutil.process_iter and get_logger so __init__ has no side effects.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from panoseti_grpc.daq_control.server import DaqControlServicer, _monitor_hashpipe, _read_stream


@pytest.fixture
def servicer() -> None:
    """Return a DaqControlServicer whose __init__ finds no hashpipe processes
    and does not attempt file/gRPC logging."""
    with (
        patch("panoseti_grpc.daq_control.server.psutil.process_iter", return_value=[]),
        patch("panoseti_grpc.daq_control.server.get_logger", return_value=MagicMock()),
    ):
        return DaqControlServicer()


# ---------------------------------------------------------------------------
# _check_disk_usage
# ---------------------------------------------------------------------------


class TestCheckDiskUsage:
    def test_returns_expected_keys(self, servicer: Any, tmp_path: Any) -> None:
        result = servicer._check_disk_usage(str(tmp_path))
        assert set(result.keys()) == {"total_disk_space", "used_disk_space", "free_disk_space"}

    def test_values_are_positive(self, servicer: Any, tmp_path: Any) -> None:
        result = servicer._check_disk_usage(str(tmp_path))
        assert result["total_disk_space"] > 0
        assert result["used_disk_space"] >= 0
        assert result["free_disk_space"] >= 0

    def test_total_greater_than_used_plus_free(self, servicer: Any, tmp_path: Any) -> None:
        # total >= used + free: Linux reserves blocks for root, so strict equality doesn't hold.
        result = servicer._check_disk_usage(str(tmp_path))
        assert result["total_disk_space"] >= result["used_disk_space"] + result["free_disk_space"]


# ---------------------------------------------------------------------------
# _check_run_dirs
# ---------------------------------------------------------------------------


class TestCheckRunDirs:
    def test_no_pffd_dirs(self, servicer: Any, tmp_path: Any) -> None:
        (tmp_path / "other_dir").mkdir()
        result = servicer._check_run_dirs(str(tmp_path))
        assert result == []

    def test_finds_pffd_dirs(self, servicer: Any, tmp_path: Any) -> None:
        (tmp_path / "run001.pffd").mkdir()
        (tmp_path / "run002.pffd").mkdir()
        result = servicer._check_run_dirs(str(tmp_path))
        assert len(result) == 2
        assert all(p.endswith(".pffd") for p in result)

    def test_ignores_pffd_files(self, servicer: Any, tmp_path: Any) -> None:
        """glob should match directories; plain files with .pffd extension are also matched
        since glob does not filter by type. Verify only real entries are returned."""
        (tmp_path / "run.pffd").mkdir()
        (tmp_path / "file.pffd").write_text("data")
        result = servicer._check_run_dirs(str(tmp_path))
        assert len(result) == 2  # glob matches both files and dirs with *.pffd

    def test_does_not_recurse(self, servicer: Any, tmp_path: Any) -> None:
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
    def test_removes_existing_directory(self, servicer: Any, tmp_path: Any) -> None:
        target = tmp_path / "rundir"
        target.mkdir()
        (target / "file.dat").write_text("data")
        result = servicer._cleanup_dir(str(target))
        assert result is True
        assert not target.exists()

    def test_returns_falsy_when_dir_not_exist(self, servicer: Any, tmp_path: Any) -> None:
        result = servicer._cleanup_dir(str(tmp_path / "ghost"))
        assert not result  # returns None (falsy) — logs a warning

    def test_removes_nested_contents(self, servicer: Any, tmp_path: Any) -> None:
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
    def test_creates_file(self, servicer: Any, tmp_path: Any) -> None:
        servicer._create_module_config(str(tmp_path), [10, 20, 30])
        config_file = tmp_path / "module.config"
        assert config_file.exists()

    def test_file_content(self, servicer: Any, tmp_path: Any) -> None:
        servicer._create_module_config(str(tmp_path), [250, 251])
        content = (tmp_path / "module.config").read_text()
        assert "250" in content
        assert "251" in content

    def test_overwrites_existing(self, servicer: Any, tmp_path: Any) -> None:
        servicer._create_module_config(str(tmp_path), [1, 2])
        servicer._create_module_config(str(tmp_path), [3])
        content = (tmp_path / "module.config").read_text()
        assert "3" in content
        assert "1" not in content


# ---------------------------------------------------------------------------
# _setup_data_directories
# ---------------------------------------------------------------------------


class TestSetupDataDirectories:
    def test_creates_run_config_dir(self, servicer: Any, tmp_path: Any) -> None:
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [10])
        assert (tmp_path / "run.pffd").is_dir()

    def test_creates_module_data_dirs(self, servicer: Any, tmp_path: Any) -> None:
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [10, 20])
        assert (tmp_path / "module_10" / "run.pffd").is_dir()
        assert (tmp_path / "module_20" / "run.pffd").is_dir()

    def test_idempotent(self, servicer: Any, tmp_path: Any) -> None:
        """Calling twice should not raise (exist_ok=True)."""
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [5])
        servicer._setup_data_directories(str(tmp_path), "run.pffd", [5])
        assert (tmp_path / "run.pffd").is_dir()


# ---------------------------------------------------------------------------
# _read_stream
# ---------------------------------------------------------------------------


class TestReadStream:
    @pytest.mark.asyncio
    async def test_forwards_lines_to_log_method(self):
        mock_stream = AsyncMock()
        mock_stream.readline = AsyncMock(side_effect=[b"line one\n", b"line two\n", b""])
        received = []
        await _read_stream(mock_stream, received.append)
        assert received == ["line one", "line two"]

    @pytest.mark.asyncio
    async def test_skips_blank_lines(self):
        mock_stream = AsyncMock()
        mock_stream.readline = AsyncMock(side_effect=[b"  \n", b"\n", b""])
        received = []
        await _read_stream(mock_stream, received.append)
        assert received == []

    @pytest.mark.asyncio
    async def test_stops_on_eof(self):
        mock_stream = AsyncMock()
        mock_stream.readline = AsyncMock(side_effect=[b""])
        received = []
        await _read_stream(mock_stream, received.append)
        assert received == []

    @pytest.mark.asyncio
    async def test_replaces_invalid_utf8(self):
        mock_stream = AsyncMock()
        mock_stream.readline = AsyncMock(side_effect=[b"\xff\xfe bad bytes\n", b""])
        received = []
        await _read_stream(mock_stream, received.append)
        assert len(received) == 1
        assert "bad bytes" in received[0]


# ---------------------------------------------------------------------------
# _monitor_hashpipe
# ---------------------------------------------------------------------------


class TestMonitorHashpipe:
    @pytest.mark.asyncio
    async def test_routes_stdout_to_stdout_logger(self):
        mock_proc = MagicMock()
        mock_proc.stdout = AsyncMock()
        mock_proc.stdout.readline = AsyncMock(side_effect=[b"stdout msg\n", b""])
        mock_proc.stderr = AsyncMock()
        mock_proc.stderr.readline = AsyncMock(side_effect=[b""])

        stdout_logger = MagicMock()
        stderr_logger = MagicMock()
        await _monitor_hashpipe(mock_proc, stdout_logger, stderr_logger)

        stdout_logger.info.assert_called_once_with("stdout msg")
        stderr_logger.error.assert_not_called()

    @pytest.mark.asyncio
    async def test_routes_stderr_to_stderr_logger(self):
        mock_proc = MagicMock()
        mock_proc.stdout = AsyncMock()
        mock_proc.stdout.readline = AsyncMock(side_effect=[b""])
        mock_proc.stderr = AsyncMock()
        mock_proc.stderr.readline = AsyncMock(side_effect=[b"error msg\n", b""])

        stdout_logger = MagicMock()
        stderr_logger = MagicMock()
        await _monitor_hashpipe(mock_proc, stdout_logger, stderr_logger)

        stderr_logger.error.assert_called_once_with("error msg")
        stdout_logger.info.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_both_streams_concurrently(self):
        mock_proc = MagicMock()
        mock_proc.stdout = AsyncMock()
        mock_proc.stdout.readline = AsyncMock(side_effect=[b"out\n", b""])
        mock_proc.stderr = AsyncMock()
        mock_proc.stderr.readline = AsyncMock(side_effect=[b"err\n", b""])

        stdout_calls, stderr_calls = [], []
        stdout_logger = MagicMock()
        stdout_logger.info = stdout_calls.append
        stderr_logger = MagicMock()
        stderr_logger.error = stderr_calls.append

        await _monitor_hashpipe(mock_proc, stdout_logger, stderr_logger)

        assert stdout_calls == ["out"]
        assert stderr_calls == ["err"]
