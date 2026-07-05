"""
Unit tests for daq_control/util.py.
"""

from unittest.mock import MagicMock, patch

import psutil

from panoseti_grpc.daq_control.util import (
    EXPECTED_HASHPIPE_THREADS,
    cleanup_stale_hashpipe_semaphores,
    hashpipe_thread_count,
    is_hashpipe_running,
)


class TestIsHashpipeRunning:
    def test_pid_not_exist(self) -> None:
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=False):
            assert is_hashpipe_running(99999) is False

    def test_hashpipe_in_cmdline(self) -> None:
        mock_proc = MagicMock()
        mock_proc.cmdline.return_value = ["hashpipe", "-p", "hashpipe.so"]
        with (
            patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True),
            patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc),
        ):
            assert is_hashpipe_running(1234) is True

    def test_different_process_in_cmdline(self) -> None:
        mock_proc = MagicMock()
        mock_proc.cmdline.return_value = ["python", "server.py"]
        with (
            patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True),
            patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc),
        ):
            assert is_hashpipe_running(1234) is False

    def test_process_raises_no_such_process(self) -> None:
        with (
            patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True),
            patch("panoseti_grpc.daq_control.util.psutil.Process", side_effect=psutil.NoSuchProcess(pid=1234)),
        ):
            assert is_hashpipe_running(1234) is False

    def test_process_raises_access_denied(self) -> None:
        with (
            patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True),
            patch("panoseti_grpc.daq_control.util.psutil.Process", side_effect=psutil.AccessDenied(pid=1234)),
        ):
            assert is_hashpipe_running(1234) is False

    def test_empty_cmdline(self) -> None:
        mock_proc = MagicMock()
        mock_proc.cmdline.return_value = []
        with (
            patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True),
            patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc),
        ):
            assert is_hashpipe_running(1234) is False


class TestHashpipeThreadCount:
    def test_returns_thread_count(self) -> None:
        mock_proc = MagicMock()
        mock_proc.num_threads.return_value = 4
        with patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc):
            assert hashpipe_thread_count(1234) == 4

    def test_stuck_process_reports_fewer_than_expected(self) -> None:
        """The exact symptom this feature exists to catch: hashpipe alive but
        never spawned net_thread/compute_thread/output_thread."""
        mock_proc = MagicMock()
        mock_proc.num_threads.return_value = 1
        with patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc):
            count = hashpipe_thread_count(1234)
            assert count == 1
            assert count < EXPECTED_HASHPIPE_THREADS

    def test_no_such_process_returns_zero(self) -> None:
        with patch(
            "panoseti_grpc.daq_control.util.psutil.Process",
            side_effect=psutil.NoSuchProcess(pid=1234),
        ):
            assert hashpipe_thread_count(1234) == 0

    def test_access_denied_returns_zero(self) -> None:
        with patch(
            "panoseti_grpc.daq_control.util.psutil.Process",
            side_effect=psutil.AccessDenied(pid=1234),
        ):
            assert hashpipe_thread_count(1234) == 0


class TestCleanupStaleHashpipeSemaphores:
    def test_removes_matching_semaphore_files(self, tmp_path, monkeypatch) -> None:
        shm = tmp_path / "dev_shm"
        shm.mkdir()
        stale = shm / "sem.home_panoseti_hashpipe_status_0"
        stale.write_text("")
        unrelated = shm / "sem.some_other_thing_0"
        unrelated.write_text("")

        monkeypatch.setattr(
            "panoseti_grpc.daq_control.util.glob.glob",
            lambda pattern: [str(stale)] if "hashpipe_status_0" in pattern else [],
        )
        removed = cleanup_stale_hashpipe_semaphores(instance_id=0)

        assert removed == [str(stale)]
        assert not stale.exists()
        assert unrelated.exists()  # never touched -- pattern didn't match it

    def test_no_stale_semaphore_is_a_noop(self, monkeypatch) -> None:
        monkeypatch.setattr("panoseti_grpc.daq_control.util.glob.glob", lambda pattern: [])
        assert cleanup_stale_hashpipe_semaphores(instance_id=0) == []

    def test_tolerates_removal_failure(self, tmp_path, monkeypatch) -> None:
        """A concurrent process removing the file first (or a permission
        issue) must not raise -- this runs on the StartDaq hot path."""
        missing = tmp_path / "sem.home_panoseti_hashpipe_status_0"  # never created
        monkeypatch.setattr(
            "panoseti_grpc.daq_control.util.glob.glob", lambda pattern: [str(missing)]
        )
        assert cleanup_stale_hashpipe_semaphores(instance_id=0) == []
