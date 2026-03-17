"""
Unit tests for daq_control/util.py.
"""
import pytest
from unittest.mock import patch, MagicMock
import psutil

from panoseti_grpc.daq_control.util import is_hashpipe_running


class TestIsHashpipeRunning:
    def test_pid_not_exist(self):
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=False):
            assert is_hashpipe_running(99999) is False

    def test_hashpipe_in_cmdline(self):
        mock_proc = MagicMock()
        mock_proc.cmdline.return_value = ["hashpipe", "-p", "hashpipe.so"]
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True), \
             patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc):
            assert is_hashpipe_running(1234) is True

    def test_different_process_in_cmdline(self):
        mock_proc = MagicMock()
        mock_proc.cmdline.return_value = ["python", "server.py"]
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True), \
             patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc):
            assert is_hashpipe_running(1234) is False

    def test_process_raises_no_such_process(self):
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True), \
             patch("panoseti_grpc.daq_control.util.psutil.Process",
                   side_effect=psutil.NoSuchProcess(pid=1234)):
            assert is_hashpipe_running(1234) is False

    def test_process_raises_access_denied(self):
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True), \
             patch("panoseti_grpc.daq_control.util.psutil.Process",
                   side_effect=psutil.AccessDenied(pid=1234)):
            assert is_hashpipe_running(1234) is False

    def test_empty_cmdline(self):
        mock_proc = MagicMock()
        mock_proc.cmdline.return_value = []
        with patch("panoseti_grpc.daq_control.util.psutil.pid_exists", return_value=True), \
             patch("panoseti_grpc.daq_control.util.psutil.Process", return_value=mock_proc):
            assert is_hashpipe_running(1234) is False
