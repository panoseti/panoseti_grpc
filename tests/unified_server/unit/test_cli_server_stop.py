"""Unit tests for `pseti-grpc server stop` (_cli/server.py).

Uses CliRunner + mocking (psutil.process_iter, os.kill, _is_process_alive) --
no real processes are spawned. Mocking style matches
tests/daq_control/unit/test_server_helpers.py's psutil.process_iter patching.
"""

from __future__ import annotations

import signal
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from panoseti_grpc._cli.server import _find_server_processes, app

runner = CliRunner()


def _fake_proc(pid: int, cmdline: list[str], status: str = "running") -> MagicMock:
    proc = MagicMock()
    proc.info = {"pid": pid, "cmdline": cmdline, "status": status}
    return proc


def test_stop_no_server_running_exits_zero() -> None:
    with patch("panoseti_grpc._cli.server.psutil.process_iter", return_value=[]):
        result = runner.invoke(app, ["stop"])
    assert result.exit_code == 0
    assert "No pseti-grpc server process is running" in result.output


def test_stop_sends_sigterm_and_succeeds_within_grace_period() -> None:
    fake = _fake_proc(4242, ["/venv/bin/pseti-grpc", "server"])
    with (
        patch("panoseti_grpc._cli.server.psutil.process_iter", return_value=[fake]),
        patch("panoseti_grpc._cli.server.os.kill") as mock_kill,
        patch("panoseti_grpc._cli.server._is_process_alive", side_effect=[False]),
        patch("panoseti_grpc._cli.server.time.sleep"),
    ):
        result = runner.invoke(app, ["stop", "--grace-period", "1"])
    assert result.exit_code == 0
    assert "Stopped 1 pseti-grpc server process" in result.output
    assert mock_kill.call_args_list[0].args == (4242, signal.SIGTERM)


def test_stop_escalates_to_sigkill_after_timeout() -> None:
    fake = _fake_proc(4242, ["/venv/bin/pseti-grpc", "server"])
    with (
        patch("panoseti_grpc._cli.server.psutil.process_iter", return_value=[fake]),
        patch("panoseti_grpc._cli.server.os.kill") as mock_kill,
        # alive through the whole poll loop, then gone after SIGKILL
        patch("panoseti_grpc._cli.server._is_process_alive", side_effect=[True, True, False]),
        patch("panoseti_grpc._cli.server.time.sleep"),
        patch("panoseti_grpc._cli.server.time.monotonic", side_effect=[0.0, 0.1, 0.2, 100.0]),
    ):
        result = runner.invoke(app, ["stop", "--grace-period", "1"])
    assert result.exit_code == 0
    assert "SIGKILL" in result.output
    kill_signals = [c.args[1] for c in mock_kill.call_args_list]
    assert signal.SIGTERM in kill_signals
    assert signal.SIGKILL in kill_signals


def test_stop_reports_permission_denied() -> None:
    fake = _fake_proc(4242, ["/venv/bin/pseti-grpc", "server"])
    with (
        patch("panoseti_grpc._cli.server.psutil.process_iter", return_value=[fake]),
        patch("panoseti_grpc._cli.server.os.kill", side_effect=PermissionError),
        patch("panoseti_grpc._cli.server.time.sleep"),
    ):
        result = runner.invoke(app, ["stop", "--grace-period", "1"])
    assert result.exit_code == 1
    assert "Permission denied" in result.output
    assert "4242" in result.output


def test_find_server_processes_excludes_self_and_zombies() -> None:
    import os

    self_proc = _fake_proc(os.getpid(), ["/venv/bin/pseti-grpc", "server"])
    zombie = _fake_proc(999, ["/venv/bin/pseti-grpc", "server"], status="zombie")
    other = _fake_proc(4242, ["/venv/bin/pseti-grpc", "server"])
    with (
        patch("panoseti_grpc._cli.server.psutil.process_iter", return_value=[self_proc, zombie, other]),
        patch("panoseti_grpc._cli.server.psutil.STATUS_ZOMBIE", "zombie"),
    ):
        pids = _find_server_processes()
    assert pids == [4242]


def test_find_server_processes_ignores_unrelated_subcommand() -> None:
    daqnode_proc = _fake_proc(5555, ["/venv/bin/pseti-grpc", "daqnode", "status"])
    with patch("panoseti_grpc._cli.server.psutil.process_iter", return_value=[daqnode_proc]):
        pids = _find_server_processes()
    assert pids == []
