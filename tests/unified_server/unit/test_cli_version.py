"""Unit tests for `pseti-grpc --version` (cli.py)."""

from __future__ import annotations

from importlib.metadata import version

from typer.testing import CliRunner

from panoseti_grpc.cli import standalone_app

runner = CliRunner()


def test_version_flag_prints_installed_version_and_exits_zero() -> None:
    result = runner.invoke(standalone_app, ["--version"])
    assert result.exit_code == 0
    assert version("panoseti-grpc") in result.output


def test_version_flag_is_eager_and_short_circuits_invalid_options() -> None:
    # --port expects an int; if --version weren't eager, Click's type
    # conversion for --port would fail before _version_callback ever runs.
    result = runner.invoke(standalone_app, ["--version", "--port", "not-an-int"])
    assert result.exit_code == 0
    assert version("panoseti-grpc") in result.output
