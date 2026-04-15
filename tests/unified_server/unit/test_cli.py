"""
Unit tests for the unified_main.py CLI (panoseti-server / python -m panoseti_grpc).

Tests are run via subprocess so they exercise the real argparse entrypoint.
No running server is required.
"""

from typing import Any
import subprocess
import sys


def run_cli(*args: str, timeout: int = 10)-> subprocess.CompletedProcess:
    """Run ``python -m panoseti_grpc <args>`` and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "panoseti_grpc", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# --list[Any]-services
# ---------------------------------------------------------------------------


def test_list_services_exit_zero() -> None:
    """--list[Any]-services exits 0 and prints all three registered service names."""
    result = run_cli("--list[Any]-services")
    assert result.returncode == 0
    assert "telemetry" in result.stdout
    assert "daq_data" in result.stdout
    assert "daq_control" in result.stdout


def test_list_services_no_server_started() -> None:
    """--list[Any]-services must not attempt to start a server (fast exit)."""
    import time

    start = time.monotonic()
    result = run_cli("--list[Any]-services")
    elapsed = time.monotonic() - start
    # Should complete well under 3 seconds (no Redis connect, no port bind)
    assert elapsed < 3.0
    assert result.returncode == 0


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------


def test_help_flag_exit_zero() -> None:
    """--help exits 0."""
    result = run_cli("--help")
    assert result.returncode == 0


def test_help_flag_documents_all_flags() -> None:
    """--help output documents --profile, --services, --config, --list[Any]-services."""
    result = run_cli("--help")
    assert "--profile" in result.stdout
    assert "--services" in result.stdout
    assert "--config" in result.stdout
    assert "--list[Any]-services" in result.stdout


def test_help_documents_profile_choices() -> None:
    """--help output lists all bundled profile names."""
    result = run_cli("--help")
    assert "daq_node" in result.stdout
    assert "headnode" in result.stdout


# ---------------------------------------------------------------------------
# --profile validation
# ---------------------------------------------------------------------------


def test_invalid_profile_exits_nonzero() -> None:
    """An invalid --profile value causes a non-zero exit (argparse error)."""
    result = run_cli("--profile", "invalid_profile_xyz")
    assert result.returncode != 0


def test_valid_profile_names_are_accepted() -> None:
    """Each valid profile name passes argparse validation (tested with --list[Any]-services)."""
    for profile in ("default", "daq_node", "headnode"):
        result = run_cli("--profile", profile, "--list[Any]-services")
        assert result.returncode == 0, f"Profile '{profile}' failed: {result.stderr}"


# ---------------------------------------------------------------------------
# --config
# ---------------------------------------------------------------------------


def test_config_nonexistent_file_exits_nonzero( tmp_path: Any) -> None:
    """--config pointing to a non-existent file causes non-zero exit."""
    result = run_cli("--config", str(tmp_path / "does_not_exist.toml"))
    assert result.returncode != 0


def test_config_valid_toml_file_with_list_services( tmp_path: Any) -> None:
    """--config with a valid minimal TOML + --list[Any]-services exits 0."""
    toml_file = tmp_path / "test_server.toml"
    toml_file.write_bytes(b"""
[server]
port = 50051
[server.services]
telemetry = true
daq_data = false
daq_control = false
""")
    result = run_cli("--config", str(toml_file), "--list[Any]-services")
    assert result.returncode == 0


# ---------------------------------------------------------------------------
# --services override flag
# ---------------------------------------------------------------------------


def test_services_flag_with_list_services_exits_zero() -> None:
    """--services telemetry --list[Any]-services exits 0 (flag is parsed correctly)."""
    result = run_cli("--services", "telemetry", "--list[Any]-services")
    assert result.returncode == 0


def test_services_flag_comma_separated_exits_zero() -> None:
    """--services telemetry,daq_data exits 0 when combined with --list[Any]-services."""
    result = run_cli("--services", "telemetry,daq_data", "--list[Any]-services")
    assert result.returncode == 0
