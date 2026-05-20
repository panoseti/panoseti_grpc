"""
Unit tests for the unified_main.py CLI (pseti-grpc server / python -m panoseti_grpc).

Tests are run via subprocess so they exercise the real argparse entrypoint.
No running server is required.
"""

import subprocess
import sys
from typing import Any


def run_cli(*args: str, timeout: int = 10) -> subprocess.CompletedProcess[str]:
    """Run ``python -m panoseti_grpc <args>`` and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "panoseti_grpc", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# --list-services
# ---------------------------------------------------------------------------


def test_list_services_exit_zero() -> None:
    """--list-services exits 0 and prints all three registered service names."""
    result = run_cli("--list-services")
    assert result.returncode == 0
    assert "telemetry" in result.stdout
    assert "daq_data" in result.stdout
    assert "daq_control" in result.stdout


def test_list_services_no_server_started() -> None:
    """--list-services must not attempt to start a server (fast exit)."""
    import time

    start = time.monotonic()
    result = run_cli("--list-services")
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
    """--help output documents --profile, --services, --config, --list-services."""
    result = run_cli("--help")
    assert "--profile" in result.stdout
    assert "--services" in result.stdout
    assert "--config" in result.stdout
    assert "--list-services" in result.stdout


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
    """Each valid profile name passes argparse validation (tested with --list-services)."""
    for profile in ("default", "daq_node", "headnode"):
        result = run_cli("--profile", profile, "--list-services")
        assert result.returncode == 0, f"Profile '{profile}' failed: {result.stderr}"


# ---------------------------------------------------------------------------
# --config
# ---------------------------------------------------------------------------


def test_config_nonexistent_file_exits_nonzero(tmp_path: Any) -> None:
    """--config pointing to a non-existent file causes non-zero exit."""
    result = run_cli("--config", str(tmp_path / "does_not_exist.toml"))
    assert result.returncode != 0


def test_config_valid_toml_file_with_list_services(tmp_path: Any) -> None:
    """--config with a valid minimal TOML + --list-services exits 0."""
    toml_file = tmp_path / "test_server.toml"
    toml_file.write_bytes(b"""
[server]
port = 50051
[server.services]
telemetry = true
daq_data = false
daq_control = false
""")
    result = run_cli("--config", str(toml_file), "--list-services")
    assert result.returncode == 0


# ---------------------------------------------------------------------------
# --services override flag
# ---------------------------------------------------------------------------


def test_services_flag_with_list_services_exits_zero() -> None:
    """--services telemetry --list-services exits 0 (flag is parsed correctly)."""
    result = run_cli("--services", "telemetry", "--list-services")
    assert result.returncode == 0


def test_services_flag_comma_separated_exits_zero() -> None:
    """--services telemetry,daq_data exits 0 when combined with --list-services."""
    result = run_cli("--services", "telemetry,daq_data", "--list-services")
    assert result.returncode == 0


# ---------------------------------------------------------------------------
# pseti-grpc daqnode
# ---------------------------------------------------------------------------


def run_pseti_grpc(*args: str, timeout: int = 15) -> subprocess.CompletedProcess[str]:
    """Run ``pseti-grpc <args>`` via the installed console script."""
    from shutil import which

    exe = which("pseti-grpc") or "pseti-grpc"
    return subprocess.run(
        [exe, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_daqnode_help_exit_zero() -> None:
    """pseti-grpc daqnode --help exits 0 and documents key options."""
    result = run_pseti_grpc("daqnode", "--help")
    assert result.returncode == 0
    assert "--skip-alloy" in result.stdout
    assert "--log-dir" in result.stdout
    assert "--alloy-host" in result.stdout


def test_daqnode_status_skip_alloy_reports_all_services() -> None:
    """pseti-grpc daqnode --skip-alloy reports all three service names even when no server is running."""
    result = run_pseti_grpc("daqnode", "--skip-alloy", "--log-dir", "/tmp")
    # Exit 1 is expected (no server running), but all service names should appear.
    assert "daqdata.DaqData" in result.stdout
    assert "panoseti.daq_control.DaqControl" in result.stdout
    assert "panoseti.telemetry.Telemetry" in result.stdout
    assert "Log disk usage" in result.stdout


def test_daqnode_status_json_skip_alloy() -> None:
    """pseti-grpc --json daqnode --skip-alloy emits valid JSON with expected keys."""
    import json

    result = run_pseti_grpc("--json", "daqnode", "--skip-alloy", "--log-dir", "/tmp")
    data = json.loads(result.stdout)
    assert "grpc_services" in data
    assert "disk" in data
    assert len(data["grpc_services"]) == 3
    service_names = {s["service"] for s in data["grpc_services"]}
    assert "daqdata.DaqData" in service_names
    assert "panoseti.daq_control.DaqControl" in service_names
    assert "panoseti.telemetry.Telemetry" in service_names


def test_daqnode_status_reports_disk() -> None:
    """pseti-grpc daqnode --skip-alloy --log-dir /tmp reports disk usage for /tmp."""
    result = run_pseti_grpc("daqnode", "--skip-alloy", "--log-dir", "/tmp")
    assert "/tmp" in result.stdout
    assert "GB" in result.stdout
