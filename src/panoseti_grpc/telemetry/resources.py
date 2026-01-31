"""
Common resources for the Telemetry service.
Handles configuration loading, logging setup, and path management.
"""
import logging
import os
from pathlib import Path
from rich.logging import RichHandler
import importlib.resources as pkg_resources

# Define the package anchor for resource loading
TELEMETRY_ANCHOR_PACKAGE = "panoseti_grpc.telemetry"
CONFIG_FILENAME = "telemetry_config.toml"


def make_rich_logger(name: str = "telemetry", level: int = logging.INFO) -> logging.Logger:
    """
    Creates a configured logger using RichHandler for beautiful, structured output.
    """
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)]
    )
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


def get_config_path() -> Path:
    """
    Locates the telemetry_config.toml file.
    Prioritizes a local file for dev, falls back to installed package resource.
    """
    # 1. Check local directory (Development mode)
    local_path = Path(CONFIG_FILENAME)
    if local_path.exists():
        return local_path.resolve()

    # 2. Check within the installed package (Production/Installed mode)
    # Note: Accessing resources varies slightly by Python version;
    # using importlib.resources.files (Python 3.9+)
    try:
        from importlib.resources import files
        pkg_path = files(TELEMETRY_ANCHOR_PACKAGE).joinpath(CONFIG_FILENAME)
        with importlib.resources.as_file(pkg_path) as path:
            if path.exists():
                return path
    except (ImportError, TypeError):
        # Fallback for older python or if file not found in package
        pass

    # 3. Fallback to default/example path if needed
    return Path(f"/app/{CONFIG_FILENAME}")