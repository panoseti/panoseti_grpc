"""
Common resources for the Telemetry service.
Handles configuration loading, logging setup, and path management.
"""
import logging
import os
from pathlib import Path
from rich.logging import RichHandler
from importlib import resources

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
    Robustly finds telemetry_config.toml.
    Priority:
    1. PROD: Explicit env var 'TELEMETRY_CONFIG_PATH'
    2. DEV: The file inside the installed package (works with pip install -e .)
    """
    # 1. Check Env Var (Best for Production/Docker)
    env_path = os.getenv("TELEMETRY_CONFIG_PATH")
    if env_path:
        return Path(env_path)

    # 2. Check Package Location (Best for pip install -e .)
    # This finds where 'panoseti_grpc.telemetry' actually lives on disk.
    try:
        with resources.path("panoseti_grpc.telemetry", "telemetry_config.toml") as p:
            return p
    except (ImportError, FileNotFoundError):
        # Fallback for older python or weird contexts
        # Assumes this file (resources.py) is in the same dir as the toml
        return Path(__file__).parent / "telemetry_config.toml"