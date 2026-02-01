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
    Resolves the telemetry config path.
    Priority:
    1. Env Var TELEMETRY_CONFIG_PATH (set by the ops daemon wrapper in the panoseti/panoseti repo)
    2. Package Resource (default fallback)
    """
    # 1. Check for Ops Override
    env_path = os.getenv("TELEMETRY_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
        print(f"Warning: Env var TELEMETRY_CONFIG_PATH={env_path} not found.")

    # 2. Fallback to package default
    try:
        # For Python 3.9+
        with resources.path("panoseti_grpc.telemetry", "telemetry_config.toml") as p:
            return p
    except (ImportError, FileNotFoundError):
        # Fallback for editable installs or older python
        return Path(__file__).parent / "telemetry_config.toml"