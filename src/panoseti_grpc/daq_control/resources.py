"""
Common resources for the Daq Control service.
Handles logging setup.
"""
import logging
import os
from pathlib import Path
from rich.logging import RichHandler
from importlib import resources

def make_rich_logger(name: str = "daq_control", level: int = logging.INFO) -> logging.Logger:
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