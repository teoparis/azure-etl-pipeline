"""
Centralised logger setup using loguru.

Usage:
    from src.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("Starting extractor", source="rest_api", date="2024-01-15")
"""

import sys
from pathlib import Path
from loguru import logger as _logger

# Remove the default loguru handler so we control formatting entirely
_logger.remove()

_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)

_FILE_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{line} | {message} | {extra}"
)


def configure_logging(log_level: str = "INFO", log_dir: str = "logs") -> None:
    """
    Configure loguru sinks (stdout + rotating file).
    Should be called once at application startup, typically in the CLI entry point.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_dir:   Directory where log files are written. Created if absent.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # Stdout handler — coloured, human-readable
    _logger.add(
        sys.stdout,
        level=log_level,
        format=_LOG_FORMAT,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )

    # File handler — JSON-friendly structured format, daily rotation, 14-day retention
    _logger.add(
        Path(log_dir) / "etl_{time:YYYY-MM-DD}.log",
        level=log_level,
        format=_FILE_FORMAT,
        rotation="00:00",       # new file at midnight
        retention="14 days",
        compression="gz",
        backtrace=True,
        diagnose=False,         # avoid leaking secrets in stack traces to file
        enqueue=True,           # non-blocking writes
        serialize=False,
    )


def get_logger(name: str):
    """
    Return a loguru logger bound with the module name.

    The returned logger supports structured key=value logging:
        log.info("Record processed", order_id="ORD-123", rows=500)

    Args:
        name: Usually __name__ of the calling module.

    Returns:
        A loguru logger with the 'name' context bound.
    """
    return _logger.bind(name=name)
