"""Centralized logging configuration for srunx."""

import sys

from loguru import logger
from loguru._logger import Logger


def configure_logging(
    level: str = "INFO",
    format_string: str | None = None,
    show_time: bool = True,
    show_level: bool = True,
    colorize: bool = True,
) -> None:
    """Configure loguru logging for srunx.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        format_string: Custom format string. If None, uses default format.
        show_time: Whether to show timestamp in logs.
        show_level: Whether to show log level in logs.
        colorize: Whether to colorize the output.
    """
    # Remove default logger
    logger.remove()

    # Build format string
    if format_string is None:
        format_parts = []
        if show_time:
            format_parts.append("<green>{time:YYYY-MM-DD HH:mm:ss}</green>")
        if show_level:
            format_parts.append("<level>{level: <8}</level>")
        format_parts.append(
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
        )
        format_parts.append("<level>{message}</level>")
        format_string = " | ".join(format_parts)

    # Add stderr handler
    logger.add(
        sys.stderr,
        format=format_string,
        level=level,
        colorize=colorize,
        backtrace=True,
        diagnose=True,
    )


def configure_cli_logging(level: str = "INFO", quiet: bool = False) -> None:
    """Configure logging specifically for CLI usage.

    Args:
        level: Logging level.
        quiet: If True, only show WARNING and above.
    """
    if quiet:
        level = "WARNING"

    # Simple format for CLI
    format_string = "<level>{level: <8}</level> | <level>{message}</level>"

    configure_logging(
        level=level,
        format_string=format_string,
        show_time=False,
        show_level=True,
        colorize=True,
    )


def configure_workflow_logging(level: str = "INFO") -> None:
    """Configure logging for workflow execution.

    Args:
        level: Logging level.
    """
    # Detailed format for workflows
    format_string = (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
    )

    configure_logging(
        level=level,
        format_string=format_string,
        show_time=True,
        show_level=True,
        colorize=True,
    )


def get_logger(name: str) -> Logger:
    """Get a logger instance for a module.

    Args:
        name: Module name (usually __name__).

    Returns:
        Logger instance.
    """
    return logger.bind(name=name)  # type: ignore


# Export logger for convenience
__all__ = [
    "logger",
    "configure_logging",
    "configure_cli_logging",
    "configure_workflow_logging",
    "get_logger",
]
