"""
Logging utilities for the ingestion framework.

This module provides functions for setting up and configuring logging
across the ingestion framework, ensuring consistent log formatting and handling.
"""

import logging
from logging.handlers import RotatingFileHandler
from sys import stdout

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def set_logger(name: str, filename: str = "ingestion.log", level: int = logging.INFO) -> logging.Logger:
    """
    Configure a logger instance with file and console handlers.

    This function creates or retrieves a logger with the given name and configures it
    with a rotating file handler and a console handler, using the predefined formatter.

    Args:
        name (str): Logger name.
        filename (str): Name of the log file. Defaults to "ingestion.log".
        level (int): Logging level. Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger instance.

    Examples:
        >>> logger = set_logger("my_module")
        >>> logger.info("This is an info message")
        >>>
        >>> # With custom log file and level
        >>> logger = set_logger("debug_module", filename="debug.log", level=logging.DEBUG)
    """
    logger = logging.getLogger(name)

    # Add rotating log handler
    rotating_handler = RotatingFileHandler(
        filename=filename,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=10,  # Max 10 log files before replacing the oldest
    )
    rotating_handler.setLevel(level)
    rotating_handler.setFormatter(FORMATTER)
    logger.addHandler(rotating_handler)

    # Add console stream handler
    console_handler = logging.StreamHandler(stream=stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(FORMATTER)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get an existing logger instance by name.

    This function retrieves an existing logger instance by name without
    adding additional handlers, assuming that the logger has already been
    configured using set_logger().

    Args:
        name (str): Name of the logger to retrieve.

    Returns:
        logging.Logger: Logger instance with the given name.

    Examples:
        >>> # First configure the logger
        >>> _ = set_logger("my_module")
        >>>
        >>> # Later in another module, retrieve the same logger
        >>> logger = get_logger("my_module")
        >>> logger.info("Using the same logger instance")
    """
    return logging.getLogger(name)
