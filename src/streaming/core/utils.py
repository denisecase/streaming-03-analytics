# src/streaming/core/utils.py

"""Shared utilities."""

import logging

from dotenv import dotenv_values

__all__ = ["log_env_vars"]


def log_env_vars(logger: logging.Logger, env_file: str = ".env") -> None:
    """Log every variable defined in the .env file.

    Reads directly from the .env file so no explicit list is needed.
    Useful for confirming the right file was loaded with the right values.

    Arguments:
        logger: The logger to write to.
        env_file: Path to the .env file. Defaults to ".env".
    """
    values = dotenv_values(env_file)
    if not values:
        logger.debug(f"No variables found in {env_file}")
        return
    max_len = max(len(k) for k in values)
    for name, value in values.items():
        logger.debug(f"{name:<{max_len}} = {value}")
