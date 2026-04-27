from __future__ import annotations

import logging
import os

import structlog

_initialized = False


def init(service: str) -> None:
    """Configure structlog JSON logger with a service label.

    Reads LOG_LEVEL env var (DEBUG|INFO|WARNING|ERROR); defaults to INFO.
    Idempotent — safe to call multiple times.
    """
    global _initialized
    if _initialized:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="time"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service)
    _initialized = True


def get(name: str = "") -> structlog.stdlib.BoundLogger:
    """Return a bound logger for the given name."""
    return structlog.get_logger(name)
