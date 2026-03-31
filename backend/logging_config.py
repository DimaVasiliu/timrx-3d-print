"""
Structured logging configuration for TimrX backend.

Sets up structlog with:
  - ISO timestamps
  - Log levels
  - Context variable merging (for trace IDs)
  - Console rendering for development, JSON for production

Usage:
    from backend.logging_config import setup_logging, get_logger

    setup_logging()  # Call once at app startup
    logger = get_logger("my_module")
    logger.info("event.happened", key="value")
"""

import logging
import os
import sys

import structlog


def setup_logging(log_level: str | None = None):
    """Configure structured logging for the entire application."""
    level = log_level or os.getenv("LOG_LEVEL", "INFO")

    # Use JSON in production, console renderer in development
    is_production = os.getenv("RENDER", "") or os.getenv("RAILWAY_ENVIRONMENT", "")
    renderer = (
        structlog.processors.JSONRenderer()
        if is_production
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    # Also configure stdlib logging to route through structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )


def get_logger(name: str):
    """Get a structured logger bound to the given module name."""
    return structlog.get_logger(name)
