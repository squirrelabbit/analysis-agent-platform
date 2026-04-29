from __future__ import annotations

import os

import structlog

from python_ai_worker.obs import logger as obs_logger


def _reset_logger_state() -> None:
    obs_logger._initialized = False
    obs_logger._service_name = ""
    structlog.reset_defaults()


def test_init_sets_initialized_and_returns_logger(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    obs_logger.init("python-ai-worker")

    assert obs_logger._initialized is True
    logger = obs_logger.get("unit")
    assert logger is not None


def test_init_is_idempotent(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "INFO")

    obs_logger.init("svc-a")
    first = obs_logger.get("first")
    obs_logger.init("svc-b")
    second = obs_logger.get("second")

    assert obs_logger._initialized is True
    assert first is not None
    assert second is not None


def test_init_invalid_log_level_falls_back_to_info(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "NOT-A-LEVEL")

    obs_logger.init("python-ai-worker")

    assert obs_logger._initialized is True
    assert os.getenv("LOG_LEVEL") == "NOT-A-LEVEL"
