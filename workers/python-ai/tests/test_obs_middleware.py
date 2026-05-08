from __future__ import annotations

from email.message import Message

import structlog

from python_ai_worker.obs import logger as obs_logger
from python_ai_worker.obs.middleware import bind_request_context, clear_request_context


def _reset_logger_state() -> None:
    obs_logger._initialized = False
    obs_logger._service_name = ""
    structlog.reset_defaults()


def test_bind_request_context_respects_incoming_header(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    obs_logger.init("python-ai-worker")

    headers = Message()
    headers["X-Request-ID"] = "req-fixed"

    request_id = bind_request_context(headers)

    assert request_id == "req-fixed"


def test_bind_request_context_generates_uuid_when_missing(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    obs_logger.init("python-ai-worker")

    request_id = bind_request_context({})

    assert request_id
    assert len(request_id) >= 32


def test_clear_request_context_can_be_called_repeatedly(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    obs_logger.init("python-ai-worker")

    bind_request_context({"X-Request-ID": "req-1"})
    clear_request_context()
    clear_request_context()

    request_id = bind_request_context({})
    assert request_id


def test_bind_request_context_rebinds_service_label(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    obs_logger.init("python-ai-worker")

    bind_request_context({"X-Request-ID": "req-1"})
    clear_request_context()
    bind_request_context({"X-Request-ID": "req-2"})

    logger = obs_logger.get("middleware")
    assert logger is not None
    assert obs_logger.service_name() == "python-ai-worker"
