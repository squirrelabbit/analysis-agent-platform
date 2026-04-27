from __future__ import annotations

import json
import threading
from http.client import HTTPConnection
from types import SimpleNamespace
from unittest.mock import patch

import structlog
import structlog.contextvars

from python_ai_worker import main as worker_main
from python_ai_worker.obs import logger as obs_logger


def _reset_logger_state() -> None:
    obs_logger._initialized = False
    obs_logger._service_name = ""
    structlog.reset_defaults()


def _test_config() -> SimpleNamespace:
    return SimpleNamespace(
        role="worker",
        queue="analysis-support",
        llm_provider="anthropic",
        anthropic_model="claude-test",
        anthropic_prepare_model="claude-test",
        anthropic_sentiment_model="claude-test",
        openai_embedding_model="text-embedding-3-small",
        openai_embedding_dimensions=1536,
        local_embedding_model="",
    )


def test_post_task_binds_and_echoes_request_id(monkeypatch):
    _reset_logger_state()
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    obs_logger.init("python-ai-worker")

    seen: dict[str, str] = {}

    def fake_run_task(name: str, payload: dict[str, object]) -> dict[str, object]:
        seen["skill_name"] = name
        seen["request_id"] = str(structlog.contextvars.get_contextvars().get("request_id") or "")
        seen["payload"] = json.dumps(payload, ensure_ascii=False)
        return {"artifact": {"skill_name": "term_frequency"}}

    server = worker_main.ThreadingHTTPServer(("127.0.0.1", 0), worker_main.make_handler(_test_config()))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        with patch("python_ai_worker.main.run_task", side_effect=fake_run_task):
            conn = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            conn.request(
                "POST",
                "/tasks/term_frequency",
                body=json.dumps({"dataset_name": "issues.csv"}),
                headers={
                    "Content-Type": "application/json",
                    "X-Request-ID": "req-from-go",
                },
            )
            response = conn.getresponse()
            body = response.read().decode("utf-8")
            conn.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert response.status == 200
    assert response.getheader("X-Request-ID") == "req-from-go"
    assert json.loads(body)["artifact"]["skill_name"] == "term_frequency"
    assert seen["skill_name"] == "term_frequency"
    assert seen["request_id"] == "req-from-go"
