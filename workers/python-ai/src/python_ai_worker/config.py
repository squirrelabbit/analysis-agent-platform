from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerConfig:
    role: str = "python-ai-worker"
    queue: str = "ai-tasks"
    log_level: str = "INFO"
    host: str = "127.0.0.1"
    port: int = 8090
    llm_provider: str = "anthropic"
    anthropic_api_key: str | None = None
    anthropic_model: str = "claude-sonnet-4-6"
    anthropic_prepare_model: str = "claude-3-5-haiku-latest"
    anthropic_api_url: str = "https://api.anthropic.com/v1/messages"
    anthropic_version: str = "2023-06-01"
    anthropic_max_tokens: int = 2048
    anthropic_timeout_sec: float = 30.0


def load_config() -> WorkerConfig:
    return WorkerConfig(
        role=os.getenv("PYTHON_AI_WORKER_ROLE", "python-ai-worker"),
        queue=os.getenv("PYTHON_AI_WORKER_QUEUE", "ai-tasks"),
        log_level=os.getenv("PYTHON_AI_WORKER_LOG_LEVEL", "INFO"),
        host=os.getenv("PYTHON_AI_WORKER_HOST", "127.0.0.1"),
        port=int(os.getenv("PYTHON_AI_WORKER_PORT", "8090")),
        llm_provider=os.getenv("PYTHON_AI_LLM_PROVIDER", "anthropic"),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY") or None,
        anthropic_model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
        anthropic_prepare_model=os.getenv("ANTHROPIC_PREPARE_MODEL", "claude-3-5-haiku-latest"),
        anthropic_api_url=os.getenv("ANTHROPIC_API_URL", "https://api.anthropic.com/v1/messages"),
        anthropic_version=os.getenv("ANTHROPIC_VERSION", "2023-06-01"),
        anthropic_max_tokens=int(os.getenv("ANTHROPIC_MAX_TOKENS", "2048")),
        anthropic_timeout_sec=float(os.getenv("ANTHROPIC_TIMEOUT_SEC", "30")),
    )
