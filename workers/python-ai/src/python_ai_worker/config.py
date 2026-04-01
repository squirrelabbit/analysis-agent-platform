from __future__ import annotations

import os
from dataclasses import dataclass

from .prompt_registry import (
    DEFAULT_PREPARE_BATCH_PROMPT_VERSION,
    DEFAULT_PREPARE_PROMPT_VERSION,
    DEFAULT_SENTIMENT_PROMPT_VERSION,
)


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
    openai_api_key: str | None = None
    openai_api_url: str = "https://api.openai.com/v1/embeddings"
    openai_embedding_model: str = "text-embedding-3-small"
    openai_embedding_dimensions: int = 0
    openai_embedding_batch_size: int = 32
    openai_timeout_sec: float = 30.0
    local_embedding_model: str = "intfloat/multilingual-e5-small"
    anthropic_prepare_prompt_version: str = DEFAULT_PREPARE_PROMPT_VERSION
    anthropic_prepare_batch_prompt_version: str = DEFAULT_PREPARE_BATCH_PROMPT_VERSION
    anthropic_sentiment_prompt_version: str = DEFAULT_SENTIMENT_PROMPT_VERSION
    evidence_context_max_entries: int = 6
    evidence_context_max_chars: int = 900
    evidence_context_entry_max_chars: int = 180
    evidence_document_total_chars: int = 1800
    evidence_document_max_chars: int = 320


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
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        openai_api_url=os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/embeddings"),
        openai_embedding_model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
        openai_embedding_dimensions=int(os.getenv("OPENAI_EMBEDDING_DIMENSIONS", "0")),
        openai_embedding_batch_size=max(1, int(os.getenv("OPENAI_EMBEDDING_BATCH_SIZE", "32"))),
        openai_timeout_sec=float(os.getenv("OPENAI_TIMEOUT_SEC", "30")),
        local_embedding_model=os.getenv("LOCAL_EMBEDDING_MODEL", "intfloat/multilingual-e5-small"),
        anthropic_prepare_prompt_version=os.getenv("ANTHROPIC_PREPARE_PROMPT_VERSION", DEFAULT_PREPARE_PROMPT_VERSION),
        anthropic_prepare_batch_prompt_version=os.getenv("ANTHROPIC_PREPARE_BATCH_PROMPT_VERSION", DEFAULT_PREPARE_BATCH_PROMPT_VERSION),
        anthropic_sentiment_prompt_version=os.getenv("ANTHROPIC_SENTIMENT_PROMPT_VERSION", DEFAULT_SENTIMENT_PROMPT_VERSION),
        evidence_context_max_entries=max(1, int(os.getenv("EVIDENCE_CONTEXT_MAX_ENTRIES", "6"))),
        evidence_context_max_chars=max(60, int(os.getenv("EVIDENCE_CONTEXT_MAX_CHARS", "900"))),
        evidence_context_entry_max_chars=max(30, int(os.getenv("EVIDENCE_CONTEXT_ENTRY_MAX_CHARS", "180"))),
        evidence_document_total_chars=max(120, int(os.getenv("EVIDENCE_DOCUMENT_TOTAL_CHARS", "1800"))),
        evidence_document_max_chars=max(40, int(os.getenv("EVIDENCE_DOCUMENT_MAX_CHARS", "320"))),
    )
