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
    anthropic_api_url: str = "https://api.anthropic.com/v1/messages"
    anthropic_version: str = "2023-06-01"
    anthropic_max_tokens: int = 2048
    anthropic_timeout_sec: float = 30.0
    anthropic_retry_max_attempts: int = 3
    anthropic_retry_base_delay_sec: float = 0.5
    anthropic_retry_max_delay_sec: float = 8.0
    anthropic_prompt_cache_enabled: bool = True
    openai_api_key: str | None = None
    openai_api_url: str = "https://api.openai.com/v1/embeddings"
    openai_embedding_model: str = "text-embedding-3-small"
    openai_embedding_dimensions: int = 0
    openai_embedding_batch_size: int = 32
    openai_timeout_sec: float = 30.0
    local_embedding_model: str = "intfloat/multilingual-e5-small"
    anthropic_input_price_per_million_tokens: float = 0.0
    anthropic_output_price_per_million_tokens: float = 0.0
    openai_embedding_price_per_million_tokens: float = 0.0
    # LLOA (사내 wisenut vLLM) — 전처리 LLM 단계(doc_genuineness, clause_label) 전용
    lloa_api_key: str | None = None
    lloa_api_url: str = "http://210.180.82.135:9023/v1/chat/completions"
    lloa_model: str = "wisenut/wise-lloa-max-v1.2.1"
    # silverone 2026-06-05 — 화면 표시용 모델명. raw model id(lloa_model)는 그대로
    # 보존하고, env로 내려주는 사람이 읽기 좋은 이름을 노출한다(하드코딩 매핑 금지,
    # 모델 교체 시 env만 바꾸면 됨). 비어 있으면 화면은 raw model로 fallback.
    lloa_model_display_name: str = ""
    lloa_max_tokens: int = 65536
    lloa_timeout_sec: float = 180.0
    lloa_reasoning_effort: str | None = "low"  # "low"/"medium"/"high"/None — 5/20 결정 default low (clause_label 속도 ↑)
    lloa_prepend_no_think: bool = True
    lloa_retry_max_attempts: int = 3
    lloa_retry_base_delay_sec: float = 1.5
    evidence_context_max_entries: int = 6
    evidence_context_max_chars: int = 900
    evidence_context_entry_max_chars: int = 180
    evidence_document_total_chars: int = 1800
    evidence_document_max_chars: int = 320
    rule_config_path: str = ""
    rule_config_json: str = ""


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
        anthropic_api_url=os.getenv("ANTHROPIC_API_URL", "https://api.anthropic.com/v1/messages"),
        anthropic_version=os.getenv("ANTHROPIC_VERSION", "2023-06-01"),
        anthropic_max_tokens=int(os.getenv("ANTHROPIC_MAX_TOKENS", "2048")),
        anthropic_timeout_sec=float(os.getenv("ANTHROPIC_TIMEOUT_SEC", "30")),
        anthropic_retry_max_attempts=max(1, int(os.getenv("ANTHROPIC_RETRY_MAX_ATTEMPTS", "3"))),
        anthropic_retry_base_delay_sec=max(0.0, float(os.getenv("ANTHROPIC_RETRY_BASE_DELAY_SEC", "0.5"))),
        anthropic_retry_max_delay_sec=max(0.0, float(os.getenv("ANTHROPIC_RETRY_MAX_DELAY_SEC", "8.0"))),
        anthropic_prompt_cache_enabled=os.getenv("ANTHROPIC_PROMPT_CACHE_ENABLED", "true").strip().lower()
        not in {"0", "false", "no", "off"},
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        openai_api_url=os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/embeddings"),
        openai_embedding_model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
        openai_embedding_dimensions=int(os.getenv("OPENAI_EMBEDDING_DIMENSIONS", "0")),
        openai_embedding_batch_size=max(1, int(os.getenv("OPENAI_EMBEDDING_BATCH_SIZE", "32"))),
        openai_timeout_sec=float(os.getenv("OPENAI_TIMEOUT_SEC", "30")),
        local_embedding_model=os.getenv("LOCAL_EMBEDDING_MODEL", "intfloat/multilingual-e5-small"),
        anthropic_input_price_per_million_tokens=max(0.0, float(os.getenv("ANTHROPIC_INPUT_PRICE_PER_MILLION_TOKENS", "0"))),
        anthropic_output_price_per_million_tokens=max(0.0, float(os.getenv("ANTHROPIC_OUTPUT_PRICE_PER_MILLION_TOKENS", "0"))),
        openai_embedding_price_per_million_tokens=max(0.0, float(os.getenv("OPENAI_EMBEDDING_PRICE_PER_MILLION_TOKENS", "0"))),
        # LLOA_API_KEY is canonical; WISENUT_* names remain direct-run fallbacks.
        lloa_api_key=(
            os.getenv("LLOA_API_KEY")
            or os.getenv("WISENUT_LLOA_API_KEY")
            or os.getenv("WISENUT_LLOA_MAX_V1_2_1_API_KEY")
            or None
        ),
        lloa_api_url=os.getenv("LLOA_API_URL", "http://210.180.82.135:9023/v1/chat/completions"),
        lloa_model=os.getenv("LLOA_MODEL", "wisenut/wise-lloa-max-v1.2.1"),
        lloa_model_display_name=os.getenv("LLOA_MODEL_DISPLAY_NAME", "").strip(),
        lloa_max_tokens=int(os.getenv("LLOA_MAX_TOKENS", "65536")),
        lloa_timeout_sec=float(os.getenv("LLOA_TIMEOUT_SEC", "180")),
        lloa_reasoning_effort=(os.getenv("LLOA_REASONING_EFFORT", "").strip() or None),
        lloa_prepend_no_think=os.getenv("LLOA_PREPEND_NO_THINK", "true").strip().lower()
        not in {"0", "false", "no", "off"},
        lloa_retry_max_attempts=max(1, int(os.getenv("LLOA_RETRY_MAX_ATTEMPTS", "3"))),
        lloa_retry_base_delay_sec=max(0.0, float(os.getenv("LLOA_RETRY_BASE_DELAY_SEC", "1.5"))),
        evidence_context_max_entries=max(1, int(os.getenv("EVIDENCE_CONTEXT_MAX_ENTRIES", "6"))),
        evidence_context_max_chars=max(60, int(os.getenv("EVIDENCE_CONTEXT_MAX_CHARS", "900"))),
        evidence_context_entry_max_chars=max(30, int(os.getenv("EVIDENCE_CONTEXT_ENTRY_MAX_CHARS", "180"))),
        evidence_document_total_chars=max(120, int(os.getenv("EVIDENCE_DOCUMENT_TOTAL_CHARS", "1800"))),
        evidence_document_max_chars=max(40, int(os.getenv("EVIDENCE_DOCUMENT_MAX_CHARS", "320"))),
        rule_config_path=os.getenv("PYTHON_AI_RULE_CONFIG_PATH", "").strip(),
        rule_config_json=os.getenv("PYTHON_AI_RULE_CONFIG_JSON", "").strip(),
    )
