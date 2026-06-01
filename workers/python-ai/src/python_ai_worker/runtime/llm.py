"""runtime LLM helpers — δ-4 (5/21) 이후 plan_v2 / executor가 쓰는
2개 helper만 유지한다.

- :func:`_anthropic_client` — 환경/설정 기반 Anthropic client 인스턴스 생성
  (None 반환 시 LLM-backed 분기 skip).
- :func:`_create_json_response_logged` — strict JSON response API 호출 +
  retry + obs structured logging.

옛 v1 layer(planner v1, issue_evidence_summary, execution_final_answer,
prompt_compaction 등)에서 쓰던 helper들은 모두 제거됐다. planner가
필요한 strict object schema 후처리는 ``clients/anthropic.py`` 안의
``_strict_object_schema``가 담당한다.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from ..clients.anthropic import AnthropicClient, AnthropicConfig, AnthropicResponseParseError
from ..config import load_config
from ..obs import get
from .llm_guards import RetryPolicy, with_retry

_LOG = get("runtime.llm")


def _retry_policy_from_config() -> RetryPolicy:
    config = load_config()
    return RetryPolicy(
        max_attempts=config.anthropic_retry_max_attempts,
        base_delay_sec=config.anthropic_retry_base_delay_sec,
        max_delay_sec=config.anthropic_retry_max_delay_sec,
    )


def _create_json_response_logged(
    client: AnthropicClient,
    *,
    operation: str,
    prompt: str,
    schema: dict[str, Any],
    max_tokens: int,
    batch_size: int = 1,
    system: str = "",
    cache_system: bool = False,
) -> Any:
    started_at = time.monotonic()
    prompt_sha256 = hashlib.sha256((prompt or "").encode("utf-8", errors="replace")).hexdigest()
    prompt_char_count = len(prompt or "")
    _LOG.info(
        "llm.call.started",
        operation=operation,
        model=client._config.model,
        max_tokens=max_tokens,
        batch_size=batch_size,
        cache_system=bool(cache_system),
        prompt_sha256=prompt_sha256,
        prompt_char_count=prompt_char_count,
    )
    try:
        response = with_retry(
            operation,
            lambda: client.create_json_response(
                prompt=prompt,
                schema=schema,
                max_tokens=max_tokens,
                system=system,
                cache_system=cache_system,
            ),
            policy=_retry_policy_from_config(),
        )
    except AnthropicResponseParseError as exc:
        # 5/6 진단: raw text 256자 + stop_reason을 obs warning에 dump해
        # max_tokens hit인지 grammar 한계인지 다른 모양 회귀인지 다음 사이클에
        # 데이터로 분기 가능. festival SNS 본문 PII 우려로 raw_text는 256자
        # truncate. with_retry는 본 예외를 retryable로 보지만 같은 schema에
        # 동일 입력이면 같은 결과라 결국 with_retry 후에도 raise.
        raw_preview = (exc.raw_text or "")[:256]
        _LOG.warning(
            "llm.call.parse_failed",
            operation=operation,
            model=client._config.model,
            batch_size=batch_size,
            duration_ms=int((time.monotonic() - started_at) * 1000),
            stop_reason=exc.stop_reason or "unknown",
            raw_text_char_count=len(exc.raw_text or ""),
            raw_text_preview=raw_preview,
            error_message=str(exc),
        )
        _LOG.error(
            "llm.call.failed",
            operation=operation,
            model=client._config.model,
            batch_size=batch_size,
            duration_ms=int((time.monotonic() - started_at) * 1000),
            error_category="AnthropicResponseParseError",
        )
        raise
    except Exception as exc:
        _LOG.error(
            "llm.call.failed",
            operation=operation,
            model=client._config.model,
            batch_size=batch_size,
            duration_ms=int((time.monotonic() - started_at) * 1000),
            error_category=type(exc).__name__,
        )
        raise
    usage = dict(getattr(response, "usage", {}) or {})
    cache_creation = int(usage.get("cache_creation_input_tokens") or 0)
    cache_read = int(usage.get("cache_read_input_tokens") or 0)
    _LOG.info(
        "llm.call.completed",
        operation=operation,
        model=client._config.model,
        batch_size=batch_size,
        duration_ms=int((time.monotonic() - started_at) * 1000),
        input_tokens=int(usage.get("input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
        total_tokens=int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0),
        cache_creation_input_tokens=cache_creation,
        cache_read_input_tokens=cache_read,
        cache_hit=cache_read > 0,
        prompt_sha256=prompt_sha256,
        prompt_char_count=prompt_char_count,
    )
    return response


def _anthropic_client() -> AnthropicClient | None:
    config = load_config()
    if config.llm_provider.lower() != "anthropic":
        return None
    return AnthropicClient(
        AnthropicConfig(
            api_key=config.anthropic_api_key,
            model=config.anthropic_model,
            api_url=config.anthropic_api_url,
            version=config.anthropic_version,
            max_tokens=config.anthropic_max_tokens,
            timeout_sec=config.anthropic_timeout_sec,
        )
    )


__all__ = [
    "_anthropic_client",
    "_create_json_response_logged",
    "_retry_policy_from_config",
]
