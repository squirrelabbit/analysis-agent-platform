"""사내 wisenut LLOA(vLLM, OpenAI 호환) 호출 클라이언트.

전처리 LLM 단계(doc_genuineness, clause_label)에서 사용한다.
AnthropicClient와 같은 패턴(dataclass + urllib + urlopen injection)을 따른다.
다른 plan skill은 기존 ``AnthropicClient``를 그대로 쓰며, 이 클라이언트는
LLOA endpoint 전용이다.

5/14~5/19 PoC 검증 사항:
- ``/no_think`` directive로 reasoning_content 출력 억제 → wall 약 3× 단축
- ``reasoning_effort='low'`` 사용 시 절 추출 약 26% 누락 → default ``None``
- ``temperature=0`` 이지만 round간 약 ±5% 변동 (LLOA 내재 비결정성)
- OpenAI 호환이라 json_schema strict mode 미지원 → markdown fence strip 후 JSON parse
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable
from urllib import request

from ..runtime.llm_guards import RetryPolicy, with_retry


@dataclass(frozen=True)
class LloaConfig:
    api_key: str | None
    api_url: str
    model: str
    max_tokens: int
    timeout_sec: float
    reasoning_effort: str | None = "low"  # "low" / "medium" / "high" / None
    prepend_no_think: bool = True
    # silverone 2026-06-29 — LLOA HTTP 호출 retry(transient 429/5xx/connection).
    # 사내 vLLM이 일시 불안정할 때 doc 단위 fallback(uncertain/빈 절)으로 조용히
    # 떨어지지 않도록 backoff 재시도. parse 실패는 non-retryable이라 즉시 raise.
    retry_max_attempts: int = 3
    retry_base_delay_sec: float = 1.5
    retry_max_delay_sec: float = 8.0


@dataclass(frozen=True)
class LloaJSONResponse:
    body: Any  # parsed JSON (list or dict — clause_label은 array, genuineness는 object)
    usage: dict[str, Any]
    finish_reason: str = ""
    reasoning: str = ""
    raw_text: str = ""


class LloaResponseParseError(ValueError):
    """LLOA 응답을 JSON으로 파싱 못 했을 때 던진다.

    Caller(``runtime/llm.py`` 또는 skill 함수)가 raw_text + finish_reason을
    obs warning에 dump할 수 있게 한다. PII 보호를 위해 raw_text는 caller가
    truncate(256자 권장)해서 logging.
    """

    def __init__(self, message: str, *, raw_text: str = "", finish_reason: str = "") -> None:
        super().__init__(message)
        self.raw_text = raw_text
        self.finish_reason = finish_reason


class LloaClient:
    def __init__(
        self,
        config: LloaConfig,
        urlopen: Callable[..., Any] | None = None,
    ) -> None:
        self._config = config
        self._urlopen = urlopen or request.urlopen

    def is_enabled(self) -> bool:
        return bool(self._config.api_key)

    def create_json_response(
        self,
        *,
        system: str,
        user: str,
        max_tokens: int | None = None,
    ) -> LloaJSONResponse:
        """LLOA chat completions 호출 + JSON parse.

        반환: ``LloaJSONResponse``
        - body: 파싱된 JSON (list 또는 dict)
        - usage: ``prompt_tokens`` / ``completion_tokens`` / ``total_tokens``
        - finish_reason: ``stop`` / ``length`` / ...
        - reasoning: LLOA가 ``reasoning_content``로 별도 출력하는 토큰 (``/no_think``로 보통 빈 값)
        - raw_text: parse 실패 case에 대비해 원본 content 보존

        예외:
        - ``ValueError("LLOA_API_KEY is required")`` — config에 api_key 미설정
        - ``LloaResponseParseError`` — JSON parse 실패 (raw_text + finish_reason 포함)
        """
        if not self.is_enabled():
            raise ValueError("LLOA_API_KEY is required")

        system_prompt = system
        if self._config.prepend_no_think and not system_prompt.lstrip().startswith("/no_think"):
            system_prompt = "/no_think\n\n" + system_prompt

        payload: dict[str, Any] = {
            "model": self._config.model,
            "max_tokens": max_tokens or self._config.max_tokens,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user},
            ],
        }
        if self._config.reasoning_effort:
            payload["reasoning_effort"] = self._config.reasoning_effort

        raw_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        http_request = request.Request(
            self._config.api_url,
            data=raw_body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._config.api_key or ''}",
            },
        )

        def _request() -> dict[str, Any]:
            with self._urlopen(http_request, timeout=self._config.timeout_sec) as response:
                return json.loads(response.read().decode("utf-8"))

        # transient HTTP/connection 오류만 backoff 재시도. JSON parse 실패 등 non-retryable은
        # with_retry가 즉시 re-raise한다(is_retryable_exception).
        body = with_retry(
            "lloa.create_json_response",
            _request,
            RetryPolicy(
                max_attempts=self._config.retry_max_attempts,
                base_delay_sec=self._config.retry_base_delay_sec,
                max_delay_sec=self._config.retry_max_delay_sec,
            ),
        )

        choice = (body.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        content = message.get("content") or ""
        reasoning = message.get("reasoning") or message.get("reasoning_content") or ""
        finish_reason = str(choice.get("finish_reason") or "")
        usage = body.get("usage") or {}

        if not content:
            raise LloaResponseParseError(
                "lloa response did not contain content",
                raw_text="",
                finish_reason=finish_reason,
            )

        stripped = _strip_markdown_fence(content)
        try:
            parsed = _parse_json_text(stripped)
        except (json.JSONDecodeError, ValueError) as exc:
            raise LloaResponseParseError(
                f"lloa response not parseable as JSON: {exc}",
                raw_text=content,
                finish_reason=finish_reason,
            ) from exc

        return LloaJSONResponse(
            body=parsed,
            usage=usage,
            finish_reason=finish_reason,
            reasoning=reasoning,
            raw_text=content,
        )


def _strip_markdown_fence(content: str) -> str:
    """LLOA가 가끔 ```json ... ``` fence로 감싸는 case strip."""
    s = content.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
        if s.lower().startswith("json\n"):
            s = s.split("\n", 1)[1]
    return s


def _parse_json_text(text: str) -> Any:
    """LLOA 응답 텍스트를 JSON(list/dict)으로 파싱.

    1차: strict json.loads
    2차: 첫 ``{`` 또는 ``[``부터 마지막 매칭 닫는 괄호까지 substring 추출 시도
    """
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start_obj = text.find("{")
        start_arr = text.find("[")
        candidates: list[int] = []
        if start_obj >= 0:
            candidates.append(start_obj)
        if start_arr >= 0:
            candidates.append(start_arr)
        if not candidates:
            raise
        start = min(candidates)
        closer = "}" if text[start] == "{" else "]"
        end = text.rfind(closer)
        if end <= start:
            raise
        return json.loads(text[start : end + 1])
