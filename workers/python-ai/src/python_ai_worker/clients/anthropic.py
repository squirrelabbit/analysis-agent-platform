from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable
from urllib import request


@dataclass(frozen=True)
class AnthropicConfig:
    api_key: str | None
    model: str
    api_url: str
    version: str
    max_tokens: int
    timeout_sec: float


@dataclass(frozen=True)
class AnthropicJSONResponse:
    body: dict[str, Any]
    usage: dict[str, Any]
    stop_reason: str = ""


class AnthropicResponseParseError(ValueError):
    """Anthropic 응답을 JSON으로 파싱 못 했을 때 던진다.

    5/6 진단: ``issue_evidence_summary``가 매번 다른 모양으로 실패
    (21초 JSONDecodeError vs 90초 timeout). 기존엔 ``JSONDecodeError``가
    그대로 올라가서 raw response나 stop_reason을 알 수 없었음. 이 클래스는
    raise 시점에 두 정보를 묶어 보존해 caller(``runtime/llm.py``)가
    obs warning에 dump할 수 있게 한다.

    PII 보호를 위해 raw_text는 caller가 truncate(256자 권장)해서 logging.
    festival 데이터는 SNS 본문 포함이라 전체 dump는 위험.
    """

    def __init__(self, message: str, *, raw_text: str = "", stop_reason: str = "") -> None:
        super().__init__(message)
        self.raw_text = raw_text
        self.stop_reason = stop_reason


class AnthropicClient:
    def __init__(
        self,
        config: AnthropicConfig,
        urlopen: Callable[..., Any] | None = None,
    ) -> None:
        self._config = config
        self._urlopen = urlopen or request.urlopen

    def is_enabled(self) -> bool:
        return bool(self._config.api_key)

    def create_json(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        max_tokens: int | None = None,
        system: str = "",
        cache_system: bool = False,
    ) -> dict[str, Any]:
        return self.create_json_response(
            prompt=prompt,
            schema=schema,
            max_tokens=max_tokens,
            system=system,
            cache_system=cache_system,
        ).body

    def create_json_response(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        max_tokens: int | None = None,
        system: str = "",
        cache_system: bool = False,
    ) -> AnthropicJSONResponse:
        if not self.is_enabled():
            raise ValueError("ANTHROPIC_API_KEY is required")

        payload: dict[str, Any] = {
            "model": self._config.model,
            "max_tokens": max_tokens or self._config.max_tokens,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "output_config": {
                "format": {
                    "type": "json_schema",
                    # Anthropic structured-output strict mode: every nested
                    # ``object`` schema must have ``additionalProperties: false``
                    # (true or missing returns HTTP 400). Normalize here so any
                    # caller's schema works without per-site fix-up. See
                    # `tests/test_anthropic_schema_strict_mode.py` for the
                    # invariant lock.
                    "schema": _strict_object_schema(schema),
                }
            },
        }
        system_text = str(system or "").strip()
        if system_text:
            system_block: dict[str, Any] = {"type": "text", "text": system_text}
            if cache_system:
                system_block["cache_control"] = {"type": "ephemeral"}
            payload["system"] = [system_block]
        raw_body = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            self._config.api_url,
            data=raw_body,
            method="POST",
            headers={
                "content-type": "application/json",
                "x-api-key": self._config.api_key or "",
                "anthropic-version": self._config.version,
            },
        )

        with self._urlopen(http_request, timeout=self._config.timeout_sec) as response:
            body = json.loads(response.read().decode("utf-8"))

        stop_reason = str(body.get("stop_reason") or "")
        text_blocks = []
        for item in body.get("content", []):
            if item.get("type") == "text":
                text_blocks.append(item.get("text", ""))
        if not text_blocks:
            raise AnthropicResponseParseError(
                "anthropic response did not contain text blocks",
                raw_text="",
                stop_reason=stop_reason,
            )

        joined_text = "".join(text_blocks)
        try:
            parsed_body = _parse_json_text(joined_text)
        except (json.JSONDecodeError, ValueError) as exc:
            # 5/6 진단: raw text + stop_reason을 caller가 볼 수 있게 wrap.
            # 기존엔 JSONDecodeError가 그대로 올라가서 issue_evidence_summary가
            # max_tokens hit인지 grammar 한계인지 모르고 fallback으로만 빠짐.
            raise AnthropicResponseParseError(
                f"anthropic response not parseable as JSON: {exc}",
                raw_text=joined_text,
                stop_reason=stop_reason,
            ) from exc
        return AnthropicJSONResponse(
            body=parsed_body,
            usage=body.get("usage") or {},
            stop_reason=stop_reason,
        )


def _parse_json_text(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end < 0 or end <= start:
            raise
        value = json.loads(text[start : end + 1])

    if not isinstance(value, dict):
        raise ValueError("expected JSON object response")
    return value


def _strict_object_schema(schema: Any) -> Any:
    """Return a deep copy of *schema* with ``additionalProperties: false`` on
    every nested ``object``.

    Anthropic's structured-output strict mode rejects any object schema where
    ``additionalProperties`` is missing or set to true. Centralising the
    normalisation here means individual skill schemas can stay readable —
    they don't have to repeat ``additionalProperties: false`` on every nested
    object — and we get a single place to lock the invariant in tests.

    Behaviour:
    - ``type == "object"`` (or ``object`` listed in a type union): set
      ``additionalProperties`` to ``False`` regardless of prior value.
    - Walks ``properties``, ``items``, ``allOf``, ``anyOf``, ``oneOf``,
      ``$defs``, ``definitions``, ``prefixItems``.
    - Non-dict values pass through unchanged.
    """
    if isinstance(schema, list):
        return [_strict_object_schema(item) for item in schema]
    if not isinstance(schema, dict):
        return schema

    normalized: dict[str, Any] = {}
    for key, value in schema.items():
        if key in {
            "properties",
            "$defs",
            "definitions",
            "patternProperties",
        } and isinstance(value, dict):
            normalized[key] = {
                child_key: _strict_object_schema(child_value)
                for child_key, child_value in value.items()
            }
        elif key in {"items", "additionalItems", "contains", "not", "if", "then", "else"}:
            normalized[key] = _strict_object_schema(value)
        elif key in {"allOf", "anyOf", "oneOf", "prefixItems"} and isinstance(value, list):
            normalized[key] = [_strict_object_schema(child) for child in value]
        else:
            normalized[key] = value

    schema_type = normalized.get("type")
    is_object = schema_type == "object" or (
        isinstance(schema_type, list) and "object" in schema_type
    )
    if is_object:
        normalized["additionalProperties"] = False
    return normalized
