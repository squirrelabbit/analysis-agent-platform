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
    ) -> dict[str, Any]:
        if not self.is_enabled():
            raise ValueError("ANTHROPIC_API_KEY is required")

        payload = {
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
                    "schema": schema,
                }
            },
        }
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

        text_blocks = []
        for item in body.get("content", []):
            if item.get("type") == "text":
                text_blocks.append(item.get("text", ""))
        if not text_blocks:
            raise ValueError("anthropic response did not contain text blocks")

        return _parse_json_text("".join(text_blocks))


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
