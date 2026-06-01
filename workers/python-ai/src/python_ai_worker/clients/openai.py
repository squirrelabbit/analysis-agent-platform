from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable
from urllib import request


@dataclass(frozen=True)
class OpenAIEmbeddingConfig:
    api_key: str | None
    model: str
    api_url: str
    timeout_sec: float
    dimensions: int | None = None


class OpenAIEmbeddingClient:
    def __init__(
        self,
        config: OpenAIEmbeddingConfig,
        urlopen: Callable[..., Any] | None = None,
    ) -> None:
        self._config = config
        self._urlopen = urlopen or request.urlopen

    def is_enabled(self) -> bool:
        return bool(self._config.api_key)

    def create_embeddings(
        self,
        *,
        inputs: list[str],
        model_override: str = "",
        dimensions_override: int | None = None,
    ) -> dict[str, Any]:
        if not self.is_enabled():
            raise ValueError("OPENAI_API_KEY is required")

        model = model_override.strip() or self._config.model
        dimensions = dimensions_override if dimensions_override and dimensions_override > 0 else self._config.dimensions
        payload: dict[str, Any] = {
            "model": model,
            "input": inputs,
        }
        if dimensions and model.startswith("text-embedding-3"):
            payload["dimensions"] = dimensions

        raw_body = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            self._config.api_url,
            data=raw_body,
            method="POST",
            headers={
                "content-type": "application/json",
                "authorization": f"Bearer {self._config.api_key or ''}",
            },
        )

        with self._urlopen(http_request, timeout=self._config.timeout_sec) as response:
            body = json.loads(response.read().decode("utf-8"))

        data = list(body.get("data") or [])
        data.sort(key=lambda item: int(item.get("index") or 0))
        embeddings = [list(item.get("embedding") or []) for item in data]
        if len(embeddings) != len(inputs):
            raise ValueError("openai embedding response size mismatch")

        return {
            "model": str(body.get("model") or model).strip() or model,
            "embeddings": embeddings,
            "usage": body.get("usage") or {},
        }
