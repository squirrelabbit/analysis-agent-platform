from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker.prompt_registry import (
    render_prepare_batch_prompt,
    render_prepare_prompt,
    render_sentiment_prompt,
)
from python_ai_worker.runtime.llm import (
    _anthropic_prepare_client,
    _label_sentiment_with_llm,
    _prepare_row_with_llm,
    _prepare_rows_with_llm,
)


class _RecordingClient:
    def __init__(self, response: dict[str, object]) -> None:
        self._response = response
        self._config = type("Config", (), {"model": "claude-haiku-test"})()
        self.last_prompt = ""

    def is_enabled(self) -> bool:
        return True

    def create_json(self, *, prompt: str, schema: dict[str, object], max_tokens: int | None = None) -> dict[str, object]:
        self.last_prompt = prompt
        return self._response


class PromptRegistryTests(unittest.TestCase):
    def test_prepare_client_defaults_to_haiku_model(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
                "ANTHROPIC_API_KEY": "test-key",
                "ANTHROPIC_MODEL": "claude-sonnet-test",
            },
            clear=True,
        ):
            client = _anthropic_prepare_client()

        self.assertIsNotNone(client)
        self.assertEqual(client._config.model, "claude-3-5-haiku-latest")

    def test_render_prepare_prompt_supports_multiple_versions(self) -> None:
        version, prompt = render_prepare_prompt("결제 오류가 반복 발생했습니다", version="dataset-prepare-anthropic-v2")

        self.assertEqual(version, "dataset-prepare-anthropic-v2")
        self.assertIn("Preserve the original language", prompt)

    def test_render_prepare_batch_prompt_supports_multiple_versions(self) -> None:
        version, prompt = render_prepare_batch_prompt(
            ["결제 오류가 반복 발생했습니다", "로그인이 자주 실패합니다"],
            version="dataset-prepare-anthropic-batch-v2",
        )

        self.assertEqual(version, "dataset-prepare-anthropic-batch-v2")
        self.assertIn("preserve issue-specific details", prompt)

    def test_render_sentiment_prompt_supports_multiple_versions(self) -> None:
        version, prompt = render_sentiment_prompt("문의 접수 후 확인 중입니다", version="sentiment-anthropic-v2")

        self.assertEqual(version, "sentiment-anthropic-v2")
        self.assertIn("Prefer neutral over negative", prompt)

    def test_prepare_row_with_llm_uses_configured_prompt_version(self) -> None:
        client = _RecordingClient(
            {
                "disposition": "keep",
                "normalized_text": "결제 오류가 반복 발생했습니다.",
                "reason": "noise removed",
                "quality_flags": ["normalized"],
            }
        )

        with patch.dict(
            "os.environ",
            {
                "ANTHROPIC_PREPARE_PROMPT_VERSION": "dataset-prepare-anthropic-v2",
            },
            clear=False,
        ):
            result = _prepare_row_with_llm(client, "결제 오류가 반복 발생했습니다!!!")

        self.assertEqual(result["prompt_version"], "dataset-prepare-anthropic-v2")
        self.assertIn("Preserve the original language", client.last_prompt)

    def test_prepare_rows_with_llm_uses_configured_batch_prompt_version(self) -> None:
        client = _RecordingClient(
            {
                "rows": [
                    {
                        "disposition": "keep",
                        "normalized_text": "결제 오류가 반복 발생했습니다.",
                        "reason": "noise removed",
                        "quality_flags": ["normalized"],
                    },
                    {
                        "disposition": "review",
                        "normalized_text": "로그인이 자주 실패합니다.",
                        "reason": "needs review",
                        "quality_flags": ["review_needed"],
                    },
                ]
            }
        )

        with patch.dict(
            "os.environ",
            {
                "ANTHROPIC_PREPARE_BATCH_PROMPT_VERSION": "dataset-prepare-anthropic-batch-v2",
            },
            clear=False,
        ):
            results = _prepare_rows_with_llm(client, ["결제 오류가 반복 발생했습니다!!!", "로그인이 자주 실패합니다"])

        self.assertEqual(results[0]["prompt_version"], "dataset-prepare-anthropic-batch-v2")
        self.assertEqual(results[1]["prompt_version"], "dataset-prepare-anthropic-batch-v2")
        self.assertIn("preserve issue-specific details", client.last_prompt)

    def test_label_sentiment_with_llm_uses_configured_prompt_version(self) -> None:
        client = _RecordingClient(
            {
                "label": "neutral",
                "confidence": 0.7,
                "reason": "status update without explicit dissatisfaction",
            }
        )

        with patch.dict(
            "os.environ",
            {
                "ANTHROPIC_SENTIMENT_PROMPT_VERSION": "sentiment-anthropic-v2",
            },
            clear=False,
        ):
            result = _label_sentiment_with_llm(client, "문의 접수 후 확인 중입니다")

        self.assertEqual(result["prompt_version"], "sentiment-anthropic-v2")
        self.assertIn("Prefer neutral over negative", client.last_prompt)


if __name__ == "__main__":
    unittest.main()
