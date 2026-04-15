from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import python_ai_worker.prompt_registry as prompt_registry_module
from python_ai_worker.anthropic_client import AnthropicJSONResponse
from python_ai_worker.prompt_registry import (
    available_prompt_versions,
    prompt_catalog,
    render_execution_final_answer_prompt,
    render_prepare_batch_prompt,
    render_prepare_prompt,
    render_sentiment_batch_prompt,
    render_sentiment_prompt,
)
from python_ai_worker.runtime.llm import (
    _anthropic_prepare_client,
    _label_sentiments_with_llm,
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

    def create_json_response(
        self,
        *,
        prompt: str,
        schema: dict[str, object],
        max_tokens: int | None = None,
    ) -> AnthropicJSONResponse:
        self.last_prompt = prompt
        return AnthropicJSONResponse(
            body=self._response,
            usage={"input_tokens": 100, "output_tokens": 20},
        )


class PromptRegistryTests(unittest.TestCase):
    def test_prompt_dir_resolves_container_style_layout(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prompt_dir = temp_dir / "app" / "config" / "prompts"
        prompt_dir.mkdir(parents=True)

        with patch.dict("os.environ", {}, clear=False):
            with patch.object(
                prompt_registry_module,
                "__file__",
                str(temp_dir / "app" / "src" / "python_ai_worker" / "prompt_registry.py"),
            ):
                resolved = prompt_registry_module._prompt_templates_dir()

        self.assertEqual(resolved, prompt_dir.resolve())

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

    def test_render_prepare_prompt_uses_markdown_template_directory_override(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        (temp_dir / "custom-prepare-v1.md").write_text(
            "## Task\n\nCustom prepare prompt\n\n{{raw_text}}\n",
            encoding="utf-8",
        )

        with patch.dict("os.environ", {"PYTHON_AI_PROMPTS_DIR": str(temp_dir)}, clear=False):
            version, prompt = render_prepare_prompt("커스텀 테스트", version="custom-prepare-v1")

        self.assertEqual(version, "custom-prepare-v1")
        self.assertIn("Custom prepare prompt", prompt)
        self.assertIn("커스텀 테스트", prompt)

    def test_prompt_catalog_reads_front_matter_and_excludes_non_templates(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        (temp_dir / "README.md").write_text("# readme", encoding="utf-8")
        (temp_dir / "CHANGELOG.md").write_text("# changelog", encoding="utf-8")
        (temp_dir / "custom-prepare-v1.md").write_text(
            "---\n"
            "title: Custom prepare\n"
            "operation: prepare\n"
            "status: experimental\n"
            "summary: custom summary\n"
            "---\n\n"
            "Prompt body {{raw_text}}\n",
            encoding="utf-8",
        )

        with patch.dict("os.environ", {"PYTHON_AI_PROMPTS_DIR": str(temp_dir)}, clear=False):
            versions = available_prompt_versions()
            catalog = prompt_catalog()
            version, prompt = render_prepare_prompt("테스트", version="custom-prepare-v1")

        self.assertEqual(versions, ["custom-prepare-v1"])
        self.assertEqual(catalog[0]["title"], "Custom prepare")
        self.assertEqual(catalog[0]["operation"], "prepare")
        self.assertEqual(catalog[0]["status"], "experimental")
        self.assertEqual(catalog[0]["summary"], "custom summary")
        self.assertNotIn("title:", prompt)
        self.assertEqual(version, "custom-prepare-v1")

    def test_render_prepare_prompt_uses_inline_template_override(self) -> None:
        version, prompt = render_prepare_prompt(
            "프로젝트 전용 테스트",
            version="project-prepare-v1",
            template_override="---\ntitle: Project prepare\noperation: prepare\n---\n프로젝트 전용 전처리\n{{raw_text}}\n",
        )

        self.assertEqual(version, "project-prepare-v1")
        self.assertIn("프로젝트 전용 전처리", prompt)
        self.assertIn("프로젝트 전용 테스트", prompt)
        self.assertNotIn("title:", prompt)

    def test_render_sentiment_batch_prompt_uses_inline_template_override_without_version_remap(self) -> None:
        version, prompt = render_sentiment_batch_prompt(
            ["결제 오류가 반복 발생했습니다", "문의 접수 후 확인 중입니다"],
            version="project-sentiment-v1",
            template_override="---\ntitle: Project sentiment batch\noperation: sentiment_batch\n---\n프로젝트 감성 배치\n{{rows_json}}\n",
        )

        self.assertEqual(version, "project-sentiment-v1")
        self.assertIn("프로젝트 감성 배치", prompt)
        self.assertIn("결제 오류가 반복 발생했습니다", prompt)

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
            result, usage = _prepare_row_with_llm(client, "결제 오류가 반복 발생했습니다!!!")

        self.assertEqual(result["prompt_version"], "dataset-prepare-anthropic-v2")
        self.assertEqual(usage["total_tokens"], 120)
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
            results, usage = _prepare_rows_with_llm(client, ["결제 오류가 반복 발생했습니다!!!", "로그인이 자주 실패합니다"])

        self.assertEqual(results[0]["prompt_version"], "dataset-prepare-anthropic-batch-v2")
        self.assertEqual(results[1]["prompt_version"], "dataset-prepare-anthropic-batch-v2")
        self.assertEqual(usage["total_tokens"], 120)
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

    def test_label_sentiments_with_llm_resolves_batch_prompt_from_row_version(self) -> None:
        client = _RecordingClient(
            {
                "rows": [
                    {"label": "negative", "confidence": 0.8, "reason": "complaint"},
                    {"label": "neutral", "confidence": 0.7, "reason": "status update"},
                ]
            }
        )

        labels, usage = _label_sentiments_with_llm(
            client,
            ["결제 오류가 반복 발생했습니다", "문의 접수 후 확인 중입니다"],
            batch_size=2,
            prompt_version_override="sentiment-anthropic-v2",
        )

        self.assertEqual(labels[0]["prompt_version"], "sentiment-anthropic-batch-v2")
        self.assertEqual(labels[1]["prompt_version"], "sentiment-anthropic-batch-v2")
        self.assertIn("batch mode", client.last_prompt)
        self.assertEqual(usage["total_tokens"], 120)

    def test_render_sentiment_batch_prompt_supports_multiple_versions(self) -> None:
        version, prompt = render_sentiment_batch_prompt(
            ["결제 오류가 반복 발생했습니다", "문의 접수 후 확인 중입니다"],
            version="sentiment-anthropic-v2",
        )

        self.assertEqual(version, "sentiment-anthropic-batch-v2")
        self.assertIn("Prefer neutral over negative", prompt)

    def test_render_execution_final_answer_prompt_supports_markdown_template(self) -> None:
        version, prompt = render_execution_final_answer_prompt(
            question="결제 오류 핵심을 알려줘",
            scenario_json='{"scenario_id":"S1"}',
            result_json='{"status":"completed"}',
            evidence_json='[{"evidence_id":"evidence-1"}]',
            version="execution-final-answer-v1",
        )

        self.assertEqual(version, "execution-final-answer-v1")
        self.assertIn("결제 오류 핵심을 알려줘", prompt)
        self.assertIn('"status":"completed"', prompt.replace(" ", ""))


if __name__ == "__main__":
    unittest.main()
