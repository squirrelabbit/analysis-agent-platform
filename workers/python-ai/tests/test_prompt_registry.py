from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import python_ai_worker.registries.prompt as prompt_registry_module
from python_ai_worker.registries.prompt import (
    available_prompt_versions,
    prompt_catalog,
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

    def test_prompt_catalog_reads_front_matter_and_excludes_non_templates(self) -> None:
        # (β2 / 5/19) prepare/sentiment dataset_build task 제거 후에도 prompt
        # 카탈로그 자체 (front-matter parse + README/CHANGELOG 제외)는
        # execution_final_answer / planner / issue_summary 등 다른 prompt
        # 그룹에서 그대로 쓰이므로 동등 fixture로 검증.
        temp_dir = Path(tempfile.mkdtemp())
        (temp_dir / "README.md").write_text("# readme", encoding="utf-8")
        (temp_dir / "CHANGELOG.md").write_text("# changelog", encoding="utf-8")
        (temp_dir / "custom-final-answer-v1.md").write_text(
            "---\n"
            "title: Custom final answer\n"
            "operation: execution_final_answer\n"
            "status: experimental\n"
            "summary: custom summary\n"
            "---\n\n"
            "Prompt body {{question}}\n",
            encoding="utf-8",
        )

        with patch.dict("os.environ", {"PYTHON_AI_PROMPTS_DIR": str(temp_dir)}, clear=False):
            versions = available_prompt_versions()
            catalog = prompt_catalog()

        self.assertEqual(versions, ["custom-final-answer-v1"])
        self.assertEqual(catalog[0]["title"], "Custom final answer")
        self.assertEqual(catalog[0]["operation"], "execution_final_answer")
        self.assertEqual(catalog[0]["status"], "experimental")
        self.assertEqual(catalog[0]["summary"], "custom summary")

    # δ-3 (5/21) — test_render_execution_final_answer_prompt_supports_markdown_template /
    # test_render_execution_final_answer_prompt_with_cache_splits_at_marker
    # 2건 제거. execution_final_answer skill과 v1 prompt 파일이 모두
    # 삭제됐다. 답변 본문 합성은 analyze_v2의 plan_v2 + present skill로
    # 이전됨.


if __name__ == "__main__":
    unittest.main()
