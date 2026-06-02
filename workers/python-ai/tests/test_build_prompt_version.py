"""dataset_build prompt version 선택 반영 (silverone 2026-06-02, 이슈 #1).

/prompt_options에서 고른 version(stem)이 doc_genuineness / clause_label 빌드의
실제 prompt 파일 로드 + artifact prompt_version 라벨에 반영되는지 잠근다.
codex no-ship 지적("선택 version이 실행에서 무시됨") 회귀 방지.

격리된 PYTHON_AI_PROMPTS_DIR fixture로 실제 config/prompts와 무관하게 검증한다.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from python_ai_worker.dataset_build.clause_label import (
    _load_prompt_template as clause_load,
)
from python_ai_worker.dataset_build.doc_genuineness import (
    _load_prompt_template as doc_load,
)
from python_ai_worker.prompt_options import PROMPTS_DIR_ENV, PromptOptionsError


@contextmanager
def _prompts_dir():
    prev = os.environ.get(PROMPTS_DIR_ENV)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for task, default in (("doc_genuineness", "v1"), ("clause_label", "v3")):
            d = root / task
            d.mkdir(parents=True)
            (d / f"{default}.md").write_text(f"DEFAULT {task} body", encoding="utf-8")
            (d / "v2.md").write_text(f"V2 {task} body", encoding="utf-8")
            (d / "index.yaml").write_text(f"default: {default}\n", encoding="utf-8")
        os.environ[PROMPTS_DIR_ENV] = str(root)
        try:
            yield root
        finally:
            if prev is None:
                os.environ.pop(PROMPTS_DIR_ENV, None)
            else:
                os.environ[PROMPTS_DIR_ENV] = prev


class DocGenuinenessVersionTests(unittest.TestCase):
    def test_default_when_version_omitted(self) -> None:
        with _prompts_dir():
            body, version = doc_load({})
        self.assertEqual(version, "v1")
        self.assertIn("DEFAULT doc_genuineness body", body)

    def test_selected_non_default_version_loaded(self) -> None:
        with _prompts_dir():
            body, version = doc_load({"doc_genuineness_prompt_version": "v2"})
        self.assertEqual(version, "v2")
        self.assertIn("V2 doc_genuineness body", body)

    def test_unknown_version_rejected(self) -> None:
        with _prompts_dir():
            with self.assertRaises(PromptOptionsError):
                doc_load({"doc_genuineness_prompt_version": "v9"})

    def test_inline_overrides_and_keeps_inline_label(self) -> None:
        with _prompts_dir():
            body, version = doc_load({
                "doc_genuineness_prompt_content": "INLINE BODY",
                "doc_genuineness_prompt_version": "custom-x",
            })
        self.assertEqual(version, "custom-x")
        self.assertEqual(body, "INLINE BODY")


class ClauseLabelVersionTests(unittest.TestCase):
    def test_default_when_version_omitted(self) -> None:
        with _prompts_dir():
            body, version = clause_load({})
        self.assertEqual(version, "v3")
        self.assertIn("DEFAULT clause_label body", body)

    def test_selected_non_default_version_loaded(self) -> None:
        with _prompts_dir():
            body, version = clause_load({"clause_label_prompt_version": "v2"})
        self.assertEqual(version, "v2")
        self.assertIn("V2 clause_label body", body)

    def test_unknown_version_rejected(self) -> None:
        with _prompts_dir():
            with self.assertRaises(PromptOptionsError):
                clause_load({"clause_label_prompt_version": "v9"})


if __name__ == "__main__":
    unittest.main()
