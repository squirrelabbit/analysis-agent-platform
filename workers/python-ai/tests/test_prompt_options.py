"""prompt_options resolver tests (silverone 2026-06-02).

task-folder 구조(config/prompts/<task>/<version>.md + index.yaml) 해석 + API
계약(version/default/label, 본문/경로 미노출) 잠금. 실제 repo config/prompts와
무관하게 PYTHON_AI_PROMPTS_DIR override로 격리된 fixture에서 검증한다.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from python_ai_worker.prompt_options import (
    PROMPTS_DIR_ENV,
    PromptOptionsError,
    list_prompt_options,
    load_prompt_body,
    resolve_prompt_path,
)


@contextmanager
def _prompts_dir():
    """격리된 prompts root를 만들고 PYTHON_AI_PROMPTS_DIR로 가리킨다."""
    prev = os.environ.get(PROMPTS_DIR_ENV)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        os.environ[PROMPTS_DIR_ENV] = str(root)
        try:
            yield root
        finally:
            if prev is None:
                os.environ.pop(PROMPTS_DIR_ENV, None)
            else:
                os.environ[PROMPTS_DIR_ENV] = prev


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class ListPromptOptionsTests(unittest.TestCase):
    def test_lists_versions_default_and_h1_label(self) -> None:
        with _prompts_dir() as root:
            _write(root / "doc_genuineness" / "v1.md", "# 문서 진성 분류 v1\n\n본문...")
            _write(root / "doc_genuineness" / "v2.md", "---\ntitle: x\n---\n# 개정판\n본문")
            _write(root / "doc_genuineness" / "index.yaml", "default: v1\n")
            result = list_prompt_options("doc_genuineness")
        self.assertEqual(result["task"], "doc_genuineness")
        self.assertEqual(result["default"], "v1")
        versions = {v["version"]: v["label"] for v in result["versions"]}
        self.assertEqual(versions, {"v1": "문서 진성 분류 v1", "v2": "개정판"})

    def test_label_falls_back_to_version_when_no_h1(self) -> None:
        with _prompts_dir() as root:
            _write(root / "clause_label" / "v3.md", "---\ntitle: t\n---\n절 라벨링 본문 (H1 없음)")
            _write(root / "clause_label" / "index.yaml", "default: v3\n")
            result = list_prompt_options("clause_label")
        self.assertEqual(result["versions"], [{"version": "v3", "label": "v3"}])

    def test_response_excludes_body_and_path(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# L\nsecret body")
            _write(root / "t" / "index.yaml", "default: v1\n")
            result = list_prompt_options("t")
        flat = repr(result)
        self.assertNotIn("secret body", flat)
        self.assertNotIn(".md", flat)
        self.assertEqual(set(result.keys()), {"task", "default", "versions"})
        self.assertEqual(set(result["versions"][0].keys()), {"version", "label"})

    def test_index_default_must_exist_as_md(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# a")
            _write(root / "t" / "index.yaml", "default: v9\n")  # v9.md 없음
            with self.assertRaises(PromptOptionsError):
                list_prompt_options("t")

    def test_unknown_task_raises(self) -> None:
        with _prompts_dir():
            with self.assertRaises(PromptOptionsError):
                list_prompt_options("nope")

    def test_path_traversal_rejected(self) -> None:
        with _prompts_dir():
            with self.assertRaises(PromptOptionsError):
                list_prompt_options("../secrets")

    def test_missing_index_raises(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# a")
            with self.assertRaises(PromptOptionsError):
                list_prompt_options("t")

    def test_index_without_default_key_raises(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# a")
            _write(root / "t" / "index.yaml", "# 주석만 있음\n")
            with self.assertRaises(PromptOptionsError):
                list_prompt_options("t")


class ResolveAndLoadTests(unittest.TestCase):
    def test_resolve_default_and_explicit_version(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# a\nbody1")
            _write(root / "t" / "v2.md", "# b\nbody2")
            _write(root / "t" / "index.yaml", "default: v2\n")
            self.assertEqual(resolve_prompt_path("t").name, "v2.md")
            self.assertEqual(resolve_prompt_path("t", "v1").name, "v1.md")

    def test_load_strips_front_matter_and_returns_stem(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "---\ntitle: x\n---\n# H\n실제 본문")
            _write(root / "t" / "index.yaml", "default: v1\n")
            body, stem = load_prompt_body("t")
        self.assertEqual(stem, "v1")
        self.assertTrue(body.startswith("# H"))
        self.assertNotIn("title: x", body)

    def test_index_comment_and_quotes_parsed(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# a")
            _write(root / "t" / "index.yaml", 'default: "v1"  # trailing comment\n')
            self.assertEqual(resolve_prompt_path("t").name, "v1.md")

    def test_unknown_version_raises(self) -> None:
        with _prompts_dir() as root:
            _write(root / "t" / "v1.md", "# a")
            _write(root / "t" / "index.yaml", "default: v1\n")
            with self.assertRaises(PromptOptionsError):
                resolve_prompt_path("t", "v9")


class RealConfigTaskFoldersTests(unittest.TestCase):
    """실제 config/prompts task-folder가 resolve되는지 (env override 없이)."""

    def test_doc_genuineness_and_clause_label_real_folders(self) -> None:
        # 2026-06-25 — doc_genuineness default가 festival 통합 base v3로 전환됨.
        # clause_label default는 v3 유지(v5 전환은 behavioral parity 후 PR2-B 후속).
        for task, default in (("doc_genuineness", "v3"), ("clause_label", "v3")):
            with self.subTest(task=task):
                result = list_prompt_options(task)
                self.assertEqual(result["task"], task)
                self.assertEqual(result["default"], default)
                stems = {v["version"] for v in result["versions"]}
                self.assertIn(default, stems)
                body, stem = load_prompt_body(task)
                self.assertEqual(stem, default)
                self.assertTrue(body.strip())
                self.assertFalse(body.lstrip().startswith("---"))


if __name__ == "__main__":
    unittest.main()
