"""clean 공백/줄바꿈 정규화 테스트 (류소현 개선요청 2026-06-30).

요청 명세: 줄바꿈·띄어쓰기가 2개 이상 연속이면 1개만 남기고, 1개면 그대로 유지.
줄바꿈은 공백으로 합치지 않고 보존(가독성).
"""
from __future__ import annotations

import unittest

from python_ai_worker import runtime as rt
from python_ai_worker.dataset_build.clean import _normalize_whitespace


class NormalizeWhitespaceTests(unittest.TestCase):
    def test_multiple_spaces_to_one(self) -> None:
        self.assertEqual(_normalize_whitespace("a     b"), "a b")

    def test_single_space_kept(self) -> None:
        self.assertEqual(_normalize_whitespace("a b c"), "a b c")

    def test_multiple_newlines_to_one(self) -> None:
        self.assertEqual(_normalize_whitespace("a\n\n\nb"), "a\nb")

    def test_blank_line_removed(self) -> None:
        # 빈 줄(\n\n)도 단일 \n (요청: 줄바꿈 2개 이상 → 1개)
        self.assertEqual(_normalize_whitespace("문장1\n\n문장2"), "문장1\n문장2")

    def test_single_newline_kept(self) -> None:
        self.assertEqual(_normalize_whitespace("a\nb"), "a\nb")

    def test_newline_preserved_not_spaced(self) -> None:
        # 핵심: 줄바꿈을 공백으로 합치지 않는다(옛 버그 = 줄바꿈 삭제).
        self.assertEqual(
            _normalize_whitespace("첫 줄\n둘째 줄"), "첫 줄\n둘째 줄"
        )

    def test_strip_line_edge_spaces(self) -> None:
        self.assertEqual(_normalize_whitespace("a  \n  b"), "a\nb")

    def test_tabs_collapse_to_space(self) -> None:
        self.assertEqual(_normalize_whitespace("a\t\tb"), "a b")

    def test_crlf_normalized(self) -> None:
        self.assertEqual(_normalize_whitespace("a\r\n\r\nb"), "a\nb")

    def test_outer_whitespace_stripped(self) -> None:
        self.assertEqual(_normalize_whitespace("  hello  "), "hello")

    def test_empty(self) -> None:
        self.assertEqual(_normalize_whitespace(""), "")

    def test_mixed_real_example(self) -> None:
        raw = "강릉  문화재야행   \n\n\n  너무 좋았어요!!   \n   재방문 의사 있음"
        self.assertEqual(
            _normalize_whitespace(raw),
            "강릉 문화재야행\n너무 좋았어요!!\n재방문 의사 있음",
        )


class CleanPipelineNewlinePreservationTests(unittest.TestCase):
    """end-to-end 회귀: `_strip_known_noise_phrases`(내부 _normalize_prepared_text) →
    `_normalize_whitespace` 전체 경로에서 줄바꿈이 살아남는지 잠근다. 1차 수정이
    _normalize_prepared_text의 `\\s+` 합치기를 놓쳐 \\n이 공백으로 죽던 회귀(#17)."""

    def _full_clean(self, text: str) -> str:
        # clean.py:245 의 실제 순서 그대로.
        return _normalize_whitespace(rt._strip_known_noise_phrases(text))

    def test_single_newline_survives_full_path(self) -> None:
        self.assertEqual(self._full_clean("제목\n본문"), "제목\n본문")

    def test_blank_lines_collapse_to_single_newline(self) -> None:
        self.assertEqual(self._full_clean("제목\n\n\n본문"), "제목\n본문")

    def test_newline_not_converted_to_space(self) -> None:
        result = self._full_clean("첫 줄\n둘째 줄")
        self.assertIn("\n", result)
        self.assertEqual(result, "첫 줄\n둘째 줄")

    def test_intraline_multiple_spaces_still_collapse(self) -> None:
        self.assertEqual(self._full_clean("a     b\nc     d"), "a b\nc d")


if __name__ == "__main__":
    unittest.main()
