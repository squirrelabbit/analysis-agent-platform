"""clean 공백/줄바꿈 정규화 테스트 (류소현 개선요청 2026-06-30).

요청 명세: 줄바꿈·띄어쓰기가 2개 이상 연속이면 1개만 남기고, 1개면 그대로 유지.
줄바꿈은 공백으로 합치지 않고 보존(가독성).
"""
from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
