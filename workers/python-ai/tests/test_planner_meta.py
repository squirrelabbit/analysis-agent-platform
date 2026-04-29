from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker.planner_meta import DEFAULT_ACTIVE_LAYERS, select_active_layers


class _FakeAnthropicClient:
    class _Config:
        model = "claude-test"

    _config = _Config()

    def is_enabled(self) -> bool:
        return True

    def create_json(self, **_: object) -> dict[str, object]:
        raise AssertionError("patched _create_json_logged should intercept create_json")


class MetaPlannerTests(unittest.TestCase):
    def test_rule_path_matches_known_query_pool(self) -> None:
        cases = [
            ("VOC 이슈를 요약해줘", {"preprocess", "aggregate", "summarize"}, "default"),
            ("최근 결제 오류 추세를 보여줘", {"preprocess", "aggregate", "summarize"}, "rule"),
            ("결제 오류 관련 명사 키워드를 추출해줘", {"preprocess", "aggregate"}, "rule"),
            ("문장 단위로 나눠서 보여줘", {"preprocess"}, "rule"),
            ("전주 대비 결제 오류가 얼마나 달라졌는지 비교해줘", {"preprocess", "aggregate", "summarize"}, "rule"),
            ("채널별 이슈를 분해해서 보여줘", {"preprocess", "aggregate", "summarize"}, "rule"),
            ("긍정 부정 감성 분포를 보여줘", {"preprocess", "summarize"}, "rule"),
            ("주요 이슈 군집을 묶어서 보여줘", {"preprocess", "retrieve", "summarize"}, "rule"),
            ("최근 문의 중에서 주요 이슈 군집을 묶어서 보여줘", {"preprocess", "retrieve", "summarize"}, "rule"),
            ("카테고리 태그 기준으로 이슈를 분류해줘", {"preprocess", "aggregate", "summarize"}, "rule"),
        ]

        for question, expected_layers, expected_confidence in cases:
            with self.subTest(question=question):
                result = select_active_layers(question)
                self.assertEqual(result.active_layers, frozenset(expected_layers))
                self.assertEqual(result.confidence, expected_confidence)

    def test_llm_fallback_is_used_when_no_rule_match_exists(self) -> None:
        with patch(
            "python_ai_worker.planner_meta._create_json_logged",
            return_value={"layers": ["retrieve", "summarize"]},
        ):
            result = select_active_layers(
                "정형화되지 않은 완전히 새로운 질문",
                anthropic_client=_FakeAnthropicClient(),
            )

        self.assertEqual(result.active_layers, frozenset({"retrieve", "summarize"}))
        self.assertEqual(result.confidence, "llm")
        self.assertEqual(result.trigger_matches, ("llm_fallback",))

    def test_default_fallback_is_used_without_rule_or_llm(self) -> None:
        result = select_active_layers("정형화되지 않은 완전히 새로운 질문")
        self.assertEqual(result.active_layers, DEFAULT_ACTIVE_LAYERS)
        self.assertEqual(result.confidence, "default")
        self.assertEqual(result.trigger_matches, ())


if __name__ == "__main__":
    unittest.main()
