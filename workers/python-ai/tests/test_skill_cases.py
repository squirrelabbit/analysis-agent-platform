from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker._migration_targets import canonical_skill_name
from python_ai_worker.devtools.skill_cases import available_skill_cases, run_skill_case
from python_ai_worker.skill_contracts import SkillOutputError
from python_ai_worker.task_router import task_handlers


class SkillCaseTests(unittest.TestCase):
    class _DummyClient:
        def __init__(self) -> None:
            self._config = type("Config", (), {"model": "claude-test"})()

        def is_enabled(self) -> bool:
            return True

    def test_skill_cases_cover_all_task_handlers(self) -> None:
        self.assertEqual(set(available_skill_cases()), set(task_handlers()))

    def test_each_skill_case_runs(self) -> None:
        with patch(
            "python_ai_worker.skills._summarize_impl.rt._anthropic_client",
            return_value=self._DummyClient(),
        ), patch(
            "python_ai_worker.skills._summarize_impl.rt._run_evidence_pack_with_llm",
            return_value={
                "notes": ["llm presenter stubbed in test"],
                "artifact": {
                    "summary": "결제 오류 근거를 정리했습니다.",
                    "key_findings": ["결제 오류 근거가 반복됩니다."],
                    "evidence": [
                        {
                            "rank": 1,
                            "source_index": 0,
                            "snippet": "결제 오류가 반복 발생했습니다.",
                            "rationale": "대표 근거입니다.",
                        }
                    ],
                    "follow_up_questions": ["대표 원문을 더 볼까요?"],
                    "citation_mode": "row",
                },
            },
        ), patch(
            "python_ai_worker.skills.presentation.rt._anthropic_client",
            return_value=self._DummyClient(),
        ), patch(
            "python_ai_worker.skills.presentation.rt._run_execution_final_answer_with_llm",
            return_value={
                "answer": {
                    "schema_version": "execution-final-answer-v1",
                    "status": "completed",
                    "generation_mode": "llm",
                    "headline": "결제 오류 요약",
                    "answer_text": "결제 오류가 반복되고 있습니다.",
                    "key_points": ["결제 오류가 반복됩니다."],
                    "caveats": ["샘플 기준 해석입니다."],
                    "evidence": [
                        {
                            "rank": 1,
                            "snippet": "결제 오류가 반복 발생했습니다.",
                            "rationale": "대표 근거입니다.",
                        }
                    ],
                    "follow_up_questions": ["대표 원문을 더 볼까요?"],
                    "model": "claude-test",
                    "generated_at": "2026-04-24T00:00:00+09:00",
                }
            },
        ):
            for skill_name in sorted(available_skill_cases()):
                with self.subTest(skill_name=skill_name):
                    if skill_name == "issue_cluster_summary":
                        with self.assertRaises(SkillOutputError):
                            run_skill_case(skill_name)
                        continue

                    result = run_skill_case(skill_name)
                    self.assertEqual(result["skill_name"], skill_name)
                    self.assertTrue(result["steps"])

                    final_result = result["final_result"]
                    if skill_name == "planner":
                        self.assertIn("plan", final_result)
                        self.assertTrue(final_result["plan"]["steps"])
                        continue

                    self.assertIn("artifact", final_result)
                    # ADR-009 F1: during the deprecation period, deprecated and
                    # canonical names share a handler — compare by canonical
                    # identity rather than raw equality.
                    self.assertEqual(
                        canonical_skill_name(final_result["artifact"]["skill_name"]),
                        canonical_skill_name(skill_name),
                    )


if __name__ == "__main__":
    unittest.main()
