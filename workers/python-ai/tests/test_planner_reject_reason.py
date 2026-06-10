"""reject reason taxonomy (silverone 2026-06-01, PR1) 잠금.

answerable=false plan을 validator/composer/executor 3층에서 검증:
- validator: reason(3종)·message·steps-empty·capability_gap shape 검증, 하위 호환.
- composer: reason별 메시지 렌더 + display=null + metadata.reason/capability_gap.
- executor: artifact 없이 short-circuit (raw row/present 생성 안 함).
"""

from __future__ import annotations

import unittest

from python_ai_worker.composer import compose_answer
from python_ai_worker.executor.service import execute_analyze_plan
from python_ai_worker.planner import collect_plan_issues


def _codes(plan: dict) -> list[str]:
    return [issue.code for issue in collect_plan_issues(plan)]


OUT_OF_SCOPE = {
    "plan_version": "v2",
    "answerable": False,
    "reason": "out_of_dataset_scope",
    "message": "선택한 데이터셋에는 날씨 정보가 없어 답변할 수 없습니다.",
    "steps": [],
}

UNSUPPORTED_SKILL = {
    "plan_version": "v2",
    "answerable": False,
    "reason": "unsupported_skill",
    "message": "데이터셋 관련 질문이지만 현재 분석 기능으로는 처리할 수 없습니다.",
    "steps": [],
    "capability_gap": {
        "requested_capability": "text_clustering",
        "suggested_skill": "cluster_texts",
        "evidence": "비슷한 주제끼리 묶어달라고 요청함",
    },
}


CLARIFICATION_REQUIRED = {
    "plan_version": "v2",
    "answerable": False,
    "reason": "clarification_required",
    "message": "기준 날짜와 전후 며칠을 알려주세요. (예: 2025-08-15 전후 7일)",
    "steps": [],
}


class RejectPlanValidatorTests(unittest.TestCase):
    def test_out_of_dataset_scope_valid(self) -> None:
        self.assertEqual(collect_plan_issues(OUT_OF_SCOPE), [])

    def test_unsupported_skill_with_capability_gap_valid(self) -> None:
        self.assertEqual(collect_plan_issues(UNSUPPORTED_SKILL), [])

    def test_clarification_required_valid(self) -> None:
        # silverone 2026-06-10 — 기간/기준 모호 거절. capability_gap 없이 valid.
        self.assertEqual(collect_plan_issues(CLARIFICATION_REQUIRED), [])

    def test_invalid_reason_rejected(self) -> None:
        plan = {**OUT_OF_SCOPE, "reason": "nope"}
        self.assertIn("plan.reason_invalid", _codes(plan))

    def test_missing_message_rejected(self) -> None:
        plan = {**OUT_OF_SCOPE, "message": "  "}
        self.assertIn("plan.reject_message_required", _codes(plan))

    def test_non_empty_steps_rejected(self) -> None:
        plan = {**OUT_OF_SCOPE, "steps": [{"id": "x", "skill": "present", "params": {}}]}
        self.assertIn("plan.reject_steps_not_empty", _codes(plan))

    def test_capability_gap_must_be_object(self) -> None:
        plan = {**UNSUPPORTED_SKILL, "capability_gap": "text_clustering"}
        self.assertIn("plan.capability_gap_not_object", _codes(plan))

    def test_answerable_true_still_requires_steps(self) -> None:
        # 하위 호환: answerable 생략/true면 빈 steps는 기존대로 거부.
        plan = {"plan_version": "v2", "steps": []}
        self.assertIn("plan.steps_empty", _codes(plan))


class RejectComposerTests(unittest.TestCase):
    def test_out_of_scope_renders_message_no_table(self) -> None:
        out = compose_answer(user_question="오늘 날씨 어때?", present=None, plan=OUT_OF_SCOPE)
        self.assertEqual(out["assistant_content"], OUT_OF_SCOPE["message"])
        self.assertIsNone(out["display"])  # 테이블/경고 없음
        self.assertEqual(out["metadata"]["mode"], "rejected")
        self.assertEqual(out["metadata"]["reason"], "out_of_dataset_scope")
        self.assertNotIn("capability_gap", out["metadata"])

    def test_unsupported_skill_passes_capability_gap_to_metadata(self) -> None:
        out = compose_answer(
            user_question="비슷한 후기끼리 묶어줘", present=None, plan=UNSUPPORTED_SKILL
        )
        self.assertIsNone(out["display"])
        self.assertEqual(out["metadata"]["reason"], "unsupported_skill")
        self.assertEqual(
            out["metadata"]["capability_gap"]["suggested_skill"], "cluster_texts"
        )

    def test_reject_ignores_present_even_if_passed(self) -> None:
        # answerable=false면 present가 넘어와도 거절 렌더(테이블 만들지 않음).
        present = {"format": "table", "rows": [{"row_id": "r0"}], "total_rows": 2121,
                   "returned_rows": 1, "truncated": True}
        out = compose_answer(user_question="오늘 날씨", present=present, plan=OUT_OF_SCOPE)
        self.assertIsNone(out["display"])
        self.assertEqual(out["metadata"]["mode"], "rejected")


class RejectExecutorTests(unittest.TestCase):
    def test_execute_short_circuits_without_artifacts(self) -> None:
        # artifact_paths=None이어도 _resolve_artifact_paths(미구현 raise)를 안 타고
        # 거절 응답을 만들어야 한다.
        resp = execute_analyze_plan("ds-1", OUT_OF_SCOPE, artifact_paths=None,
                                    user_question="오늘 날씨 어때?")
        self.assertEqual(resp["steps"], [])
        self.assertIsNone(resp["present"])
        self.assertIsNone(resp["artifact_paths"])
        self.assertEqual(resp["plan_version"], "v2")
        self.assertEqual(resp["composer"]["metadata"]["reason"], "out_of_dataset_scope")
        self.assertIsNone(resp["composer"]["display"])


if __name__ == "__main__":
    unittest.main()
