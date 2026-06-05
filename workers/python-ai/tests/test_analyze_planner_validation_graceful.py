"""작업 A — planner validation 실패 graceful 처리 (silverone 2026-06-05).

planner가 repair 후에도 유효 plan을 못 만들면(PlanValidationError) user_question
경로(plan_and_execute_analyze)는 raw 예외(→400/500)가 아니라 answerable=false
graceful 거절(reason=planner_validation_error)을 200으로 반환해야 한다.
direct-plan 디버그 경로(execute_analyze_plan 직접)는 그대로 raise 유지.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from python_ai_worker.executor import service
from python_ai_worker.planner import PlanValidationError


class PlannerValidationGracefulTest(unittest.TestCase):
    def test_validation_error_returns_graceful_reject(self):
        issues = [
            SimpleNamespace(
                code="params.input_unknown",
                message="'input' references unknown table or step id 'distribution_result'",
            )
        ]
        with patch.object(
            service, "generate_plan", side_effect=PlanValidationError(issues)
        ):
            result = service.plan_and_execute_analyze(
                "v1",
                "비진성으로 분류된 문서 수와 전체 대비 비율을 알려줘",
                anthropic_client=object(),  # _default_anthropic_client 우회
            )
        self.assertIsInstance(result, dict)
        # 거절 응답: 실행 안 함.
        self.assertEqual(result["steps"], [])
        self.assertIsNone(result["present"])
        self.assertFalse(result["plan"].get("answerable"))
        # structured reason 보존.
        meta = result["composer"]["metadata"]
        self.assertEqual(meta["mode"], "rejected")
        self.assertEqual(meta["reason"], "planner_validation_error")

    def test_direct_plan_path_still_raises(self):
        # execute_analyze_plan 직접 호출(direct-plan 디버그)은 dangling input을 그대로 raise.
        bad_plan = {
            "plan_version": "v2",
            "answerable": True,
            "steps": [
                {
                    "id": "p",
                    "skill": "present",
                    "params": {"input": "does_not_exist", "format": "table", "columns": ["x"]},
                }
            ],
        }
        with self.assertRaises(Exception):
            service.execute_analyze_plan(
                "v1", bad_plan, artifact_paths=None, user_question="x"
            )


if __name__ == "__main__":
    unittest.main()
