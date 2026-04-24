from __future__ import annotations

import unittest

from python_ai_worker.skill_contracts import SkillOutputError, validate_task_result


class SkillContractValidationTests(unittest.TestCase):
    def test_validate_task_result_requires_artifact_for_non_planner_tasks(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "artifact object"):
            validate_task_result("keyword_frequency", {"notes": []})

    def test_validate_task_result_requires_matching_skill_name(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "skill_name mismatch"):
            validate_task_result(
                "keyword_frequency",
                {
                    "artifact": {
                        "skill_name": "document_filter",
                    }
                },
            )

    def test_validate_task_result_allows_planner_payload_with_steps(self) -> None:
        validate_task_result(
            "planner",
            {
                "plan": {
                    "steps": [],
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
