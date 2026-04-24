from __future__ import annotations

import unittest

from python_ai_worker.skill_contracts import SkillOutputError, validate_task_result


class SkillContractValidationTests(unittest.TestCase):
    def test_validate_task_result_requires_artifact_for_non_planner_tasks(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "artifact object"):
            validate_task_result("keyword_frequency", {}, {"notes": []})

    def test_validate_task_result_requires_matching_skill_name(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "skill_name mismatch"):
            validate_task_result(
                "keyword_frequency",
                {},
                {
                    "artifact": {
                        "skill_name": "document_filter",
                    }
                },
            )

    def test_validate_task_result_allows_planner_payload_with_steps(self) -> None:
        validate_task_result(
            "planner",
            {},
            {
                "plan": {
                    "steps": [],
                    "metadata": {
                        "contains_llm_stage": False,
                        "llm_stages": [],
                    },
                }
            },
        )

    def test_validate_task_result_rejects_strict_fail_empty_cluster_summary(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "strict_fail"):
            validate_task_result(
                "issue_cluster_summary",
                {},
                {
                    "artifact": {
                        "skill_name": "issue_cluster_summary",
                        "ranked_issues": [],
                        "coverage": {
                            "documents_considered": 0,
                            "total_documents": 3,
                        },
                        "result_scope": "cluster_subset",
                        "runtime_result_scope": "cluster_subset",
                        "quality_tier": "heuristic",
                    }
                },
            )


if __name__ == "__main__":
    unittest.main()
