from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker.runtime.llm import _build_planner_system_prompt, _normalize_planner_response


class LLMPlannerPromptTests(unittest.TestCase):
    def test_build_planner_system_prompt_hides_deprecated_aliases(self) -> None:
        prompt = _build_planner_system_prompt(
            {
                "dataset_name": "issues.csv",
                "data_type": "unstructured",
                "goal": "결제 오류를 요약해줘",
            }
        )

        self.assertIn("term_frequency", prompt)
        self.assertIn("issue_evidence_summary", prompt)
        self.assertNotIn("keyword_frequency", prompt)
        self.assertNotIn("evidence_pack", prompt)
        self.assertIn(
            "For general unstructured text analysis. Prefer document_filter, term_frequency, document_sample, unstructured_issue_summary, issue_evidence_summary.",
            prompt,
        )

    def test_build_planner_system_prompt_reflects_bundle_driven_skills_and_recommendations(self) -> None:
        with patch(
            "python_ai_worker.runtime.llm.planner_visible_skills",
            return_value=[
                {
                    "name": "custom_summary",
                    "description": "Summarize custom records.",
                    "goal_input": "query",
                }
            ],
        ), patch(
            "python_ai_worker.runtime.llm.planner_visible_skill_names",
            return_value=["custom_summary"],
        ), patch(
            "python_ai_worker.runtime.llm.planner_recommendations",
            return_value=[{"sequence_name": "custom_default", "when": "For custom analysis."}],
        ), patch(
            "python_ai_worker.runtime.llm.planner_sequence",
            return_value=["custom_summary"],
        ):
            prompt = _build_planner_system_prompt(
                {
                    "dataset_name": "custom.csv",
                    "data_type": "unstructured",
                    "goal": "커스텀 분석",
                }
            )

        self.assertIn("Allowed skills: custom_summary.", prompt)
        self.assertIn("- custom_summary: Summarize custom records.", prompt)
        self.assertIn("When custom_summary is used, set inputs.query to the user goal.", prompt)
        self.assertIn("For custom analysis. Prefer custom_summary.", prompt)

    def test_normalize_planner_response_canonicalizes_deprecated_aliases(self) -> None:
        result = _normalize_planner_response(
            {
                "plan": {
                    "steps": [
                        {"skill_name": "keyword_frequency", "dataset_name": "issues.csv", "inputs": {}},
                        {"skill_name": "evidence_pack", "dataset_name": "issues.csv", "inputs": {}},
                        {"skill_name": "not_registered", "dataset_name": "issues.csv", "inputs": {}},
                    ]
                }
            },
            {
                "dataset_name": "issues.csv",
                "goal": "결제 오류를 요약해줘",
            },
            planner_model="claude-test",
        )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["term_frequency", "issue_evidence_summary"],
        )
        self.assertEqual(result["plan"]["steps"][1]["inputs"]["query"], "결제 오류를 요약해줘")


if __name__ == "__main__":
    unittest.main()
