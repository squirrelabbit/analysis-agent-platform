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
        self.assertIn(
            "For general unstructured text analysis. Prefer document_filter, term_frequency, document_sample, unstructured_issue_summary, issue_evidence_summary.",
            prompt,
        )
        self.assertIn("result_kind=summary_ranked", prompt)
        self.assertIn("prior_artifacts=none", prompt)

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
        self.assertIn("result_kind=unknown", prompt)
        self.assertIn("prior_artifacts=none", prompt)

    def test_build_planner_system_prompt_hides_inactive_layers(self) -> None:
        prompt = _build_planner_system_prompt(
            {
                "dataset_name": "issues.csv",
                "data_type": "unstructured",
                "goal": "결제 오류를 요약해줘",
                "active_layers": ["preprocess"],
            }
        )

        self.assertIn("Active runtime layers: preprocess.", prompt)
        self.assertIn("document_filter", prompt)
        self.assertIn("document_sample", prompt)
        self.assertNotIn("term_frequency", prompt)
        self.assertNotIn("issue_evidence_summary", prompt)
        self.assertNotIn(
            "For general unstructured text analysis. Prefer document_filter, term_frequency, document_sample, unstructured_issue_summary, issue_evidence_summary.",
            prompt,
        )

    def test_normalize_planner_response_keeps_registered_canonical_skills(self) -> None:
        result = _normalize_planner_response(
            {
                "plan": {
                    "steps": [
                        {"skill_name": "term_frequency", "dataset_name": "issues.csv", "inputs": {}},
                        {"skill_name": "issue_evidence_summary", "dataset_name": "issues.csv", "inputs": {}},
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

    def test_normalize_planner_response_filters_out_inactive_layer_skills(self) -> None:
        result = _normalize_planner_response(
            {
                "plan": {
                    "steps": [
                        {"skill_name": "document_filter", "dataset_name": "issues.csv", "inputs": {}},
                        {"skill_name": "term_frequency", "dataset_name": "issues.csv", "inputs": {}},
                    ]
                }
            },
            {
                "dataset_name": "issues.csv",
                "goal": "문장 단위로 나눠서 보여줘",
                "active_layers": ["preprocess"],
            },
            planner_model="claude-test",
        )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter"],
        )


if __name__ == "__main__":
    unittest.main()
