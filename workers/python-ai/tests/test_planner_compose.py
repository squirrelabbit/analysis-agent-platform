from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker.planner_compose import PlannerComposeError, compose_plan


class PlannerComposeTests(unittest.TestCase):
    def test_compose_reorders_cluster_steps_by_dependency(self) -> None:
        composed = compose_plan(
            [
                {"skill_name": "issue_cluster_summary", "dataset_name": "issues.csv", "inputs": {}},
                {"skill_name": "cluster_label_candidates", "dataset_name": "issues.csv", "inputs": {}},
                {"skill_name": "embedding_cluster", "dataset_name": "issues.csv", "inputs": {}},
            ],
            frozenset({"retrieve", "summarize"}),
        )

        self.assertEqual(
            [step["skill_name"] for step in composed.steps],
            ["embedding_cluster", "cluster_label_candidates", "issue_cluster_summary"],
        )

    def test_compose_adds_missing_hard_dependencies_and_expands_layers(self) -> None:
        composed = compose_plan(
            [
                {"skill_name": "issue_cluster_summary", "dataset_name": "issues.csv", "inputs": {}},
            ],
            frozenset({"summarize"}),
        )

        self.assertEqual(
            [step["skill_name"] for step in composed.steps],
            ["embedding_cluster", "cluster_label_candidates", "issue_cluster_summary"],
        )
        self.assertEqual(composed.active_layers, frozenset({"retrieve", "summarize"}))

    def test_compose_adds_nested_dependency_for_cluster_labels(self) -> None:
        composed = compose_plan(
            [
                {"skill_name": "cluster_label_candidates", "dataset_name": "issues.csv", "inputs": {}},
            ],
            frozenset({"retrieve"}),
        )

        self.assertEqual(
            [step["skill_name"] for step in composed.steps],
            ["embedding_cluster", "cluster_label_candidates"],
        )

    def test_compose_preserves_original_order_when_no_hard_dependency_forces_change(self) -> None:
        composed = compose_plan(
            [
                {"skill_name": "document_filter", "dataset_name": "issues.csv", "inputs": {}},
                {"skill_name": "time_bucket_count", "dataset_name": "issues.csv", "inputs": {}},
                {"skill_name": "document_sample", "dataset_name": "issues.csv", "inputs": {}},
                {"skill_name": "issue_trend_summary", "dataset_name": "issues.csv", "inputs": {}},
            ],
            frozenset({"preprocess", "aggregate", "summarize"}),
        )

        self.assertEqual(
            [step["skill_name"] for step in composed.steps],
            ["document_filter", "time_bucket_count", "document_sample", "issue_trend_summary"],
        )

    def test_compose_rejects_cycle(self) -> None:
        fake_defs = {
            "cycle_a": {"requires_prior_skills": ["cycle_b"]},
            "cycle_b": {"requires_prior_skills": ["cycle_a"]},
        }

        with patch(
            "python_ai_worker.planner_compose.skill_definition",
            side_effect=lambda name: fake_defs.get(name),
        ), patch(
            "python_ai_worker.planner_compose.layer_for_skill",
            side_effect=lambda _name: "retrieve",
        ), patch(
            "python_ai_worker.planner_compose.default_inputs_for_skill",
            return_value={},
        ):
            with self.assertRaisesRegex(PlannerComposeError, "cycle"):
                compose_plan(
                    [{"skill_name": "cycle_a", "dataset_name": "issues.csv", "inputs": {}}],
                    frozenset({"retrieve"}),
                )


if __name__ == "__main__":
    unittest.main()
