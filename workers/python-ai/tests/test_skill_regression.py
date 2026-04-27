from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from python_ai_worker.planner_meta import select_active_layers
from python_ai_worker.tasks import run_planner


class SkillRegressionPlannerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = Path(__file__).resolve().parents[3] / "docs" / "eval" / "skill_regression_v1.yaml"
        cls.cases = json.loads(path.read_text(encoding="utf-8"))

    def test_regression_yaml_format_is_parseable(self) -> None:
        self.assertIsInstance(self.cases, list)
        self.assertEqual(len(self.cases), 12)
        for case in self.cases:
            with self.subTest(query=case.get("query")):
                self.assertIn("query", case)
                self.assertIn("dataset_ref", case)
                self.assertIn("data_type", case)
                self.assertIn("expected_layers", case)
                self.assertIn("expected_step_family", case)

    def test_regression_cases_lock_layer_subset_and_canonical_step_family(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            for case in self.cases:
                with self.subTest(query=case["query"]):
                    expected_layers = sorted(case["expected_layers"])
                    meta_plan = select_active_layers(case["query"])
                    self.assertEqual(sorted(meta_plan.active_layers), expected_layers)

                    result = run_planner(
                        {
                            "dataset_name": case["dataset_ref"],
                            "data_type": case["data_type"],
                            "goal": case["query"],
                        }
                    )

                    self.assertEqual(sorted(result["plan"]["metadata"]["active_layers"]), expected_layers)
                    self.assertEqual(
                        self._canonical_step_family(result["plan"]["steps"]),
                        self._normalize_expected_family(case["expected_step_family"]),
                    )

    @staticmethod
    def _canonical_step_family(steps: list[dict[str, object]]) -> list[list[str]]:
        normalized: list[list[str]] = []
        for step in steps:
            skill_name = str(step.get("skill_name") or "").strip()
            normalized.append([skill_name])
        return normalized

    @staticmethod
    def _normalize_expected_family(expected: list[object]) -> list[list[str]]:
        normalized: list[list[str]] = []
        for item in expected:
            if isinstance(item, list):
                values = [str(value or "").strip() for value in item if str(value or "").strip()]
            else:
                values = [str(item or "").strip()]
            normalized.append(values)
        return normalized


if __name__ == "__main__":
    unittest.main()
