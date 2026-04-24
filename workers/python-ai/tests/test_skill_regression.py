from __future__ import annotations

import json
import unittest
from pathlib import Path


class SkillRegressionFormatTests(unittest.TestCase):
    def test_regression_yaml_format_is_parseable(self) -> None:
        path = Path("docs/eval/skill_regression_v1.yaml")
        cases = json.loads(path.read_text(encoding="utf-8"))
        self.assertIsInstance(cases, list)
        self.assertTrue(cases)
        case = cases[0]
        self.assertIn("query", case)
        self.assertIn("dataset_ref", case)
        self.assertIn("expected_plan_shape", case)
        self.assertIn("expected_output_keys", case)
        self.assertIn("forbidden_keys", case)

    @unittest.skip("실제 regression query fixture는 별도 제공 예정")
    def test_regression_runner_placeholder(self) -> None:
        self.fail("query fixture not provided")


if __name__ == "__main__":
    unittest.main()
