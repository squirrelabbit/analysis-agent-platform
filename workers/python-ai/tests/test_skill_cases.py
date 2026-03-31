from __future__ import annotations

import unittest

from python_ai_worker.devtools.skill_cases import available_skill_cases, run_skill_case
from python_ai_worker.task_router import task_handlers


class SkillCaseTests(unittest.TestCase):
    def test_skill_cases_cover_all_task_handlers(self) -> None:
        self.assertEqual(set(available_skill_cases()), set(task_handlers()))

    def test_each_skill_case_runs(self) -> None:
        for skill_name in sorted(available_skill_cases()):
            with self.subTest(skill_name=skill_name):
                result = run_skill_case(skill_name)
                self.assertEqual(result["skill_name"], skill_name)
                self.assertTrue(result["steps"])

                final_result = result["final_result"]
                if skill_name == "planner":
                    self.assertIn("plan", final_result)
                    self.assertTrue(final_result["plan"]["steps"])
                    continue

                self.assertIn("artifact", final_result)
                self.assertEqual(final_result["artifact"]["skill_name"], skill_name)


if __name__ == "__main__":
    unittest.main()
