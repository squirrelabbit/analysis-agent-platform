from __future__ import annotations

import unittest

from python_ai_worker.skill_bundle import capability_skills, skill_bundle
from python_ai_worker.task_router import capability_names, task_handlers


class SkillBundleContractTests(unittest.TestCase):
    def test_worker_capabilities_only_advertise_runnable_tasks(self) -> None:
        names = set(capability_names())

        self.assertNotIn("structured_kpi_summary", names)
        self.assertIn("dataset_cluster_build", names)
        self.assertIn("planner", names)
        self.assertIn("execution_final_answer", names)

    def test_python_ai_bundle_skills_have_handlers(self) -> None:
        handlers = set(task_handlers())
        bundle_skills = capability_skills()

        for skill in bundle_skills:
            if skill.get("engine") != "python-ai":
                continue
            name = str(skill.get("name") or "").strip()
            task_path = str(skill.get("task_path") or "").strip()
            with self.subTest(skill_name=name):
                self.assertIn(name, handlers)
                self.assertEqual(task_path, f"/tasks/{name}")

    def test_default_plans_and_planner_sequences_reference_known_bundle_skills(self) -> None:
        bundle = skill_bundle()
        known = {str(skill.get("name") or "").strip() for skill in capability_skills()}

        for plan_name, skill_names in (bundle.get("default_plans") or {}).items():
            for skill_name in skill_names:
                with self.subTest(plan_name=plan_name, skill_name=skill_name):
                    self.assertIn(skill_name, known)

        for sequence_name, skill_names in (bundle.get("planner_sequences") or {}).items():
            for skill_name in skill_names:
                with self.subTest(sequence_name=sequence_name, skill_name=skill_name):
                    self.assertIn(skill_name, known)

    def test_prior_skill_contracts_reference_known_bundle_skills(self) -> None:
        known = {str(skill.get("name") or "").strip() for skill in capability_skills()}

        for skill in capability_skills():
            name = str(skill.get("name") or "").strip()
            required_prior_skills = list(skill.get("requires_prior_skills") or [])
            required_any_prior_skills = list(skill.get("requires_any_prior_skills") or [])
            for prior_skill_name in required_prior_skills + required_any_prior_skills:
                with self.subTest(skill_name=name, prior_skill_name=prior_skill_name):
                    self.assertIn(prior_skill_name, known)


if __name__ == "__main__":
    unittest.main()
