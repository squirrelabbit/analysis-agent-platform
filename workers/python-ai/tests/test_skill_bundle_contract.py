from __future__ import annotations

import unittest

from python_ai_worker._migration_targets import (
    DEPRECATED_ALIASES,
    LEGACY_SKILL_NAMES,
    canonical_skill_name,
)
from python_ai_worker.skill_bundle import capability_skills, skill_bundle
from python_ai_worker.task_router import capability_names, task_handlers

RESULT_KINDS = {
    "preprocessing",
    "evidence",
    "summary_ranked",
    "summary_narrative",
    "metric_table",
    "cluster_output",
    "dataset_artifact",
}
RESULT_SCOPES = {
    "full_dataset",
    "document_subset",
    "cluster_subset",
    "partial_build",
}
RESULT_SCOPE_POLICIES = {
    "static",
    "inherits_from_input",
    "dynamic",
}
FALLBACK_POLICIES = {
    "strict_fail",
    "graceful_empty",
    "rule_fallback_allowed",
}
QUALITY_TIERS = {
    "deterministic",
    "heuristic",
    "llm_dependent",
}


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

    def test_bundle_skills_define_valid_result_contract_fields(self) -> None:
        for skill in capability_skills():
            name = str(skill.get("name") or "").strip()
            with self.subTest(skill_name=name):
                self.assertIn(str(skill.get("result_kind") or "").strip(), RESULT_KINDS)
                self.assertIn(str(skill.get("result_scope") or "").strip(), RESULT_SCOPES)
                self.assertIn(str(skill.get("result_scope_policy") or "").strip(), RESULT_SCOPE_POLICIES)
                self.assertIn(str(skill.get("fallback_policy") or "").strip(), FALLBACK_POLICIES)
                self.assertIn(str(skill.get("quality_tier") or "").strip(), QUALITY_TIERS)
                if str(skill.get("result_scope_policy") or "").strip() == "dynamic":
                    allowed_runtime_scopes = list(skill.get("allowed_runtime_result_scopes") or [])
                    self.assertTrue(allowed_runtime_scopes)
                    for runtime_scope in allowed_runtime_scopes:
                        self.assertIn(str(runtime_scope or "").strip(), RESULT_SCOPES)

    def test_legacy_skill_names_match_audit_inventory(self) -> None:
        """ADR-009 F4: the canonical migration scope is exactly 17 names
        (the audit's actual inventory, not the prompt's 14)."""

        self.assertEqual(len(LEGACY_SKILL_NAMES), 17)

    def test_legacy_skill_names_exist_in_bundle(self) -> None:
        """Every name in LEGACY_SKILL_NAMES must currently exist as a
        bundle entry — the migration target list cannot reference a name
        that has already been removed without an explicit drop record."""

        bundle_names = {str(skill.get("name") or "").strip() for skill in capability_skills()}
        missing = LEGACY_SKILL_NAMES - bundle_names
        self.assertFalse(missing, f"legacy names missing from bundle: {sorted(missing)}")

    def test_deprecated_aliases_are_well_formed(self) -> None:
        """For each (deprecated → canonical) pair: the deprecated entry
        carries `deprecated_alias_of` pointing at the canonical, the
        canonical entry exists in the bundle, and both task_router
        handlers resolve to the same callable."""

        bundle_by_name = {
            str(skill.get("name") or "").strip(): skill
            for skill in capability_skills()
        }
        handlers = task_handlers()

        for deprecated, canonical in DEPRECATED_ALIASES.items():
            with self.subTest(deprecated=deprecated, canonical=canonical):
                self.assertIn(deprecated, bundle_by_name)
                self.assertIn(canonical, bundle_by_name)
                self.assertEqual(
                    str(bundle_by_name[deprecated].get("deprecated_alias_of") or "").strip(),
                    canonical,
                )
                self.assertIn(deprecated, handlers)
                self.assertIn(canonical, handlers)
                self.assertIs(handlers[deprecated], handlers[canonical])

    def test_deprecated_alias_inventory_matches_current_phases(self) -> None:
        self.assertEqual(
            DEPRECATED_ALIASES,
            {
                "keyword_frequency": "term_frequency",
                "evidence_pack": "issue_evidence_summary",
            },
        )

    def test_canonical_skill_name_resolves_alias(self) -> None:
        for deprecated, canonical in DEPRECATED_ALIASES.items():
            with self.subTest(deprecated=deprecated):
                self.assertEqual(canonical_skill_name(deprecated), canonical)
        self.assertEqual(canonical_skill_name("noun_frequency"), "noun_frequency")
        self.assertEqual(canonical_skill_name("totally_unrelated"), "totally_unrelated")


if __name__ == "__main__":
    unittest.main()
