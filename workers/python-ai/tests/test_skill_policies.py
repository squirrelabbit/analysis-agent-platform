from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import python_ai_worker.skill_policy_registry as skill_policy_registry_module
from python_ai_worker.skill_policy_registry import (
    load_cluster_label_policy,
    load_embedding_cluster_policy,
    load_issue_evidence_summary_policy,
    skill_policy_catalog,
    validate_skill_policies,
)
from python_ai_worker.tasks import run_cluster_label_candidates, run_issue_evidence_summary


class SkillPolicyRegistryTests(unittest.TestCase):
    def test_skill_policy_dir_resolves_container_style_layout(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "app"
            policy_dir = root / "config" / "skill_policies"
            policy_dir.mkdir(parents=True)

            with patch.dict("os.environ", {}, clear=False):
                with patch.object(
                    skill_policy_registry_module,
                    "__file__",
                    str(root / "src" / "python_ai_worker" / "skill_policy_registry.py"),
                ):
                    resolved = skill_policy_registry_module._skill_policies_dir()

        self.assertEqual(resolved, policy_dir.resolve())

    def test_default_skill_policies_are_valid(self) -> None:
        validation = validate_skill_policies()
        self.assertTrue(validation["valid"])
        versions = {item["version"] for item in skill_policy_catalog()}
        self.assertIn("embedding-cluster-v1", versions)
        self.assertIn("cluster-label-candidates-v1", versions)
        self.assertIn("issue-evidence-summary-v1", versions)

    def test_load_default_policies_returns_hash_and_expected_skill(self) -> None:
        embedding_policy = load_embedding_cluster_policy()
        cluster_label_policy = load_cluster_label_policy()
        issue_evidence_policy = load_issue_evidence_summary_policy()
        self.assertEqual(embedding_policy["skill_name"], "embedding_cluster")
        self.assertEqual(cluster_label_policy["skill_name"], "cluster_label_candidates")
        self.assertEqual(issue_evidence_policy["skill_name"], "issue_evidence_summary")
        self.assertTrue(embedding_policy["policy_hash"])
        self.assertTrue(cluster_label_policy["policy_hash"])
        self.assertTrue(issue_evidence_policy["policy_hash"])


class SkillPolicyBehaviorTests(unittest.TestCase):
    def test_cluster_label_candidates_uses_policy_filtered_terms(self) -> None:
        result = run_cluster_label_candidates(
            {
                "dataset_name": "issues.csv",
                "prior_artifacts": {
                    "cluster": {
                        "skill_name": "embedding_cluster",
                        "clusters": [
                            {
                                "cluster_id": "cluster-1",
                                "document_count": 3,
                                "top_terms": [
                                    {"term": "이슈", "count": 5},
                                    {"term": "결제", "count": 4},
                                    {"term": "오류", "count": 4},
                                ],
                                "sample_documents": [{"text": "결제 오류가 반복됩니다."}],
                            }
                        ],
                    }
                },
            }
        )

        artifact = result["artifact"]
        self.assertEqual(artifact["policy_version"], "cluster-label-candidates-v1")
        self.assertEqual(artifact["clusters"][0]["label"], "결제 / 오류")
        self.assertEqual(artifact["summary"]["label_rule"], "top_terms")

    def test_issue_evidence_summary_limits_selected_documents_by_policy(self) -> None:
        result = run_issue_evidence_summary(
            {
                "dataset_name": "issues.csv",
                "goal": "결제 오류 근거를 보여줘",
                "sample_n": 5,
                "prior_artifacts": {
                    "semantic": {
                        "skill_name": "semantic_search",
                        "matches": [
                            {"source_index": 0, "score": 0.9, "text": "결제 오류가 반복됩니다."},
                            {"source_index": 1, "score": 0.8, "text": "결제가 계속 실패합니다."},
                            {"source_index": 2, "score": 0.7, "text": "결제 승인 오류가 있습니다."},
                            {"source_index": 3, "score": 0.6, "text": "주문 결제 에러가 발생했습니다."},
                        ],
                    }
                },
            }
        )

        artifact = result["artifact"]
        self.assertEqual(artifact["policy_version"], "issue-evidence-summary-v1")
        self.assertEqual(artifact["selection_source"], "semantic_search")
        self.assertLessEqual(len(artifact["evidence"]), 3)


if __name__ == "__main__":
    unittest.main()
