from __future__ import annotations

import unittest

from python_ai_worker.runtime.artifacts import _select_evidence_candidates, _selected_source_indices
from python_ai_worker.runtime.common import _match_taxonomies
from python_ai_worker.runtime.constants import DEFAULT_TAXONOMY_RULES
from python_ai_worker.runtime.llm import _normalize_planner_response
from python_ai_worker.runtime.payloads import _normalize_dictionary_tagging_payload


class RuntimeHelperTests(unittest.TestCase):
    def test_selected_source_indices_intersects_filter_and_dedup(self) -> None:
        prior_artifacts = {
            "filter": {
                "skill_name": "document_filter",
                "matched_indices": [1, 2, 3],
            },
            "dedup": {
                "skill_name": "deduplicate_documents",
                "canonical_indices": [2, 3, 5],
            },
        }

        result = _selected_source_indices(prior_artifacts)

        self.assertEqual(result, {2, 3})

    def test_select_evidence_candidates_prefers_semantic_search(self) -> None:
        payload = {
            "prior_artifacts": {
                "semantic": {
                    "skill_name": "semantic_search",
                    "matches": [
                        {"source_index": 7, "score": 0.91, "text": "결제 오류가 반복 발생했습니다"},
                    ],
                },
                "sample": {
                    "skill_name": "document_sample",
                    "samples": [
                        {"source_index": 3, "score": 1, "text": "이 항목은 선택되면 안 됩니다"},
                    ],
                },
            }
        }
        normalized = {
            "dataset_name": "unused.csv",
            "text_column": "text",
            "query": "결제 오류",
            "sample_n": 1,
        }

        selected, source = _select_evidence_candidates(payload, normalized)

        self.assertEqual(source, "semantic_search")
        self.assertEqual(selected[0]["source_index"], 7)

    def test_normalize_dictionary_tagging_payload_uses_default_rules_for_invalid_input(self) -> None:
        normalized = _normalize_dictionary_tagging_payload(
            {
                "dataset_name": "issues.csv",
                "taxonomy_rules": ["invalid"],
            }
        )

        self.assertIn("payment_billing", normalized["taxonomy_rules"])
        self.assertEqual(
            normalized["taxonomy_rules"]["payment_billing"]["label"],
            DEFAULT_TAXONOMY_RULES["payment_billing"]["label"],
        )

    def test_match_taxonomies_respects_max_tags_per_document(self) -> None:
        rules = {
            "payment": {"label": "결제", "patterns": ["결제", "환불"]},
            "failure": {"label": "장애", "patterns": ["오류", "실패"]},
        }

        matched = _match_taxonomies("결제 오류와 환불 실패가 반복됩니다", rules, 1)

        self.assertEqual(len(matched), 1)
        self.assertIn(matched[0], {"payment", "failure"})

    def test_normalize_planner_response_falls_back_when_llm_returns_no_valid_skill(self) -> None:
        fallback_result = {
            "plan": {"steps": [{"skill_name": "document_filter"}]},
            "planner_type": "python-ai",
        }

        result = _normalize_planner_response(
            {
                "plan": {
                    "notes": "invalid",
                    "steps": [{"skill_name": "not_registered", "dataset_name": "issues.csv", "inputs": {}}],
                }
            },
            {
                "dataset_name": "issues.csv",
                "goal": "결제 오류를 보여줘",
            },
            planner_model="claude-test",
            fallback_planner=lambda payload: fallback_result,
        )

        self.assertIs(result, fallback_result)


if __name__ == "__main__":
    unittest.main()
