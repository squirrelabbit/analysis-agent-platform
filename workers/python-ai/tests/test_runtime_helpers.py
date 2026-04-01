from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker.runtime.artifacts import _analysis_context_entries, _select_evidence_candidates, _selected_source_indices
from python_ai_worker.runtime.common import _match_taxonomies
from python_ai_worker.runtime.constants import DEFAULT_TAXONOMY_RULES
from python_ai_worker.runtime.embeddings import _generate_dense_embeddings, _generate_query_embedding
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

    def test_analysis_context_entries_summarize_prior_artifacts(self) -> None:
        prior_artifacts = {
            "trend": {
                "skill_name": "issue_trend_summary",
                "bucket": "day",
                "summary": {
                    "peak_bucket": "2026-03-27",
                    "peak_count": 3,
                },
            },
            "compare": {
                "skill_name": "issue_period_compare",
                "summary": {
                    "current_count": 3,
                    "previous_count": 1,
                    "count_delta": 2,
                },
            },
            "breakdown": {
                "skill_name": "issue_breakdown_summary",
                "summary": {
                    "dimension_column": "channel",
                    "top_group": "app",
                    "top_group_count": 2,
                },
            },
        }

        entries = _analysis_context_entries(prior_artifacts)

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["source_skill"], "issue_trend_summary")
        self.assertIn("피크 구간", entries[0]["summary"])
        self.assertEqual(entries[1]["source_skill"], "issue_breakdown_summary")
        self.assertIn("최다 그룹", entries[1]["summary"])
        self.assertEqual(entries[2]["source_skill"], "issue_period_compare")
        self.assertIn("증가", entries[2]["summary"])

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

    def test_generate_dense_embeddings_routes_openai_models_to_openai_backend(self) -> None:
        with (
            patch(
                "python_ai_worker.runtime.embeddings._generate_openai_embeddings",
                return_value={"provider": "openai", "model": "text-embedding-3-small", "dimensions": 3, "embeddings": [[0.1, 0.2, 0.3]]},
            ) as generate_openai,
            patch(
                "python_ai_worker.runtime.embeddings._generate_local_embeddings",
                return_value=None,
            ) as generate_local,
        ):
            result = _generate_dense_embeddings(["결제 오류"], model="text-embedding-3-small")

        self.assertEqual(result["provider"], "openai")
        generate_openai.assert_called_once()
        generate_local.assert_not_called()

    def test_generate_dense_embeddings_routes_local_models_to_fastembed_backend(self) -> None:
        with (
            patch(
                "python_ai_worker.runtime.embeddings._generate_local_embeddings",
                return_value={"provider": "fastembed", "model": "intfloat/multilingual-e5-small", "dimensions": 384, "embeddings": [[0.1, 0.2]]},
            ) as generate_local,
            patch(
                "python_ai_worker.runtime.embeddings._generate_openai_embeddings",
                return_value=None,
            ) as generate_openai,
        ):
            result = _generate_dense_embeddings(["결제 오류"], model="intfloat/multilingual-e5-small")

        self.assertEqual(result["provider"], "fastembed")
        generate_local.assert_called_once_with(["결제 오류"], model="intfloat/multilingual-e5-small", task_type="passage")
        generate_openai.assert_not_called()

    def test_generate_query_embedding_routes_local_models_to_query_backend(self) -> None:
        with patch(
            "python_ai_worker.runtime.embeddings._generate_local_embeddings",
            return_value={"provider": "fastembed", "model": "intfloat/multilingual-e5-small", "dimensions": 2, "embeddings": [[0.9, 0.1]]},
        ) as generate_local:
            result = _generate_query_embedding("결제 오류", model="intfloat/multilingual-e5-small")

        self.assertEqual(result, [0.9, 0.1])
        generate_local.assert_called_once_with(["결제 오류"], model="intfloat/multilingual-e5-small", task_type="query")


if __name__ == "__main__":
    unittest.main()
