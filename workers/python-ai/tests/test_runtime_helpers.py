from __future__ import annotations

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from python_ai_worker.runtime.artifacts import (
    _analysis_context_entries,
    _cluster_embedding_records,
    _select_evidence_candidates,
    _selected_source_indices,
)
from python_ai_worker.runtime.common import _match_taxonomies
from python_ai_worker.runtime.constants import DEFAULT_TAXONOMY_RULES
from python_ai_worker.runtime.embeddings import _generate_dense_embeddings, _generate_query_embedding
from python_ai_worker.runtime.llm import (
    _anthropic_prepare_client,
    _anthropic_sentiment_client,
    _compact_analysis_context,
    _compact_evidence_documents_for_prompt,
    _normalize_planner_response,
)
from python_ai_worker.config import load_config
from python_ai_worker.runtime.common import (
    _apply_prepare_regex_rules,
    _match_garbage_rules,
    _normalize_garbage_rule_names,
    _normalize_prepare_regex_rule_names,
    _normalize_taxonomy_rules,
)
from python_ai_worker.runtime.payloads import _normalize_dictionary_tagging_payload


class RuntimeHelperTests(unittest.TestCase):
    def test_load_config_uses_sentiment_model_override_when_set(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ANTHROPIC_PREPARE_MODEL": "claude-prepare-test",
                "ANTHROPIC_SENTIMENT_MODEL": "claude-sentiment-test",
            },
            clear=False,
        ):
            config = load_config()

        self.assertEqual(config.anthropic_prepare_model, "claude-prepare-test")
        self.assertEqual(config.anthropic_sentiment_model, "claude-sentiment-test")

    def test_load_config_falls_back_to_prepare_model_for_sentiment(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ANTHROPIC_PREPARE_MODEL": "claude-prepare-test",
            },
            clear=False,
        ):
            previous = os.environ.pop("ANTHROPIC_SENTIMENT_MODEL", None)
            try:
                config = load_config()
            finally:
                if previous is not None:
                    os.environ["ANTHROPIC_SENTIMENT_MODEL"] = previous

        self.assertEqual(config.anthropic_sentiment_model, "claude-prepare-test")

    def test_anthropic_sentiment_client_uses_sentiment_model(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
                "ANTHROPIC_API_KEY": "test-key",
                "ANTHROPIC_PREPARE_MODEL": "claude-prepare-test",
                "ANTHROPIC_SENTIMENT_MODEL": "claude-sentiment-test",
            },
            clear=False,
        ):
            client = _anthropic_sentiment_client()

        self.assertIsNotNone(client)
        self.assertEqual(client._config.model, "claude-sentiment-test")

    def test_anthropic_prepare_client_disabled_mode_forces_fallback(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
                "ANTHROPIC_API_KEY": "test-key",
                "ANTHROPIC_PREPARE_MODEL": "claude-prepare-test",
            },
            clear=False,
        ):
            client = _anthropic_prepare_client(llm_mode="disabled")

        self.assertIsNone(client)

    def test_anthropic_sentiment_client_disabled_mode_forces_fallback(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
                "ANTHROPIC_API_KEY": "test-key",
                "ANTHROPIC_SENTIMENT_MODEL": "claude-sentiment-test",
            },
            clear=False,
        ):
            client = _anthropic_sentiment_client(llm_mode="disabled")

        self.assertIsNone(client)

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

    def test_selected_source_indices_rehydrates_sidecar_filters(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        sidecar_path = temp_dir / "garbage.rows.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {"source_row_index": 1, "filter_status": "retained"},
                    {"source_row_index": 2, "filter_status": "removed"},
                    {"source_row_index": 3, "filter_status": "retained"},
                ]
            ),
            sidecar_path,
        )
        prior_artifacts = {
            "filter": {
                "skill_name": "garbage_filter",
                "artifact_ref": str(sidecar_path),
                "source_index_column": "source_row_index",
                "status_column": "filter_status",
            }
        }

        result = _selected_source_indices(prior_artifacts)

        self.assertEqual(result, {1, 3})

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

    def test_compact_analysis_context_truncates_and_omits_when_limits_are_small(self) -> None:
        with patch.dict(
            os.environ,
            {
                "EVIDENCE_CONTEXT_MAX_ENTRIES": "2",
                "EVIDENCE_CONTEXT_MAX_CHARS": "90",
                "EVIDENCE_CONTEXT_ENTRY_MAX_CHARS": "40",
            },
            clear=False,
        ):
            compacted, metadata = _compact_analysis_context(
                [
                    {"source_skill": "issue_trend_summary", "summary": "a" * 80},
                    {"source_skill": "issue_breakdown_summary", "summary": "b" * 80},
                    {"source_skill": "issue_period_compare", "summary": "c" * 80},
                ]
            )

        self.assertTrue(metadata["applied"])
        self.assertEqual(metadata["input_entry_count"], 3)
        self.assertEqual(metadata["output_entry_count"], 2)
        self.assertEqual(metadata["omitted_entry_count"], 1)
        self.assertGreaterEqual(metadata["truncated_entry_count"], 2)
        self.assertLessEqual(sum(len(item["summary"]) for item in compacted), 90)

    def test_compact_evidence_documents_for_prompt_truncates_to_budget(self) -> None:
        with patch.dict(
            os.environ,
            {
                "EVIDENCE_DOCUMENT_MAX_CHARS": "80",
                "EVIDENCE_DOCUMENT_TOTAL_CHARS": "120",
            },
            clear=False,
        ):
            compacted, metadata = _compact_evidence_documents_for_prompt(
                [
                    {"source_index": 0, "text": "가" * 120},
                    {"source_index": 1, "text": "나" * 120},
                    {"source_index": 2, "text": "다" * 20},
                ]
            )

        self.assertTrue(metadata["applied"])
        self.assertEqual(metadata["input_document_count"], 3)
        self.assertLess(metadata["output_document_count"], 3)
        self.assertLessEqual(metadata["output_text_chars"], 120)
        self.assertLessEqual(len(compacted[0]["text"]), 80)

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

    def test_rule_config_path_overrides_prepare_regex_defaults(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        rule_path = temp_dir / "rule-config.json"
        rule_path.write_text(
            json.dumps(
                {
                    "prepare_regex_rules": {
                        "emoji_cleanup": {
                            "description": "emoji 제거",
                            "patterns": ["🙂"],
                            "replacement": " ",
                        }
                    },
                    "default_prepare_regex_rule_names": ["emoji_cleanup"],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with patch.dict("os.environ", {"PYTHON_AI_RULE_CONFIG_PATH": str(rule_path)}, clear=False):
            normalized_names = _normalize_prepare_regex_rule_names(None)
            cleaned, applied = _apply_prepare_regex_rules("문의🙂내용", normalized_names)

        self.assertEqual(normalized_names, ["emoji_cleanup"])
        self.assertEqual(cleaned, "문의 내용")
        self.assertEqual(applied, ["emoji_cleanup"])

    def test_rule_config_json_overrides_garbage_defaults(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_RULE_CONFIG_JSON": json.dumps(
                    {
                        "garbage_rules": {
                            "campaign_marker": {
                                "description": "체험단 문구",
                                "patterns": ["체험단후기"],
                            }
                        },
                        "default_garbage_rule_names": ["campaign_marker"],
                    },
                    ensure_ascii=False,
                )
            },
            clear=False,
        ):
            normalized_names = _normalize_garbage_rule_names(None)
            matched = _match_garbage_rules("체험단후기 입니다", normalized_names)

        self.assertEqual(normalized_names, ["campaign_marker"])
        self.assertEqual(matched, ["campaign_marker"])

    def test_rule_config_json_overrides_taxonomy_defaults(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_RULE_CONFIG_JSON": json.dumps(
                    {
                        "taxonomy_rules": {
                            "custom_topic": {
                                "label": "커스텀",
                                "patterns": ["맞춤주제"],
                            }
                        }
                    },
                    ensure_ascii=False,
                )
            },
            clear=False,
        ):
            normalized = _normalize_taxonomy_rules(None)

        self.assertIn("custom_topic", normalized)
        self.assertEqual(normalized["custom_topic"]["label"], "커스텀")
        self.assertEqual(normalized["custom_topic"]["patterns"], ["맞춤주제"])

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

    def test_dense_hybrid_outperforms_dense_only_on_generic_overlap_fixture(self) -> None:
        records = [
            {
                "source_index": 0,
                "row_id": "version-generic:row:0",
                "chunk_id": "version-generic:row:0:chunk:0",
                "text": "결제 오류가 계속 발생합니다",
                "token_counts": {"결제": 1, "오류": 1, "계속": 1, "발생합니다": 1},
                "embedding": [1.0, 0.0],
            },
            {
                "source_index": 1,
                "row_id": "version-generic:row:1",
                "chunk_id": "version-generic:row:1:chunk:0",
                "text": "결제 승인 문제가 반복됩니다",
                "token_counts": {"결제": 1, "승인": 1, "문제": 1, "반복됩니다": 1},
                "embedding": [0.999, 0.001],
            },
            {
                "source_index": 2,
                "row_id": "version-generic:row:2",
                "chunk_id": "version-generic:row:2:chunk:0",
                "text": "로그인 오류가 계속 발생합니다",
                "token_counts": {"로그인": 1, "오류": 1, "계속": 1, "발생합니다": 1},
                "embedding": [0.998, 0.002],
            },
            {
                "source_index": 3,
                "row_id": "version-generic:row:3",
                "chunk_id": "version-generic:row:3:chunk:0",
                "text": "로그인 인증 문제가 반복됩니다",
                "token_counts": {"로그인": 1, "인증": 1, "문제": 1, "반복됩니다": 1},
                "embedding": [0.997, 0.003],
            },
            {
                "source_index": 4,
                "row_id": "version-generic:row:4",
                "chunk_id": "version-generic:row:4:chunk:0",
                "text": "배송 오류가 계속 발생합니다",
                "token_counts": {"배송": 1, "오류": 1, "계속": 1, "발생합니다": 1},
                "embedding": [0.996, 0.004],
            },
            {
                "source_index": 5,
                "row_id": "version-generic:row:5",
                "chunk_id": "version-generic:row:5:chunk:0",
                "text": "배송 조회 문제가 반복됩니다",
                "token_counts": {"배송": 1, "조회": 1, "문제": 1, "반복됩니다": 1},
                "embedding": [0.995, 0.005],
            },
        ]

        dense_only = _cluster_embedding_records(records, 0.2, 2, 3, similarity_mode="dense-only")
        dense_hybrid = _cluster_embedding_records(records, 0.2, 2, 3, similarity_mode="dense-hybrid")

        self.assertEqual(len(dense_only), 1)
        self.assertEqual(dense_only[0]["similarity_backend"], "dense-only")
        self.assertEqual(len(dense_hybrid), 3)
        self.assertEqual(dense_hybrid[0]["similarity_backend"], "dense-hybrid")
        self.assertEqual(
            [cluster["member_source_indices"] for cluster in dense_hybrid],
            [[0, 1], [2, 3], [4, 5]],
        )


if __name__ == "__main__":
    unittest.main()
