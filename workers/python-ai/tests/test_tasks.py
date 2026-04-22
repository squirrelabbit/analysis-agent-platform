from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from python_ai_worker.anthropic_client import AnthropicJSONResponse
from python_ai_worker.tasks import (
    run_cluster_label_candidates,
    run_deduplicate_documents,
    run_dictionary_tagging,
    run_document_filter,
    run_document_sample,
    run_dataset_cluster_build,
    run_dataset_prepare,
    run_embedding,
    run_embedding_cluster,
    run_evidence_pack,
    run_execution_final_answer,
    run_garbage_filter,
    run_issue_breakdown_summary,
    run_issue_cluster_summary,
    run_issue_evidence_summary,
    run_issue_period_compare,
    run_issue_sentiment_summary,
    run_issue_taxonomy_summary,
    run_issue_trend_summary,
    run_keyword_frequency,
    run_meta_group_count,
    run_noun_frequency,
    run_planner,
    run_semantic_search,
    run_sentiment_label,
    run_sentence_split,
    run_time_bucket_count,
)


class TaskTests(unittest.TestCase):
    @staticmethod
    def _read_parquet_rows(path: Path) -> list[dict[str, object]]:
        return [dict(row) for row in pq.read_table(path).to_pylist()]

    @staticmethod
    def _write_csv_rows(path: Path, rows: list[str]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            for row in rows:
                writer.writerow({"text": row})

    class _DummyPrepareClient:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows
            self._config = type("Config", (), {"model": "claude-test"})()

        def is_enabled(self) -> bool:
            return True

        def create_json(self, *, prompt: str, schema: dict[str, object], max_tokens: int | None = None) -> dict[str, object]:
            return {"rows": self._rows}

        def create_json_response(
            self,
            *,
            prompt: str,
            schema: dict[str, object],
            max_tokens: int | None = None,
        ) -> AnthropicJSONResponse:
            return AnthropicJSONResponse(
                body={"rows": self._rows},
                usage={
                    "input_tokens": 120,
                    "output_tokens": 30,
                },
            )

    class _DummyEnabledClient:
        def __init__(self) -> None:
            self._config = type("Config", (), {"model": "claude-test"})()

        def is_enabled(self) -> bool:
            return True

    class _FailingPrepareClient:
        def __init__(self) -> None:
            self._config = type("Config", (), {"model": "claude-haiku-4-5"})()

        def is_enabled(self) -> bool:
            return True

        def create_json_response(
            self,
            *,
            prompt: str,
            schema: dict[str, object],
            max_tokens: int | None = None,
        ) -> AnthropicJSONResponse:
            raise RuntimeError("model unavailable")

    def test_run_execution_final_answer_fallback(self) -> None:
        result = run_execution_final_answer(
            {
                "execution_id": "exec-1",
                "project_id": "project-1",
                "question": "결제 오류 핵심을 알려줘",
                "result_v1": {
                    "status": "completed",
                    "primary_skill_name": "issue_evidence_summary",
                    "answer": {
                        "summary": "결제 오류 이슈가 반복되고 있습니다.",
                        "key_findings": ["결제 오류 VOC가 반복된다."],
                        "evidence": [{"snippet": "결제 오류가 반복 발생했습니다."}],
                        "follow_up_questions": ["결제 실패 구간을 더 볼까요?"],
                    },
                    "warnings": ["확인 필요: 샘플 수가 제한적입니다."],
                    "step_results": [
                        {
                            "step_id": "step-1",
                            "skill_name": "issue_evidence_summary",
                            "status": "completed",
                            "summary": "결제 오류 이슈가 반복되고 있습니다.",
                        }
                    ],
                },
            }
        )

        answer = result["answer"]
        self.assertEqual(answer["schema_version"], "execution-final-answer-v1")
        self.assertEqual(answer["generation_mode"], "fallback")
        self.assertEqual(answer["answer_text"], "결제 오류 이슈가 반복되고 있습니다.")
        self.assertEqual(answer["key_points"], ["결제 오류 VOC가 반복된다."])
        self.assertIn("확인 필요: 샘플 수가 제한적입니다.", answer["caveats"])
        artifact = result["artifact"]
        self.assertEqual(artifact["skill_name"], "execution_final_answer")
        self.assertEqual(artifact["answer_text"], "결제 오류 이슈가 반복되고 있습니다.")

    def test_run_execution_final_answer_fallback_derives_limits_from_sparse_evidence(self) -> None:
        result = run_execution_final_answer(
            {
                "execution_id": "exec-2",
                "project_id": "project-1",
                "question": "로그인 오류 핵심을 알려줘",
                "result_v1": {
                    "status": "completed",
                    "primary_skill_name": "issue_cluster_summary",
                    "answer": {
                        "summary": "로그인 오류 군집이 가장 크게 나타났습니다.",
                        "evidence": [{"snippet": "로그인이 자주 실패합니다."}],
                    },
                    "warnings": [],
                    "step_results": [
                        {
                            "step_id": "step-1",
                            "skill_name": "issue_cluster_summary",
                            "status": "completed",
                            "summary": "로그인 오류 군집이 12건으로 가장 큽니다.",
                        }
                    ],
                },
            }
        )

        answer = result["answer"]
        self.assertEqual(answer["generation_mode"], "fallback")
        self.assertEqual(answer["key_points"], ["로그인 오류 군집이 12건으로 가장 큽니다."])
        self.assertIn("확인 필요: 현재 final_answer 근거 후보가 1건뿐이라 해석 범위가 제한적입니다.", answer["caveats"])
        self.assertTrue(answer["follow_up_questions"])

    def test_dataset_cluster_build_materializes_cluster_artifact_and_embedding_cluster_reads_it(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            for row in (
                "결제 오류가 반복 발생했습니다",
                "결제 승인 오류가 다시 발생했습니다",
                "로그인이 자주 실패합니다",
                "로그인 인증 오류가 반복됩니다",
                "배송 문의가 계속 들어옵니다",
            ):
                writer.writerow({"text": row})

        embedding_result = run_embedding(
            {
                "dataset_version_id": "version-cluster-build",
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(temp_dir / "issues_cluster.embeddings.jsonl"),
            }
        )
        cluster_build_result = run_dataset_cluster_build(
            {
                "dataset_version_id": "version-cluster-build",
                "dataset_name": str(csv_path),
                "embedding_index_source_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "chunk_ref": embedding_result["artifact"]["chunk_ref"],
                "output_path": str(temp_dir / "issues_cluster.clusters.json"),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        cluster_path = Path(cluster_build_result["artifact"]["cluster_ref"])
        membership_path = Path(cluster_build_result["artifact"]["cluster_membership_ref"])
        self.assertTrue(cluster_path.exists())
        self.assertTrue(membership_path.exists())
        materialized = json.loads(cluster_path.read_text(encoding="utf-8"))
        self.assertEqual(materialized["skill_name"], "embedding_cluster")
        self.assertEqual(materialized["cluster_execution_mode"], "materialized_full_dataset")
        self.assertEqual(materialized["cluster_materialization_scope"], "full_dataset")
        self.assertTrue(materialized["cluster_materialized_ref_used"])
        self.assertEqual(materialized["cluster_fallback_reason"], "")
        self.assertGreaterEqual(materialized["summary"]["cluster_count"], 1)
        self.assertEqual(materialized["summary"]["cluster_similarity_threshold"], 0.2)
        self.assertEqual(materialized["summary"]["top_n"], 3)
        self.assertEqual(materialized["summary"]["sample_n"], 2)
        self.assertEqual(materialized["cluster_membership_ref"], str(membership_path))
        self.assertEqual(materialized["cluster_membership_format"], "parquet")
        self.assertGreaterEqual(materialized["summary"]["cluster_membership_row_count"], 1)

        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_index_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "cluster_ref": str(cluster_path),
                "cluster_format": "json",
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        self.assertEqual(cluster_result["artifact"]["cluster_ref"], str(cluster_path))
        self.assertEqual(cluster_result["artifact"]["cluster_membership_ref"], str(membership_path))
        self.assertEqual(cluster_result["artifact"]["cluster_execution_mode"], "materialized_full_dataset")
        self.assertEqual(cluster_result["artifact"]["cluster_materialization_scope"], "full_dataset")
        self.assertTrue(cluster_result["artifact"]["cluster_materialized_ref_used"])
        self.assertEqual(cluster_result["artifact"]["cluster_fallback_reason"], "")
        self.assertEqual(cluster_result["artifact"]["summary"], materialized["summary"])
        self.assertEqual(cluster_result["artifact"]["clusters"], materialized["clusters"])
        self.assertIn("precomputed cluster artifact", cluster_result["notes"][0])

        fallback_cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_index_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "cluster_ref": str(cluster_path),
                "cluster_format": "json",
                "cluster_similarity_threshold": 0.4,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        self.assertNotIn("precomputed cluster artifact", fallback_cluster_result["notes"][0])
        self.assertEqual(fallback_cluster_result["artifact"]["cluster_execution_mode"], "on_demand_full_dataset")
        self.assertEqual(fallback_cluster_result["artifact"]["cluster_materialization_scope"], "full_dataset")
        self.assertFalse(fallback_cluster_result["artifact"]["cluster_materialized_ref_used"])
        self.assertEqual(fallback_cluster_result["artifact"]["cluster_fallback_reason"], "cluster_request_mismatch")

    def test_embedding_cluster_subset_fallback_exposes_reason_and_scope(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_subset.csv"
        self._write_csv_rows(
            csv_path,
            [
                "결제 오류가 반복 발생했습니다",
                "결제 승인 오류가 다시 발생했습니다",
                "로그인이 자주 실패합니다",
                "로그인 인증 오류가 반복됩니다",
                "배송 문의가 계속 들어옵니다",
            ],
        )

        embedding_result = run_embedding(
            {
                "dataset_version_id": "version-cluster-subset",
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(temp_dir / "issues_cluster_subset.embeddings.jsonl"),
            }
        )
        cluster_build_result = run_dataset_cluster_build(
            {
                "dataset_version_id": "version-cluster-subset",
                "dataset_name": str(csv_path),
                "embedding_index_source_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "chunk_ref": embedding_result["artifact"]["chunk_ref"],
                "output_path": str(temp_dir / "issues_cluster_subset.clusters.json"),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        subset_cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_uri": embedding_result["artifact"]["embedding_uri"],
                "embedding_index_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "chunk_ref": embedding_result["artifact"]["chunk_ref"],
                "chunk_format": embedding_result["artifact"]["chunk_format"],
                "cluster_ref": cluster_build_result["artifact"]["cluster_ref"],
                "cluster_format": "json",
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:document_filter": {
                        "skill_name": "document_filter",
                        "matched_indices": [0, 1],
                    }
                },
            }
        )

        self.assertEqual(subset_cluster_result["artifact"]["cluster_execution_mode"], "on_demand_subset_fallback")
        self.assertEqual(subset_cluster_result["artifact"]["cluster_materialization_scope"], "subset_selection")
        self.assertFalse(subset_cluster_result["artifact"]["cluster_materialized_ref_used"])
        self.assertEqual(subset_cluster_result["artifact"]["cluster_fallback_reason"], "prior_artifacts_present")
        self.assertEqual(subset_cluster_result["artifact"]["summary"]["clustered_document_count"], 2)

    def test_rule_based_planner_without_key(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues.csv",
                    "data_type": "unstructured",
                    "goal": "VOC 이슈를 요약해줘",
                }
            )

        self.assertEqual(result["planner_model"], "rule-based-v1")
        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "keyword_frequency", "document_sample", "unstructured_issue_summary", "issue_evidence_summary"],
        )
        self.assertEqual(result["plan"]["steps"][-1]["inputs"]["query"], "VOC 이슈를 요약해줘")

    def test_rule_based_planner_builds_issue_trend_summary(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_trend.csv",
                    "data_type": "unstructured",
                    "goal": "최근 결제 오류 추세를 보여줘",
                }
            )

        self.assertEqual(result["planner_model"], "rule-based-v1")
        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "time_bucket_count", "document_sample", "issue_trend_summary", "issue_evidence_summary"],
        )

    def test_rule_based_planner_builds_noun_frequency_sequence(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues.csv",
                    "data_type": "unstructured",
                    "goal": "결제 오류 관련 명사 키워드를 추출해줘",
                }
            )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "noun_frequency"],
        )

    def test_rule_based_planner_builds_sentence_split_sequence(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues.csv",
                    "data_type": "unstructured",
                    "goal": "문장 단위로 나눠서 보여줘",
                }
            )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "sentence_split"],
        )

    def test_rule_based_planner_builds_issue_period_compare(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_compare.csv",
                    "data_type": "unstructured",
                    "goal": "전주 대비 결제 오류가 얼마나 달라졌는지 비교해줘",
                }
            )

        self.assertEqual(result["planner_model"], "rule-based-v1")
        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "document_sample", "issue_period_compare", "issue_evidence_summary"],
        )

    def test_rule_based_planner_builds_issue_breakdown_summary(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_breakdown.csv",
                    "data_type": "unstructured",
                    "goal": "채널별 이슈를 분해해서 보여줘",
                }
            )

        self.assertEqual(result["planner_model"], "rule-based-v1")
        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "meta_group_count", "document_sample", "issue_breakdown_summary", "issue_evidence_summary"],
        )

    def test_rule_based_planner_builds_issue_sentiment_summary(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_sentiment.jsonl",
                    "data_type": "unstructured",
                    "goal": "긍정 부정 감성 분포를 보여줘",
                }
            )

        self.assertEqual(result["planner_model"], "rule-based-v1")
        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "document_sample", "issue_sentiment_summary", "issue_evidence_summary"],
        )

    def test_rule_based_planner_builds_issue_cluster_summary(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_cluster.csv",
                    "data_type": "unstructured",
                    "goal": "주요 이슈 군집을 묶어서 보여줘",
                }
            )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["embedding_cluster", "cluster_label_candidates", "issue_cluster_summary", "issue_evidence_summary"],
        )

    def test_rule_based_planner_builds_issue_cluster_summary_subset_sequence(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_cluster.csv",
                    "data_type": "unstructured",
                    "goal": "최근 문의 중에서 주요 이슈 군집을 묶어서 보여줘",
                }
            )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "deduplicate_documents", "embedding_cluster", "cluster_label_candidates", "issue_cluster_summary", "issue_evidence_summary"],
        )

    def test_rule_based_planner_builds_issue_taxonomy_summary(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PYTHON_AI_LLM_PROVIDER": "anthropic",
            },
            clear=False,
        ):
            result = run_planner(
                {
                    "dataset_name": "issues_taxonomy.csv",
                    "data_type": "unstructured",
                    "goal": "카테고리 태그 기준으로 이슈를 분류해줘",
                }
            )

        self.assertEqual(
            [step["skill_name"] for step in result["plan"]["steps"]],
            ["document_filter", "dictionary_tagging", "issue_taxonomy_summary", "issue_evidence_summary"],
        )

    def test_support_skills_filter_keywords_and_samples(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["channel", "text"])
            writer.writeheader()
            writer.writerow({"channel": "app", "text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"channel": "web", "text": "배송 문의가 계속 들어옵니다"})
            writer.writerow({"channel": "app", "text": "결제 승인 오류가 늘었습니다"})

        filter_result = run_document_filter(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "query": "결제 오류",
                "sample_n": 2,
            }
        )
        prior_artifacts = {
            "step:filter:document_filter": json.dumps(filter_result["artifact"], ensure_ascii=False),
        }
        keyword_result = run_keyword_frequency(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "top_n": 3,
                "prior_artifacts": prior_artifacts,
            }
        )
        sample_result = run_document_sample(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "query": "결제 오류",
                "sample_n": 2,
                "prior_artifacts": prior_artifacts,
            }
        )

        self.assertEqual(filter_result["artifact"]["summary"]["filtered_row_count"], 2)
        self.assertEqual(keyword_result["artifact"]["summary"]["document_count"], 2)
        self.assertEqual(keyword_result["artifact"]["top_terms"][0]["term"], "결제")
        self.assertEqual(sample_result["artifact"]["summary"]["sample_count"], 2)
        self.assertEqual(sample_result["artifact"]["samples"][0]["source_index"], 0)

    def test_garbage_filter_removes_ad_and_placeholder_rows(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "garbage.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "광고 협찬으로 진행된 후기입니다. 프로필 링크 클릭"})
            writer.writerow({"text": "존재하지 않는 이미지입니다"})
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})

        result = run_garbage_filter(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "garbage_filter")
        self.assertEqual(result["artifact"]["summary"]["removed_row_count"], 2)
        self.assertEqual(result["artifact"]["retained_indices"], [2, 3])
        self.assertEqual(result["artifact"]["removed_indices"], [0, 1])
        self.assertIn("ad_marker", result["artifact"]["removed_samples"][0]["matched_rules"])

    def test_garbage_filter_writes_sidecar_parquet_when_output_path_is_provided(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "garbage.csv"
        output_path = temp_dir / "garbage_filter.rows.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["row_id", "text"])
            writer.writeheader()
            writer.writerow({"row_id": "row-0", "text": "광고 협찬으로 진행된 후기입니다. 프로필 링크 클릭"})
            writer.writerow({"row_id": "row-1", "text": "결제 오류가 반복 발생했습니다"})

        result = run_garbage_filter(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "artifact_output_path": str(output_path),
            }
        )

        self.assertEqual(result["artifact"]["artifact_storage_mode"], "sidecar_ref")
        self.assertEqual(result["artifact"]["artifact_ref"], str(output_path))
        self.assertEqual(result["artifact"]["artifact_format"], "parquet")
        rows = self._read_parquet_rows(output_path)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["row_id"], "row-0")
        self.assertEqual(rows[0]["filter_status"], "removed")
        self.assertIn("ad_marker", rows[0]["matched_rules"])
        self.assertEqual(rows[1]["filter_status"], "retained")

    def test_noun_frequency_counts_nouns_from_filtered_rows(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "nouns.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "결제 승인 오류가 늘었습니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})

        filter_result = run_document_filter(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "query": "결제 오류",
                "sample_n": 3,
            }
        )
        result = run_noun_frequency(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "top_n": 5,
                "sample_n": 2,
                "prior_artifacts": {
                    "step:filter": filter_result["artifact"],
                },
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "noun_frequency")
        self.assertEqual(result["artifact"]["summary"]["document_count"], 2)
        self.assertIn(result["artifact"]["summary"]["analyzer_backend"], {"kiwi", "regex_fallback"})
        self.assertEqual(result["artifact"]["top_nouns"][0]["term"], "결제")
        self.assertGreaterEqual(int(result["artifact"]["top_nouns"][0]["document_frequency"]), 1)

    def test_document_filter_match_mode_all_requires_all_query_tokens(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "filter_all.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "벚꽃 축제 일정이 공개됐습니다"})
            writer.writerow({"text": "축제 일정이 공개됐습니다"})
            writer.writerow({"text": "벚꽃 개화 소식이 올라왔습니다"})

        result = run_document_filter(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "query": "벚꽃 축제",
                "match_mode": "all",
                "sample_n": 3,
            }
        )

        self.assertEqual(result["artifact"]["match_mode"], "all")
        self.assertEqual(result["artifact"]["summary"]["selection_mode"], "lexical_overlap_all")
        self.assertEqual(result["artifact"]["summary"]["filtered_row_count"], 1)
        self.assertEqual(result["artifact"]["matched_indices"], [0])

    def test_sentence_split_writes_sidecar_parquet_when_output_path_is_provided(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "sentences.csv"
        output_path = temp_dir / "sentence_split.rows.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["row_id", "text"])
            writer.writeheader()
            writer.writerow({"row_id": "row-0", "text": "결제 오류가 반복 발생했습니다. 환불 문의도 늘었습니다."})
            writer.writerow({"row_id": "row-1", "text": "로그인이 자주 실패합니다! 인증 오류가 함께 보입니다."})

        result = run_sentence_split(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "artifact_output_path": str(output_path),
                "sample_n": 2,
                "preview_sentences_per_row": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "sentence_split")
        self.assertEqual(result["artifact"]["artifact_storage_mode"], "sidecar_ref")
        self.assertEqual(result["artifact"]["artifact_ref"], str(output_path))
        self.assertIn(result["artifact"]["summary"]["splitter_backend"], {"kss", "regex"})
        rows = self._read_parquet_rows(output_path)
        self.assertGreaterEqual(len(rows), 4)
        self.assertEqual(rows[0]["row_id"], "row-0")
        self.assertEqual(rows[0]["sentence_index"], 0)
        self.assertTrue(str(rows[0]["sentence_text"]).startswith("결제 오류"))

    def test_document_filter_writes_sidecar_parquet_when_output_path_is_provided(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "filter.csv"
        output_path = temp_dir / "document_filter.matches.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["row_id", "text"])
            writer.writeheader()
            writer.writerow({"row_id": "row-0", "text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"row_id": "row-1", "text": "배송 문의가 계속 들어옵니다"})
            writer.writerow({"row_id": "row-2", "text": "결제 승인 오류가 늘었습니다"})

        result = run_document_filter(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "query": "결제 오류",
                "artifact_output_path": str(output_path),
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["artifact_storage_mode"], "sidecar_ref")
        self.assertEqual(result["artifact"]["artifact_ref"], str(output_path))
        rows = self._read_parquet_rows(output_path)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["row_id"], "row-0")
        self.assertEqual(rows[0]["rank"], 1)
        self.assertEqual(rows[1]["row_id"], "row-2")

    def test_deduplicate_documents_writes_sidecar_parquet_when_output_path_is_provided(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "dedup.csv"
        output_path = temp_dir / "deduplicate_documents.rows.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["row_id", "text"])
            writer.writeheader()
            writer.writerow({"row_id": "row-0", "text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"row_id": "row-1", "text": "결제 오류가 반복 발생했습니다!!"})
            writer.writerow({"row_id": "row-2", "text": "로그인이 자주 실패합니다"})

        result = run_deduplicate_documents(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "duplicate_threshold": 0.8,
                "artifact_output_path": str(output_path),
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["artifact_storage_mode"], "sidecar_ref")
        self.assertEqual(result["artifact"]["artifact_ref"], str(output_path))
        rows = self._read_parquet_rows(output_path)
        self.assertEqual(len(rows), 3)
        canonical_rows = [row for row in rows if row["dedup_status"] == "canonical"]
        duplicate_rows = [row for row in rows if row["dedup_status"] == "duplicate"]
        self.assertEqual(len(canonical_rows), 2)
        self.assertEqual(len(duplicate_rows), 1)
        self.assertEqual(duplicate_rows[0]["canonical_row_id"], "row-0")

    def test_deduplicate_documents_reduces_selected_rows(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "duplicates.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "결제 오류가 반복 발생했습니다!!"})
            writer.writerow({"text": "로그인이 자주 실패합니다"})

        dedup_result = run_deduplicate_documents(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "sample_n": 2,
                "duplicate_threshold": 0.8,
            }
        )
        keyword_result = run_keyword_frequency(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "top_n": 3,
                "prior_artifacts": {
                    "step:dedup": dedup_result["artifact"],
                },
            }
        )

        self.assertEqual(dedup_result["artifact"]["summary"]["canonical_row_count"], 2)
        self.assertEqual(dedup_result["artifact"]["summary"]["duplicate_row_count"], 1)
        self.assertEqual(keyword_result["artifact"]["summary"]["document_count"], 2)

    def test_support_skills_group_and_bucket(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["occurred_at", "channel", "text"])
            writer.writeheader()
            writer.writerow({"occurred_at": "2026-03-24", "channel": "app", "text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"occurred_at": "2026-03-24", "channel": "app", "text": "결제 승인 오류가 늘었습니다"})
            writer.writerow({"occurred_at": "2026-03-25", "channel": "web", "text": "배송 문의가 계속 들어옵니다"})

        time_bucket_result = run_time_bucket_count(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "time_column": "occurred_at",
                "bucket": "day",
                "top_n": 3,
                "sample_n": 2,
            }
        )
        meta_group_result = run_meta_group_count(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "dimension_column": "channel",
                "top_n": 3,
                "sample_n": 2,
            }
        )

        self.assertEqual(time_bucket_result["artifact"]["summary"]["bucket_count"], 2)
        self.assertEqual(time_bucket_result["artifact"]["summary"]["peak_bucket"], "2026-03-24")
        self.assertEqual(meta_group_result["artifact"]["summary"]["top_group"], "app")
        self.assertEqual(meta_group_result["artifact"]["breakdown"][0]["count"], 2)

    def test_issue_trend_summary(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_trend.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["occurred_at", "text"])
            writer.writeheader()
            writer.writerow({"occurred_at": "2026-03-24", "text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"occurred_at": "2026-03-24", "text": "결제 오류로 주문이 실패했습니다"})
            writer.writerow({"occurred_at": "2026-03-25", "text": "배송 문의가 계속 들어옵니다"})
            writer.writerow({"occurred_at": "2026-03-26", "text": "결제 승인 지연 문의가 늘었습니다"})

        result = run_issue_trend_summary(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "time_column": "occurred_at",
                "bucket": "day",
                "top_n": 3,
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "issue_trend_summary")
        self.assertEqual(result["artifact"]["summary"]["bucket_count"], 3)
        self.assertEqual(result["artifact"]["summary"]["peak_bucket"], "2026-03-24")
        self.assertEqual(result["artifact"]["series"][0]["count"], 2)
        self.assertEqual(result["artifact"]["highlights"][0]["top_terms"][0]["term"], "결제")

    def test_issue_period_compare(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_compare.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["occurred_at", "text"])
            writer.writeheader()
            writer.writerow({"occurred_at": "2026-03-24", "text": "로그인 오류가 간헐적으로 발생합니다"})
            writer.writerow({"occurred_at": "2026-03-25", "text": "배송 문의가 증가했습니다"})
            writer.writerow({"occurred_at": "2026-03-26", "text": "결제 오류가 발생했습니다"})
            writer.writerow({"occurred_at": "2026-03-27", "text": "결제 오류가 다시 증가했습니다"})
            writer.writerow({"occurred_at": "2026-03-27", "text": "결제 승인 오류가 반복됩니다"})
            writer.writerow({"occurred_at": "2026-03-27", "text": "결제 실패 문의가 늘었습니다"})

        result = run_issue_period_compare(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "time_column": "occurred_at",
                "bucket": "day",
                "window_size": 1,
                "top_n": 3,
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "issue_period_compare")
        self.assertEqual(result["artifact"]["summary"]["current_count"], 3)
        self.assertEqual(result["artifact"]["summary"]["previous_count"], 1)
        self.assertEqual(result["artifact"]["summary"]["count_delta"], 2)
        self.assertEqual(result["artifact"]["periods"]["current"]["start_bucket"], "2026-03-27")
        self.assertEqual(result["artifact"]["top_term_deltas"][0]["term"], "결제")

    def test_issue_breakdown_summary(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_breakdown.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["channel", "text"])
            writer.writeheader()
            writer.writerow({"channel": "app", "text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"channel": "app", "text": "로그인이 자주 실패합니다"})
            writer.writerow({"channel": "web", "text": "배송 문의가 계속 들어옵니다"})

        result = run_issue_breakdown_summary(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "dimension_column": "channel",
                "top_n": 3,
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "issue_breakdown_summary")
        self.assertEqual(result["artifact"]["summary"]["group_count"], 2)
        self.assertEqual(result["artifact"]["summary"]["top_group"], "app")
        self.assertEqual(result["artifact"]["breakdown"][0]["count"], 2)

    def test_dataset_prepare_fallback(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["channel", "text"])
            writer.writeheader()
            writer.writerow({"channel": "app", "text": "결제 오류가 반복 발생했습니다!!!"})
            writer.writerow({"channel": "web", "text": "   "})
            writer.writerow({"channel": "call", "text": "로그인이 자주 실패하고 오류가 보입니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-1",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                }
            )

        self.assertEqual(result["artifact"]["skill_name"], "dataset_prepare")
        self.assertEqual(result["artifact"]["prepare_format"], "parquet")
        self.assertEqual(result["artifact"]["prepared_ref"], str(prepared_path))
        self.assertEqual(result["artifact"]["row_id_column"], "row_id")
        self.assertEqual(
            result["artifact"]["prepare_regex_rule_names"],
            ["media_placeholder", "html_artifact", "url_cleanup", "zero_width_cleanup"],
        )
        self.assertEqual(result["artifact"]["summary"]["input_row_count"], 3)
        self.assertEqual(result["artifact"]["summary"]["output_row_count"], 2)
        self.assertEqual(result["artifact"]["usage"]["provider"], "deterministic-fallback")
        self.assertEqual(result["artifact"]["usage"]["request_count"], 2)
        self.assertEqual(result["artifact"]["usage"]["input_text_count"], 2)
        self.assertEqual(result["artifact"]["usage"]["cost_estimation_status"], "free_fallback")
        self.assertTrue(prepared_path.exists())

        prepared_rows = self._read_parquet_rows(prepared_path)

        self.assertEqual(len(prepared_rows), 2)
        self.assertEqual(prepared_rows[0]["row_id"], "version-1:row:0")
        self.assertEqual(prepared_rows[0]["normalized_text"], "결제 오류가 반복 발생했습니다.")
        self.assertEqual(prepared_rows[0]["prepare_disposition"], "keep")
        self.assertEqual(prepared_rows[0]["prepare_regex_applied_rules"], [])
        self.assertEqual(prepared_rows[0]["channel"], "app")

    def test_dataset_prepare_joins_multiple_text_columns(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["title", "body"])
            writer.writeheader()
            writer.writerow({"title": "결제 오류", "body": "카드 결제가 실패합니다!!!"})
            writer.writerow({"title": "   ", "body": "   "})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-multi",
                    "dataset_name": str(csv_path),
                    "text_columns": ["title", "body"],
                    "output_path": str(prepared_path),
                }
            )

        self.assertEqual(result["artifact"]["text_column"], "title + body")
        self.assertEqual(result["artifact"]["text_columns"], ["title", "body"])
        self.assertEqual(result["artifact"]["text_joiner"], "\n\n")
        self.assertEqual(result["artifact"]["summary"]["output_row_count"], 1)
        self.assertEqual(result["artifact"]["summary"]["text_columns"], ["title", "body"])

        prepared_rows = self._read_parquet_rows(prepared_path)
        self.assertEqual(len(prepared_rows), 1)
        self.assertEqual(prepared_rows[0]["raw_text"], "결제 오류\n\n카드 결제가 실패합니다!!!")
        self.assertEqual(prepared_rows[0]["normalized_text"], "결제 오류 카드 결제가 실패합니다.")

    def test_dataset_prepare_applies_regex_rules_before_fallback(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "존재하지 않는 이미지입니다 https://example.com"})
            writer.writerow({"text": "문의 내용은 <br> 결제 오류입니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-regex",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                }
            )

        self.assertEqual(result["artifact"]["summary"]["output_row_count"], 1)
        self.assertEqual(result["artifact"]["summary"]["prepare_regex_rule_hits"]["media_placeholder"], 1)
        self.assertEqual(result["artifact"]["summary"]["prepare_regex_rule_hits"]["url_cleanup"], 1)
        self.assertEqual(result["artifact"]["summary"]["prepare_regex_rule_hits"]["html_artifact"], 1)

        prepared_rows = self._read_parquet_rows(prepared_path)
        self.assertEqual(len(prepared_rows), 1)
        self.assertEqual(prepared_rows[0]["normalized_text"], "문의 내용은 결제 오류입니다")
        self.assertIn("html_artifact", prepared_rows[0]["prepare_regex_applied_rules"])

    def test_dataset_prepare_batches_llm_requests(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["channel", "text"])
            writer.writeheader()
            writer.writerow({"channel": "app", "text": "결제 오류가 반복 발생했습니다!!!"})
            writer.writerow({"channel": "call", "text": "로그인이 자주 실패하고 오류가 보입니다"})

        dummy_client = self._DummyPrepareClient(
            [
                {
                    "disposition": "keep",
                    "normalized_text": "결제 오류가 반복 발생했습니다.",
                    "reason": "noise removed",
                    "quality_flags": ["normalized"],
                },
                {
                    "disposition": "review",
                    "normalized_text": "로그인이 자주 실패하고 오류가 보입니다.",
                    "reason": "needs review",
                    "quality_flags": ["review_needed"],
                },
            ]
        )

        with patch("python_ai_worker.skills.dataset_build.rt._anthropic_prepare_client", return_value=dummy_client):
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-2",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                    "prepare_batch_size": 2,
                }
            )

        self.assertEqual(result["artifact"]["prepare_strategy"], "anthropic-batch")
        self.assertEqual(result["artifact"]["prepare_batch_size"], 2)
        self.assertEqual(result["artifact"]["summary"]["review_count"], 1)
        self.assertEqual(result["artifact"]["usage"]["provider"], "anthropic")
        self.assertEqual(result["artifact"]["usage"]["total_tokens"], 150)
        self.assertEqual(result["artifact"]["usage"]["request_count"], 1)

        prepared_rows = self._read_parquet_rows(prepared_path)

        self.assertEqual(prepared_rows[0]["prepare_prompt_version"], "dataset-prepare-anthropic-batch-v1")
        self.assertEqual(prepared_rows[0]["row_id"], "version-2:row:0")
        self.assertEqual(prepared_rows[1]["prepare_disposition"], "review")

    def test_dataset_prepare_records_fallback_when_batch_llm_fails(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        self._write_csv_rows(csv_path, ["결제 오류가 반복 발생했습니다", "로그인이 실패합니다"])

        with patch("python_ai_worker.skills.dataset_build.rt._anthropic_prepare_client", return_value=self._FailingPrepareClient()):
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-batch-fallback",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                    "prepare_batch_size": 2,
                }
            )

        self.assertEqual(result["artifact"]["prepare_model"], "fallback-normalizer-v1")
        self.assertEqual(result["artifact"]["prepare_strategy"], "deterministic-fallback")
        self.assertEqual(result["artifact"]["usage"]["provider"], "deterministic-fallback")
        self.assertEqual(result["artifact"]["usage"]["request_count"], 2)
        self.assertEqual(result["artifact"]["usage"]["input_text_count"], 2)
        prepared_rows = self._read_parquet_rows(prepared_path)
        self.assertIn("llm_batch_fallback:model unavailable", prepared_rows[0]["quality_flags"])

    def test_dataset_prepare_uses_prompt_version_override(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        self._write_csv_rows(csv_path, ["결제 오류가 반복 발생했습니다"])

        dummy_client = self._DummyPrepareClient(
            [
                {
                    "disposition": "keep",
                    "normalized_text": "결제 오류가 반복 발생했습니다.",
                    "reason": "normalized",
                    "quality_flags": ["normalized"],
                }
            ]
        )

        with patch("python_ai_worker.skills.dataset_build.rt._anthropic_prepare_client", return_value=dummy_client):
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-prepare-profile",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                    "prepare_batch_size": 1,
                    "prepare_prompt_version": "dataset-prepare-anthropic-v2",
                }
            )

        self.assertEqual(result["artifact"]["prepare_prompt_version"], "dataset-prepare-anthropic-v2")
        prepared_rows = self._read_parquet_rows(prepared_path)
        self.assertEqual(prepared_rows[0]["prepare_prompt_version"], "dataset-prepare-anthropic-v2")

    def test_dataset_prepare_passes_project_prompt_templates(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        self._write_csv_rows(csv_path, ["결제 오류가 반복 발생했습니다"])

        with patch(
            "python_ai_worker.skills.dataset_build.rt._anthropic_prepare_client",
            return_value=self._DummyEnabledClient(),
        ), patch(
            "python_ai_worker.skills.dataset_build.rt._prepare_rows",
            return_value=(
                [
                    {
                        "disposition": "keep",
                        "normalized_text": "결제 오류가 반복 발생했습니다.",
                        "reason": "normalized",
                        "quality_flags": [],
                        "prompt_version": "project-prepare-v1",
                    }
                ],
                {
                    "provider": "anthropic",
                    "model": "claude-test",
                    "operation": "dataset_prepare",
                    "request_count": 1,
                    "input_tokens": 12,
                    "output_tokens": 4,
                    "total_tokens": 16,
                },
            ),
        ) as prepare_mock:
            result = run_dataset_prepare(
                {
                    "dataset_version_id": "version-prepare-project-prompt",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                    "prepare_batch_size": 1,
                    "prepare_prompt_version": "project-prepare-v1",
                    "prepare_prompt_template": "---\noperation: prepare\n---\n{{raw_text}}\n",
                    "prepare_batch_prompt_template": "---\noperation: prepare_batch\n---\n{{rows_json}}\n",
                }
            )

        _, kwargs = prepare_mock.call_args
        self.assertEqual(kwargs["prompt_template_override"], "---\noperation: prepare\n---\n{{raw_text}}")
        self.assertEqual(kwargs["batch_prompt_template_override"], "---\noperation: prepare_batch\n---\n{{rows_json}}")
        self.assertEqual(result["artifact"]["prepare_prompt_version"], "project-prepare-v1")

    def test_dataset_prepare_passes_llm_mode_to_client_builder(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.parquet"
        self._write_csv_rows(csv_path, ["결제 오류가 반복 발생했습니다"])

        with patch("python_ai_worker.skills.dataset_build.rt._anthropic_prepare_client", return_value=None) as mock_client:
            run_dataset_prepare(
                {
                    "dataset_version_id": "version-prepare-default",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(prepared_path),
                    "model": "claude-haiku-4-5",
                    "llm_mode": "disabled",
                }
            )

        mock_client.assert_called_once_with("claude-haiku-4-5", llm_mode="disabled")

    def test_sentiment_label_fallback(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.parquet"
        sentiment_path = temp_dir / "issues.sentiment.parquet"
        table = pa.Table.from_pylist(
            [
                {"normalized_text": "결제 오류가 반복 발생했습니다", "channel": "app"},
                {"normalized_text": "빠르게 해결되어 만족합니다", "channel": "app"},
            ]
        )
        pq.write_table(table, prepared_path)

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_sentiment_label(
                {
                    "dataset_version_id": "version-1",
                    "dataset_name": str(prepared_path),
                    "text_column": "normalized_text",
                    "output_path": str(sentiment_path),
                }
            )

        self.assertEqual(result["artifact"]["skill_name"], "sentiment_label")
        self.assertEqual(result["artifact"]["sentiment_format"], "parquet")
        self.assertEqual(result["artifact"]["sentiment_ref"], str(sentiment_path))
        self.assertEqual(result["artifact"]["row_id_column"], "row_id")
        self.assertEqual(result["artifact"]["summary"]["labeled_row_count"], 2)
        self.assertEqual(result["artifact"]["usage"]["provider"], "deterministic-fallback")
        self.assertEqual(result["artifact"]["usage"]["request_count"], 2)
        self.assertEqual(result["artifact"]["usage"]["cost_estimation_status"], "free_fallback")

    def test_sentiment_label_joins_multiple_text_columns(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.parquet"
        sentiment_path = temp_dir / "issues.sentiment.parquet"
        table = pa.Table.from_pylist(
            [
                {"title": "결제 오류", "body": "카드 결제가 실패합니다"},
                {"title": "만족", "body": "빠르게 해결되었습니다"},
                {"title": "   ", "body": "   "},
            ]
        )
        pq.write_table(table, prepared_path)

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_sentiment_label(
                {
                    "dataset_version_id": "version-multi-sentiment",
                    "dataset_name": str(prepared_path),
                    "text_columns": ["title", "body"],
                    "output_path": str(sentiment_path),
                }
            )

        self.assertEqual(result["artifact"]["text_column"], "title + body")
        self.assertEqual(result["artifact"]["text_columns"], ["title", "body"])
        self.assertEqual(result["artifact"]["text_joiner"], "\n\n")
        self.assertEqual(result["artifact"]["summary"]["labeled_row_count"], 2)
        self.assertEqual(result["artifact"]["summary"]["text_columns"], ["title", "body"])

        labeled_rows = self._read_parquet_rows(sentiment_path)
        self.assertEqual(len(labeled_rows), 2)
        self.assertEqual(labeled_rows[0]["sentiment_label"], "negative")
        self.assertEqual(labeled_rows[1]["sentiment_label"], "positive")

    def test_sentiment_label_uses_prompt_version_override(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.parquet"
        sentiment_path = temp_dir / "issues.sentiment.parquet"
        table = pa.Table.from_pylist(
            [
                {"normalized_text": "결제 오류가 반복 발생했습니다", "channel": "app"},
                {"normalized_text": "문의 접수 후 확인 중입니다", "channel": "app"},
            ]
        )
        pq.write_table(table, prepared_path)

        with patch(
            "python_ai_worker.skills.dataset_build.rt._anthropic_sentiment_client",
            return_value=self._DummyEnabledClient(),
        ), patch(
            "python_ai_worker.skills.dataset_build.rt._label_sentiments",
            return_value=(
                [
                    {
                        "label": "negative",
                        "confidence": 0.82,
                        "reason": "negative markers detected",
                        "prompt_version": "sentiment-anthropic-batch-v2",
                        "usage": {
                            "provider": "anthropic",
                            "model": "claude-test",
                            "operation": "sentiment_label",
                            "request_count": 1,
                            "input_tokens": 12,
                            "output_tokens": 4,
                            "total_tokens": 16,
                        },
                    },
                    {
                        "label": "neutral",
                        "confidence": 0.74,
                        "reason": "status update",
                        "prompt_version": "sentiment-anthropic-batch-v2",
                        "usage": {
                            "provider": "anthropic",
                            "model": "claude-test",
                            "operation": "sentiment_label",
                            "request_count": 1,
                            "input_tokens": 12,
                            "output_tokens": 4,
                            "total_tokens": 16,
                        },
                    },
                ],
                {
                    "provider": "anthropic",
                    "model": "claude-test",
                    "operation": "sentiment_label",
                    "request_count": 1,
                    "input_tokens": 24,
                    "output_tokens": 8,
                    "total_tokens": 32,
                },
            ),
        ) as label_mock:
            result = run_sentiment_label(
                {
                    "dataset_version_id": "version-sentiment-profile",
                    "dataset_name": str(prepared_path),
                    "text_column": "normalized_text",
                    "output_path": str(sentiment_path),
                    "sentiment_prompt_version": "sentiment-anthropic-v2",
                    "sentiment_batch_size": 8,
                }
            )

        _, kwargs = label_mock.call_args
        self.assertEqual(kwargs["prompt_version_override"], "sentiment-anthropic-v2")
        self.assertEqual(kwargs["batch_size"], 8)
        self.assertEqual(result["artifact"]["sentiment_prompt_version"], "sentiment-anthropic-batch-v2")
        self.assertTrue(sentiment_path.exists())
        labeled_rows = self._read_parquet_rows(sentiment_path)
        self.assertEqual(labeled_rows[0]["row_id"], "version-sentiment-profile:row:0")
        self.assertEqual(labeled_rows[0]["source_row_index"], 0)
        self.assertEqual(labeled_rows[0]["sentiment_label"], "negative")
        self.assertEqual(labeled_rows[1]["sentiment_label"], "neutral")
        self.assertNotIn("normalized_text", labeled_rows[0])

    def test_sentiment_label_passes_project_prompt_templates(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.parquet"
        sentiment_path = temp_dir / "issues.sentiment.parquet"
        table = pa.Table.from_pylist(
            [
                {"normalized_text": "결제 오류가 반복 발생했습니다", "channel": "app"},
            ]
        )
        pq.write_table(table, prepared_path)

        with patch(
            "python_ai_worker.skills.dataset_build.rt._anthropic_sentiment_client",
            return_value=self._DummyEnabledClient(),
        ), patch(
            "python_ai_worker.skills.dataset_build.rt._label_sentiments",
            return_value=(
                [
                    {
                        "label": "negative",
                        "confidence": 0.82,
                        "reason": "negative markers detected",
                        "prompt_version": "project-sentiment-v1",
                        "usage": {
                            "provider": "anthropic",
                            "model": "claude-test",
                            "operation": "sentiment_label",
                            "request_count": 1,
                            "input_tokens": 12,
                            "output_tokens": 4,
                            "total_tokens": 16,
                        },
                    }
                ],
                {
                    "provider": "anthropic",
                    "model": "claude-test",
                    "operation": "sentiment_label",
                    "request_count": 1,
                    "input_tokens": 12,
                    "output_tokens": 4,
                    "total_tokens": 16,
                },
            ),
        ) as label_mock:
            result = run_sentiment_label(
                {
                    "dataset_version_id": "version-sentiment-project-prompt",
                    "dataset_name": str(prepared_path),
                    "text_column": "normalized_text",
                    "output_path": str(sentiment_path),
                    "sentiment_prompt_version": "project-sentiment-v1",
                    "sentiment_batch_size": 1,
                    "sentiment_prompt_template": "---\noperation: sentiment\n---\n{{text}}\n",
                    "sentiment_batch_prompt_template": "---\noperation: sentiment_batch\n---\n{{rows_json}}\n",
                }
            )

        _, kwargs = label_mock.call_args
        self.assertEqual(kwargs["prompt_template_override"], "---\noperation: sentiment\n---\n{{text}}")
        self.assertEqual(kwargs["batch_prompt_template_override"], "---\noperation: sentiment_batch\n---\n{{rows_json}}")
        self.assertEqual(result["artifact"]["sentiment_prompt_version"], "project-sentiment-v1")

    def test_sentiment_label_passes_llm_mode_to_client_builder(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.parquet"
        sentiment_path = temp_dir / "issues.sentiment.parquet"
        table = pa.Table.from_pylist([{"normalized_text": "결제 오류가 반복 발생했습니다", "channel": "app"}])
        pq.write_table(table, prepared_path)

        with patch("python_ai_worker.skills.dataset_build.rt._anthropic_sentiment_client", return_value=None) as mock_client:
            run_sentiment_label(
                {
                    "dataset_version_id": "version-sentiment-default",
                    "dataset_name": str(prepared_path),
                    "text_column": "normalized_text",
                    "output_path": str(sentiment_path),
                    "llm_mode": "disabled",
                }
            )

        mock_client.assert_called_once_with("", llm_mode="disabled")

    def test_issue_sentiment_summary(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.parquet"
        parquet_path = temp_dir / "issues.sentiment.parquet"
        prepared_rows = [
            {"source_row_index": 0, "row_id": "version-1:row:0", "normalized_text": "결제 오류가 반복 발생했습니다"},
            {"source_row_index": 1, "row_id": "version-1:row:1", "normalized_text": "빠르게 해결되어 만족합니다"},
            {"source_row_index": 2, "row_id": "version-1:row:2", "normalized_text": "문의 접수 후 확인 중입니다"},
            {"source_row_index": 3, "row_id": "version-1:row:3", "normalized_text": "결제 오류가 다시 발생했습니다"},
        ]
        sentiment_rows = [
            {"source_row_index": 0, "row_id": "version-1:row:0", "sentiment_label": "negative"},
            {"source_row_index": 1, "row_id": "version-1:row:1", "sentiment_label": "positive"},
            {"source_row_index": 2, "row_id": "version-1:row:2", "sentiment_label": "neutral"},
            {"source_row_index": 3, "row_id": "version-1:row:3", "sentiment_label": "negative"},
        ]
        pq.write_table(pa.Table.from_pylist(prepared_rows), prepared_path)
        pq.write_table(pa.Table.from_pylist(sentiment_rows), parquet_path)

        result = run_issue_sentiment_summary(
            {
                "dataset_name": str(parquet_path),
                "prepared_dataset_name": str(prepared_path),
                "text_column": "normalized_text",
                "sentiment_column": "sentiment_label",
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "issue_sentiment_summary")
        self.assertEqual(result["artifact"]["prepared_dataset_name"], str(prepared_path))
        self.assertEqual(result["artifact"]["summary"]["document_count"], 4)
        self.assertEqual(result["artifact"]["summary"]["dominant_label"], "negative")
        self.assertEqual(result["artifact"]["summary"]["negative_count"], 2)
        self.assertEqual(result["artifact"]["breakdown"][0]["sentiment_label"], "negative")
        self.assertEqual(result["artifact"]["breakdown"][0]["samples"][0], "결제 오류가 반복 발생했습니다")

    def test_dictionary_tagging_and_issue_taxonomy_summary(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_taxonomy.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 승인 오류가 반복 발생했습니다"})
            writer.writerow({"text": "환불 요청과 결제 문의가 계속 들어옵니다"})
            writer.writerow({"text": "로그인이 계속 실패합니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})

        tagging_result = run_dictionary_tagging(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "top_n": 3,
                "sample_n": 2,
            }
        )
        taxonomy_result = run_issue_taxonomy_summary(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "top_n": 3,
                "sample_n": 2,
                "prior_artifacts": {
                    "step:tagging": tagging_result["artifact"],
                },
            }
        )

        self.assertGreaterEqual(tagging_result["artifact"]["summary"]["taxonomy_count"], 3)
        self.assertEqual(taxonomy_result["artifact"]["summary"]["dominant_taxonomy"], "payment_billing")
        self.assertEqual(taxonomy_result["artifact"]["taxonomy_breakdown"][0]["count"], 2)

    def test_embedding_cluster_and_issue_cluster_summary(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster.csv"
        embedding_path = temp_dir / "issues_cluster.embeddings.jsonl"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "결제 승인 오류가 다시 발생했습니다"})
            writer.writerow({"text": "로그인이 계속 실패합니다"})
            writer.writerow({"text": "로그인 인증 오류가 반복됩니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})
            writer.writerow({"text": "결제 오류가 반복 발생했습니다!!"})

        dedup_result = run_deduplicate_documents(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "sample_n": 2,
                "duplicate_threshold": 0.8,
            }
        )
        run_embedding(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(embedding_path),
            }
        )
        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_uri": str(embedding_path),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:dedup": dedup_result["artifact"],
                },
            }
        )
        label_result = run_cluster_label_candidates(
            {
                "dataset_name": str(csv_path),
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:cluster": cluster_result["artifact"],
                },
            }
        )
        summary_result = run_issue_cluster_summary(
            {
                "dataset_name": str(csv_path),
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:cluster": cluster_result["artifact"],
                    "step:labels": label_result["artifact"],
                },
            }
        )

        self.assertEqual(cluster_result["artifact"]["summary"]["clustered_document_count"], 5)
        self.assertEqual(label_result["artifact"]["summary"]["cluster_count"], 3)
        self.assertEqual(summary_result["artifact"]["summary"]["dominant_cluster_count"], 2)
        self.assertIn("결제", summary_result["artifact"]["clusters"][0]["label"])

    def test_issue_cluster_summary_uses_cluster_membership_to_expand_samples(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_membership.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            for row in (
                "결제 오류가 반복 발생했습니다",
                "결제 승인 오류가 다시 발생했습니다",
                "결제 오류 문의가 접수됐습니다",
                "로그인이 계속 실패합니다",
                "배송 문의가 계속 들어옵니다",
            ):
                writer.writerow({"text": row})

        embedding_result = run_embedding(
            {
                "dataset_version_id": "version-cluster-membership",
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(temp_dir / "issues_cluster_membership.embeddings.jsonl"),
            }
        )
        cluster_build_result = run_dataset_cluster_build(
            {
                "dataset_version_id": "version-cluster-membership",
                "dataset_name": str(csv_path),
                "embedding_index_source_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "chunk_ref": embedding_result["artifact"]["chunk_ref"],
                "output_path": str(temp_dir / "issues_cluster_membership.clusters.json"),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )
        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_index_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "cluster_ref": cluster_build_result["artifact"]["cluster_ref"],
                "cluster_format": "json",
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )
        label_result = run_cluster_label_candidates(
            {
                "dataset_name": str(csv_path),
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:cluster": cluster_result["artifact"],
                },
            }
        )
        summary_result = run_issue_cluster_summary(
            {
                "dataset_name": str(csv_path),
                "sample_n": 3,
                "top_n": 3,
                "prior_artifacts": {
                    "step:cluster": cluster_result["artifact"],
                    "step:labels": label_result["artifact"],
                },
            }
        )

        self.assertEqual(label_result["artifact"]["cluster_membership_ref"], cluster_result["artifact"]["cluster_membership_ref"])
        self.assertEqual(summary_result["artifact"]["cluster_membership_ref"], cluster_result["artifact"]["cluster_membership_ref"])
        self.assertEqual(len(summary_result["artifact"]["clusters"][0]["samples"]), 3)
        self.assertEqual(summary_result["artifact"]["clusters"][0]["samples"][2]["text"], "결제 오류 문의가 접수됐습니다")

    def test_embedding_cluster_uses_dense_vectors_when_available(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_dense.csv"
        embedding_path = temp_dir / "issues_cluster_dense.embeddings.jsonl"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류"})
            writer.writerow({"text": "인증 오류"})
            writer.writerow({"text": "배송 문의"})

        dense_records = [
            {
                "source_index": 0,
                "row_id": "version-dense:row:0",
                "chunk_id": "version-dense:row:0:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 5,
                "text": "결제 오류",
                "token_counts": {"결제": 1, "오류": 1},
                "norm": 1.0,
                "embedding": [1.0, 0.0],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 1,
                "row_id": "version-dense:row:1",
                "chunk_id": "version-dense:row:1:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 6,
                "text": "인증 오류",
                "token_counts": {"인증": 1, "오류": 1},
                "norm": 1.0,
                "embedding": [0.98, 0.02],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 2,
                "row_id": "version-dense:row:2",
                "chunk_id": "version-dense:row:2:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 5,
                "text": "배송 문의",
                "token_counts": {"배송": 1, "문의": 1},
                "norm": 1.0,
                "embedding": [0.0, 1.0],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
        ]
        with embedding_path.open("w", encoding="utf-8") as handle:
            for record in dense_records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_uri": str(embedding_path),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        self.assertEqual(cluster_result["artifact"]["summary"]["similarity_backend"], "dense-hybrid")
        self.assertEqual(cluster_result["artifact"]["summary"]["cluster_count"], 2)
        self.assertEqual(cluster_result["artifact"]["clusters"][0]["similarity_backend"], "dense-hybrid")
        self.assertEqual(cluster_result["artifact"]["clusters"][0]["member_source_indices"], [0, 1])

    def test_embedding_cluster_dense_guardrail_prevents_single_cluster_collapse(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_guardrail.csv"
        embedding_path = temp_dir / "issues_cluster_guardrail.embeddings.jsonl"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 승인"})
            writer.writerow({"text": "결제 실패"})
            writer.writerow({"text": "로그인 인증"})
            writer.writerow({"text": "로그인 차단"})
            writer.writerow({"text": "배송 조회"})
            writer.writerow({"text": "배송 지연"})

        dense_records = [
            {
                "source_index": 0,
                "row_id": "version-guardrail:row:0",
                "chunk_id": "version-guardrail:row:0:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 5,
                "text": "결제 승인",
                "token_counts": {"결제": 1, "승인": 1},
                "norm": 1.0,
                "embedding": [1.0, 0.0],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 1,
                "row_id": "version-guardrail:row:1",
                "chunk_id": "version-guardrail:row:1:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 5,
                "text": "결제 실패",
                "token_counts": {"결제": 1, "실패": 1},
                "norm": 1.0,
                "embedding": [0.999, 0.001],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 2,
                "row_id": "version-guardrail:row:2",
                "chunk_id": "version-guardrail:row:2:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 6,
                "text": "로그인 인증",
                "token_counts": {"로그인": 1, "인증": 1},
                "norm": 1.0,
                "embedding": [0.998, 0.002],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 3,
                "row_id": "version-guardrail:row:3",
                "chunk_id": "version-guardrail:row:3:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 6,
                "text": "로그인 차단",
                "token_counts": {"로그인": 1, "차단": 1},
                "norm": 1.0,
                "embedding": [0.997, 0.003],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 4,
                "row_id": "version-guardrail:row:4",
                "chunk_id": "version-guardrail:row:4:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 5,
                "text": "배송 조회",
                "token_counts": {"배송": 1, "조회": 1},
                "norm": 1.0,
                "embedding": [0.996, 0.004],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 5,
                "row_id": "version-guardrail:row:5",
                "chunk_id": "version-guardrail:row:5:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 5,
                "text": "배송 지연",
                "token_counts": {"배송": 1, "지연": 1},
                "norm": 1.0,
                "embedding": [0.995, 0.005],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
        ]
        with embedding_path.open("w", encoding="utf-8") as handle:
            for record in dense_records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_uri": str(embedding_path),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        self.assertEqual(cluster_result["artifact"]["summary"]["similarity_backend"], "dense-hybrid")
        self.assertEqual(cluster_result["artifact"]["summary"]["cluster_count"], 3)
        self.assertEqual(
            [cluster["member_source_indices"] for cluster in cluster_result["artifact"]["clusters"]],
            [[0, 1], [2, 3], [4, 5]],
        )

    def test_embedding_cluster_idf_guardrail_separates_generic_overlap_topics(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_generic_overlap.csv"
        embedding_path = temp_dir / "issues_cluster_generic_overlap.embeddings.jsonl"
        self._write_csv_rows(
            csv_path,
            [
                "결제 오류가 계속 발생합니다",
                "결제 승인 문제가 반복됩니다",
                "로그인 오류가 계속 발생합니다",
                "로그인 인증 문제가 반복됩니다",
                "배송 오류가 계속 발생합니다",
                "배송 조회 문제가 반복됩니다",
            ],
        )

        dense_records = [
            {
                "source_index": 0,
                "row_id": "version-generic:row:0",
                "chunk_id": "version-generic:row:0:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 14,
                "text": "결제 오류가 계속 발생합니다",
                "token_counts": {"결제": 1, "오류": 1, "계속": 1, "발생합니다": 1},
                "norm": 1.0,
                "embedding": [1.0, 0.0],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 1,
                "row_id": "version-generic:row:1",
                "chunk_id": "version-generic:row:1:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 14,
                "text": "결제 승인 문제가 반복됩니다",
                "token_counts": {"결제": 1, "승인": 1, "문제": 1, "반복됩니다": 1},
                "norm": 1.0,
                "embedding": [0.999, 0.001],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 2,
                "row_id": "version-generic:row:2",
                "chunk_id": "version-generic:row:2:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 15,
                "text": "로그인 오류가 계속 발생합니다",
                "token_counts": {"로그인": 1, "오류": 1, "계속": 1, "발생합니다": 1},
                "norm": 1.0,
                "embedding": [0.998, 0.002],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 3,
                "row_id": "version-generic:row:3",
                "chunk_id": "version-generic:row:3:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 15,
                "text": "로그인 인증 문제가 반복됩니다",
                "token_counts": {"로그인": 1, "인증": 1, "문제": 1, "반복됩니다": 1},
                "norm": 1.0,
                "embedding": [0.997, 0.003],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 4,
                "row_id": "version-generic:row:4",
                "chunk_id": "version-generic:row:4:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 14,
                "text": "배송 오류가 계속 발생합니다",
                "token_counts": {"배송": 1, "오류": 1, "계속": 1, "발생합니다": 1},
                "norm": 1.0,
                "embedding": [0.996, 0.004],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
            {
                "source_index": 5,
                "row_id": "version-generic:row:5",
                "chunk_id": "version-generic:row:5:chunk:0",
                "chunk_index": 0,
                "char_start": 0,
                "char_end": 14,
                "text": "배송 조회 문제가 반복됩니다",
                "token_counts": {"배송": 1, "조회": 1, "문제": 1, "반복됩니다": 1},
                "norm": 1.0,
                "embedding": [0.995, 0.005],
                "embedding_dim": 2,
                "embedding_provider": "fastembed",
            },
        ]
        with embedding_path.open("w", encoding="utf-8") as handle:
            for record in dense_records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_uri": str(embedding_path),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        self.assertEqual(cluster_result["artifact"]["summary"]["similarity_backend"], "dense-hybrid")
        self.assertEqual(cluster_result["artifact"]["summary"]["cluster_count"], 3)
        self.assertEqual(
            [cluster["member_source_indices"] for cluster in cluster_result["artifact"]["clusters"]],
            [[0, 1], [2, 3], [4, 5]],
        )

    def test_local_embedding_cluster_fixture_is_stable_for_topic_groups(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_local_eval.csv"
        embedding_path = temp_dir / "issues_cluster_local_eval.embeddings.jsonl"
        self._write_csv_rows(
            csv_path,
            [
                "결제 오류가 계속 발생합니다",
                "결제 승인 문제가 반복됩니다",
                "로그인 오류가 계속 발생합니다",
                "로그인 인증 문제가 반복됩니다",
                "배송 오류가 계속 발생합니다",
                "배송 조회 문제가 반복됩니다",
            ],
        )

        local_embedding_vectors = {
            "결제 오류가 계속 발생합니다": [1.0, 0.0],
            "결제 승인 문제가 반복됩니다": [0.999, 0.001],
            "로그인 오류가 계속 발생합니다": [0.998, 0.002],
            "로그인 인증 문제가 반복됩니다": [0.997, 0.003],
            "배송 오류가 계속 발생합니다": [0.996, 0.004],
            "배송 조회 문제가 반복됩니다": [0.995, 0.005],
        }

        with patch(
            "python_ai_worker.skills.dataset_build.rt._generate_dense_embeddings",
            return_value={
                "provider": "fastembed",
                "model": "intfloat/multilingual-e5-small",
                "dimensions": 2,
                "embeddings": [local_embedding_vectors[text] for text in local_embedding_vectors],
            },
        ):
            embedding_result = run_embedding(
                {
                    "dataset_version_id": "version-local-cluster",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(embedding_path),
                    "embedding_model": "intfloat/multilingual-e5-small",
                }
            )

        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_uri": embedding_result["artifact"]["embedding_uri"],
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )

        self.assertEqual(cluster_result["artifact"]["summary"]["similarity_backend"], "dense-hybrid")
        self.assertEqual(cluster_result["artifact"]["summary"]["cluster_count"], 3)
        self.assertEqual(
            [cluster["member_source_indices"] for cluster in cluster_result["artifact"]["clusters"]],
            [[0, 1], [2, 3], [4, 5]],
        )

    def test_local_embedding_fixture_drives_semantic_search_ranking(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_semantic_local_eval.csv"
        embedding_path = temp_dir / "issues_semantic_local_eval.embeddings.jsonl"
        self._write_csv_rows(
            csv_path,
            [
                "결제 오류가 반복 발생했습니다",
                "로그인이 자주 실패하고 오류가 보입니다",
                "배송 문의가 계속 들어옵니다",
            ],
        )

        local_embedding_vectors = {
            "결제 오류가 반복 발생했습니다": [1.0, 0.0],
            "로그인이 자주 실패하고 오류가 보입니다": [0.82, 0.18],
            "배송 문의가 계속 들어옵니다": [0.0, 1.0],
        }

        with patch(
            "python_ai_worker.skills.dataset_build.rt._generate_dense_embeddings",
            return_value={
                "provider": "fastembed",
                "model": "intfloat/multilingual-e5-small",
                "dimensions": 2,
                "embeddings": [local_embedding_vectors[text] for text in local_embedding_vectors],
            },
        ):
            embedding_result = run_embedding(
                {
                    "dataset_version_id": "version-local-semantic",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(embedding_path),
                    "embedding_model": "intfloat/multilingual-e5-small",
                }
            )

        chunk_path = Path(str(embedding_result["artifact"]["chunk_ref"]))
        embedding_index_path = Path(str(embedding_result["artifact"]["embedding_index_source_ref"]))
        index_rows = self._read_parquet_rows(embedding_index_path)

        def fake_query_pgvector_rows(dataset_version_id: str, query_vector: list[float], sample_n: int) -> list[dict[str, object]]:
            self.assertEqual(dataset_version_id, "version-local-semantic")
            scored_rows: list[dict[str, object]] = []
            for row in index_rows:
                embedding_json = str(row.get("embedding_json") or "").strip()
                if not embedding_json:
                    continue
                vector = [float(item) for item in json.loads(embedding_json)]
                score = sum(
                    float(query_vector[index]) * vector[index]
                    for index in range(min(len(query_vector), len(vector)))
                )
                scored_rows.append(
                    {
                        "chunk_id": row["chunk_id"],
                        "row_id": row["row_id"],
                        "source_row_index": row["source_index"],
                        "chunk_index": row["chunk_index"],
                        "chunk_ref": str(chunk_path),
                        "metadata": {
                            "char_start": row["char_start"],
                            "char_end": row["char_end"],
                        },
                        "score": score,
                    }
                )
            scored_rows.sort(key=lambda item: (-float(item["score"]), int(item["source_row_index"])))
            return scored_rows[:sample_n]

        with (
            patch(
                "python_ai_worker.skills.retrieve._lookup_pgvector_index_metadata",
                return_value={"embedding_model": "intfloat/multilingual-e5-small", "vector_dim": 2},
            ),
            patch(
                "python_ai_worker.skills.retrieve.rt._generate_query_embedding",
                return_value=[1.0, 0.0],
            ),
            patch(
                "python_ai_worker.skills.retrieve._query_pgvector_rows",
                side_effect=fake_query_pgvector_rows,
            ),
        ):
            result = run_semantic_search(
                {
                    "dataset_name": str(csv_path),
                    "dataset_version_id": "version-local-semantic",
                    "embedding_index_ref": "pgvector://embedding_index_chunks?dataset_version_id=version-local-semantic",
                    "chunk_ref": str(chunk_path),
                    "chunk_format": "parquet",
                    "query": "결제 오류 관련 근거를 찾아줘",
                    "sample_n": 3,
                    "text_column": "text",
                }
            )

        self.assertEqual(result["artifact"]["retrieval_backend"], "pgvector")
        self.assertEqual(result["artifact"]["embedding_uri"], "")
        self.assertEqual(result["artifact"]["matches"][0]["text"], "결제 오류가 반복 발생했습니다")
        self.assertEqual(result["artifact"]["matches"][1]["text"], "로그인이 자주 실패하고 오류가 보입니다")
        self.assertEqual(result["artifact"]["matches"][0]["chunk_ref"], str(chunk_path))

    def test_embedding_cluster_prefers_pgvector_when_available(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_pgvector.csv"
        chunk_path = temp_dir / "issues_cluster_pgvector.chunks.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류"})
            writer.writerow({"text": "인증 오류"})
            writer.writerow({"text": "배송 문의"})

        chunk_rows = [
            {
                "source_row_index": 0,
                "row_id": "version-pgvector:row:0",
                "chunk_id": "version-pgvector:row:0:chunk:0",
                "chunk_index": 0,
                "chunk_text": "결제 오류",
                "char_start": 0,
                "char_end": 5,
            },
            {
                "source_row_index": 1,
                "row_id": "version-pgvector:row:1",
                "chunk_id": "version-pgvector:row:1:chunk:0",
                "chunk_index": 0,
                "chunk_text": "인증 오류",
                "char_start": 0,
                "char_end": 5,
            },
            {
                "source_row_index": 2,
                "row_id": "version-pgvector:row:2",
                "chunk_id": "version-pgvector:row:2:chunk:0",
                "chunk_index": 0,
                "chunk_text": "배송 문의",
                "char_start": 0,
                "char_end": 5,
            },
        ]
        pq.write_table(pa.Table.from_pylist(chunk_rows), chunk_path)

        with patch(
            "python_ai_worker.skills.retrieve._query_pgvector_cluster_rows",
            return_value=[
                {
                    "chunk_id": "version-pgvector:row:0:chunk:0",
                    "row_id": "version-pgvector:row:0",
                    "source_row_index": 0,
                    "chunk_index": 0,
                    "chunk_ref": str(chunk_path),
                    "embedding_model": "intfloat/multilingual-e5-small",
                    "vector_dim": 2,
                    "embedding_literal": "[1.0,0.0]",
                    "metadata": {},
                },
                {
                    "chunk_id": "version-pgvector:row:1:chunk:0",
                    "row_id": "version-pgvector:row:1",
                    "source_row_index": 1,
                    "chunk_index": 0,
                    "chunk_ref": str(chunk_path),
                    "embedding_model": "intfloat/multilingual-e5-small",
                    "vector_dim": 2,
                    "embedding_literal": "[0.98,0.02]",
                    "metadata": {},
                },
                {
                    "chunk_id": "version-pgvector:row:2:chunk:0",
                    "row_id": "version-pgvector:row:2",
                    "source_row_index": 2,
                    "chunk_index": 0,
                    "chunk_ref": str(chunk_path),
                    "embedding_model": "intfloat/multilingual-e5-small",
                    "vector_dim": 2,
                    "embedding_literal": "[0.0,1.0]",
                    "metadata": {},
                },
            ],
        ):
            cluster_result = run_embedding_cluster(
                {
                    "dataset_name": str(csv_path),
                    "dataset_version_id": "version-pgvector",
                    "embedding_index_ref": "pgvector://embedding_index_chunks?dataset_version_id=version-pgvector",
                    "chunk_ref": str(chunk_path),
                    "chunk_format": "parquet",
                    "cluster_similarity_threshold": 0.2,
                    "sample_n": 2,
                    "top_n": 3,
                }
            )

        self.assertEqual(cluster_result["artifact"]["summary"]["embedding_source_backend"], "pgvector")
        self.assertEqual(cluster_result["artifact"]["embedding_uri"], "")
        self.assertEqual(cluster_result["artifact"]["summary"]["similarity_backend"], "dense-hybrid")
        self.assertEqual(cluster_result["artifact"]["summary"]["cluster_count"], 2)
        self.assertEqual(cluster_result["artifact"]["clusters"][0]["member_source_indices"], [0, 1])

    def test_issue_evidence_summary_alias(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_issue_evidence_summary(
                {
                    "dataset_name": str(csv_path),
                    "query": "결제 오류 관련 근거를 보여줘",
                    "sample_n": 2,
                }
            )

        self.assertEqual(result["artifact"]["skill_name"], "issue_evidence_summary")
        self.assertEqual(len(result["artifact"]["evidence"]), 2)
        self.assertEqual(result["artifact"]["analysis_context"], [])

    def test_issue_evidence_summary_includes_prior_analysis_context(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "결제 승인 오류가 다시 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_issue_evidence_summary(
                {
                    "dataset_name": str(csv_path),
                    "query": "결제 오류 관련 근거를 보여줘",
                    "sample_n": 2,
                    "prior_artifacts": {
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
                    },
                }
            )

        context = result["artifact"]["analysis_context"]
        self.assertEqual(len(context), 2)
        self.assertEqual(context[0]["source_skill"], "issue_trend_summary")
        self.assertIn("피크 구간", context[0]["summary"])
        self.assertIn("issue_period_compare", result["artifact"]["summary"])
        self.assertIn("증가", result["artifact"]["summary"])

    def test_issue_evidence_summary_prefers_cluster_membership_for_cluster_goal(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_cluster_evidence.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            for row in (
                "결제 오류가 반복 발생했습니다",
                "결제 승인 오류가 다시 발생했습니다",
                "로그인이 자주 실패합니다",
                "배송 문의가 계속 들어옵니다",
            ):
                writer.writerow({"text": row})

        embedding_result = run_embedding(
            {
                "dataset_version_id": "version-cluster-evidence",
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(temp_dir / "issues_cluster_evidence.embeddings.jsonl"),
            }
        )
        cluster_build_result = run_dataset_cluster_build(
            {
                "dataset_version_id": "version-cluster-evidence",
                "dataset_name": str(csv_path),
                "embedding_index_source_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "chunk_ref": embedding_result["artifact"]["chunk_ref"],
                "output_path": str(temp_dir / "issues_cluster_evidence.clusters.json"),
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )
        cluster_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "embedding_index_ref": embedding_result["artifact"]["embedding_index_source_ref"],
                "cluster_ref": cluster_build_result["artifact"]["cluster_ref"],
                "cluster_format": "json",
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            }
        )
        label_result = run_cluster_label_candidates(
            {
                "dataset_name": str(csv_path),
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:cluster": cluster_result["artifact"],
                },
            }
        )
        summary_result = run_issue_cluster_summary(
            {
                "dataset_name": str(csv_path),
                "sample_n": 2,
                "top_n": 3,
                "prior_artifacts": {
                    "step:cluster": cluster_result["artifact"],
                    "step:labels": label_result["artifact"],
                },
            }
        )

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_issue_evidence_summary(
                {
                    "dataset_name": str(csv_path),
                    "query": "주요 이슈 군집을 설명해줘",
                    "sample_n": 2,
                    "prior_artifacts": {
                        "step:cluster": cluster_result["artifact"],
                        "step:summary": summary_result["artifact"],
                    },
                }
            )

        self.assertEqual(result["artifact"]["selection_source"], "cluster_membership")
        self.assertEqual(result["artifact"]["evidence"][0]["chunk_id"], "version-cluster-evidence:row:0:chunk:0")
        self.assertIn("결제", result["artifact"]["evidence"][0]["snippet"])

    def test_issue_evidence_summary_compacts_analysis_context_when_limits_are_small(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "결제 승인 오류가 다시 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})

        prior_artifacts = {
            "trend": {
                "skill_name": "issue_trend_summary",
                "bucket": "day",
                "summary": {
                    "peak_bucket": "2026-03-27",
                    "peak_count": 3,
                },
            },
            "breakdown": {
                "skill_name": "issue_breakdown_summary",
                "summary": {
                    "dimension_column": "channel",
                    "top_group": "app",
                    "top_group_count": 5,
                },
            },
            "compare": {
                "skill_name": "issue_period_compare",
                "summary": {
                    "current_count": 5,
                    "previous_count": 2,
                    "count_delta": 3,
                },
            },
        }

        with (
            patch.dict(
                "os.environ",
                {
                    "ANTHROPIC_API_KEY": "",
                    "EVIDENCE_CONTEXT_MAX_ENTRIES": "2",
                    "EVIDENCE_CONTEXT_MAX_CHARS": "80",
                    "EVIDENCE_CONTEXT_ENTRY_MAX_CHARS": "40",
                },
                clear=False,
            ),
        ):
            result = run_issue_evidence_summary(
                {
                    "dataset_name": str(csv_path),
                    "query": "결제 오류 관련 근거를 보여줘",
                    "sample_n": 2,
                    "prior_artifacts": prior_artifacts,
                }
            )

        context = result["artifact"]["analysis_context"]
        self.assertEqual(len(context), 2)
        self.assertIn("prompt_compaction", result["artifact"])
        self.assertEqual(result["artifact"]["prompt_compaction"]["analysis_context"]["input_entry_count"], 3)
        self.assertEqual(result["artifact"]["prompt_compaction"]["analysis_context"]["output_entry_count"], 2)
        self.assertTrue(any("analysis_context compacted" in note for note in result["notes"]))

    def test_evidence_pack_fallback(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_evidence_pack(
                {
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "query": "오류 관련 이슈를 알려줘",
                    "sample_n": 2,
                }
            )

        self.assertIn("artifact", result)
        self.assertEqual(result["artifact"]["skill_name"], "evidence_pack")
        self.assertEqual(len(result["artifact"]["evidence"]), 2)
        self.assertTrue(result["artifact"]["summary"])

    def test_evidence_pack_uses_semantic_search_candidates(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        chunk_path = temp_dir / "issues.chunks.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})
            writer.writerow({"text": "로그인이 자주 실패합니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_evidence_pack(
                {
                    "dataset_name": str(csv_path),
                    "query": "결제 오류 관련 근거를 보여줘",
                    "sample_n": 2,
                    "prior_artifacts": {
                        "step:semantic_search": {
                            "skill_name": "semantic_search",
                            "matches": [
                                {
                                    "rank": 1,
                                    "source_index": 2,
                                    "score": 0.91,
                                    "text": "로그인이 자주 실패합니다",
                                    "row_id": "version:row:2",
                                    "chunk_id": "version:row:2:chunk:0",
                                    "chunk_index": 0,
                                    "char_start": 0,
                                    "char_end": 15,
                                    "chunk_ref": str(chunk_path),
                                    "chunk_format": "parquet",
                                },
                                {
                                    "rank": 2,
                                    "source_index": 0,
                                    "score": 0.89,
                                    "text": "결제 오류가 반복 발생했습니다",
                                    "row_id": "version:row:0",
                                    "chunk_id": "version:row:0:chunk:0",
                                    "chunk_index": 0,
                                    "char_start": 0,
                                    "char_end": 16,
                                    "chunk_ref": str(chunk_path),
                                    "chunk_format": "parquet",
                                },
                            ],
                        }
                    },
                }
            )

        self.assertEqual(result["artifact"]["selection_source"], "semantic_search")
        self.assertEqual(result["artifact"]["citation_mode"], "chunk")
        self.assertEqual(result["artifact"]["chunk_ref"], str(chunk_path))
        self.assertEqual(result["artifact"]["evidence"][0]["source_index"], 2)
        self.assertEqual(result["artifact"]["evidence"][0]["chunk_id"], "version:row:2:chunk:0")
        self.assertEqual(result["artifact"]["evidence"][0]["char_end"], 15)
        self.assertIn("selection source: semantic_search", result["notes"])

    def test_issue_evidence_summary_preserves_chunk_citations(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        chunk_path = temp_dir / "issues.chunks.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = run_issue_evidence_summary(
                {
                    "dataset_name": str(csv_path),
                    "query": "결제 오류 근거를 정리해줘",
                    "sample_n": 1,
                    "prior_artifacts": {
                        "step:semantic_search": {
                            "skill_name": "semantic_search",
                            "matches": [
                                {
                                    "rank": 1,
                                    "source_index": 0,
                                    "score": 0.93,
                                    "text": "결제 오류가 반복 발생했습니다",
                                    "row_id": "version:row:0",
                                    "chunk_id": "version:row:0:chunk:0",
                                    "chunk_index": 0,
                                    "char_start": 0,
                                    "char_end": 16,
                                    "chunk_ref": str(chunk_path),
                                    "chunk_format": "parquet",
                                }
                            ],
                        }
                    },
                }
            )

        evidence = result["artifact"]["evidence"][0]
        self.assertEqual(result["artifact"]["selection_source"], "semantic_search")
        self.assertEqual(result["artifact"]["citation_mode"], "chunk")
        self.assertEqual(result["artifact"]["chunk_ref"], str(chunk_path))
        self.assertEqual(evidence["chunk_id"], "version:row:0:chunk:0")
        self.assertEqual(evidence["chunk_index"], 0)
        self.assertEqual(evidence["char_start"], 0)
        self.assertEqual(evidence["char_end"], 16)

    def test_embedding_and_semantic_search(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        embedding_path = temp_dir / "issues.csv.embeddings.jsonl"
        chunk_path = temp_dir / "issues.csv.embeddings.chunks.parquet"
        embedding_index_path = temp_dir / "issues.csv.embeddings.index.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})
            writer.writerow({"text": "배송 문의가 계속 들어옵니다"})

        embedding_result = run_embedding(
            {
                "dataset_version_id": "version-embed",
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(embedding_path),
            }
        )
        self.assertEqual(embedding_result["artifact"]["embedding_uri"], str(embedding_path))
        self.assertEqual(embedding_result["artifact"]["embedding_ref"], str(embedding_path))
        self.assertEqual(embedding_result["artifact"]["embedding_format"], "jsonl")
        self.assertTrue(embedding_result["artifact"]["embedding_debug_export_enabled"])
        self.assertEqual(embedding_result["artifact"]["embedding_index_source_ref"], str(embedding_index_path))
        self.assertEqual(embedding_result["artifact"]["embedding_index_source_format"], "parquet")
        self.assertEqual(embedding_result["artifact"]["chunk_ref"], str(chunk_path))
        self.assertEqual(embedding_result["artifact"]["chunk_format"], "parquet")
        self.assertEqual(embedding_result["artifact"]["row_id_column"], "row_id")
        self.assertEqual(embedding_result["artifact"]["chunk_id_column"], "chunk_id")
        self.assertEqual(embedding_result["artifact"]["chunk_index_column"], "chunk_index")
        self.assertEqual(embedding_result["artifact"]["chunk_text_column"], "chunk_text")
        self.assertEqual(embedding_result["artifact"]["chunking_strategy"], "text-window-v1")
        self.assertTrue(embedding_path.exists())
        self.assertTrue(chunk_path.exists())
        self.assertTrue(embedding_index_path.exists())

        embedding_rows = []
        with embedding_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                embedding_rows.append(json.loads(line))
        chunk_rows = self._read_parquet_rows(chunk_path)
        embedding_index_rows = self._read_parquet_rows(embedding_index_path)
        self.assertEqual(embedding_rows[0]["row_id"], "version-embed:row:0")
        self.assertEqual(embedding_rows[0]["chunk_id"], "version-embed:row:0:chunk:0")
        self.assertEqual(chunk_rows[0]["chunk_id"], "version-embed:row:0:chunk:0")
        self.assertEqual(chunk_rows[0]["chunk_text"], "결제 오류가 반복 발생했습니다")
        self.assertEqual(embedding_index_rows[0]["chunk_id"], "version-embed:row:0:chunk:0")
        self.assertIn("token_counts_json", embedding_index_rows[0])

        search_result = run_semantic_search(
            {
                "dataset_name": str(csv_path),
                "query": "결제 오류 관련 문서를 찾아줘",
                "sample_n": 2,
                "embedding_uri": str(embedding_path),
            }
        )
        self.assertEqual(search_result["artifact"]["skill_name"], "semantic_search")
        self.assertEqual(search_result["artifact"]["summary"]["match_count"], 2)
        self.assertEqual(search_result["artifact"]["summary"]["citation_mode"], "chunk")
        self.assertEqual(search_result["artifact"]["matches"][0]["source_index"], 0)
        self.assertEqual(search_result["artifact"]["matches"][0]["row_id"], "version-embed:row:0")
        self.assertEqual(search_result["artifact"]["matches"][0]["chunk_id"], "version-embed:row:0:chunk:0")
        self.assertEqual(search_result["artifact"]["matches"][0]["chunk_index"], 0)
        self.assertEqual(search_result["artifact"]["matches"][0]["char_start"], 0)
        self.assertEqual(search_result["artifact"]["matches"][0]["char_end"], len("결제 오류가 반복 발생했습니다"))
        self.assertEqual(search_result["artifact"]["chunk_ref"], str(chunk_path))
        self.assertEqual(search_result["artifact"]["chunk_format"], "parquet")

    def test_embedding_defaults_to_index_parquet_without_jsonl_export(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_default.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패하고 오류가 보입니다"})

        result = run_embedding(
            {
                "dataset_version_id": "version-default",
                "dataset_name": str(csv_path),
                "text_column": "text",
            }
        )

        chunk_path = Path(str(result["artifact"]["chunk_ref"]))
        embedding_index_path = Path(str(result["artifact"]["embedding_index_source_ref"]))
        self.assertEqual(result["artifact"]["embedding_uri"], "")
        self.assertEqual(result["artifact"]["embedding_ref"], "")
        self.assertEqual(result["artifact"]["embedding_format"], "")
        self.assertFalse(result["artifact"]["embedding_debug_export_enabled"])
        self.assertTrue(chunk_path.exists())
        self.assertTrue(embedding_index_path.exists())

    def test_embedding_writes_schemaful_empty_parquet_outputs(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_empty.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": ""})
            writer.writerow({"text": "   "})

        result = run_embedding(
            {
                "dataset_version_id": "version-empty",
                "dataset_name": str(csv_path),
                "text_column": "text",
            }
        )

        chunk_path = Path(str(result["artifact"]["chunk_ref"]))
        embedding_index_path = Path(str(result["artifact"]["embedding_index_source_ref"]))
        chunk_table = pq.read_table(chunk_path)
        embedding_index_table = pq.read_table(embedding_index_path)

        self.assertEqual(chunk_table.num_rows, 0)
        self.assertEqual(embedding_index_table.num_rows, 0)
        self.assertEqual(chunk_table.column_names, ["source_row_index", "row_id", "chunk_id", "chunk_index", "chunk_text", "char_start", "char_end"])
        self.assertEqual(
            embedding_index_table.column_names,
            [
                "source_index",
                "row_id",
                "chunk_id",
                "chunk_index",
                "char_start",
                "char_end",
                "embedding_json",
                "embedding_dim",
                "embedding_provider",
                "token_counts_json",
            ],
        )

    def test_semantic_search_prefers_pgvector_when_available(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        chunk_path = temp_dir / "issues.chunks.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패합니다"})

        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "source_row_index": 0,
                        "row_id": "version-1:row:0",
                        "chunk_id": "version-1:row:0:chunk:0",
                        "chunk_index": 0,
                        "chunk_text": "결제 오류가 반복 발생했습니다",
                        "char_start": 0,
                        "char_end": 16,
                    },
                    {
                        "source_row_index": 1,
                        "row_id": "version-1:row:1",
                        "chunk_id": "version-1:row:1:chunk:0",
                        "chunk_index": 0,
                        "chunk_text": "로그인이 자주 실패합니다",
                        "char_start": 0,
                        "char_end": 15,
                    },
                ]
            ),
            chunk_path,
        )

        with (
            patch(
                "python_ai_worker.skills.retrieve._lookup_pgvector_index_metadata",
                return_value={"embedding_model": "token-overlap-v1", "vector_dim": 64},
            ),
            patch(
                "python_ai_worker.skills.retrieve._query_pgvector_rows",
                return_value=[
                    {
                        "chunk_id": "version-1:row:0:chunk:0",
                        "row_id": "version-1:row:0",
                        "source_row_index": 0,
                        "chunk_index": 0,
                        "chunk_ref": str(chunk_path),
                        "metadata": {"char_start": 0, "char_end": 16},
                        "score": 0.88,
                    }
                ],
            ),
        ):
            result = run_semantic_search(
                {
                    "dataset_name": str(csv_path),
                    "query": "결제 오류 관련 문서를 찾아줘",
                    "sample_n": 2,
                    "dataset_version_id": "version-1",
                    "embedding_uri": str(temp_dir / "issues.embeddings.jsonl"),
                    "chunk_ref": str(chunk_path),
                    "chunk_format": "parquet",
                }
            )

        self.assertEqual(result["artifact"]["retrieval_backend"], "pgvector")
        self.assertEqual(result["artifact"]["summary"]["retrieval_backend"], "pgvector")
        self.assertEqual(result["artifact"]["matches"][0]["text"], "결제 오류가 반복 발생했습니다")
        self.assertEqual(result["artifact"]["matches"][0]["chunk_id"], "version-1:row:0:chunk:0")
        self.assertIn("semantic search executed with pgvector index", result["notes"])

    def test_semantic_search_returns_empty_pgvector_matches_without_sidecar_fallback(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})

        with (
            patch(
                "python_ai_worker.skills.retrieve._lookup_pgvector_index_metadata",
                return_value={"embedding_model": "token-overlap-v1", "vector_dim": 64},
            ),
            patch(
                "python_ai_worker.skills.retrieve._query_pgvector_rows",
                return_value=[],
            ),
        ):
            result = run_semantic_search(
                {
                    "dataset_name": str(csv_path),
                    "dataset_version_id": "version-empty-pgvector",
                    "query": "결제 오류 관련 문서를 찾아줘",
                    "sample_n": 2,
                    "embedding_index_ref": "pgvector://embedding_index_chunks?dataset_version_id=version-empty-pgvector",
                    "chunk_format": "parquet",
                }
            )

        self.assertEqual(result["artifact"]["retrieval_backend"], "pgvector")
        self.assertEqual(result["artifact"]["summary"]["candidate_count"], 0)
        self.assertEqual(result["artifact"]["summary"]["match_count"], 0)
        self.assertEqual(result["artifact"]["matches"], [])
        self.assertEqual(result["artifact"]["embedding_uri"], "")
        self.assertIn("semantic search executed with pgvector index", result["notes"])

    def test_embedding_adds_dense_vectors_when_available(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_dense.csv"
        embedding_path = temp_dir / "issues_dense.embeddings.jsonl"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})

        with patch(
            "python_ai_worker.skills.dataset_build.rt._generate_dense_embeddings",
            return_value={
                "provider": "openai",
                "model": "text-embedding-3-small",
                "dimensions": 3,
                "embeddings": [[0.1, 0.2, 0.3]],
                "usage_prompt_tokens": 12,
                "usage": {
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "operation": "embedding",
                    "request_count": 1,
                    "prompt_tokens": 12,
                    "input_text_count": 1,
                    "vector_count": 1,
                    "cost_estimation_status": "not_configured",
                },
            },
        ):
            result = run_embedding(
                {
                    "dataset_version_id": "version-dense",
                    "dataset_name": str(csv_path),
                    "text_column": "text",
                    "output_path": str(embedding_path),
                    "embedding_model": "text-embedding-3-small",
                }
            )

        embedding_rows = []
        with embedding_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                embedding_rows.append(json.loads(line))

        self.assertEqual(result["artifact"]["embedding_model"], "text-embedding-3-small")
        self.assertEqual(result["artifact"]["embedding_provider"], "openai")
        self.assertEqual(result["artifact"]["embedding_vector_dim"], 3)
        self.assertEqual(result["artifact"]["embedding_representation"], "dense+token-overlap")
        self.assertEqual(result["artifact"]["usage"]["provider"], "openai")
        self.assertEqual(result["artifact"]["usage"]["prompt_tokens"], 12)
        self.assertEqual(result["artifact"]["usage"]["vector_count"], 1)
        self.assertEqual(embedding_rows[0]["embedding"], [0.1, 0.2, 0.3])
        self.assertEqual(embedding_rows[0]["embedding_dim"], 3)
        self.assertEqual(embedding_rows[0]["embedding_provider"], "openai")
        self.assertIn("embedding provider: openai", result["notes"])

    def test_semantic_search_uses_dense_query_embedding_when_index_is_dense(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_dense_search.csv"
        chunk_path = temp_dir / "issues_dense_search.chunks.parquet"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "!!!"})

        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "source_row_index": 0,
                        "row_id": "version-dense:row:0",
                        "chunk_id": "version-dense:row:0:chunk:0",
                        "chunk_index": 0,
                        "chunk_text": "결제 오류가 반복 발생했습니다",
                        "char_start": 0,
                        "char_end": 16,
                    }
                ]
            ),
            chunk_path,
        )

        captured_vectors: list[list[float]] = []

        def _capture_query(dataset_version_id: str, query_vector: list[float], sample_n: int) -> list[dict[str, object]]:
            captured_vectors.append(list(query_vector))
            return [
                {
                    "chunk_id": "version-dense:row:0:chunk:0",
                    "row_id": "version-dense:row:0",
                    "source_row_index": 0,
                    "chunk_index": 0,
                    "chunk_ref": str(chunk_path),
                    "metadata": {"char_start": 0, "char_end": 16},
                    "score": 0.93,
                }
            ]

        with (
            patch(
                "python_ai_worker.skills.retrieve._lookup_pgvector_index_metadata",
                return_value={"embedding_model": "text-embedding-3-small", "vector_dim": 3},
            ),
            patch(
                "python_ai_worker.skills.retrieve.rt._generate_query_embedding",
                return_value=[0.9, 0.1, 0.4],
            ) as generate_query_embedding,
            patch(
                "python_ai_worker.skills.retrieve._query_pgvector_rows",
                side_effect=_capture_query,
            ),
        ):
            result = run_semantic_search(
                {
                    "dataset_name": str(csv_path),
                    "query": "!!!",
                    "sample_n": 1,
                    "dataset_version_id": "version-dense",
                    "embedding_uri": str(temp_dir / "issues_dense_search.embeddings.jsonl"),
                    "chunk_ref": str(chunk_path),
                    "chunk_format": "parquet",
                }
            )

        generate_query_embedding.assert_called_once_with("!!!", model="text-embedding-3-small", dimensions=3)
        self.assertEqual(captured_vectors, [[0.9, 0.1, 0.4]])
        self.assertEqual(result["artifact"]["retrieval_backend"], "pgvector")
        self.assertEqual(result["artifact"]["matches"][0]["chunk_id"], "version-dense:row:0:chunk:0")

    def test_embedding_creates_multiple_chunks_for_long_text(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_long.csv"
        embedding_path = temp_dir / "issues_long.embeddings.jsonl"
        long_text = " ".join(["결제 오류가 반복 발생했습니다"] * 40)
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": long_text})

        result = run_embedding(
            {
                "dataset_version_id": "version-chunk",
                "dataset_name": str(csv_path),
                "text_column": "text",
                "output_path": str(embedding_path),
                "chunk_max_chars": 80,
                "chunk_overlap_chars": 10,
            }
        )

        chunk_rows = self._read_parquet_rows(Path(result["artifact"]["chunk_ref"]))
        self.assertGreater(len(chunk_rows), 1)
        self.assertEqual(result["artifact"]["source_row_count"], 1)
        self.assertEqual(result["artifact"]["chunk_count"], len(chunk_rows))
        self.assertEqual(chunk_rows[0]["row_id"], "version-chunk:row:0")
        self.assertEqual(chunk_rows[0]["chunk_index"], 0)
        self.assertLessEqual(len(str(chunk_rows[0]["chunk_text"])), 80)


if __name__ == "__main__":
    unittest.main()
