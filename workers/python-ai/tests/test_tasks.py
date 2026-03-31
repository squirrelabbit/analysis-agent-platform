from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from python_ai_worker.tasks import (
    run_cluster_label_candidates,
    run_deduplicate_documents,
    run_dictionary_tagging,
    run_document_filter,
    run_document_sample,
    run_dataset_prepare,
    run_embedding,
    run_embedding_cluster,
    run_evidence_pack,
    run_issue_breakdown_summary,
    run_issue_cluster_summary,
    run_issue_evidence_summary,
    run_issue_period_compare,
    run_issue_sentiment_summary,
    run_issue_taxonomy_summary,
    run_issue_trend_summary,
    run_keyword_frequency,
    run_meta_group_count,
    run_planner,
    run_semantic_search,
    run_sentiment_label,
    run_time_bucket_count,
)


class TaskTests(unittest.TestCase):
    class _DummyPrepareClient:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows
            self._config = type("Config", (), {"model": "claude-test"})()

        def is_enabled(self) -> bool:
            return True

        def create_json(self, *, prompt: str, schema: dict[str, object], max_tokens: int | None = None) -> dict[str, object]:
            return {"rows": self._rows}

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
        prepared_path = temp_dir / "issues_raw.prepared.jsonl"
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
        self.assertEqual(result["artifact"]["prepare_format"], "jsonl")
        self.assertEqual(result["artifact"]["prepared_ref"], str(prepared_path))
        self.assertEqual(result["artifact"]["row_id_column"], "row_id")
        self.assertEqual(result["artifact"]["summary"]["input_row_count"], 3)
        self.assertEqual(result["artifact"]["summary"]["output_row_count"], 2)
        self.assertTrue(prepared_path.exists())

        prepared_rows = []
        with prepared_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                prepared_rows.append(json.loads(line))

        self.assertEqual(len(prepared_rows), 2)
        self.assertEqual(prepared_rows[0]["row_id"], "version-1:row:0")
        self.assertEqual(prepared_rows[0]["normalized_text"], "결제 오류가 반복 발생했습니다.")
        self.assertEqual(prepared_rows[0]["prepare_disposition"], "keep")
        self.assertEqual(prepared_rows[0]["channel"], "app")

    def test_dataset_prepare_batches_llm_requests(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues_raw.csv"
        prepared_path = temp_dir / "issues_raw.prepared.jsonl"
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

        prepared_rows = []
        with prepared_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                prepared_rows.append(json.loads(line))

        self.assertEqual(prepared_rows[0]["prepare_prompt_version"], "dataset-prepare-anthropic-batch-v1")
        self.assertEqual(prepared_rows[0]["row_id"], "version-2:row:0")
        self.assertEqual(prepared_rows[1]["prepare_disposition"], "review")

    def test_sentiment_label_fallback(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        prepared_path = temp_dir / "issues.prepared.jsonl"
        sentiment_path = temp_dir / "issues.sentiment.jsonl"
        with prepared_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps({"normalized_text": "결제 오류가 반복 발생했습니다", "channel": "app"}, ensure_ascii=False))
            handle.write("\n")
            handle.write(json.dumps({"normalized_text": "빠르게 해결되어 만족합니다", "channel": "app"}, ensure_ascii=False))
            handle.write("\n")

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
        self.assertEqual(result["artifact"]["sentiment_format"], "jsonl")
        self.assertEqual(result["artifact"]["sentiment_ref"], str(sentiment_path))
        self.assertEqual(result["artifact"]["row_id_column"], "row_id")
        self.assertEqual(result["artifact"]["summary"]["labeled_row_count"], 2)
        self.assertTrue(sentiment_path.exists())
        labeled_rows = []
        with sentiment_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                labeled_rows.append(json.loads(line))
        self.assertEqual(labeled_rows[0]["row_id"], "version-1:row:0")
        self.assertEqual(labeled_rows[0]["sentiment_label"], "negative")
        self.assertEqual(labeled_rows[1]["sentiment_label"], "positive")

    def test_issue_sentiment_summary(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        jsonl_path = temp_dir / "issues.sentiment.jsonl"
        rows = [
            {"normalized_text": "결제 오류가 반복 발생했습니다", "sentiment_label": "negative"},
            {"normalized_text": "빠르게 해결되어 만족합니다", "sentiment_label": "positive"},
            {"normalized_text": "문의 접수 후 확인 중입니다", "sentiment_label": "neutral"},
            {"normalized_text": "결제 오류가 다시 발생했습니다", "sentiment_label": "negative"},
        ]
        with jsonl_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False))
                handle.write("\n")

        result = run_issue_sentiment_summary(
            {
                "dataset_name": str(jsonl_path),
                "text_column": "normalized_text",
                "sentiment_column": "sentiment_label",
                "sample_n": 2,
            }
        )

        self.assertEqual(result["artifact"]["skill_name"], "issue_sentiment_summary")
        self.assertEqual(result["artifact"]["summary"]["document_count"], 4)
        self.assertEqual(result["artifact"]["summary"]["dominant_label"], "negative")
        self.assertEqual(result["artifact"]["summary"]["negative_count"], 2)
        self.assertEqual(result["artifact"]["breakdown"][0]["sentiment_label"], "negative")

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
                                {"rank": 1, "source_index": 2, "score": 0.91, "text": "로그인이 자주 실패합니다"},
                                {"rank": 2, "source_index": 0, "score": 0.89, "text": "결제 오류가 반복 발생했습니다"},
                            ],
                        }
                    },
                }
            )

        self.assertEqual(result["artifact"]["selection_source"], "semantic_search")
        self.assertEqual(result["artifact"]["evidence"][0]["source_index"], 2)
        self.assertIn("selection source: semantic_search", result["notes"])

    def test_embedding_and_semantic_search(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        embedding_path = temp_dir / "issues.csv.embeddings.jsonl"
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
        self.assertEqual(embedding_result["artifact"]["row_id_column"], "row_id")
        self.assertEqual(embedding_result["artifact"]["chunk_id_column"], "chunk_id")
        self.assertTrue(embedding_path.exists())

        embedding_rows = []
        with embedding_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                embedding_rows.append(json.loads(line))
        self.assertEqual(embedding_rows[0]["row_id"], "version-embed:row:0")
        self.assertEqual(embedding_rows[0]["chunk_id"], "version-embed:row:0:chunk:0")

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
        self.assertEqual(search_result["artifact"]["matches"][0]["source_index"], 0)
        self.assertEqual(search_result["artifact"]["matches"][0]["row_id"], "version-embed:row:0")
        self.assertEqual(search_result["artifact"]["matches"][0]["chunk_id"], "version-embed:row:0:chunk:0")


if __name__ == "__main__":
    unittest.main()
