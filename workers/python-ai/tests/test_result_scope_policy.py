from __future__ import annotations

import csv
import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from python_ai_worker.skill_contracts import SkillOutputError, validate_task_result
from python_ai_worker.tasks import (
    run_cluster_label_candidates,
    run_dataset_prepare,
    run_embedding_cluster,
    run_issue_cluster_summary,
    run_sentiment_label,
    run_task,
)
from python_ai_worker.runtime import infer_runtime_scope_from_prior


class ResultScopePolicyTests(unittest.TestCase):
    class _DummyClient:
        def __init__(self, model: str = "claude-test") -> None:
            self._config = type("Config", (), {"model": model})()

        def is_enabled(self) -> bool:
            return True

    def test_pydantic_runtime_dependency_is_required(self) -> None:
        module = importlib.import_module("pydantic")
        self.assertTrue(hasattr(module, "BaseModel"))
        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module("python_ai_worker.skills._pydantic_compat")

    def test_static_skill_rejects_runtime_scope_mismatch(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "static policy"):
            validate_task_result(
                "embedding",
                {},
                {
                    "artifact": {
                        "skill_name": "embedding",
                        "result_scope": "full_dataset",
                        "runtime_result_scope": "document_subset",
                    }
                },
            )

    def test_infer_runtime_scope_from_prior_requires_declared_scope_without_prior(self) -> None:
        with self.assertRaisesRegex(ValueError, "could not be inferred"):
            infer_runtime_scope_from_prior({})

    def test_inherits_scope_uses_prior_runtime_scope(self) -> None:
        validate_task_result(
            "term_frequency",
            {
                "prior_artifacts": {
                    "step:cluster": {
                        "skill_name": "embedding_cluster",
                        "result_scope": "cluster_subset",
                        "runtime_result_scope": "cluster_subset",
                    }
                }
            },
            {
                "artifact": {
                    "skill_name": "term_frequency",
                    "result_scope": "document_subset",
                    "runtime_result_scope": "cluster_subset",
                }
            },
        )

    def test_dynamic_skill_rejects_runtime_scope_outside_allowed_set(self) -> None:
        with self.assertRaisesRegex(SkillOutputError, "outside allowed_runtime_result_scopes"):
            validate_task_result(
                "embedding_cluster",
                {},
                {
                    "artifact": {
                        "skill_name": "embedding_cluster",
                        "result_scope": "cluster_subset",
                        "runtime_result_scope": "partial_build",
                        "clusters": [{"cluster_id": "cluster-01"}],
                    }
                },
            )

    def test_dynamic_skills_populate_runtime_scope(self) -> None:
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "issues.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            writer.writerow({"text": "결제 오류가 반복 발생했습니다"})
            writer.writerow({"text": "로그인이 자주 실패합니다"})
        embedding_jsonl = temp_dir / "issues.embeddings.jsonl"
        embedding_rows = [
            {
                "source_index": 0,
                "row_id": "row-0",
                "chunk_id": "row-0:chunk:0",
                "chunk_index": 0,
                "text": "결제 오류가 반복 발생했습니다",
                "token_counts": {"결제": 1, "오류": 1},
                "norm": 2.0,
            },
            {
                "source_index": 1,
                "row_id": "row-1",
                "chunk_id": "row-1:chunk:0",
                "chunk_index": 0,
                "text": "로그인이 자주 실패합니다",
                "token_counts": {"로그인": 1, "실패": 1},
                "norm": 2.0,
            },
        ]
        with embedding_jsonl.open("w", encoding="utf-8") as handle:
            for row in embedding_rows:
                handle.write(json.dumps(row, ensure_ascii=False))
                handle.write("\n")

        issue_summary = run_task(
            "unstructured_issue_summary",
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "sample_n": 2,
                "top_n": 2,
            },
        )
        self.assertEqual(issue_summary["artifact"]["runtime_result_scope"], "full_dataset")

        embedding_result = run_embedding_cluster(
            {
                "dataset_name": str(csv_path),
                "dataset_version_id": "dataset-version-1",
                "text_column": "text",
                "embedding_uri": str(embedding_jsonl),
                "sample_n": 2,
                "top_n": 2,
                "cluster_similarity_threshold": 0.3,
                "prior_artifacts": {
                    "step:filter": {
                        "skill_name": "document_filter",
                        "result_scope": "document_subset",
                        "runtime_result_scope": "document_subset",
                        "matched_indices": [0],
                    }
                },
            }
        )
        self.assertEqual(embedding_result["artifact"]["runtime_result_scope"], "cluster_subset")

        labeled_clusters = run_cluster_label_candidates(
            {
                "dataset_name": str(csv_path),
                "sample_n": 2,
                "top_n": 2,
                "prior_artifacts": {
                    "step:cluster": embedding_result["artifact"],
                },
            }
        )
        issue_cluster_result = run_issue_cluster_summary(
            {
                "dataset_name": str(csv_path),
                "text_column": "text",
                "sample_n": 2,
                "top_n": 2,
                "prior_artifacts": {
                    "step:cluster-labels": labeled_clusters["artifact"],
                },
            }
        )
        self.assertEqual(issue_cluster_result["artifact"]["runtime_result_scope"], "cluster_subset")

        with patch(
            "python_ai_worker.skills.dataset_build.rt._anthropic_prepare_client",
            return_value=self._DummyClient(),
        ), patch(
            "python_ai_worker.skills.dataset_build.rt._prepare_rows",
            return_value=(
                [
                    {
                        "normalized_text": "결제 오류",
                        "disposition": "keep",
                        "reason": "ok",
                        "quality_flags": [],
                        "prompt_version": "prepare-test-v1",
                    }
                ],
                {"provider": "anthropic"},
            ),
        ):
            prepare_result = run_dataset_prepare(
                {
                    "dataset_name": str(csv_path),
                    "dataset_version_id": "dataset-version-1",
                    "output_path": str(temp_dir / "prepared.parquet"),
                    "progress_path": str(temp_dir / "prepare.progress.json"),
                    "text_column": "text",
                    "text_columns": ["text"],
                    "text_joiner": "\n\n",
                    "max_rows": 1,
                    "prepare_batch_size": 1,
                    "model": "claude-test",
                    "llm_mode": "enabled",
                }
            )
        self.assertEqual(prepare_result["artifact"]["runtime_result_scope"], "partial_build")

        with patch(
            "python_ai_worker.skills.dataset_build.rt._anthropic_sentiment_client",
            return_value=self._DummyClient(),
        ), patch(
            "python_ai_worker.skills.dataset_build.rt._label_sentiments",
            return_value=(
                [
                    {
                        "label": "negative",
                        "confidence": 0.9,
                        "reason": "테스트",
                        "prompt_version": "sentiment-test-v1",
                    }
                ],
                {"provider": "anthropic"},
            ),
        ):
            sentiment_result = run_sentiment_label(
                {
                    "dataset_name": str(csv_path),
                    "dataset_version_id": "dataset-version-1",
                    "output_path": str(temp_dir / "sentiment.parquet"),
                    "text_column": "text",
                    "text_columns": ["text"],
                    "text_joiner": "\n\n",
                    "max_rows": 1,
                    "sentiment_batch_size": 1,
                    "model": "claude-test",
                    "llm_mode": "enabled",
                }
            )
        self.assertEqual(sentiment_result["artifact"]["runtime_result_scope"], "partial_build")


if __name__ == "__main__":
    unittest.main()
