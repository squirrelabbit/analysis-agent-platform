"""artifact provenance 표준 블록 테스트 (ADR-031 2단계)."""
from __future__ import annotations

import unittest

from python_ai_worker.dataset_build._common import (
    ARTIFACT_SCHEMA_VERSION,
    DatasetBuildFailureRateExceeded,
    build_provenance,
    check_failure_rate,
)


class BuildProvenanceTests(unittest.TestCase):
    def test_required_fields_present(self) -> None:
        p = build_provenance(
            producer_task="dataset_doc_genuineness",
            dataset_version_id="v1",
            model_id="wisenut/wise-lloa-max-v1.2.1",
            prompt_version="v1",
        )
        for key in (
            "artifact_schema_version", "producer_task", "dataset_version_id",
            "model_id", "judge_model_id", "prompt_version", "taxonomy_id",
            "verify_mode", "chunking_config_hash", "input_artifact_refs",
            "deterministic_hash", "created_at",
        ):
            self.assertIn(key, p, f"provenance에 표준 필드 누락: {key}")
        self.assertEqual(p["artifact_schema_version"], ARTIFACT_SCHEMA_VERSION)
        self.assertEqual(p["producer_task"], "dataset_doc_genuineness")
        self.assertEqual(p["input_artifact_refs"], [])

    def test_hash_ignores_dataset_version_id(self) -> None:
        # deterministic_hash는 "설정 식별자"라 dataset_version_id(데이터 식별)와 무관.
        # 같은 설정으로 다른 버전을 빌드해도 같은 설정임을 식별할 수 있어야 한다.
        a = build_provenance(producer_task="t", dataset_version_id="vA", model_id="m", prompt_version="p1")
        b = build_provenance(producer_task="t", dataset_version_id="vB", model_id="m", prompt_version="p1")
        self.assertEqual(a["deterministic_hash"], b["deterministic_hash"])

    def test_hash_changes_with_config(self) -> None:
        base = build_provenance(producer_task="t", dataset_version_id="v", model_id="m1", prompt_version="p1")
        diff_model = build_provenance(producer_task="t", dataset_version_id="v", model_id="m2", prompt_version="p1")
        diff_prompt = build_provenance(producer_task="t", dataset_version_id="v", model_id="m1", prompt_version="p2")
        self.assertNotEqual(base["deterministic_hash"], diff_model["deterministic_hash"])
        self.assertNotEqual(base["deterministic_hash"], diff_prompt["deterministic_hash"])

    def test_chunking_config_hash(self) -> None:
        with_chunk = build_provenance(
            producer_task="t", dataset_version_id="v",
            chunking_config={"threshold_chars": 20000, "max_chunk_sentences": 60},
        )
        without = build_provenance(producer_task="t", dataset_version_id="v", chunking_config=None)
        self.assertIsNotNone(with_chunk["chunking_config_hash"])
        self.assertIsNone(without["chunking_config_hash"])

    def test_verify_and_judge_model(self) -> None:
        p = build_provenance(
            producer_task="dataset_doc_genuineness", dataset_version_id="v",
            model_id="a", judge_model_id="j", verify_mode="cross_model",
        )
        self.assertEqual(p["judge_model_id"], "j")
        self.assertEqual(p["verify_mode"], "cross_model")


class FailureRateGuardTests(unittest.TestCase):
    """partial-failure 공통 가드 (ADR-031 3단계)."""

    def test_raises_when_at_or_above_threshold(self) -> None:
        with self.assertRaises(DatasetBuildFailureRateExceeded):
            check_failure_rate(task="t", failures=5, total=10, max_rate=0.5, detail="x")

    def test_no_raise_below_threshold(self) -> None:
        check_failure_rate(task="t", failures=4, total=10, max_rate=0.5, detail="x")

    def test_no_raise_when_total_zero(self) -> None:
        check_failure_rate(task="t", failures=0, total=0, max_rate=0.5, detail="x")

    def test_is_runtime_error_subclass(self) -> None:
        # 기존 RuntimeError를 잡던 호출부와 호환되어야 한다.
        self.assertTrue(issubclass(DatasetBuildFailureRateExceeded, RuntimeError))


if __name__ == "__main__":
    unittest.main()
