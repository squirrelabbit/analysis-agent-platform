from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker.devtools.evaluate_embedding_model import evaluate_embedding_model


class EmbeddingEvalTests(unittest.TestCase):
    def test_evaluate_embedding_model_reports_dense_hybrid_advantage(self) -> None:
        passage_vectors = {
            "결제 오류가 반복 발생했습니다": [1.0, 0.0],
            "로그인이 자주 실패하고 오류가 보입니다": [0.82, 0.18],
            "배송 문의가 계속 들어옵니다": [0.0, 1.0],
            "결제 승인 오류가 다시 발생했습니다": [0.98, 0.02],
            "로그인이 계속 실패합니다": [0.9, 0.1],
            "로그인 인증 오류가 반복됩니다": [0.89, 0.11],
            "결제 오류가 계속 발생합니다": [1.0, 0.0],
            "결제 승인 문제가 반복됩니다": [0.999, 0.001],
            "로그인 오류가 계속 발생합니다": [0.998, 0.002],
            "로그인 인증 문제가 반복됩니다": [0.997, 0.003],
            "배송 오류가 계속 발생합니다": [0.996, 0.004],
            "배송 조회 문제가 반복됩니다": [0.995, 0.005],
            "결제 오류": [1.0, 0.0],
            "인증 오류": [0.98, 0.02],
            "배송 문의": [0.0, 1.0],
        }
        query_vectors = {
            "결제 오류 관련 근거를 찾아줘": [1.0, 0.0],
            "로그인 실패 원인을 찾고 싶어": [0.9, 0.1],
        }

        def fake_dense_embeddings(texts: list[str], *, model: str = "", dimensions: int = 0) -> dict[str, object]:
            return {
                "provider": "fastembed",
                "model": model or "intfloat/multilingual-e5-small",
                "dimensions": 2,
                "embeddings": [passage_vectors[text] for text in texts],
            }

        def fake_query_embedding(text: str, *, model: str = "", dimensions: int = 0) -> list[float]:
            return list(query_vectors[text])

        with (
            patch("python_ai_worker.devtools.evaluate_embedding_model._generate_dense_embeddings", side_effect=fake_dense_embeddings),
            patch("python_ai_worker.devtools.evaluate_embedding_model._generate_query_embedding", side_effect=fake_query_embedding),
        ):
            report = evaluate_embedding_model("intfloat/multilingual-e5-small")

        self.assertEqual(report["summary"]["search_top1_pass_count"], report["summary"]["search_case_count"])
        self.assertEqual(report["summary"]["search_topk_pass_count"], report["summary"]["search_case_count"])
        self.assertEqual(report["summary"]["cluster_dense_hybrid_pass_count"], report["summary"]["cluster_case_count"])
        self.assertLess(report["summary"]["cluster_dense_only_pass_count"], report["summary"]["cluster_case_count"])


if __name__ == "__main__":
    unittest.main()
