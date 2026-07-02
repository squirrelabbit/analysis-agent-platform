import json
import tempfile
import unittest
from pathlib import Path

from python_ai_worker.clause_keywords_view import run_clause_keywords_view


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


class ClauseKeywordsViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ref = Path(self._tmp.name) / "clause_keywords.jsonl"
        # long-format: 절-키워드 1행. d0-0 절은 두 doc(clause_id 2종)에 반복(리포스트).
        _write_jsonl(
            self.ref,
            [
                {"doc_id": "d0", "clause_id": "d0-0", "clause": "맥주가 맛있다", "keyword": "맥주", "aspect": "food", "sentiment": "positive", "keyword_rank_in_clause": 0},
                {"doc_id": "d0", "clause_id": "d0-0", "clause": "맥주가 맛있다", "keyword": "맛", "aspect": "food", "sentiment": "positive", "keyword_rank_in_clause": 1},
                {"doc_id": "d1", "clause_id": "d1-0", "clause": "맥주가 맛있다", "keyword": "맥주", "aspect": "food", "sentiment": "positive", "keyword_rank_in_clause": 0},
                {"doc_id": "d1", "clause_id": "d1-1", "clause": "주차가 불편했다", "keyword": "주차", "aspect": "operation", "sentiment": "negative", "keyword_rank_in_clause": 0},
                {"doc_id": "d2", "clause_id": "d2-0", "clause": "수제맥주 종류가 많다", "keyword": "수제맥주", "aspect": "food", "sentiment": "positive", "keyword_rank_in_clause": 0},
            ],
        )

    def test_summary_and_keyword_items(self) -> None:
        result = run_clause_keywords_view({"ref": str(self.ref)})
        summary = result["summary"]
        self.assertEqual(summary["total_keyword_count"], 5)
        self.assertEqual(summary["unique_keyword_count"], 4)
        self.assertEqual(summary["clause_count"], 4)
        self.assertEqual(summary["aspect"], {"food": 4, "operation": 1})
        self.assertEqual(summary["top_keywords_positive"][0], {"keyword": "맥주", "count": "2"})
        cloud = summary["aspect_sentiment_keywords"]["food"]["positive"]
        self.assertEqual(cloud[0], {"keyword": "맥주", "count": "2", "weight": "1"})  # 최상위 weight=1 (Go 'g' 포맷)
        self.assertEqual(result["total"], 4)
        first = result["items"][0]
        # Go scanArtifactRows(NullString) — 수치도 문자열.
        self.assertEqual(first["keyword"], "맥주")
        self.assertEqual(first["count"], "2")
        self.assertEqual(first["document_count"], "2")
        self.assertEqual(first["dominant_sentiment"], "positive")
        self.assertEqual(first["dominant_sentiment_ratio"], "1")

    def test_aspect_filter_selected_summary(self) -> None:
        result = run_clause_keywords_view({"ref": str(self.ref), "aspect": "food"})
        summary = result["summary"]
        self.assertEqual(summary["selected_aspect"], "food")
        self.assertEqual(summary["selected_aspect_total"], 4)
        self.assertEqual(summary["selected_aspect_sentiment"], {"positive": 4})
        self.assertEqual(result["total"], 3)  # food 키워드 3종

    def test_q_filter(self) -> None:
        result = run_clause_keywords_view({"ref": str(self.ref), "q": "주차"})
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["keyword"], "주차")

    def test_clause_group_dedup_and_order(self) -> None:
        result = run_clause_keywords_view({"ref": str(self.ref), "group": "clause"})
        self.assertEqual(result["total"], 3)  # 절 텍스트 dedup(맥주가 맛있다 2회→1)
        first = result["items"][0]
        self.assertEqual(first["clause"], "맥주가 맛있다")
        self.assertEqual(first["keywords"], ["맥주", "맛"])  # rank 순
        self.assertEqual(first["occurrence_count"], "2")

    def test_dictionary_block_and_synonym_overlay(self) -> None:
        rules = [
            {"rule_type": "block", "source_term": "맛", "target_term": "", "active": True},
            {"rule_type": "synonym", "source_term": "수제맥주", "target_term": "맥주", "active": True},
            {"rule_type": "block", "source_term": "주차", "target_term": "", "active": False},  # 비활성 무시
        ]
        result = run_clause_keywords_view({"ref": str(self.ref), "rules": rules})
        summary = result["summary"]
        # 맛 제외(-1) → 4행, 수제맥주→맥주 병합으로 고유 키워드 {맥주, 주차}.
        self.assertEqual(summary["total_keyword_count"], 4)
        self.assertEqual(summary["unique_keyword_count"], 2)
        self.assertEqual(result["items"][0]["keyword"], "맥주")
        self.assertEqual(result["items"][0]["count"], "3")


if __name__ == "__main__":
    unittest.main()
