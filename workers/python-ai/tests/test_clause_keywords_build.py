"""dataset_clause_keywords build step 잠금 (silverone 2026-06-10).

long-format(절-키워드 1행) 출력 + clause_id가 executor clauses view 규칙과 동일
({doc_id}__{ROW_NUMBER PARTITION BY doc_id ORDER BY clause,source,prompt_version})
+ festival 불용어 제거 + summary 필드를 검증한다.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from python_ai_worker.dataset_build.clause_keywords import (
    _assign_clause_ids,
    run_dataset_clause_keywords,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as dst:
        for row in rows:
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")


class AssignClauseIdsTests(unittest.TestCase):
    def test_matches_executor_rule(self) -> None:
        # 같은 doc_id 안에서 (clause, source, prompt_version) 정렬 → 1-based 인덱스.
        recs = [
            {"doc_id": "d1", "clause": "나", "source": "lloa", "prompt_version": "v3"},
            {"doc_id": "d1", "clause": "가", "source": "lloa", "prompt_version": "v3"},
            {"doc_id": "d2", "clause": "다", "source": "lloa", "prompt_version": "v3"},
        ]
        out = {(r["doc_id"], r["clause"]): r["clause_id"] for r in _assign_clause_ids(recs)}
        # "가" < "나" 정렬이므로 d1__1='가', d1__2='나'
        self.assertEqual(out[("d1", "가")], "d1__1")
        self.assertEqual(out[("d1", "나")], "d1__2")
        self.assertEqual(out[("d2", "다")], "d2__1")


class RunClauseKeywordsTests(unittest.TestCase):
    def test_long_format_output_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            inp = tmp_path / "clause_label.jsonl"
            out = tmp_path / "clause_keywords.jsonl"
            _write_jsonl(
                inp,
                [
                    {"doc_id": "d1", "clause": "푸드트럭 가격이 비쌌어요", "sentiment": "negative", "aspect": "food", "source": "lloa", "prompt_version": "v3"},
                    {"doc_id": "d1", "clause": "축제 방문 좋았어요", "sentiment": "positive", "aspect": "etc", "source": "lloa", "prompt_version": "v3"},
                ],
            )
            result = run_dataset_clause_keywords(
                {
                    "dataset_version_id": "ver1",
                    "clause_label_ref": str(inp),
                    "output_path": str(out),
                }
            )
            rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line.strip()]
            # long format — 한 행 = 한 (clause, keyword)
            self.assertTrue(all({"doc_id", "clause_id", "clause", "aspect", "sentiment", "keyword"} <= set(r) for r in rows))
            keywords = {r["keyword"] for r in rows}
            # 추출된 키워드는 있고, festival 불용어(축제/방문)는 빠져야 한다.
            self.assertIn("푸드트럭", keywords)
            self.assertIn("가격", keywords)
            self.assertNotIn("축제", keywords)
            self.assertNotIn("방문", keywords)
            # clause_id가 doc_id__N 규칙
            self.assertTrue(all(r["clause_id"].startswith("d1__") for r in rows))
            # extractor_version + source 메타
            from python_ai_worker.dataset_build.keyword_extractor import KIWI_EXTRACTOR_VERSION
            self.assertTrue(all(r["extractor_version"] == KIWI_EXTRACTOR_VERSION for r in rows))
            # 반환 artifact contract
            art = result["artifact"]
            self.assertEqual(art["clause_keywords_ref"], str(out))
            self.assertEqual(art["clause_keywords_input_source"], "clause_label")
            summary = art["summary"]
            self.assertEqual(summary["clause_count"], 2)
            self.assertEqual(summary["keyword_row_count"], len(rows))
            self.assertGreaterEqual(summary["unique_keyword_count"], 2)
            self.assertIn("top_keywords", summary)

    def test_missing_required_payload_raises(self) -> None:
        with self.assertRaises(ValueError):
            run_dataset_clause_keywords({"dataset_version_id": "v"})

    def test_missing_input_file_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                run_dataset_clause_keywords(
                    {
                        "dataset_version_id": "v",
                        "clause_label_ref": str(Path(tmp) / "nope.jsonl"),
                        "output_path": str(Path(tmp) / "out.jsonl"),
                    }
                )


if __name__ == "__main__":
    unittest.main()
