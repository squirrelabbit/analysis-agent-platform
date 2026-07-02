import json
import tempfile
import unittest
from pathlib import Path

from python_ai_worker.artifact_views import (
    extract_source_url,
    run_clause_label_view,
    run_doc_genuineness_view,
)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


def _write_clean_parquet(path: Path, rows: list[dict]) -> None:
    import duckdb

    con = duckdb.connect()
    try:
        jsonl = path.with_suffix(".tmp.jsonl")
        _write_jsonl(jsonl, rows)
        con.execute(
            f"COPY (SELECT * FROM read_json('{jsonl}', format='newline_delimited')) TO '{path}' (FORMAT PARQUET)"
        )
        jsonl.unlink()
    finally:
        con.close()


class DocGenuinenessViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.ref = self.root / "doc_genuineness.jsonl"
        _write_jsonl(
            self.ref,
            [
                {"doc_id": "v:row:0", "genuineness": "genuine_review", "reason": "r0", "source": "s", "prompt_version": "v1"},
                {"doc_id": "v:row:1", "genuineness": "non_review", "reason": "r1", "source": "s", "prompt_version": "v1"},
                {"doc_id": "v:row:2", "genuineness": "genuine_review", "reason": "r2", "source": "s", "prompt_version": "v1"},
            ],
        )
        self.clean = self.root / "cleaned.parquet"
        _write_clean_parquet(
            self.clean,
            [
                {"row_id": "v:row:0", "cleaned_text": "본문0", "source_json": json.dumps({"url": "https://a.example/0", "title": "t"})},
                {"row_id": "v:row:1", "cleaned_text": "본문1", "source_json": json.dumps({"title": "no url"})},
                {"row_id": "v:row:2", "cleaned_text": "본문2", "source_json": None},
            ],
        )

    def test_single_mode_with_clean_join(self) -> None:
        result = run_doc_genuineness_view(
            {"ref": str(self.ref), "clean_ref": str(self.clean), "limit": 10, "offset": 0}
        )
        self.assertEqual(result["summary"], {"total": 3, "genuineness": {"genuine_review": 2, "non_review": 1}})
        self.assertEqual(result["prompt_version"], "v1")
        self.assertEqual(result["total"], 3)
        self.assertEqual(len(result["items"]), 3)
        first = result["items"][0]
        self.assertEqual(first["doc_id"], "v:row:0")
        self.assertEqual(first["cleaned_text"], "본문0")
        self.assertEqual(first["source_url"], "https://a.example/0")
        self.assertNotIn("source_json", first)
        self.assertEqual(result["items"][1]["source_url"], "")

    def test_single_mode_filter_and_pagination(self) -> None:
        result = run_doc_genuineness_view(
            {"ref": str(self.ref), "limit": 1, "offset": 1, "genuineness": "genuine_review"}
        )
        # summary는 전체 유지, total/items만 필터+페이징.
        self.assertEqual(result["summary"]["total"], 3)
        self.assertEqual(result["total"], 2)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["items"][0]["doc_id"], "v:row:2")
        # clean_ref 없으면 본문/원문 URL 키 자체가 없다 (Go fallback schema).
        self.assertNotIn("cleaned_text", result["items"][0])
        self.assertNotIn("source_url", result["items"][0])

    def test_verify_mode(self) -> None:
        verify_ref = self.root / "doc_genuineness_verify.jsonl"
        _write_jsonl(
            verify_ref,
            [
                {
                    "doc_id": "v:row:0", "final_label": "genuine_review", "needs_review": False,
                    "resolution": "agree", "is_disagreement": False, "prompt_version": "v1",
                    "model_a_result": {"label": "genuine_review", "reason": "a-근거"},
                    "model_b_result": {"label": "genuine_review", "reason": "b-근거"},
                    "judge_result": {"label": None, "reason": None},
                },
                {
                    "doc_id": "v:row:1", "final_label": "non_review", "needs_review": True,
                    "resolution": "judge", "is_disagreement": True, "prompt_version": "v1",
                    "model_a_result": {"label": "genuine_review", "reason": "a-근거"},
                    "model_b_result": {"label": "non_review", "reason": "b-근거"},
                    "judge_result": {"label": "non_review", "reason": "판정-근거"},
                },
            ],
        )
        result = run_doc_genuineness_view(
            {"ref": str(verify_ref), "clean_ref": str(self.clean), "mode": "verify"}
        )
        self.assertEqual(result["summary"], {"total": 2, "genuineness": {"genuine_review": 1, "non_review": 1}})
        agree, disagree = result["items"]
        self.assertEqual(agree["genuineness"], "genuine_review")
        self.assertIsNone(agree["judge_result"])  # 전부-null judge는 None
        self.assertEqual(agree["reason"], "a-근거")  # 합의 → model_a 사유
        self.assertFalse(agree["is_disagreement"])
        self.assertTrue(disagree["needs_review"])
        self.assertEqual(disagree["reason"], "판정-근거")  # judge 사유 우선
        self.assertEqual(disagree["model_b_result"]["label"], "non_review")

        filtered = run_doc_genuineness_view(
            {"ref": str(verify_ref), "mode": "verify", "disagreement_only": True}
        )
        self.assertEqual(filtered["total"], 1)
        self.assertEqual(filtered["items"][0]["doc_id"], "v:row:1")


class ClauseLabelViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.ref = self.root / "clause_label.jsonl"
        _write_jsonl(
            self.ref,
            [
                {"doc_id": "d0", "clause": "c0", "sentiment": "positive", "aspect": "operation", "source": "s", "prompt_version": "v3"},
                {"doc_id": "d0", "clause": "c1", "sentiment": "negative", "aspect": "operation", "source": "s", "prompt_version": "v3"},
                {"doc_id": "d1", "clause": "c2", "sentiment": "positive", "aspect": "food", "source": "s", "prompt_version": "v3"},
                {"doc_id": "d1", "clause": "c3", "sentiment": None, "aspect": "food", "source": "s", "prompt_version": "v3"},
            ],
        )

    def test_single_mode_summary_and_clause_id(self) -> None:
        result = run_clause_label_view({"ref": str(self.ref)})
        summary = result["summary"]
        self.assertEqual(summary["total"], 4)
        self.assertEqual(summary["sentiment"], {"positive": 2, "negative": 1, "unknown": 1})
        self.assertEqual(summary["aspect"], {"operation": 2, "food": 2})
        operation = summary["aspect_sentiment"]["operation"]
        self.assertEqual(operation["total"], 2)
        self.assertEqual(operation["sentiment"]["positive"], {"count": 1, "percent": 50})
        self.assertEqual(operation["sentiment"]["neutral"], {"count": 0, "percent": 0})
        food = summary["aspect_sentiment"]["food"]
        self.assertEqual(food["sentiment"]["unknown"], {"count": 1, "percent": 50})
        self.assertEqual(result["prompt_version"], "v3")
        self.assertEqual(
            [item["clause_id"] for item in result["items"]],
            ["d0-0", "d0-1", "d1-0", "d1-1"],
        )

    def test_single_mode_filters(self) -> None:
        result = run_clause_label_view({"ref": str(self.ref), "aspect": "operation", "sentiment": "negative"})
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["clause_id"], "d0-1")
        self.assertEqual(result["summary"]["total"], 4)  # summary는 전체 유지

    def test_verify_mode(self) -> None:
        verify_ref = self.root / "clause_label_verify.jsonl"
        _write_jsonl(
            verify_ref,
            [
                {
                    "doc_id": "d0", "clause": "c0", "sentiment": "positive", "aspect": "operation",
                    "source": "s", "resolution": "agree", "needs_review": False,
                    "sentence_index": 0, "chunk_index": 0,
                    "model_a_result": {"sentiment": "positive"}, "model_b_result": {"sentiment": "positive"},
                    "judge_result": {"sentiment": None},
                },
                {
                    "doc_id": "d0", "clause": "c1", "sentiment": "negative", "aspect": "operation",
                    "source": "s", "resolution": "partial_classify", "needs_review": True,
                    "sentence_index": 1, "chunk_index": 0,
                    "model_a_result": {"sentiment": "negative"}, "model_b_result": None,
                    "judge_result": None,
                },
            ],
        )
        result = run_clause_label_view({"ref": str(verify_ref), "mode": "verify"})
        summary = result["summary"]
        self.assertEqual(summary["resolution"], {"agree": 1, "partial_classify": 1})
        self.assertEqual(summary["needs_review_count"], 1)
        first, second = result["items"]
        self.assertEqual(first["clause_id"], "d0-0")
        self.assertEqual(first["sentence_index"], 0)
        self.assertIsNone(first["judge_result"])
        self.assertTrue(second["needs_review"])
        # clean_ref 없으면 cleaned_text 키 자체가 없다.
        self.assertNotIn("cleaned_text", first)

        filtered = run_clause_label_view({"ref": str(verify_ref), "mode": "verify", "needs_review_only": True})
        self.assertEqual(filtered["total"], 1)
        self.assertEqual(filtered["items"][0]["clause_id"], "d0-1")


class ExtractSourceURLTests(unittest.TestCase):
    def test_first_url_column_by_sorted_key(self) -> None:
        record = {"z_link": "https://z.example", "a_link": "https://a.example"}
        self.assertEqual(extract_source_url(json.dumps(record)), "https://a.example")

    def test_url_with_whitespace_excluded(self) -> None:
        record = {"body": "https://a.example 뒤에 본문", "link": "http://ok.example/path"}
        self.assertEqual(extract_source_url(json.dumps(record)), "http://ok.example/path")

    def test_invalid_json(self) -> None:
        self.assertEqual(extract_source_url("not-json"), "")
        self.assertEqual(extract_source_url(None), "")


if __name__ == "__main__":
    unittest.main()
