"""#24 — executor clause_keywords view에 정제 사전(block/synonym) overlay 적용 잠금.

채팅(analyze) 경로가 키워드 뷰·보고서와 동일하게 제외/병합을 반영하는지 검증한다.
control plane이 analyze payload로 넘긴 활성 규칙이 ArtifactPaths.keyword_block_terms /
keyword_synonym_map로 들어와 clause_keywords view에 반영된다.
"""
from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from python_ai_worker.executor import ArtifactPaths, ExecutorContext


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
        encoding="utf-8",
    )


def _base_fixtures(tmpdir: Path) -> tuple[Path, Path, Path, Path]:
    docs_path = tmpdir / "docs.parquet"
    clauses_path = tmpdir / "clauses.jsonl"
    genuineness_path = tmpdir / "genuineness.jsonl"
    keywords_path = tmpdir / "clause_keywords.jsonl"

    pq.write_table(
        pa.table(
            {
                "doc_id": ["d1"],
                "row_id": ["v1__0"],
                "raw_text": ["맥주 비어 강릉 좋았다"],
                "cleaned_text": ["맥주 비어 강릉 좋았다"],
                "created_at": [dt.datetime(2026, 5, 1, 9, 0, 0).isoformat()],
            }
        ),
        docs_path,
    )
    _write_jsonl(
        clauses_path,
        [{"doc_id": "d1", "clause": "맥주 비어 강릉 좋았다", "sentiment": "positive",
          "aspect": "food", "prompt_version": "v3", "source": "lloa"}],
    )
    _write_jsonl(
        genuineness_path,
        [{"doc_id": "d1", "genuineness": "genuine_review", "reason": "후기",
          "prompt_version": "v1", "source": "lloa"}],
    )
    # long-format clause_keywords — clause 1개에서 추출된 키워드 4개.
    _write_jsonl(
        keywords_path,
        [
            {"doc_id": "d1", "clause_id": "d1__1", "clause": "맥주 비어 강릉 좋았다",
             "aspect": "food", "sentiment": "positive", "keyword": kw, "source": "kiwi",
             "extractor_version": "v1", "keyword_rank_in_clause": i}
            for i, kw in enumerate(["맥주", "비어", "강릉", "좋았다"])
        ],
    )
    return docs_path, clauses_path, genuineness_path, keywords_path


def _keywords(ctx: ExecutorContext) -> list[str]:
    rows = ctx.connection.execute("SELECT keyword FROM clause_keywords ORDER BY keyword_rank_in_clause").fetchall()
    return [r[0] for r in rows]


class ClauseKeywordsOverlayTests(unittest.TestCase):
    def test_no_rules_passthrough(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d, c, g, k = _base_fixtures(Path(tmp))
            with ExecutorContext(ArtifactPaths(docs=d, clauses=c, genuineness=g, clause_keywords=k)) as ctx:
                self.assertEqual(_keywords(ctx), ["맥주", "비어", "강릉", "좋았다"])

    def test_block_removes_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d, c, g, k = _base_fixtures(Path(tmp))
            paths = ArtifactPaths(
                docs=d, clauses=c, genuineness=g, clause_keywords=k,
                keyword_block_terms=("강릉",),
            )
            with ExecutorContext(paths) as ctx:
                self.assertEqual(_keywords(ctx), ["맥주", "비어", "좋았다"])

    def test_synonym_rewrites_keyword(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d, c, g, k = _base_fixtures(Path(tmp))
            paths = ArtifactPaths(
                docs=d, clauses=c, genuineness=g, clause_keywords=k,
                keyword_synonym_map=(("비어", "맥주"),),
            )
            with ExecutorContext(paths) as ctx:
                # '비어' → '맥주' 치환 → 맥주 2건.
                self.assertEqual(_keywords(ctx), ["맥주", "맥주", "강릉", "좋았다"])

    def test_block_and_synonym_combined(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d, c, g, k = _base_fixtures(Path(tmp))
            paths = ArtifactPaths(
                docs=d, clauses=c, genuineness=g, clause_keywords=k,
                keyword_block_terms=("강릉",),
                keyword_synonym_map=(("비어", "맥주"),),
            )
            with ExecutorContext(paths) as ctx:
                self.assertEqual(_keywords(ctx), ["맥주", "맥주", "좋았다"])

    def test_quote_escaping_in_term(self) -> None:
        # 작은따옴표 포함 키워드도 SQL injection 없이 안전하게 처리되는지.
        with tempfile.TemporaryDirectory() as tmp:
            d, c, g, k = _base_fixtures(Path(tmp))
            paths = ArtifactPaths(
                docs=d, clauses=c, genuineness=g, clause_keywords=k,
                keyword_block_terms=("it's",),  # 매치 없음 — 그냥 안 깨지면 됨
            )
            with ExecutorContext(paths) as ctx:
                self.assertEqual(_keywords(ctx), ["맥주", "비어", "강릉", "좋았다"])


if __name__ == "__main__":
    unittest.main()
