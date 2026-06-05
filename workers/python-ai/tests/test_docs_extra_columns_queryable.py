"""advertised=queryable invariant — docs-extra 컬럼은 실제 docs view 컬럼이어야 한다.

silverone 2026-06-05 (Q2 후속). clean이 source text_columns(제목/본문)를 raw_text로
병합 + 나머지를 source_json에 넣으면 그 원본 컬럼은 cleaned parquet(docs view)에 없다.
control-plane이 source 기준으로 docs-extra를 보내도, worker가 실제 docs view 컬럼으로
걸러 planner에 없는 컬럼이 새지 않게 한다(없으면 Binder Error).
"""

import os
import unittest
from pathlib import Path

import duckdb

from python_ai_worker.executor.context import ArtifactPaths, read_docs_columns
from python_ai_worker.executor.service import _filter_docs_extra_columns
from python_ai_worker.planner import DatasetSpecificColumn


def _docs_fixture(tmp: Path) -> ArtifactPaths:
    docs = tmp / "cleaned.parquet"
    con = duckdb.connect(":memory:")
    try:
        # 실제 cleaned 출력 형태: 표준 컬럼 + 병합 안 된 진짜 extra(rating).
        # 제목/본문/수집채널은 source_json으로 들어가 컬럼으로 없음.
        con.execute(
            "COPY (SELECT 'd1' AS doc_id, 'raw' AS raw_text, 'clean' AS cleaned_text, "
            "TIMESTAMP '2026-01-01' AS created_at, 5 AS rating) "
            f"TO '{docs}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    return ArtifactPaths(docs=docs, clauses=tmp / "c.jsonl", genuineness=tmp / "g.jsonl")


class DocsExtraColumnsQueryableTest(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile

        self._dir = tempfile.mkdtemp()
        self.paths = _docs_fixture(Path(self._dir))

    def test_read_docs_columns(self):
        cols = read_docs_columns(self.paths)
        self.assertEqual(
            set(cols), {"doc_id", "raw_text", "cleaned_text", "created_at", "rating"}
        )

    def test_filters_non_queryable_advertised_columns(self):
        advertised = [
            DatasetSpecificColumn(name="제목", type="string"),
            DatasetSpecificColumn(name="수집채널", type="string"),
            DatasetSpecificColumn(name="rating", type="int"),  # 실제 docs 컬럼
        ]
        kept = _filter_docs_extra_columns(advertised, self.paths)
        self.assertEqual([c.name for c in kept], ["rating"])

    def test_all_non_queryable_dropped_to_empty(self):
        advertised = [
            DatasetSpecificColumn(name="제목"),
            DatasetSpecificColumn(name="본문"),
        ]
        kept = _filter_docs_extra_columns(advertised, self.paths)
        self.assertEqual(kept, [])

    def test_none_paths_returns_unchanged(self):
        advertised = [DatasetSpecificColumn(name="제목")]
        self.assertIs(_filter_docs_extra_columns(advertised, None), advertised)

    def test_missing_docs_artifact_degrades_to_unchanged(self):
        advertised = [DatasetSpecificColumn(name="제목")]
        missing = ArtifactPaths(
            docs=Path(self._dir) / "nope.parquet",
            clauses=Path(self._dir) / "c.jsonl",
            genuineness=Path(self._dir) / "g.jsonl",
        )
        self.assertEqual(_filter_docs_extra_columns(advertised, missing), advertised)


if __name__ == "__main__":
    unittest.main()
