"""executor genuineness 뷰가 단일모드/교차검증(verify) 산출물 둘 다 지원하는지.

ADR-026 verify는 최종 라벨을 ``final_label``(reason 없음)로, 단일모드는 ``genuineness``/
``reason``으로 쓴다. executor가 verify 산출물에 ``genuineness`` 컬럼을 못 찾아 plan_v2
실행이 Binder Error로 깨지던 회귀(2026-06-22)를 잠근다. 둘 다 ``genuineness`` 컬럼으로
통일 노출.
"""

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from python_ai_worker.executor.context import ArtifactPaths, ExecutorContext


def _common_inputs(tmp: Path) -> tuple[Path, Path]:
    docs = tmp / "cleaned.parquet"
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "COPY (SELECT 'd1' AS doc_id, 'raw' AS raw_text, 'clean' AS cleaned_text, "
            "TIMESTAMP '2026-01-01' AS created_at) "
            f"TO '{docs}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    clauses = tmp / "clauses.jsonl"
    clauses.write_text(
        json.dumps({"doc_id": "d1", "clause": "맛있다", "sentiment": "positive",
                    "aspect": "food", "prompt_version": "v4", "source": "single"}) + "\n",
        encoding="utf-8",
    )
    return docs, clauses


class GenuinenessViewVerifyCompatTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = Path(tempfile.mkdtemp())
        self.docs, self.clauses = _common_inputs(self._dir)

    def _ctx(self, genuineness: Path) -> ExecutorContext:
        return ExecutorContext(ArtifactPaths(docs=self.docs, clauses=self.clauses, genuineness=genuineness))

    def test_single_mode_schema(self):
        g = self._dir / "g_single.jsonl"
        g.write_text(
            json.dumps({"doc_id": "d1", "genuineness": "genuine_review", "reason": "현장 후기",
                        "prompt_version": "v1", "source": "single"}) + "\n",
            encoding="utf-8",
        )
        with self._ctx(g) as ctx:
            rows = ctx.connection.execute("SELECT doc_id, genuineness, reason FROM genuineness").fetchall()
        self.assertEqual(rows, [("d1", "genuine_review", "현장 후기")])

    def test_verify_mode_schema_maps_final_label(self):
        g = self._dir / "g_verify.jsonl"
        g.write_text(
            json.dumps({"doc_id": "d1", "final_label": "non_review", "resolution": "model_agreement",
                        "needs_review": False, "is_disagreement": False, "judge_required": False,
                        "model_a": "max", "model_b": "ultra", "prompt_version": "v1",
                        "source": "verify"}) + "\n",
            encoding="utf-8",
        )
        with self._ctx(g) as ctx:
            cols = {r[0] for r in ctx.connection.execute("DESCRIBE SELECT * FROM genuineness").fetchall()}
            rows = ctx.connection.execute("SELECT doc_id, genuineness, reason FROM genuineness").fetchall()
        self.assertIn("genuineness", cols)
        self.assertEqual(rows, [("d1", "non_review", None)])


if __name__ == "__main__":
    unittest.main()
