"""SQL audit (2026-05-26) regression 잠금 — executor-level (R2, R6).

R2: divide by zero → executor success, result NULL (SQL-2.1 C3 가드).
R6: timestamp column + ISO string filter → executor CAST AS TIMESTAMP (SQL-3.1 C4).

test_sql_regression.py는 validator-only, 이 파일은 실제 DuckDB 실행 결과를
검증한다.
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from python_ai_worker.executor import ArtifactPaths, ExecutorContext, execute_plan
from python_ai_worker.executor.skills import calculate


def _write_docs_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def _fixture_paths(tmpdir: Path) -> ArtifactPaths:
    """test_executor_smoke 패턴을 차용한 mini fixture (3 docs, 5 clauses, 3 gen)."""

    docs_path = tmpdir / "docs.parquet"
    clauses_path = tmpdir / "clauses.jsonl"
    genuineness_path = tmpdir / "genuineness.jsonl"
    _write_docs_parquet(
        docs_path,
        [
            {
                "doc_id": "d1",
                "row_id": "v1__0",
                "raw_text": "공주 군밤축제 개막",
                "cleaned_text": "공주 군밤축제 개막",
                "created_at": dt.datetime(2025, 3, 10, 12, 0, 0).isoformat(),
            },
            {
                "doc_id": "d2",
                "row_id": "v1__1",
                "raw_text": "강릉 야행 좋았다",
                "cleaned_text": "강릉 야행 좋았다",
                "created_at": dt.datetime(2026, 4, 15, 19, 30, 0).isoformat(),
            },
            {
                "doc_id": "d3",
                "row_id": "v1__2",
                "raw_text": "축제 음식 비쌌다",
                "cleaned_text": "축제 음식 비쌌다",
                "created_at": dt.datetime(2026, 5, 1, 9, 0, 0).isoformat(),
            },
        ],
    )
    _write_jsonl(
        clauses_path,
        [
            {"doc_id": "d1", "clause": "개막했다", "sentiment": "neutral", "aspect": "show_program", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d2", "clause": "좋았다", "sentiment": "positive", "aspect": "ambiance_scenery", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d2", "clause": "맛있었다", "sentiment": "positive", "aspect": "food", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d3", "clause": "비쌌다", "sentiment": "negative", "aspect": "food", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d3", "clause": "분위기 좋다", "sentiment": "positive", "aspect": "ambiance_scenery", "prompt_version": "v3", "source": "lloa"},
        ],
    )
    _write_jsonl(
        genuineness_path,
        [
            {"doc_id": "d1", "genuineness": "non_review", "reason": "공지", "prompt_version": "v1", "source": "lloa"},
            {"doc_id": "d2", "genuineness": "genuine_review", "reason": "후기", "prompt_version": "v1", "source": "lloa"},
            {"doc_id": "d3", "genuineness": "genuine_review", "reason": "후기", "prompt_version": "v1", "source": "lloa"},
        ],
    )
    return ArtifactPaths(docs=docs_path, clauses=clauses_path, genuineness=genuineness_path)


class SqlRegressionR2DivideByZero(unittest.TestCase):
    """R2 (SQL-2.1, audit C3) — divide operation의 분모가 0/NULL이면 결과 NULL.
    fixture 없이 DuckDB 직접 실행으로 SQL 패턴 + 실제 결과를 함께 잠근다.
    """

    def test_divide_by_zero_returns_null(self) -> None:
        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE TABLE agg (numerator INTEGER, denominator INTEGER)"
        )
        con.execute(
            "INSERT INTO agg VALUES (10, 0), (10, 2), (10, NULL), (NULL, 4)"
        )

        sql, _ = calculate.build_sql(
            {
                "input": "agg",
                "expressions": [
                    {
                        "name": "ratio",
                        "operation": "divide",
                        "left": "numerator",
                        "right": "denominator",
                    }
                ],
            },
            None,  # type: ignore[arg-type]  # build_sql은 context 미사용
        )
        rows = con.execute(sql).fetchall()
        # 컬럼 순서: numerator, denominator, ratio (SELECT *, expression)
        ratios = [row[2] for row in rows]
        self.assertIsNone(ratios[0], f"divide by 0 → NULL (got {ratios[0]})")
        self.assertEqual(ratios[1], 5.0, f"10/2 = 5.0 (got {ratios[1]})")
        self.assertIsNone(ratios[2], f"divide by NULL → NULL (got {ratios[2]})")
        self.assertIsNone(ratios[3], f"NULL/4 = NULL (numerator NULL → NULL) (got {ratios[3]})")


class SqlRegressionR6TimestampCast(unittest.TestCase):
    """R6 (SQL-3.1, audit C4) — timestamp column + ISO 8601 string value 비교는
    executor가 `CAST(value AS TIMESTAMP)`로 명시 cast한다. 실제 plan 실행으로
    필터 결과 row 수를 잠근다."""

    def test_filter_docs_by_created_at_iso_string_between(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = _fixture_paths(Path(tmp))
            plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "this_year_docs",
                        "skill": "filter",
                        "params": {
                            "input": "docs",
                            "column": "created_at",
                            "operator": "between",
                            "value": ["2026-01-01", "2026-12-31"],
                        },
                    },
                    {
                        "id": "out",
                        "skill": "present",
                        "params": {"input": "this_year_docs", "format": "table"},
                    },
                ],
            }
            with ExecutorContext(paths) as ctx:
                result = execute_plan(ctx, plan)
                # 2026년 docs는 d2 (2026-04) + d3 (2026-05) = 2건
                self.assertEqual(result["this_year_docs"].row_count, 2)


if __name__ == "__main__":
    unittest.main()
