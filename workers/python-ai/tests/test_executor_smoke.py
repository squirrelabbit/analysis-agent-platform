"""executor smoke test — LLM 없이 plan_v2가 실제 DuckDB 연산으로
실행되는지 증명한다.

silverone 2026-05-21 3단계 목표: "LLM 없이 plan_v2가 실제 DuckDB 연산으로
실행되는 것을 smoke test로 증명".
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from python_ai_worker.executor import (
    ArtifactPaths,
    ExecutorContext,
    ExecutorContextError,
    ExecutorError,
    execute_plan,
)
from python_ai_worker.planner import PlanValidationError


def _write_docs_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def _fixture_paths(tmpdir: Path) -> ArtifactPaths:
    docs_path = tmpdir / "docs.parquet"
    clauses_path = tmpdir / "clauses.jsonl"
    genuineness_path = tmpdir / "genuineness.jsonl"

    _write_docs_parquet(
        docs_path,
        [
            {
                "doc_id": "d1",
                "row_id": "v1__0",
                "raw_text": "공주 군밤축제 오늘 개막",
                "cleaned_text": "공주 군밤축제 오늘 개막",
                "created_at": dt.datetime(2025, 3, 10, 12, 0, 0).isoformat(),
                "channel": "다음 카페",
            },
            {
                "doc_id": "d2",
                "row_id": "v1__1",
                "raw_text": "강릉 야행 정말 좋았어요",
                "cleaned_text": "강릉 야행 정말 좋았어요",
                "created_at": dt.datetime(2026, 4, 15, 19, 30, 0).isoformat(),
                "channel": "인스타그램",
            },
            {
                "doc_id": "d3",
                "row_id": "v1__2",
                "raw_text": "축제 음식 너무 비쌌다",
                "cleaned_text": "축제 음식 너무 비쌌다",
                "created_at": dt.datetime(2026, 5, 1, 9, 0, 0).isoformat(),
                "channel": "다음 카페",
            },
        ],
    )

    _write_jsonl(
        clauses_path,
        [
            {"doc_id": "d1", "clause": "오늘 개막했다", "sentiment": "neutral", "aspect": "show_program", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d2", "clause": "야행 정말 좋았어요", "sentiment": "positive", "aspect": "ambiance_scenery", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d2", "clause": "음식도 맛있었어요", "sentiment": "positive", "aspect": "food", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d3", "clause": "음식 너무 비쌌다", "sentiment": "negative", "aspect": "food", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d3", "clause": "분위기는 좋았다", "sentiment": "positive", "aspect": "ambiance_scenery", "prompt_version": "v3", "source": "lloa"},
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


class ContextRegistrationTests(unittest.TestCase):
    def test_three_standard_tables_registered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            with ExecutorContext(paths) as ctx:
                self.assertEqual(ctx.count_rows("docs"), 3)
                self.assertEqual(ctx.count_rows("clauses"), 5)
                self.assertEqual(ctx.count_rows("genuineness"), 3)

    def test_clauses_gets_clause_id_auto_generated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            with ExecutorContext(paths) as ctx:
                rows = ctx.fetch_rows("clauses")
                clause_ids = [row["clause_id"] for row in rows]
                self.assertEqual(len(clause_ids), len(set(clause_ids)), "clause_id must be unique")
                for row in rows:
                    self.assertTrue(row["clause_id"].startswith(row["doc_id"] + "__"))

    def test_docs_created_at_castable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            with ExecutorContext(paths) as ctx:
                row = ctx.connection.execute(
                    "SELECT CAST(created_at AS TIMESTAMP) FROM docs ORDER BY created_at LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(row)

    def test_missing_created_at_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            docs_path = tmpdir / "docs.parquet"
            _write_docs_parquet(
                docs_path,
                [{"doc_id": "d1", "row_id": "v1__0", "raw_text": "x", "cleaned_text": "x"}],
            )
            clauses_path = tmpdir / "clauses.jsonl"
            _write_jsonl(clauses_path, [])
            genuineness_path = tmpdir / "genuineness.jsonl"
            _write_jsonl(genuineness_path, [])
            with self.assertRaises(ExecutorContextError) as ctx_err:
                ExecutorContext(
                    ArtifactPaths(docs=docs_path, clauses=clauses_path, genuineness=genuineness_path)
                )
            self.assertIn("created_at", str(ctx_err.exception))


class FilterAndAggregateTests(unittest.TestCase):
    def test_filter_then_aggregate_on_clauses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "positive_clauses",
                        "skill": "filter",
                        "params": {
                            "input": "clauses",
                            "column": "sentiment",
                            "operator": "eq",
                            "value": "positive",
                        },
                    },
                    {
                        "id": "positive_by_aspect",
                        "skill": "aggregate",
                        "params": {
                            "input": "positive_clauses",
                            "group_by": ["aspect"],
                            "metrics": [{"name": "count", "function": "count", "column": "*"}],
                        },
                    },
                ],
            }
            with ExecutorContext(paths) as ctx:
                result = execute_plan(ctx, plan)
                self.assertEqual(result["positive_clauses"].row_count, 3)
                aspect_counts = {
                    row["aspect"]: row["count"]
                    for row in result["positive_by_aspect"].sample_rows
                }
                self.assertEqual(aspect_counts.get("ambiance_scenery"), 2)
                self.assertEqual(aspect_counts.get("food"), 1)


class JoinTests(unittest.TestCase):
    def test_doc_clauses_join_then_filter_by_channel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "clause_docs",
                        "skill": "join",
                        "params": {
                            "left": "clauses",
                            "right": "docs",
                            "on": ["doc_id"],
                            "how": "inner",
                        },
                    },
                    {
                        "id": "daum_cafe_clauses",
                        "skill": "filter",
                        "params": {
                            "input": "clause_docs",
                            "column": "channel",
                            "operator": "eq",
                            "value": "다음 카페",
                        },
                    },
                ],
            }
            with ExecutorContext(paths) as ctx:
                result = execute_plan(ctx, plan)
                self.assertEqual(result["clause_docs"].row_count, 5)
                # d1 (1 clause) + d3 (2 clauses) = 3 (다음 카페 채널)
                self.assertEqual(result["daum_cafe_clauses"].row_count, 3)


class CompareAndCalculateTests(unittest.TestCase):
    def test_year_over_year_aspect_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "clause_docs",
                        "skill": "join",
                        "params": {"left": "clauses", "right": "docs", "on": ["doc_id"], "how": "inner"},
                    },
                    {
                        "id": "last_year_clauses",
                        "skill": "filter",
                        "params": {
                            "input": "clause_docs",
                            "column": "created_at",
                            "operator": "between",
                            "value": ["2025-01-01T00:00:00", "2025-12-31T23:59:59"],
                        },
                    },
                    {
                        "id": "this_year_clauses",
                        "skill": "filter",
                        "params": {
                            "input": "clause_docs",
                            "column": "created_at",
                            "operator": "between",
                            "value": ["2026-01-01T00:00:00", "2026-12-31T23:59:59"],
                        },
                    },
                    {
                        "id": "last_aspect_count",
                        "skill": "aggregate",
                        "params": {
                            "input": "last_year_clauses",
                            "group_by": ["aspect"],
                            "metrics": [{"name": "count", "function": "count", "column": "*"}],
                        },
                    },
                    {
                        "id": "this_aspect_count",
                        "skill": "aggregate",
                        "params": {
                            "input": "this_year_clauses",
                            "group_by": ["aspect"],
                            "metrics": [{"name": "count", "function": "count", "column": "*"}],
                        },
                    },
                    {
                        "id": "aspect_compare",
                        "skill": "compare",
                        "params": {
                            "left": "last_aspect_count",
                            "right": "this_aspect_count",
                            "join_key": ["aspect"],
                            "left_label": "last",
                            "right_label": "this",
                        },
                    },
                    {
                        "id": "aspect_delta",
                        "skill": "calculate",
                        "params": {
                            "input": "aspect_compare",
                            "expressions": [
                                {
                                    "name": "delta_count",
                                    "operation": "subtract",
                                    "left": "this_count",
                                    "right": "last_count",
                                }
                            ],
                        },
                    },
                    {
                        "id": "ranked",
                        "skill": "sort",
                        "params": {"input": "aspect_delta", "by": ["delta_count"], "order": "desc"},
                    },
                    {
                        "id": "out",
                        "skill": "present",
                        "params": {"input": "ranked", "format": "table"},
                    },
                ],
            }

            with ExecutorContext(paths) as ctx:
                result = execute_plan(ctx, plan)

                last_rows = {row["aspect"]: row["count"] for row in ctx.fetch_rows("last_aspect_count")}
                self.assertEqual(last_rows, {"show_program": 1})  # 2025 docs: d1 only (1 clause, contents)

                this_rows = {row["aspect"]: row["count"] for row in ctx.fetch_rows("this_aspect_count")}
                self.assertEqual(this_rows, {"ambiance_scenery": 2, "food": 2})

                aspect_delta_rows = {row["aspect"]: row["delta_count"] for row in ctx.fetch_rows("aspect_delta")}
                self.assertEqual(aspect_delta_rows["show_program"], -1)
                self.assertEqual(aspect_delta_rows["ambiance_scenery"], 2)
                self.assertEqual(aspect_delta_rows["food"], 2)

                ranked_rows = result["ranked"].sample_rows
                top = ranked_rows[0]
                self.assertIn(top["aspect"], {"ambiance_scenery", "food"})
                self.assertEqual(top["delta_count"], 2)


class GuardrailTests(unittest.TestCase):
    def test_invalid_plan_raises_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            bad_plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "bad-id",  # contains dash → identifier 위반
                        "skill": "filter",
                        "params": {"input": "clauses", "column": "aspect", "operator": "eq", "value": "x"},
                    }
                ],
            }
            with ExecutorContext(paths) as ctx, self.assertRaises(PlanValidationError):
                execute_plan(ctx, bad_plan)

    def test_summarize_skill_unsupported_in_first_cut(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _fixture_paths(tmpdir)
            plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "agg",
                        "skill": "aggregate",
                        "params": {
                            "input": "clauses",
                            "group_by": ["aspect"],
                            "metrics": [{"name": "count", "function": "count", "column": "*"}],
                        },
                    },
                    {
                        "id": "sum_step",
                        "skill": "summarize",
                        "params": {"input": "agg", "focus": "상위 aspect"},
                    },
                ],
            }
            with ExecutorContext(paths) as ctx, self.assertRaises(ExecutorError) as exc:
                execute_plan(ctx, plan)
            self.assertIn("summarize", str(exc.exception))


if __name__ == "__main__":
    unittest.main()
