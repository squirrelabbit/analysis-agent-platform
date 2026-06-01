"""analyze_v2 endpoint smoke — execute_analyze_plan service 흐름을 검증한다.

task_router의 _run_analyze_v2 분기는 (a) payload coercion → (b) service call의
얇은 wrapper. 환경(Python 3.9) 한계로 task_router 전체 import는 사전부터
실패하므로(33 errors baseline) 이 테스트는 service 진입점을 직접 호출한다.
task_router 분기 자체는 lint + 코드 리뷰로 확인.
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
    ArtifactPathResolutionError,
    coerce_artifact_paths_payload,
    execute_analyze_plan,
)
from python_ai_worker.planner import PlanValidationError


def run_task(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """task_router._run_analyze_v2 분기를 재현한 thin shim.

    실제 task_router 구현은 동일하지만 이 함수만 직접 호출해 환경 의존성을 피한다.
    payload coercion + execute_analyze_plan 호출 흐름을 그대로 따른다.
    """

    if name != "analyze_v2":
        raise ValueError(f"unsupported capability: {name}")
    if not isinstance(payload, dict):
        raise ValueError("analyze_v2 payload must be an object")
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    if not dataset_version_id:
        raise ValueError("analyze_v2 payload requires 'dataset_version_id'")
    plan = payload.get("plan")
    if not isinstance(plan, dict):
        raise ValueError("analyze_v2 payload requires 'plan' object")
    artifact_paths = coerce_artifact_paths_payload(payload.get("artifact_paths"))
    return execute_analyze_plan(
        dataset_version_id,
        plan,
        artifact_paths=artifact_paths,
    )


def _write_docs(path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "doc_id": "d1",
                    "row_id": "v1__0",
                    "raw_text": "a",
                    "cleaned_text": "a",
                    "created_at": dt.datetime(2026, 4, 1).isoformat(),
                    "channel": "다음 카페",
                },
                {
                    "doc_id": "d2",
                    "row_id": "v1__1",
                    "raw_text": "b",
                    "cleaned_text": "b",
                    "created_at": dt.datetime(2026, 4, 2).isoformat(),
                    "channel": "인스타그램",
                },
            ]
        ),
        path,
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def _make_fixture(tmpdir: Path) -> dict[str, str]:
    docs_path = tmpdir / "docs.parquet"
    clauses_path = tmpdir / "clauses.jsonl"
    genuineness_path = tmpdir / "genuineness.jsonl"
    _write_docs(docs_path)
    _write_jsonl(
        clauses_path,
        [
            {"doc_id": "d1", "clause": "좋아요", "sentiment": "positive", "aspect": "ambiance_scenery", "prompt_version": "v3", "source": "lloa"},
            {"doc_id": "d2", "clause": "별로", "sentiment": "negative", "aspect": "food", "prompt_version": "v3", "source": "lloa"},
        ],
    )
    _write_jsonl(
        genuineness_path,
        [
            {"doc_id": "d1", "genuineness": "genuine_review", "reason": "후기", "prompt_version": "v1", "source": "lloa"},
            {"doc_id": "d2", "genuineness": "genuine_review", "reason": "후기", "prompt_version": "v1", "source": "lloa"},
        ],
    )
    return {"docs": str(docs_path), "clauses": str(clauses_path), "genuineness": str(genuineness_path)}


def _minimal_plan() -> dict[str, Any]:
    return {
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
                "id": "out",
                "skill": "present",
                "params": {"input": "positive_clauses", "format": "table", "title": "긍정 절"},
            },
        ],
    }


class AnalyzeV2DispatchTests(unittest.TestCase):
    def test_payload_validation_rejects_missing_dataset_version_id(self) -> None:
        with self.assertRaises(ValueError) as exc:
            run_task("analyze_v2", {"plan": _minimal_plan()})
        self.assertIn("dataset_version_id", str(exc.exception))

    def test_payload_validation_rejects_missing_plan(self) -> None:
        with self.assertRaises(ValueError) as exc:
            run_task("analyze_v2", {"dataset_version_id": "v1"})
        self.assertIn("plan", str(exc.exception))

    def test_resolver_stub_raises_without_artifact_paths(self) -> None:
        with self.assertRaises(ArtifactPathResolutionError):
            run_task(
                "analyze_v2",
                {"dataset_version_id": "v1", "plan": _minimal_plan()},
            )

    def test_invalid_artifact_paths_payload(self) -> None:
        with self.assertRaises(ValueError) as exc:
            run_task(
                "analyze_v2",
                {
                    "dataset_version_id": "v1",
                    "plan": _minimal_plan(),
                    "artifact_paths": {"docs": "x"},  # clauses/genuineness 누락
                },
            )
        self.assertIn("artifact_paths", str(exc.exception))

    def test_invalid_plan_raises_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _make_fixture(tmpdir)
            bad_plan = {
                "plan_version": "v2",
                "steps": [
                    {
                        "id": "bad-id",  # dash → identifier 위반
                        "skill": "filter",
                        "params": {"input": "clauses", "column": "aspect", "operator": "eq", "value": "x"},
                    }
                ],
            }
            with self.assertRaises(PlanValidationError):
                run_task(
                    "analyze_v2",
                    {"dataset_version_id": "v1", "plan": bad_plan, "artifact_paths": paths},
                )


class AnalyzeV2EndToEndTests(unittest.TestCase):
    def test_minimal_plan_returns_present_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            paths = _make_fixture(tmpdir)
            result = run_task(
                "analyze_v2",
                {
                    "dataset_version_id": "v1",
                    "plan": _minimal_plan(),
                    "artifact_paths": paths,
                },
            )
            self.assertEqual(result["dataset_version_id"], "v1")
            self.assertEqual(result["plan_version"], "v2")
            self.assertEqual(result["artifact_paths"], paths)
            self.assertEqual(len(result["steps"]), 2)
            self.assertEqual(result["present"]["step_id"], "out")
            self.assertEqual(result["present"]["format"], "table")
            self.assertEqual(result["present"]["title"], "긍정 절")
            self.assertEqual(result["present"]["row_count"], 1)


if __name__ == "__main__":
    unittest.main()
