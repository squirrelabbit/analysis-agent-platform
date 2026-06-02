"""present skill SQL-4 정책 잠금:
- resolve_max_rows (default / 명시 / cap / 비정수 처리)
- _build_response present_payload (total_rows / returned_rows / max_rows / truncated / row_count 호환)
"""

from __future__ import annotations

import unittest

from python_ai_worker.executor.runner import ExecutionStepResult
from python_ai_worker.executor.service import _build_response
from python_ai_worker.executor.skills import present


class ResolveMaxRowsTests(unittest.TestCase):
    def test_default_when_limit_missing(self) -> None:
        self.assertEqual(present.resolve_max_rows({}), 1000)

    def test_default_when_limit_none(self) -> None:
        self.assertEqual(present.resolve_max_rows({"limit": None}), 1000)

    def test_explicit_limit_used(self) -> None:
        self.assertEqual(present.resolve_max_rows({"limit": 500}), 500)

    def test_explicit_limit_clamped_to_hard_cap(self) -> None:
        # validator가 10000 초과를 reject하지만 executor도 보호 차원에서 clamp.
        self.assertEqual(
            present.resolve_max_rows({"limit": 99999}),
            present.PRESENT_HARD_CAP_ROWS,
        )

    def test_non_integer_falls_back_to_default(self) -> None:
        # bool / str / float은 default. validator가 사실상 reject하지만
        # executor가 받았을 때 KeyError 안 나도록 정책 잠금.
        self.assertEqual(present.resolve_max_rows({"limit": True}), 1000)
        self.assertEqual(present.resolve_max_rows({"limit": "200"}), 1000)
        self.assertEqual(present.resolve_max_rows({"limit": 3.5}), 1000)

    def test_zero_or_negative_falls_back_to_default(self) -> None:
        self.assertEqual(present.resolve_max_rows({"limit": 0}), 1000)
        self.assertEqual(present.resolve_max_rows({"limit": -5}), 1000)


def _make_plan(present_extra: dict) -> dict:
    return {
        "plan_version": "v2",
        "steps": [
            {"id": "out", "skill": "present", "params": {"input": "docs", "format": "table"}},
        ],
        "_present_extra": present_extra,
    }


def _make_step_results(*, total_rows: int, rows_count: int, max_rows: int) -> dict[str, ExecutionStepResult]:
    sample_rows = [{"i": idx} for idx in range(rows_count)]
    return {
        "out": ExecutionStepResult(
            step_id="out",
            skill="present",
            row_count=total_rows,
            sample_rows=sample_rows,
            extra={"format": "table", "title": "t", "max_rows": max_rows},
        )
    }


class BuildResponsePresentPayloadTests(unittest.TestCase):
    """_build_response가 present_payload에 SQL-4 필드를 정확히 채우는지 잠금."""

    def _call(self, *, total_rows: int, returned_rows: int, max_rows: int) -> dict:
        plan = {
            "plan_version": "v2",
            "steps": [
                {"id": "out", "skill": "present", "params": {"input": "docs", "format": "table"}},
            ],
        }
        # ArtifactPaths is a dataclass with str-able paths; we don't need real files
        from pathlib import Path
        from python_ai_worker.executor.context import ArtifactPaths

        paths = ArtifactPaths(docs=Path("/x/d"), clauses=Path("/x/c"), genuineness=Path("/x/g"))
        return _build_response(
            dataset_version_id="v1",
            plan=plan,
            artifact_paths=paths,
            step_results=_make_step_results(total_rows=total_rows, rows_count=returned_rows, max_rows=max_rows),
        )

    def test_truncated_true_when_returned_less_than_total(self) -> None:
        response = self._call(total_rows=2000, returned_rows=1000, max_rows=1000)
        present_payload = response["present"]
        self.assertEqual(present_payload["total_rows"], 2000)
        self.assertEqual(present_payload["returned_rows"], 1000)
        self.assertEqual(present_payload["max_rows"], 1000)
        self.assertTrue(present_payload["truncated"])

    def test_truncated_false_when_total_equals_returned(self) -> None:
        response = self._call(total_rows=42, returned_rows=42, max_rows=1000)
        present_payload = response["present"]
        self.assertEqual(present_payload["total_rows"], 42)
        self.assertEqual(present_payload["returned_rows"], 42)
        self.assertFalse(present_payload["truncated"])

    def test_row_count_compat_field_matches_total_rows(self) -> None:
        response = self._call(total_rows=123, returned_rows=42, max_rows=50)
        present_payload = response["present"]
        self.assertEqual(present_payload["row_count"], 123)
        self.assertEqual(present_payload["row_count"], present_payload["total_rows"])

    def test_max_rows_falls_back_to_1000_when_missing(self) -> None:
        # extra에 max_rows 없거나 0이면 1000 default.
        plan = {
            "plan_version": "v2",
            "steps": [
                {"id": "out", "skill": "present", "params": {"input": "docs", "format": "table"}},
            ],
        }
        from pathlib import Path
        from python_ai_worker.executor.context import ArtifactPaths

        paths = ArtifactPaths(docs=Path("/x/d"), clauses=Path("/x/c"), genuineness=Path("/x/g"))
        step_results = {
            "out": ExecutionStepResult(
                step_id="out",
                skill="present",
                row_count=10,
                sample_rows=[{"i": idx} for idx in range(10)],
                extra={"format": "table", "title": None},  # max_rows 누락
            )
        }
        response = _build_response(
            dataset_version_id="v1",
            plan=plan,
            artifact_paths=paths,
            step_results=step_results,
        )
        self.assertEqual(response["present"]["max_rows"], 1000)


if __name__ == "__main__":
    unittest.main()


# silverone 2026-06-02 — present.columns projection (hard constraint).
class PresentColumnsProjectionTests(unittest.TestCase):
    def _sql(self, params: dict) -> str:
        return present.build_sql({"input": "agg", "format": "table", **params}, None)[0]

    def test_columns_projection_selects_only_given(self) -> None:
        sql = self._sql({"columns": ["sentiment", "count"]})
        self.assertIn("sentiment", sql)
        self.assertIn("count", sql)
        self.assertNotIn("*", sql)

    def test_no_columns_selects_all(self) -> None:
        self.assertIn("SELECT * FROM", self._sql({}))

    def test_empty_columns_falls_back_to_all(self) -> None:
        self.assertIn("SELECT * FROM", self._sql({"columns": []}))

    def test_invalid_columns_entry_falls_back_to_all(self) -> None:
        # 빈 문자열/비문자열 entry는 무시하고 * (validator가 별도로 잡음).
        self.assertIn("SELECT * FROM", self._sql({"columns": ["sentiment", ""]}))
