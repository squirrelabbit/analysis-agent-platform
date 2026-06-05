"""calculate skill SQL builder 잠금 (SQL-2 NULL/zero 정책)."""

from __future__ import annotations

import unittest

from python_ai_worker.executor.skills import calculate


def _build(operation: str, **expr_extra: str) -> str:
    """build_sql 호출 헬퍼. context는 사용 안 하므로 None."""

    expression = {"name": "result", "operation": operation, **expr_extra}
    sql, _ = calculate.build_sql(
        {"input": "agg", "expressions": [expression]},
        None,  # type: ignore[arg-type]  # build_sql은 context를 사용하지 않음
    )
    return sql


class CalculateNullPolicyTests(unittest.TestCase):
    """silverone 2026-05-26 (SQL-2) — NULL / zero 정책 잠금."""

    def test_add_wraps_both_operands_in_coalesce_zero(self) -> None:
        sql = _build("add", left="a", right="b")
        self.assertIn('COALESCE("a", 0) + COALESCE("b", 0)', sql)

    def test_subtract_wraps_both_operands_in_coalesce_zero(self) -> None:
        sql = _build("subtract", left="a", right="b")
        self.assertIn('COALESCE("a", 0) - COALESCE("b", 0)', sql)

    def test_multiply_preserves_null(self) -> None:
        sql = _build("multiply", left="a", right="b")
        self.assertIn('("a" * "b")', sql)
        # multiply는 COALESCE / CASE 가드 없음 (NULL 보존).
        self.assertNotIn('COALESCE("a"', sql)
        self.assertNotIn("CASE WHEN", sql)


class CalculateDivideZeroGuardTests(unittest.TestCase):
    """SQL-2.1 (audit C3) — divide operation 분모 0/NULL 가드 잠금."""

    def test_divide_has_zero_and_null_guard(self) -> None:
        sql = _build("divide", left="numerator", right="denominator")
        self.assertIn('CASE WHEN "denominator" IS NULL OR "denominator" = 0 THEN NULL', sql)
        self.assertIn('ELSE "numerator" * 1.0 / "denominator" END', sql)

    def test_percent_change_guards_base_zero_and_null(self) -> None:
        sql = _build("percent_change", base="prev", current="curr")
        self.assertIn('CASE WHEN "prev" IS NULL OR "prev" = 0 THEN NULL', sql)

    def test_ratio_guards_denominator_zero_and_null(self) -> None:
        sql = _build("ratio", numerator="n", denominator="d")
        self.assertIn('CASE WHEN "d" IS NULL OR "d" = 0 THEN NULL', sql)


class CalculateShareOfTotalTests(unittest.TestCase):
    """silverone 2026-06-02 — share_of_total: 전체(또는 group) 합 대비 비중(0~1)."""

    def test_global_share_uses_window_over_empty(self) -> None:
        sql = _build("share_of_total", value="count")
        self.assertIn('"count" * 1.0 / NULLIF(SUM("count") OVER (), 0)', sql)

    def test_partitioned_share_uses_partition_by(self) -> None:
        expression = {
            "name": "result",
            "operation": "share_of_total",
            "value": "count",
            "partition_by": ["sentiment"],
        }
        sql, _ = calculate.build_sql(
            {"input": "agg", "expressions": [expression]},
            None,  # type: ignore[arg-type]
        )
        self.assertIn('SUM("count") OVER (PARTITION BY "sentiment")', sql)

    def test_empty_partition_by_falls_back_to_global(self) -> None:
        expression = {
            "name": "result",
            "operation": "share_of_total",
            "value": "count",
            "partition_by": [],
        }
        sql, _ = calculate.build_sql(
            {"input": "agg", "expressions": [expression]},
            None,  # type: ignore[arg-type]
        )
        self.assertIn('SUM("count") OVER ()', sql)
        self.assertNotIn("PARTITION BY", sql)


if __name__ == "__main__":
    unittest.main()
