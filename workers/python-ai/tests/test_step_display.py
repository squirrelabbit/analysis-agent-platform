"""Skill Contract v2 Step 3 — Python plan-step display가 Go display와 byte 동일한지
잠근다 (silverone 2026-06-04).

golden 케이스는 Go plan_step_display_test.go(displayX)와 동일. parity가 깨지면
worker가 내보낸 display와 Go fallback이 달라져 프론트 표시가 흔들리므로, 8 skill
전부 잠근다.
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner.step_display import build_step_display, plan_with_step_display


def _step(skill: str, params: dict) -> dict:
    return {"skill": skill, "params": params}


def _calc(expr: dict) -> dict:
    return _step("calculate", {"expressions": [expr]})


class FilterDisplayParityTests(unittest.TestCase):
    def test_all_operators(self) -> None:
        cases = [
            ("eq", "active", "WHERE c = 'active'"),
            ("gt", 10, "WHERE c > 10"),
            ("gte", 10.5, "WHERE c >= 10.5"),
            ("lt", 0, "WHERE c < 0"),
            ("lte", -1, "WHERE c <= -1"),
            ("in", ["a", "b"], "WHERE c IN ('a', 'b')"),
            ("not_in", [1, 2], "WHERE c NOT IN (1, 2)"),
            ("contains", "foo", "WHERE c LIKE 'foo'"),
            ("between", [1, 10], "WHERE c BETWEEN 1 AND 10"),
            ("is_null", None, "WHERE c IS NULL"),
            ("not_null", None, "WHERE c IS NOT NULL"),
        ]
        for op, value, want in cases:
            with self.subTest(op=op):
                out = build_step_display(_step("filter", {"column": "c", "operator": op, "value": value}))
                self.assertEqual(out, {"label": "조건 필터", "expression": want})

    def test_missing_column_returns_none(self) -> None:
        self.assertIsNone(build_step_display(_step("filter", {"operator": "eq", "value": "x"})))


class JoinDisplayParityTests(unittest.TestCase):
    def test_inner_join_with_on(self) -> None:
        out = build_step_display(_step("join", {
            "left": "clauses", "right": "real_reviews", "how": "inner", "on": ["doc_id"],
        }))
        self.assertEqual(out, {"label": "데이터 연결", "expression": "clauses INNER JOIN real_reviews ON doc_id"})

    def test_how_defaults_to_inner(self) -> None:
        out = build_step_display(_step("join", {"left": "a", "right": "b", "on": ["id"]}))
        self.assertIn("INNER JOIN", out["expression"])


class AggregateDisplayParityTests(unittest.TestCase):
    def test_count_star(self) -> None:
        out = build_step_display(_step("aggregate", {
            "group_by": ["aspect"],
            "metrics": [{"name": "count", "function": "count", "column": ""}],
        }))
        self.assertEqual(out, {"label": "aspect별 집계", "expression": "GROUP BY aspect · COUNT(*) AS count"})

    def test_multiple_metrics(self) -> None:
        out = build_step_display(_step("aggregate", {
            "group_by": ["aspect"],
            "metrics": [
                {"name": "cnt", "function": "count"},
                {"name": "avg_score", "function": "avg", "column": "score"},
            ],
        }))
        self.assertEqual(out["expression"], "GROUP BY aspect · COUNT(*) AS cnt, AVG(score) AS avg_score")


class CompareDisplayParityTests(unittest.TestCase):
    def test_compare_with_labels(self) -> None:
        out = build_step_display(_step("compare", {
            "left": "agg_last", "right": "agg_this",
            "left_label": "last_year", "right_label": "this_year", "join_key": ["aspect"],
        }))
        self.assertEqual(out, {"label": "두 결과 비교", "expression": "COMPARE last_year vs this_year ON aspect"})


class CalculateDisplayParityTests(unittest.TestCase):
    def test_ratio(self) -> None:
        out = build_step_display(_calc({
            "name": "negative_ratio", "operation": "ratio",
            "left": "negative_count", "right": "total_count",
        }))
        self.assertEqual(out, {"label": "비율 계산", "expression": "negative_ratio = negative_count / total_count * 100"})

    def test_percent_change(self) -> None:
        out = build_step_display(_calc({
            "name": "delta_rate", "operation": "percent_change", "left": "last_count", "right": "this_count",
        }))
        self.assertEqual(out, {"label": "증감률 계산", "expression": "delta_rate = (this_count - last_count) / last_count * 100"})

    def test_share_of_total_global(self) -> None:
        out = build_step_display(_calc({"name": "ratio", "operation": "share_of_total", "value": "count"}))
        self.assertEqual(out, {"label": "비중 계산", "expression": "ratio = count / 전체 합계 * 100"})

    def test_share_of_total_partitioned(self) -> None:
        out = build_step_display(_calc({
            "name": "ratio", "operation": "share_of_total", "value": "count", "partition_by": ["sentiment"],
        }))
        self.assertEqual(out["expression"], "ratio = count / sentiment별 합계 * 100")

    def test_arithmetic_symbol(self) -> None:
        out = build_step_display(_calc({"name": "delta", "operation": "subtract", "left": "this", "right": "last"}))
        self.assertEqual(out, {"label": "값 계산", "expression": "delta = this - last"})

    def test_empty_expressions_returns_none(self) -> None:
        self.assertIsNone(build_step_display(_step("calculate", {"expressions": []})))


class SortDisplayParityTests(unittest.TestCase):
    def test_order_only(self) -> None:
        out = build_step_display(_step("sort", {"by": ["count"], "order": "desc"}))
        self.assertEqual(out, {"label": "정렬", "expression": "ORDER BY count DESC"})

    def test_order_with_limit(self) -> None:
        out = build_step_display(_step("sort", {"by": ["count"], "order": "desc", "limit": 10}))
        self.assertEqual(out["expression"], "ORDER BY count DESC LIMIT 10")


class PresentDisplayParityTests(unittest.TestCase):
    def test_present_with_title_and_limit(self) -> None:
        out = build_step_display(_step("present", {
            "input": "sorted", "format": "table", "title": "아스펙트별 건수", "limit": 100,
        }))
        self.assertEqual(out, {"label": "결과 표시", "expression": "TABLE: 아스펙트별 건수 (LIMIT 100)"})

    def test_present_no_title(self) -> None:
        out = build_step_display(_step("present", {"format": "table"}))
        self.assertEqual(out["expression"], "TABLE")


class ScopeAndShapeTests(unittest.TestCase):
    def test_unknown_skill_returns_none(self) -> None:
        self.assertIsNone(build_step_display(_step("nope", {"x": 1})))

    def test_display_shape_is_label_expression(self) -> None:
        out = build_step_display(_step("present", {"format": "json"}))
        self.assertEqual(set(out.keys()), {"label", "expression"})


class PlanWithStepDisplayTests(unittest.TestCase):
    def test_attaches_display_non_destructive(self) -> None:
        plan = {"plan_version": "v2", "steps": [
            {"id": "a", "skill": "sort", "params": {"by": ["count"], "order": "desc"}},
            {"id": "b", "skill": "present", "params": {"format": "table"}},
        ]}
        out = plan_with_step_display(plan)
        # 비파괴: 원본 step엔 display 없음
        self.assertNotIn("display", plan["steps"][0])
        # 반환본: 각 step에 display 부착
        self.assertEqual(out["steps"][0]["display"]["expression"], "ORDER BY count DESC")
        self.assertEqual(out["steps"][1]["display"]["label"], "결과 표시")
        self.assertEqual(out["plan_version"], "v2")

    def test_unknown_skill_step_has_no_display_key(self) -> None:
        plan = {"steps": [{"id": "x", "skill": "mystery", "params": {}}]}
        out = plan_with_step_display(plan)
        self.assertNotIn("display", out["steps"][0])


if __name__ == "__main__":
    unittest.main()
