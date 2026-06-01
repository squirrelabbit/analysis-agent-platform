"""SQL audit (2026-05-26) regression 잠금 — R1~R9.

이 파일은 audit ``executor_sql_audit_2026-05-26``의 regression 후보를 fixture로
정리한 잠금이다. 기존 unit test(test_planner_validator / test_executor_calculate /
test_executor_present)와 일부 중복되지만 의도된 중복 — 향후 누군가 audit를 다시
열었을 때 R{N} 명명으로 빠르게 추적할 수 있게 한다.

분류:
- validator-only (이 파일): R1, R3, R4, R5, R7, R8, R9
- executor-level (test_sql_regression_exec.py): R2, R6

각 케이스는 plan 본문 + 기대 issue code를 한 함수에 묶는다.
"""

from __future__ import annotations

import unittest
from typing import Any

from python_ai_worker.planner import collect_plan_issues


def _wrap(steps: list[dict[str, Any]]) -> dict[str, Any]:
    return {"plan_version": "v2", "steps": steps}


def _codes(plan: dict[str, Any]) -> list[str]:
    return [issue.code for issue in collect_plan_issues(plan)]


class SqlRegressionR1(unittest.TestCase):
    """R1 (SQL-1.3, audit aggregate metric prefix) — metric name에 비교 label
    prefix(last_/this_/prev_/curr_/year_/month_/baseline_)가 들어가면 compare가
    다시 prefix를 붙여 중복 prefix가 생긴다. validator가 사전 reject."""

    def test_aggregate_metric_with_last_prefix_rejected(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "last_count", "function": "count", "column": "*"}],
            },
        }
        self.assertIn("params.metric_name_label_prefix", _codes(_wrap([step])))


class SqlRegressionR3(unittest.TestCase):
    """R3 (SQL-1.5, audit M2) — is_null/not_null operator에 의미 있는 value가
    들어오면 validation issue로 reject."""

    def test_filter_is_null_with_value_rejected(self) -> None:
        step = {
            "id": "f",
            "skill": "filter",
            "params": {
                "input": "clauses",
                "column": "aspect",
                "operator": "is_null",
                "value": "etc",
            },
        }
        self.assertIn("params.value_unexpected", _codes(_wrap([step])))


class SqlRegressionR4(unittest.TestCase):
    """R4 (SQL-2.3, audit M8) — calculate input이 RESERVED 테이블이고 expression
    의 left/right가 그 테이블의 string column이면 reject."""

    def test_calculate_on_clauses_string_column_rejected(self) -> None:
        step = {
            "id": "calc",
            "skill": "calculate",
            "params": {
                "input": "clauses",
                "expressions": [
                    {"name": "x", "operation": "subtract", "left": "sentiment", "right": "aspect"},
                ],
            },
        }
        self.assertIn("params.expression_column_not_numeric", _codes(_wrap([step])))


class SqlRegressionR5(unittest.TestCase):
    """R5 (SQL-1.4, audit C6) — compare.left_label/right_label은 SQL identifier
    형태여야 한다. 공백/한국어/숫자 시작은 reject."""

    def _compare_plan(self, *, left_label: str, right_label: str) -> dict[str, Any]:
        return _wrap(
            [
                {
                    "id": "a",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "b",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "cmp",
                    "skill": "compare",
                    "params": {
                        "left": "a",
                        "right": "b",
                        "join_key": ["aspect"],
                        "left_label": left_label,
                        "right_label": right_label,
                    },
                },
            ]
        )

    def test_compare_label_with_space_rejected(self) -> None:
        codes = _codes(self._compare_plan(left_label="last", right_label="this year"))
        self.assertIn("params.right_label_unsafe", codes)


class SqlRegressionR7(unittest.TestCase):
    """R7 (SQL-3.2, audit M3) — aggregate sum/avg는 RESERVED 테이블의 string
    column에 적용 불가."""

    def test_aggregate_sum_on_sentiment_rejected(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "s", "function": "sum", "column": "sentiment"}],
            },
        }
        self.assertIn("params.metric_column_not_numeric", _codes(_wrap([step])))


class SqlRegressionR8(unittest.TestCase):
    """R8 (SQL-3.3, audit M4) — join.on의 각 key가 양쪽 input의 inferred output
    schema에 존재해야 한다. aggregate output은 group_by + metric만 갖는다."""

    def test_join_key_missing_in_aggregate_output_rejected(self) -> None:
        plan = _wrap(
            [
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
                    "id": "j",
                    "skill": "join",
                    "params": {"left": "agg", "right": "clauses", "on": ["doc_id"], "how": "inner"},
                },
            ]
        )
        self.assertIn("params.join_key_unknown", _codes(plan))


class SqlRegressionR10(unittest.TestCase):
    """R10 (SQL-6.1, Q4 plan stability audit 2026-05-27) — compare.join_key가
    left/right step의 inferred output에 모두 있어야 한다. group_by가 다른 두
    aggregate를 같은 join_key로 묶으면 DuckDB Binder Error → validator가 사전
    reject."""

    def test_compare_join_key_missing_in_left_step_output_rejected(self) -> None:
        # left_agg.group_by=["sentiment"] → output에 aspect 없음
        plan = _wrap(
            [
                {
                    "id": "left_agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["sentiment"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "right_agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "cmp",
                    "skill": "compare",
                    "params": {
                        "left": "left_agg",
                        "right": "right_agg",
                        "join_key": ["aspect"],
                        "left_label": "neg",
                        "right_label": "total",
                    },
                },
            ]
        )
        self.assertIn("params.compare_join_key_unknown", _codes(plan))


class SqlRegressionR9(unittest.TestCase):
    """R9 (SQL-3.4, audit M6) — sort.by의 각 column이 inferred output에 존재해야
    한다. aggregate 결과를 ghost column으로 sort하면 reject."""

    def test_sort_ghost_column_rejected(self) -> None:
        plan = _wrap(
            [
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
                    "id": "s",
                    "skill": "sort",
                    "params": {"input": "agg", "by": ["ghost"], "order": "desc"},
                },
            ]
        )
        self.assertIn("params.sort_by_unknown", _codes(plan))


if __name__ == "__main__":
    unittest.main()
