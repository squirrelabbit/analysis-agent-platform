"""plan_v2 validator tests — 일반 5 규칙 + skill별 hardcoded rule 잠금."""

from __future__ import annotations

import unittest
from typing import Any

from python_ai_worker.planner import (
    PlanValidationError,
    collect_plan_issues,
    validate_plan,
)


def _wrap(steps: list[dict[str, Any]]) -> dict[str, Any]:
    return {"plan_version": "v2", "steps": steps}


def _filter_step(step_id: str, **overrides: Any) -> dict[str, Any]:
    params = {
        "input": "docs",
        "column": "created_at",
        "operator": "between",
        "value": ["2025-01-01", "2025-12-31"],
    }
    params.update(overrides.pop("params", {}))
    base = {"id": step_id, "skill": "filter", "params": params}
    base.update(overrides)
    return base


def _codes(plan: dict[str, Any]) -> list[str]:
    return [issue.code for issue in collect_plan_issues(plan)]


class PlanEnvelopeTests(unittest.TestCase):
    def test_valid_minimal_plan_has_no_issues(self) -> None:
        plan = _wrap([_filter_step("last_year")])
        self.assertEqual(collect_plan_issues(plan), [])

    def test_validate_plan_raises_on_error(self) -> None:
        with self.assertRaises(PlanValidationError):
            validate_plan({"plan_version": "v1", "steps": [_filter_step("a")]})

    def test_plan_version_mismatch(self) -> None:
        self.assertIn("plan.version_mismatch", _codes({"plan_version": "v1", "steps": [_filter_step("a")]}))

    def test_plan_not_object(self) -> None:
        self.assertEqual([issue.code for issue in collect_plan_issues([])], ["plan.not_object"])

    def test_steps_not_list(self) -> None:
        self.assertIn("plan.steps_not_list", _codes({"plan_version": "v2", "steps": {}}))

    def test_steps_empty(self) -> None:
        self.assertIn("plan.steps_empty", _codes({"plan_version": "v2", "steps": []}))


class StepIdRuleTests(unittest.TestCase):
    def test_id_missing(self) -> None:
        step = _filter_step("temp")
        step["id"] = ""
        self.assertIn("step.id_missing", _codes(_wrap([step])))

    def test_id_reserved_docs(self) -> None:
        step = _filter_step("docs")
        self.assertIn("step.id_reserved", _codes(_wrap([step])))

    def test_id_reserved_clauses(self) -> None:
        step = _filter_step("clauses")
        self.assertIn("step.id_reserved", _codes(_wrap([step])))

    def test_id_reserved_genuineness(self) -> None:
        step = _filter_step("genuineness")
        self.assertIn("step.id_reserved", _codes(_wrap([step])))

    def test_id_duplicated(self) -> None:
        plan = _wrap([_filter_step("a"), _filter_step("a")])
        self.assertIn("step.id_duplicated", _codes(plan))

    def test_id_invalid_starts_with_digit(self) -> None:
        step = _filter_step("2024_data")
        self.assertIn("step.id_invalid", _codes(_wrap([step])))

    def test_id_invalid_dash(self) -> None:
        step = _filter_step("last-year")
        self.assertIn("step.id_invalid", _codes(_wrap([step])))

    def test_id_invalid_space(self) -> None:
        step = _filter_step("last year")
        self.assertIn("step.id_invalid", _codes(_wrap([step])))

    def test_id_invalid_quote_injection(self) -> None:
        step = _filter_step("a\";DROP")
        self.assertIn("step.id_invalid", _codes(_wrap([step])))

    def test_id_valid_underscore_and_digit(self) -> None:
        plan = _wrap([_filter_step("step_2024")])
        self.assertEqual(collect_plan_issues(plan), [])


class InputRefRuleTests(unittest.TestCase):
    def test_input_to_standard_table_ok(self) -> None:
        self.assertEqual(collect_plan_issues(_wrap([_filter_step("a", params={"input": "clauses", "column": "aspect"})])), [])

    def test_input_to_prior_step_ok(self) -> None:
        plan = _wrap(
            [
                _filter_step("step_a"),
                _filter_step("step_b", params={"input": "step_a", "column": "doc_id"}),
            ]
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_input_unknown(self) -> None:
        step = _filter_step("a", params={"input": "ghost", "column": "doc_id"})
        self.assertIn("params.input_unknown", _codes(_wrap([step])))

    def test_input_forward_ref(self) -> None:
        plan = _wrap(
            [
                _filter_step("first", params={"input": "second", "column": "doc_id"}),
                _filter_step("second"),
            ]
        )
        self.assertIn("params.input_forward_ref", _codes(plan))

    def test_input_self_ref_treated_as_forward(self) -> None:
        step = _filter_step("self_step", params={"input": "self_step", "column": "doc_id"})
        self.assertIn("params.input_forward_ref", _codes(_wrap([step])))


class ColumnExistenceTests(unittest.TestCase):
    def test_filter_unknown_column_on_clauses(self) -> None:
        step = _filter_step("a", params={"input": "clauses", "column": "no_such_col"})
        self.assertIn("params.column_unknown", _codes(_wrap([step])))

    def test_filter_column_skipped_on_dynamic_table(self) -> None:
        # docs는 dynamic_columns=True라 column 검증 보류
        step = _filter_step("a", params={"input": "docs", "column": "dataset_specific_col"})
        self.assertNotIn("params.column_unknown", _codes(_wrap([step])))

    def test_filter_column_skipped_on_step_id_input(self) -> None:
        # step output schema는 추적 안 함 → 검증 보류
        plan = _wrap(
            [
                _filter_step("a"),
                _filter_step("b", params={"input": "a", "column": "anything"}),
            ]
        )
        self.assertNotIn("params.column_unknown", _codes(plan))


class FilterRuleTests(unittest.TestCase):
    def test_operator_invalid(self) -> None:
        step = _filter_step("a", params={"input": "docs", "column": "created_at", "operator": "approx", "value": 1})
        self.assertIn("params.operator_invalid", _codes(_wrap([step])))

    def test_between_arity(self) -> None:
        step = _filter_step("a", params={"input": "docs", "column": "created_at", "operator": "between", "value": ["2025-01-01"]})
        self.assertIn("params.value_between_arity", _codes(_wrap([step])))

    def test_in_requires_list(self) -> None:
        step = _filter_step("a", params={"input": "clauses", "column": "aspect", "operator": "in", "value": "food"})
        self.assertIn("params.value_not_list", _codes(_wrap([step])))

    def test_is_null_ignores_value(self) -> None:
        step = _filter_step("a", params={"input": "clauses", "column": "aspect", "operator": "is_null", "value": None})
        self.assertEqual(collect_plan_issues(_wrap([step])), [])

    def test_is_null_rejects_value(self) -> None:
        step = _filter_step("a", params={"input": "clauses", "column": "aspect", "operator": "is_null", "value": "etc"})
        self.assertIn("params.value_unexpected", _codes(_wrap([step])))


class JoinRuleTests(unittest.TestCase):
    def test_valid_join(self) -> None:
        step = {
            "id": "join_step",
            "skill": "join",
            "params": {"left": "docs", "right": "clauses", "on": ["doc_id"], "how": "inner"},
        }
        self.assertEqual(collect_plan_issues(_wrap([step])), [])

    def test_how_invalid(self) -> None:
        step = {
            "id": "join_step",
            "skill": "join",
            "params": {"left": "docs", "right": "clauses", "on": ["doc_id"], "how": "fuzzy"},
        }
        self.assertIn("params.how_invalid", _codes(_wrap([step])))

    def test_on_empty(self) -> None:
        step = {
            "id": "join_step",
            "skill": "join",
            "params": {"left": "docs", "right": "clauses", "on": [], "how": "inner"},
        }
        self.assertIn("params.on_not_list", _codes(_wrap([step])))


class AggregateRuleTests(unittest.TestCase):
    def test_valid_aggregate(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "count", "function": "count", "column": "*"}],
            },
        }
        self.assertEqual(collect_plan_issues(_wrap([step])), [])

    def test_group_by_empty_total_mode_allowed(self) -> None:
        # silverone 2026-06-05 — group_by=[]는 total mode(전체 1행 집계)로 허용된다.
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": [],
                "metrics": [{"name": "c", "function": "count", "column": "*"}],
            },
        }
        self.assertEqual(collect_plan_issues(_wrap([step])), [])

    def test_group_by_not_list_rejected(self) -> None:
        # 리스트가 아니면 여전히 거절.
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": "aspect",
                "metrics": [{"name": "c", "function": "count", "column": "*"}],
            },
        }
        self.assertIn("params.group_by_not_list", _codes(_wrap([step])))

    def test_metric_function_invalid(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "c", "function": "median", "column": "doc_id"}],
            },
        }
        self.assertIn("params.metric_function_invalid", _codes(_wrap([step])))

    def test_metric_column_unknown(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "s", "function": "sum", "column": "ghost"}],
            },
        }
        self.assertIn("params.column_unknown", _codes(_wrap([step])))

    def test_metric_name_duplicated(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [
                    {"name": "c", "function": "count", "column": "*"},
                    {"name": "c", "function": "count", "column": "*"},
                ],
            },
        }
        self.assertIn("params.metric_name_duplicated", _codes(_wrap([step])))


class CompareRuleTests(unittest.TestCase):
    def test_valid_compare_between_prior_steps(self) -> None:
        plan = _wrap(
            [
                {
                    "id": "last",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "this",
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
                        "left": "last",
                        "right": "this",
                        "join_key": ["aspect"],
                        "left_label": "last_year",
                        "right_label": "this_year",
                    },
                },
            ]
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_empty_labels_rejected(self) -> None:
        step = {
            "id": "cmp",
            "skill": "compare",
            "params": {
                "left": "clauses",
                "right": "clauses",
                "join_key": ["aspect"],
                "left_label": "",
                "right_label": "",
            },
        }
        codes = _codes(_wrap([step]))
        self.assertIn("params.left_label_missing", codes)
        self.assertIn("params.right_label_missing", codes)

    def _scalar_agg(self, step_id: str) -> dict[str, Any]:
        return {
            "id": step_id,
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": [],
                "metrics": [{"name": "count", "function": "count", "column": "*"}],
            },
        }

    def _group_agg(self, step_id: str) -> dict[str, Any]:
        return {
            "id": step_id,
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "count", "function": "count", "column": "*"}],
            },
        }

    def test_join_key_empty_scalar_aggregates_allowed(self) -> None:
        # silverone 2026-06-05 — 양쪽이 group_by=[] aggregate면 join_key=[] (scalar) 허용.
        plan = _wrap(
            [
                self._scalar_agg("a"),
                self._scalar_agg("b"),
                {
                    "id": "cmp",
                    "skill": "compare",
                    "params": {
                        "left": "a",
                        "right": "b",
                        "join_key": [],
                        "left_label": "a",
                        "right_label": "b",
                    },
                },
            ]
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_join_key_empty_non_scalar_rejected(self) -> None:
        # 양쪽이 group_by 있는 aggregate면 join_key=[]는 거절(N×M cross product 방지).
        plan = _wrap(
            [
                self._group_agg("a"),
                self._group_agg("b"),
                {
                    "id": "cmp",
                    "skill": "compare",
                    "params": {
                        "left": "a",
                        "right": "b",
                        "join_key": [],
                        "left_label": "a",
                        "right_label": "b",
                    },
                },
            ]
        )
        self.assertIn("params.join_key_empty_not_scalar", _codes(plan))

    def test_join_key_empty_reserved_table_rejected(self) -> None:
        # RESERVED 테이블 직접 참조는 scalar가 아니므로 join_key=[] 거절.
        step = {
            "id": "cmp",
            "skill": "compare",
            "params": {
                "left": "clauses",
                "right": "clauses",
                "join_key": [],
                "left_label": "a",
                "right_label": "b",
            },
        }
        self.assertIn("params.join_key_empty_not_scalar", _codes(_wrap([step])))

    def test_join_key_empty_one_side_scalar_rejected(self) -> None:
        # 한쪽만 scalar여도 거절(둘 다 scalar여야 함).
        plan = _wrap(
            [
                self._scalar_agg("a"),
                self._group_agg("b"),
                {
                    "id": "cmp",
                    "skill": "compare",
                    "params": {
                        "left": "a",
                        "right": "b",
                        "join_key": [],
                        "left_label": "a",
                        "right_label": "b",
                    },
                },
            ]
        )
        self.assertIn("params.join_key_empty_not_scalar", _codes(plan))


class CalculateRuleTests(unittest.TestCase):
    def test_valid_calculate(self) -> None:
        plan = _wrap(
            [
                _filter_step("base"),
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "base",
                        "expressions": [
                            {"name": "delta", "operation": "subtract", "left": "a", "right": "b"},
                            {"name": "rate", "operation": "percent_change", "base": "b", "current": "a"},
                        ],
                    },
                },
            ]
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_operation_invalid(self) -> None:
        plan = _wrap(
            [
                _filter_step("base"),
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "base",
                        "expressions": [{"name": "x", "operation": "log"}],
                    },
                },
            ]
        )
        self.assertIn("params.expression_operation_invalid", _codes(plan))


class RecipeValidatorTests(unittest.TestCase):
    """validator가 runtime-enabled recipe step을 인식·검증 (unknown 거절 X).
    recipe step은 실행 직전 atomic step으로 deterministic lower된다."""

    def _dist(self, **params):
        base = {"input": "clauses", "group_by": ["sentiment"], "metric": "count",
                "include_share": True, "count_column": "count", "share_column": "ratio", "title": "t"}
        base.update(params)
        return _wrap([{"id": "d", "skill": "distribution", "params": base}])

    def _event_window(self, **params):
        base = {
            "input": "docs",
            "event_date": "2026-04-20",
            "date_column": "created_at",
            "before_days": 7,
            "after_days": 7,
            "grain": "day",
            "count_column": "count",
            "title": "window",
        }
        base.update(params)
        return _wrap([{"id": "w", "skill": "event_window_count", "params": base}])

    def _topn(self, **params):
        base = {
            "input": "clauses",
            "group_by": ["aspect"],
            "metric": "count",
            "filters": [{"column": "sentiment", "op": "=", "value": "negative"}],
            "sort": {"column": "count", "direction": "desc"},
            "limit": 10,
            "count_column": "count",
            "title": "top",
        }
        base.update(params)
        return _wrap([{"id": "t", "skill": "top_n", "params": base}])

    def test_valid_distribution_passes(self) -> None:
        self.assertEqual(collect_plan_issues(self._dist()), [])

    def test_distribution_minimal_passes(self) -> None:
        # 선택 param 생략(input/group_by만)도 통과
        plan = _wrap([{"id": "d", "skill": "distribution", "params": {"input": "clauses", "group_by": ["aspect"]}}])
        self.assertEqual(collect_plan_issues(plan), [])

    def test_missing_input(self) -> None:
        plan = _wrap([{"id": "d", "skill": "distribution", "params": {"group_by": ["sentiment"]}}])
        self.assertIn("params.missing_keys", _codes(plan))

    def test_bad_group_by(self) -> None:
        self.assertIn("params.recipe_group_by_invalid", _codes(self._dist(group_by=[])))
        self.assertIn("params.recipe_group_by_invalid", _codes(self._dist(group_by="sentiment")))

    def test_bad_metric(self) -> None:
        self.assertIn("params.recipe_metric_unsupported", _codes(self._dist(metric="sum")))

    def test_bad_include_share(self) -> None:
        self.assertIn("params.recipe_include_share_invalid", _codes(self._dist(include_share="yes")))

    def test_bad_column_name(self) -> None:
        self.assertIn("params.recipe_column_name_invalid", _codes(self._dist(count_column="")))
        self.assertIn("params.recipe_column_name_invalid", _codes(self._dist(share_column=5)))

    def test_bad_title(self) -> None:
        self.assertIn("params.recipe_title_invalid", _codes(self._dist(title=123)))

    def test_valid_event_window_count_passes(self) -> None:
        self.assertEqual(collect_plan_issues(self._event_window()), [])

    def test_event_window_count_minimal_passes(self) -> None:
        plan = _wrap([{"id": "w", "skill": "event_window_count", "params": {"input": "docs", "event_date": "2026-04-20"}}])
        self.assertEqual(collect_plan_issues(plan), [])

    def test_event_window_count_bad_date(self) -> None:
        self.assertIn("params.recipe_event_date_invalid", _codes(self._event_window(event_date="2026-99-99")))
        self.assertIn("params.recipe_event_date_invalid", _codes(self._event_window(event_date="")))

    def test_event_window_count_bad_window_and_grain(self) -> None:
        self.assertIn("params.recipe_window_invalid", _codes(self._event_window(before_days=-1)))
        self.assertIn("params.recipe_window_invalid", _codes(self._event_window(after_days="7")))
        self.assertIn("params.recipe_grain_unsupported", _codes(self._event_window(grain="week")))

    def test_event_window_count_bad_columns_and_title(self) -> None:
        self.assertIn("params.recipe_date_column_invalid", _codes(self._event_window(date_column="raw_text")))
        self.assertIn("params.recipe_column_name_invalid", _codes(self._event_window(count_column="")))
        self.assertIn(
            "params.recipe_column_name_invalid",
            _codes(self._event_window(count_column="created_at")),
        )
        self.assertIn("params.recipe_title_invalid", _codes(self._event_window(title=123)))

    def test_valid_top_n_passes(self) -> None:
        self.assertEqual(collect_plan_issues(self._topn()), [])

    def test_top_n_minimal_passes(self) -> None:
        plan = _wrap([{"id": "t", "skill": "top_n", "params": {"input": "clauses", "group_by": ["aspect"]}}])
        self.assertEqual(collect_plan_issues(plan), [])

    def test_top_n_bad_group_by(self) -> None:
        self.assertIn("params.recipe_group_by_invalid", _codes(self._topn(group_by=[])))
        self.assertIn("params.recipe_group_by_invalid", _codes(self._topn(group_by="aspect")))

    def test_top_n_bad_filter(self) -> None:
        self.assertIn(
            "params.recipe_filter_invalid",
            _codes(self._topn(filters=[{"column": "sentiment", "op": "~=", "value": "x"}])),
        )
        self.assertIn(
            "params.recipe_filter_value_invalid",
            _codes(self._topn(filters=[{"column": "sentiment", "op": "in", "value": "negative"}])),
        )

    def test_top_n_bad_sort_and_limit(self) -> None:
        self.assertIn("params.recipe_sort_invalid", _codes(self._topn(sort={"direction": "sideways"})))
        self.assertIn("params.recipe_sort_invalid", _codes(self._topn(sort={"column": "missing"})))
        self.assertIn("params.recipe_limit_invalid", _codes(self._topn(limit=0)))

    def test_top_n_bad_column_and_title(self) -> None:
        self.assertIn("params.recipe_column_name_invalid", _codes(self._topn(count_column="")))
        self.assertIn("params.recipe_title_invalid", _codes(self._topn(title=123)))


class ShareOfTotalRuleTests(unittest.TestCase):
    """silverone 2026-06-02 — calculate.share_of_total 계약 검증."""

    def _share_plan(self, expr: dict[str, Any]) -> dict[str, Any]:
        return _wrap(
            [
                {
                    "id": "counts",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["sentiment"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "share",
                    "skill": "calculate",
                    "params": {"input": "counts", "expressions": [expr]},
                },
            ]
        )

    def test_valid_global_share(self) -> None:
        plan = self._share_plan(
            {"name": "ratio", "operation": "share_of_total", "value": "count"}
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_valid_partitioned_share(self) -> None:
        plan = self._share_plan(
            {
                "name": "ratio",
                "operation": "share_of_total",
                "value": "count",
                "partition_by": ["sentiment"],
            }
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_missing_value_key(self) -> None:
        plan = self._share_plan({"name": "ratio", "operation": "share_of_total"})
        self.assertIn("params.expression_keys_missing", _codes(plan))

    def test_value_column_unknown(self) -> None:
        plan = self._share_plan(
            {"name": "ratio", "operation": "share_of_total", "value": "nope"}
        )
        self.assertIn("params.expression_column_unknown", _codes(plan))

    def test_partition_by_not_list(self) -> None:
        plan = self._share_plan(
            {
                "name": "ratio",
                "operation": "share_of_total",
                "value": "count",
                "partition_by": "sentiment",
            }
        )
        self.assertIn("params.expression_partition_by_not_list", _codes(plan))

    def test_partition_by_column_unknown(self) -> None:
        plan = self._share_plan(
            {
                "name": "ratio",
                "operation": "share_of_total",
                "value": "count",
                "partition_by": ["nope"],
            }
        )
        self.assertIn("params.expression_column_unknown", _codes(plan))


class PrefixContractTests(unittest.TestCase):
    """silverone 2026-05-26 (B안) — aggregate metric name에 비교 label prefix가
    들어가면 compare가 prefix를 다시 붙여 중복 prefix(`last_last_count`)가 발생.
    validator가 이를 사전에 잡아 self-correct retry로 가게 한다."""

    def _aspect_delta_plan(
        self,
        *,
        last_metric_name: str = "count",
        this_metric_name: str = "count",
        calc_left: str = "this_count",
        calc_right: str = "last_count",
    ) -> dict[str, Any]:
        return _wrap(
            [
                {
                    "id": "last_year_clauses",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": last_metric_name, "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "this_year_clauses",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": this_metric_name, "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "delta_pair",
                    "skill": "compare",
                    "params": {
                        "left": "last_year_clauses",
                        "right": "this_year_clauses",
                        "join_key": ["aspect"],
                        "left_label": "last",
                        "right_label": "this",
                    },
                },
                {
                    "id": "delta",
                    "skill": "calculate",
                    "params": {
                        "input": "delta_pair",
                        "expressions": [
                            {
                                "name": "delta_count",
                                "operation": "subtract",
                                "left": calc_left,
                                "right": calc_right,
                            }
                        ],
                    },
                },
            ]
        )

    def test_valid_b_contract_passes(self) -> None:
        """generic metric name `count` + compare가 prefix 부여 + calculate가
        `last_count` / `this_count` 참조 → 통과."""
        plan = self._aspect_delta_plan()
        self.assertEqual(collect_plan_issues(plan), [])

    def test_aggregate_metric_with_last_prefix_rejected(self) -> None:
        plan = self._aspect_delta_plan(
            last_metric_name="last_count", this_metric_name="this_count"
        )
        self.assertIn("params.metric_name_label_prefix", _codes(plan))

    def test_aggregate_metric_with_prev_prefix_rejected(self) -> None:
        plan = _wrap(
            [
                {
                    "id": "agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "prev_count", "function": "count", "column": "*"}],
                    },
                }
            ]
        )
        self.assertIn("params.metric_name_label_prefix", _codes(plan))

    def test_aggregate_metric_case_insensitive(self) -> None:
        plan = _wrap(
            [
                {
                    "id": "agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "Current_Sum_Value", "function": "sum", "column": "score"}],
                    },
                }
            ]
        )
        self.assertIn("params.metric_name_label_prefix", _codes(plan))

    def test_generic_metric_names_accepted(self) -> None:
        # `count`, `sum_value`, `avg_score` 같은 generic name은 OK.
        for metric_name in ("count", "sum_value", "avg_score", "n", "total"):
            with self.subTest(metric_name=metric_name):
                plan = _wrap(
                    [
                        {
                            "id": "agg",
                            "skill": "aggregate",
                            "params": {
                                "input": "clauses",
                                "group_by": ["aspect"],
                                "metrics": [
                                    {"name": metric_name, "function": "count", "column": "*"}
                                ],
                            },
                        }
                    ]
                )
                self.assertNotIn("params.metric_name_label_prefix", _codes(plan))

    def test_calculate_references_unknown_column_rejected(self) -> None:
        """compare 결과는 `last_count` / `this_count`가 정답. calculate가 prefix
        없는 옛 이름(`this_count` → ok, but `count` → unknown)을 reference 하면
        unknown 검출."""
        plan = self._aspect_delta_plan(calc_left="count", calc_right="last_count")
        self.assertIn("params.expression_column_unknown", _codes(plan))

    def test_calculate_references_double_prefix_unknown(self) -> None:
        """중복 prefix(`this_this_count`)는 compare output에 없으므로 unknown."""
        plan = self._aspect_delta_plan(
            calc_left="this_this_count", calc_right="last_last_count"
        )
        self.assertIn("params.expression_column_unknown", _codes(plan))

    def test_calculate_against_filter_from_reserved_table_skipped(self) -> None:
        """input이 reserved table을 거친 filter면 추론 불가 → skip (false positive 방지)."""
        plan = _wrap(
            [
                _filter_step("base"),
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "base",
                        "expressions": [
                            {
                                "name": "delta",
                                "operation": "subtract",
                                "left": "any_column",
                                "right": "another_column",
                            }
                        ],
                    },
                },
            ]
        )
        self.assertNotIn("params.expression_column_unknown", _codes(plan))


class SortRuleTests(unittest.TestCase):
    def test_valid_sort(self) -> None:
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
                    "id": "top",
                    "skill": "sort",
                    "params": {"input": "agg", "by": ["count"], "order": "desc", "limit": 5},
                },
            ]
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_order_invalid(self) -> None:
        step = {"id": "s", "skill": "sort", "params": {"input": "clauses", "by": ["aspect"], "order": "random"}}
        self.assertIn("params.order_invalid", _codes(_wrap([step])))

    def test_limit_zero_rejected(self) -> None:
        step = {"id": "s", "skill": "sort", "params": {"input": "clauses", "by": ["aspect"], "limit": 0}}
        self.assertIn("params.limit_invalid", _codes(_wrap([step])))


class PresentRuleTests(unittest.TestCase):
    def test_valid_present(self) -> None:
        plan = _wrap(
            [
                _filter_step("a"),
                {"id": "out", "skill": "present", "params": {"input": "a", "format": "table"}},
            ]
        )
        self.assertEqual(collect_plan_issues(plan), [])

    def test_format_invalid(self) -> None:
        step = {"id": "out", "skill": "present", "params": {"input": "clauses", "format": "xml"}}
        self.assertIn("params.format_invalid", _codes(_wrap([step])))


class SummarizeRemovedTests(unittest.TestCase):
    # summarize는 2026-06-29 SKILL_CATALOG에서 제거 — executor 빌더가 없어 hard-fail이었고
    # 자연어 요약은 composer가 합성한다. plan에 들어오면 unknown skill로 거절되어야 한다.
    def test_summarize_rejected(self) -> None:
        step = {"id": "sum", "skill": "summarize", "params": {"input": "clauses", "focus": "x"}}
        self.assertNotEqual(collect_plan_issues(_wrap([step])), [])


class SkillCatalogTests(unittest.TestCase):
    def test_unknown_skill(self) -> None:
        step = {"id": "x", "skill": "wave_hands", "params": {}}
        self.assertIn("step.skill_unknown", _codes(_wrap([step])))

    def test_missing_skill(self) -> None:
        step = {"id": "x", "params": {}}
        self.assertIn("step.skill_missing", _codes(_wrap([step])))


class SqlContractTests(unittest.TestCase):
    """silverone 2026-05-26 (SQL-1) — executor SQL builder가 빌드/실행
    시점에 fail-loud 하는 gap을 validator 단에서 미리 reject."""

    # ----- SQL-1.1 (C1) — schema inference 가 join/sort chain까지 전파 -----

    def test_calculate_on_sort_of_aggregate_resolves_metric_column(self) -> None:
        """aggregate → sort → calculate chain에서 calculate가 metric column을
        참조 가능 (sort는 input pass-through)."""
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
                    "id": "ranked",
                    "skill": "sort",
                    "params": {"input": "agg", "by": ["count"], "order": "desc"},
                },
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "ranked",
                        "expressions": [
                            {
                                "name": "double_count",
                                "operation": "multiply",
                                "left": "count",
                                "right": "count",
                            }
                        ],
                    },
                },
            ]
        )
        self.assertNotIn("params.expression_column_unknown", _codes(plan))

    def test_calculate_on_sort_of_aggregate_unknown_column_rejected(self) -> None:
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
                    "id": "ranked",
                    "skill": "sort",
                    "params": {"input": "agg", "by": ["count"], "order": "desc"},
                },
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "ranked",
                        "expressions": [
                            {
                                "name": "x",
                                "operation": "subtract",
                                "left": "ghost",
                                "right": "count",
                            }
                        ],
                    },
                },
            ]
        )
        self.assertIn("params.expression_column_unknown", _codes(plan))

    def test_calculate_on_join_of_aggregates_resolves_right_prefix(self) -> None:
        """join은 on_keys + left non-key + right non-key(right_ prefix on collision)
        를 output column으로 노출. 같은 metric name이 양쪽에 있으면 오른쪽은
        `right_count`로 노출되고 calculate에서 참조 가능."""
        plan = _wrap(
            [
                {
                    "id": "last_y",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "this_y",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "joined",
                    "skill": "join",
                    "params": {"left": "last_y", "right": "this_y", "on": ["aspect"], "how": "inner"},
                },
                {
                    "id": "delta",
                    "skill": "calculate",
                    "params": {
                        "input": "joined",
                        "expressions": [
                            {
                                "name": "diff",
                                "operation": "subtract",
                                "left": "count",
                                "right": "right_count",
                            }
                        ],
                    },
                },
            ]
        )
        self.assertNotIn("params.expression_column_unknown", _codes(plan))

    # ----- SQL-1.2 (C2) — join.on object-array reject -----

    def test_join_on_object_array_rejected(self) -> None:
        step = {
            "id": "j",
            "skill": "join",
            "params": {
                "left": "docs",
                "right": "clauses",
                "on": [{"left": "doc_id", "right": "doc_id"}],
                "how": "inner",
            },
        }
        self.assertIn("params.on_not_string_list", _codes(_wrap([step])))

    def test_join_on_mixed_string_and_object_rejected(self) -> None:
        step = {
            "id": "j",
            "skill": "join",
            "params": {
                "left": "docs",
                "right": "clauses",
                "on": ["doc_id", {"left": "x", "right": "y"}],
                "how": "left",
            },
        }
        self.assertIn("params.on_not_string_list", _codes(_wrap([step])))

    # ----- SQL-1.3 (C5) — aggregate metric.name ↔ group_by collision -----

    def test_aggregate_metric_name_collides_with_group_by_rejected(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "aspect", "function": "count", "column": "*"}],
            },
        }
        self.assertIn("params.metric_name_collides_group_by", _codes(_wrap([step])))

    def test_aggregate_metric_distinct_name_accepted(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "aspect_count", "function": "count", "column": "*"}],
            },
        }
        self.assertNotIn("params.metric_name_collides_group_by", _codes(_wrap([step])))

    # ----- SQL-1.4 (C6) — compare label SQL identifier safety -----

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

    def test_compare_left_label_korean_rejected(self) -> None:
        codes = _codes(self._compare_plan(left_label="작년", right_label="this"))
        self.assertIn("params.left_label_unsafe", codes)

    def test_compare_right_label_with_space_rejected(self) -> None:
        codes = _codes(self._compare_plan(left_label="last", right_label="this year"))
        self.assertIn("params.right_label_unsafe", codes)

    def test_compare_label_digit_start_rejected(self) -> None:
        codes = _codes(self._compare_plan(left_label="2024", right_label="2025"))
        self.assertIn("params.left_label_unsafe", codes)
        self.assertIn("params.right_label_unsafe", codes)

    def test_compare_label_hyphen_rejected(self) -> None:
        codes = _codes(self._compare_plan(left_label="last-year", right_label="this"))
        self.assertIn("params.left_label_unsafe", codes)

    def test_compare_label_underscore_and_alnum_accepted(self) -> None:
        codes = _codes(self._compare_plan(left_label="last_year", right_label="this_year"))
        self.assertNotIn("params.left_label_unsafe", codes)
        self.assertNotIn("params.right_label_unsafe", codes)

    def test_compare_labels_identical_rejected(self) -> None:
        codes = _codes(self._compare_plan(left_label="same", right_label="same"))
        self.assertIn("params.compare_labels_identical", codes)

    # ----- SQL-6.1 (Q4 audit) — compare.join_key step-output 검증 -----

    def _compare_with_aggregates(
        self,
        *,
        left_group_by: list[str],
        right_group_by: list[str],
        join_key: list[str],
    ) -> dict[str, Any]:
        return _wrap(
            [
                {
                    "id": "left_agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": left_group_by,
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "right_agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": right_group_by,
                        "metrics": [{"name": "count", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "cmp",
                    "skill": "compare",
                    "params": {
                        "left": "left_agg",
                        "right": "right_agg",
                        "join_key": join_key,
                        "left_label": "a",
                        "right_label": "b",
                    },
                },
            ]
        )

    def test_compare_join_key_missing_in_right_rejected(self) -> None:
        # left에 aspect 있지만 right에 없음 → right_side 누락 reject
        plan = self._compare_with_aggregates(
            left_group_by=["aspect"], right_group_by=["sentiment"], join_key=["aspect"]
        )
        codes = _codes(plan)
        self.assertIn("params.compare_join_key_unknown", codes)
        # 메시지에 side 정보가 포함되어야 한다 ('right' 단서).
        msgs = [
            issue.message
            for issue in collect_plan_issues(plan)
            if issue.code == "params.compare_join_key_unknown"
        ]
        self.assertTrue(any("right" in m for m in msgs), f"expected 'right' in {msgs}")

    def test_compare_join_key_missing_in_both_rejected(self) -> None:
        # 양쪽 다 doc_id 없음. left/right 각 1건씩 issue.
        plan = self._compare_with_aggregates(
            left_group_by=["aspect"], right_group_by=["sentiment"], join_key=["doc_id"]
        )
        codes = _codes(plan)
        # 2건 모두 노출 — caller가 한 번에 두 side를 보고 plan 수정 가능.
        self.assertEqual(codes.count("params.compare_join_key_unknown"), 2)

    def test_compare_join_key_present_in_both_passes(self) -> None:
        plan = self._compare_with_aggregates(
            left_group_by=["aspect"], right_group_by=["aspect"], join_key=["aspect"]
        )
        codes = _codes(plan)
        self.assertNotIn("params.compare_join_key_unknown", codes)

    # ----- SQL-1.5 (M2) — is_null/not_null + value reject -----

    def test_not_null_rejects_value(self) -> None:
        step = _filter_step(
            "a",
            params={"input": "clauses", "column": "aspect", "operator": "not_null", "value": "etc"},
        )
        self.assertIn("params.value_unexpected", _codes(_wrap([step])))

    def test_is_null_with_zero_rejected(self) -> None:
        step = _filter_step(
            "a",
            params={"input": "clauses", "column": "aspect", "operator": "is_null", "value": 0},
        )
        self.assertIn("params.value_unexpected", _codes(_wrap([step])))

    def test_not_null_with_empty_list_passes(self) -> None:
        step = _filter_step(
            "a",
            params={"input": "clauses", "column": "aspect", "operator": "not_null", "value": []},
        )
        self.assertNotIn("params.value_unexpected", _codes(_wrap([step])))

    # ----- SQL-2.3 (M8) — calculate 수치 expression이 RESERVED string column reject -----

    def test_calculate_on_clauses_string_column_rejected(self) -> None:
        """calculate input이 RESERVED table(clauses)이고 expression이 string
        column(sentiment)을 참조하면 reject."""
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

    def test_calculate_on_docs_created_at_timestamp_allowed(self) -> None:
        """docs.created_at은 timestamp라 다른 timestamp와 subtract 의미가 있을 수
        있어 reject하지 않는다."""
        step = {
            "id": "calc",
            "skill": "calculate",
            "params": {
                "input": "docs",
                "expressions": [
                    {"name": "x", "operation": "subtract", "left": "created_at", "right": "created_at"},
                ],
            },
        }
        codes = _codes(_wrap([step]))
        self.assertNotIn("params.expression_column_not_numeric", codes)

    def test_calculate_on_aggregate_metric_passes(self) -> None:
        """RESERVED chain을 거친 aggregate output(metric)에 calculate 적용은 통과."""
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
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "agg",
                        "expressions": [
                            {"name": "double", "operation": "multiply", "left": "count", "right": "count"},
                        ],
                    },
                },
            ]
        )
        codes = _codes(plan)
        self.assertNotIn("params.expression_column_not_numeric", codes)

    # ----- 2026-06-04 — calculate VARCHAR numeric-op 방어 (prior step output 타입 추론) -----

    @staticmethod
    def _agg_then_calc(expr: dict[str, Any]) -> dict[str, Any]:
        """clauses → aggregate(group_by=[aspect], count) → calculate(expr) 플랜.

        aggregate output: aspect(string, group 컬럼) / cnt(numeric, count metric).
        """
        return _wrap(
            [
                {
                    "id": "agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "cnt", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {"input": "agg", "expressions": [expr]},
                },
            ]
        )

    def test_calc_add_on_prior_string_group_column_rejected(self) -> None:
        # add/subtract/multiply/divide: left/right가 prior aggregate의 string group
        # 컬럼(aspect)이면 reject.
        for op in ("add", "subtract", "multiply", "divide"):
            plan = self._agg_then_calc(
                {"name": "x", "operation": op, "left": "aspect", "right": "cnt"}
            )
            self.assertIn(
                "params.expression_column_not_numeric",
                _codes(plan),
                msg=f"{op} on string group column should be rejected",
            )

    def test_calc_ratio_on_prior_string_column_rejected(self) -> None:
        # ratio numerator/denominator가 string group 컬럼이면 reject.
        plan = self._agg_then_calc(
            {"name": "r", "operation": "ratio", "numerator": "aspect", "denominator": "cnt"}
        )
        self.assertIn("params.expression_column_not_numeric", _codes(plan))

    def test_calc_share_of_total_on_prior_string_column_rejected(self) -> None:
        # share_of_total.value가 string group 컬럼이면 reject.
        plan = self._agg_then_calc(
            {"name": "s", "operation": "share_of_total", "value": "aspect"}
        )
        self.assertIn("params.expression_column_not_numeric", _codes(plan))

    def test_calc_share_of_total_on_prior_count_metric_passes(self) -> None:
        # 정상 케이스 — count metric(numeric)에 share_of_total은 통과해야 한다.
        plan = self._agg_then_calc(
            {"name": "s", "operation": "share_of_total", "value": "cnt"}
        )
        self.assertNotIn("params.expression_column_not_numeric", _codes(plan))

    def test_calc_numeric_ops_on_prior_count_metric_pass(self) -> None:
        for op in ("add", "subtract", "multiply", "divide"):
            plan = self._agg_then_calc(
                {"name": "x", "operation": op, "left": "cnt", "right": "cnt"}
            )
            self.assertNotIn(
                "params.expression_column_not_numeric",
                _codes(plan),
                msg=f"{op} on numeric count metric should pass",
            )

    def test_calc_on_filtered_aggregate_string_column_rejected(self) -> None:
        # filter는 pass-through라 filter(aggregate) 체인을 거쳐도 string 타입 추론 유지.
        plan = _wrap(
            [
                {
                    "id": "agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["aspect"],
                        "metrics": [{"name": "cnt", "function": "count", "column": "*"}],
                    },
                },
                {
                    "id": "f",
                    "skill": "filter",
                    "params": {"input": "agg", "column": "cnt", "operator": "gt", "value": 1},
                },
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "f",
                        "expressions": [
                            {"name": "x", "operation": "multiply", "left": "aspect", "right": "cnt"}
                        ],
                    },
                },
            ]
        )
        self.assertIn("params.expression_column_not_numeric", _codes(plan))

    def test_calc_on_min_string_metric_rejected(self) -> None:
        # min(string 컬럼) metric 출력은 string → 그 컬럼에 numeric op는 reject.
        plan = _wrap(
            [
                {
                    "id": "agg",
                    "skill": "aggregate",
                    "params": {
                        "input": "clauses",
                        "group_by": ["doc_id"],
                        "metrics": [{"name": "min_aspect", "function": "min", "column": "aspect"}],
                    },
                },
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "agg",
                        "expressions": [
                            {"name": "x", "operation": "multiply", "left": "min_aspect", "right": "min_aspect"}
                        ],
                    },
                },
            ]
        )
        self.assertIn("params.expression_column_not_numeric", _codes(plan))

    # ----- SQL-3.1 (C4) — filter type mismatch -----

    def test_filter_contains_on_timestamp_column_rejected(self) -> None:
        """contains는 string/text column에만 적용 가능. timestamp는 reject."""
        step = {
            "id": "f",
            "skill": "filter",
            "params": {
                "input": "docs",
                "column": "created_at",
                "operator": "contains",
                "value": "2025",
            },
        }
        self.assertIn("params.value_type_mismatch", _codes(_wrap([step])))

    def test_filter_timestamp_with_iso_string_passes(self) -> None:
        """timestamp column + ISO string value는 executor CAST로 처리 → 통과."""
        step = {
            "id": "f",
            "skill": "filter",
            "params": {
                "input": "docs",
                "column": "created_at",
                "operator": "between",
                "value": ["2025-01-01", "2025-12-31"],
            },
        }
        codes = _codes(_wrap([step]))
        self.assertNotIn("params.value_type_mismatch", codes)

    def test_filter_timestamp_with_numeric_value_rejected(self) -> None:
        step = {
            "id": "f",
            "skill": "filter",
            "params": {
                "input": "docs",
                "column": "created_at",
                "operator": "eq",
                "value": 2025,
            },
        }
        self.assertIn("params.value_type_mismatch", _codes(_wrap([step])))

    # ----- SQL-3.2 (M3) — aggregate sum/avg non-numeric reject -----

    def test_aggregate_sum_on_string_column_rejected(self) -> None:
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

    def test_aggregate_max_on_timestamp_column_allowed(self) -> None:
        """min/max는 timestamp도 의미 있음."""
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "docs",
                "group_by": ["doc_id"],
                "metrics": [{"name": "last", "function": "max", "column": "created_at"}],
            },
        }
        codes = _codes(_wrap([step]))
        self.assertNotIn("params.metric_column_not_numeric", codes)
        self.assertNotIn("params.metric_column_not_orderable", codes)

    def test_aggregate_count_on_string_column_allowed(self) -> None:
        step = {
            "id": "agg",
            "skill": "aggregate",
            "params": {
                "input": "clauses",
                "group_by": ["aspect"],
                "metrics": [{"name": "n", "function": "count", "column": "sentiment"}],
            },
        }
        codes = _codes(_wrap([step]))
        self.assertNotIn("params.metric_column_not_numeric", codes)

    # ----- SQL-3.3 (M4) — join key step output 검증 -----

    def test_join_key_missing_in_step_output_rejected(self) -> None:
        """sort step이 input pass-through인 점을 이용한 케이스. agg는 group_by +
        metric만 출력 → join key가 'doc_id'면 inferred에 없으므로 reject."""
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

    # ----- SQL-3.4 (M6) — sort.by step output 검증 -----

    def test_sort_by_unknown_column_in_step_output_rejected(self) -> None:
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

    # ----- SQL-4 (M7) — present.limit 한도 검증 -----

    def test_present_without_limit_passes(self) -> None:
        step = {
            "id": "out",
            "skill": "present",
            "params": {"input": "clauses", "format": "table"},
        }
        codes = _codes(_wrap([step]))
        self.assertNotIn("params.limit_invalid", codes)
        self.assertNotIn("params.limit_cap_exceeded", codes)

    def test_present_limit_within_range_passes(self) -> None:
        step = {
            "id": "out",
            "skill": "present",
            "params": {"input": "clauses", "format": "table", "limit": 500},
        }
        codes = _codes(_wrap([step]))
        self.assertNotIn("params.limit_invalid", codes)
        self.assertNotIn("params.limit_cap_exceeded", codes)

    def test_present_limit_zero_rejected(self) -> None:
        step = {
            "id": "out",
            "skill": "present",
            "params": {"input": "clauses", "format": "table", "limit": 0},
        }
        self.assertIn("params.limit_invalid", _codes(_wrap([step])))

    def test_present_limit_negative_rejected(self) -> None:
        step = {
            "id": "out",
            "skill": "present",
            "params": {"input": "clauses", "format": "table", "limit": -5},
        }
        self.assertIn("params.limit_invalid", _codes(_wrap([step])))

    def test_present_limit_non_integer_rejected(self) -> None:
        step = {
            "id": "out",
            "skill": "present",
            "params": {"input": "clauses", "format": "table", "limit": "100"},
        }
        self.assertIn("params.limit_invalid", _codes(_wrap([step])))

    def test_present_limit_over_hard_cap_rejected(self) -> None:
        step = {
            "id": "out",
            "skill": "present",
            "params": {"input": "clauses", "format": "table", "limit": 50000},
        }
        self.assertIn("params.limit_cap_exceeded", _codes(_wrap([step])))


if __name__ == "__main__":
    unittest.main()


# silverone 2026-06-02 — present.columns hard constraint validation.
def _agg_present_plan(present_columns: Any) -> dict[str, Any]:
    agg = {
        "id": "by_sentiment",
        "skill": "aggregate",
        "params": {
            "input": "clauses",
            "group_by": ["sentiment"],
            "metrics": [{"name": "count", "function": "count", "column": "clause_id"}],
        },
    }
    present_params: dict[str, Any] = {"input": "by_sentiment", "format": "table"}
    if present_columns is not _OMIT:
        present_params["columns"] = present_columns
    present = {"id": "out", "skill": "present", "params": present_params}
    return _wrap([agg, present])


_OMIT = object()


class PresentColumnsValidatorTests(unittest.TestCase):
    def test_columns_in_input_output_valid(self) -> None:
        # aggregate 출력 = {sentiment, count} → 둘 다 존재 → issue 없음.
        self.assertEqual(collect_plan_issues(_agg_present_plan(["sentiment", "count"])), [])

    def test_columns_missing_from_input_rejected(self) -> None:
        # ratio는 aggregate 출력에 없음 → repair 대상.
        self.assertIn("params.columns_unknown", _codes(_agg_present_plan(["sentiment", "ratio"])))

    def test_columns_not_list_rejected(self) -> None:
        self.assertIn("params.columns_not_list", _codes(_agg_present_plan("sentiment")))

    def test_columns_empty_string_entry_rejected(self) -> None:
        self.assertIn("params.columns_invalid", _codes(_agg_present_plan(["sentiment", ""])))

    def test_columns_omitted_is_valid(self) -> None:
        self.assertEqual(collect_plan_issues(_agg_present_plan(_OMIT)), [])

    def test_columns_null_is_valid(self) -> None:
        self.assertEqual(collect_plan_issues(_agg_present_plan(None)), [])
