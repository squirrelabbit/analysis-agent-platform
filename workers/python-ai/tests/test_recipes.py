"""Composite recipe lowering/runtime tests.

검증: (1) lowering 결과가 기대 atomic steps와 동일, (2) lowered plan이 기존
validator를 통과, (3) 같은 params → 항상 같은 lowered plan(결정성), (4) runtime
활성/미활성 recipe 처리.
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner.recipes import (
    DISTRIBUTION_SPEC,
    EVENT_WINDOW_COUNT_SPEC,
    RECIPE_SPECS,
    RecipeError,
    RUNTIME_ENABLED_RECIPES,
    TOP_N_SPEC,
    expand_recipes,
    lower_distribution,
    lower_event_window_count,
    lower_period_compare_count,
    lower_period_compare_distribution,
    lower_recipe,
    lower_top_n,
)
from python_ai_worker.planner.validator import collect_plan_issues


def _distribution_step(**param_overrides):
    params = {
        "input": "clauses",
        "group_by": ["sentiment"],
        "metric": "count",
        "include_share": True,
        "count_column": "count",
        "share_column": "ratio",
        "title": "감성별 반응 비율",
    }
    params.update(param_overrides)
    return {"id": "sentiment_distribution", "skill": "distribution", "params": params}


class DistributionLoweringTests(unittest.TestCase):
    def test_lowers_to_expected_atomic_steps(self) -> None:
        steps = lower_recipe(_distribution_step())
        self.assertEqual(steps, [
            {
                "id": "sentiment_distribution_agg",
                "skill": "aggregate",
                "params": {
                    "input": "clauses",
                    "group_by": ["sentiment"],
                    "metrics": [{"name": "count", "function": "count", "column": "*"}],
                },
            },
            {
                "id": "sentiment_distribution_share",
                "skill": "calculate",
                "params": {
                    "input": "sentiment_distribution_agg",
                    "expressions": [
                        {"name": "ratio", "operation": "share_of_total", "value": "count"}
                    ],
                },
            },
            {
                "id": "sentiment_distribution_present",
                "skill": "present",
                "params": {
                    "input": "sentiment_distribution_share",
                    "format": "table",
                    "columns": ["sentiment", "count", "ratio"],
                    "title": "감성별 반응 비율",
                },
            },
        ])

    def test_lowered_plan_passes_validator(self) -> None:
        steps = lower_recipe(_distribution_step())
        plan = {"plan_version": "v2", "steps": steps}
        self.assertEqual(collect_plan_issues(plan), [])

    def test_deterministic_same_params_same_plan(self) -> None:
        a = lower_recipe(_distribution_step())
        b = lower_recipe(_distribution_step())
        self.assertEqual(a, b)

    def test_include_share_false_drops_calculate(self) -> None:
        steps = lower_recipe(_distribution_step(include_share=False))
        self.assertEqual([s["skill"] for s in steps], ["aggregate", "present"])
        present = steps[-1]
        self.assertEqual(present["params"]["input"], "sentiment_distribution_agg")
        self.assertEqual(present["params"]["columns"], ["sentiment", "count"])
        # share 없는 plan도 validator 통과
        self.assertEqual(collect_plan_issues({"plan_version": "v2", "steps": steps}), [])

    def test_custom_column_names(self) -> None:
        steps = lower_recipe(_distribution_step(count_column="n", share_column="share"))
        self.assertEqual(steps[0]["params"]["metrics"][0]["name"], "n")
        self.assertEqual(steps[1]["params"]["expressions"][0], {
            "name": "share", "operation": "share_of_total", "value": "n",
        })
        self.assertEqual(steps[2]["params"]["columns"], ["sentiment", "n", "share"])

    def test_title_optional(self) -> None:
        steps = lower_distribution(_distribution_step(title=None))
        self.assertNotIn("title", steps[-1]["params"])

    def test_multi_group_by(self) -> None:
        steps = lower_recipe(_distribution_step(group_by=["sentiment", "aspect"]))
        self.assertEqual(steps[0]["params"]["group_by"], ["sentiment", "aspect"])
        self.assertEqual(steps[-1]["params"]["columns"], ["sentiment", "aspect", "count", "ratio"])


class DistributionErrorTests(unittest.TestCase):
    def test_missing_input(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_distribution_step(input=""))

    def test_missing_group_by(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_distribution_step(group_by=[]))

    def test_group_by_not_list(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_distribution_step(group_by="sentiment"))

    def test_unsupported_metric(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_distribution_step(metric="sum"))


def _event_window_step(**param_overrides):
    params = {
        "input": "docs",
        "date_column": "created_at",
        "event_date": "2024-08-15",
        "before_days": 7,
        "after_days": 7,
        "grain": "day",
        "count_column": "count",
        "title": "축제일 기준 전후 일주일 문서 발생량",
    }
    params.update(param_overrides)
    return {"id": "festival_window_count", "skill": "event_window_count", "params": params}


class EventWindowCountLoweringTests(unittest.TestCase):
    def test_lowers_to_expected_atomic_steps(self) -> None:
        steps = lower_recipe(_event_window_step())
        self.assertEqual(steps, [
            {
                "id": "festival_window_count_window",
                "skill": "filter",
                "params": {
                    "input": "docs",
                    "column": "created_at",
                    "operator": "between",
                    "value": ["2024-08-08", "2024-08-22"],
                },
            },
            {
                "id": "festival_window_count_by_date",
                "skill": "aggregate",
                "params": {
                    "input": "festival_window_count_window",
                    "group_by": ["created_at"],
                    "metrics": [{"name": "count", "function": "count", "column": "*"}],
                },
            },
            {
                "id": "festival_window_count_sorted",
                "skill": "sort",
                "params": {"input": "festival_window_count_by_date", "by": ["created_at"], "order": "asc"},
            },
            {
                "id": "festival_window_count_present",
                "skill": "present",
                "params": {
                    "input": "festival_window_count_sorted",
                    "format": "table",
                    "columns": ["created_at", "count"],
                    "title": "축제일 기준 전후 일주일 문서 발생량",
                },
            },
        ])

    def test_inclusive_boundary_15_days(self) -> None:
        # before=after=7 → [event-7, event+7] inclusive = 08-08 .. 08-22 (기준일 포함 15일)
        steps = lower_recipe(_event_window_step())
        self.assertEqual(steps[0]["params"]["value"], ["2024-08-08", "2024-08-22"])

    def test_asymmetric_window(self) -> None:
        steps = lower_recipe(_event_window_step(before_days=3, after_days=1))
        self.assertEqual(steps[0]["params"]["value"], ["2024-08-12", "2024-08-16"])

    def test_lowered_plan_passes_validator(self) -> None:
        steps = lower_recipe(_event_window_step())
        self.assertEqual(collect_plan_issues({"plan_version": "v2", "steps": steps}), [])

    def test_deterministic(self) -> None:
        self.assertEqual(lower_recipe(_event_window_step()), lower_recipe(_event_window_step()))

    def test_defaults_before_after_7(self) -> None:
        step = _event_window_step()
        del step["params"]["before_days"]
        del step["params"]["after_days"]
        steps = lower_recipe(step)
        self.assertEqual(steps[0]["params"]["value"], ["2024-08-08", "2024-08-22"])

    def test_title_optional(self) -> None:
        steps = lower_event_window_count(_event_window_step(title=None))
        self.assertNotIn("title", steps[-1]["params"])

    def test_invalid_event_date(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_event_window_step(event_date="2024-13-40"))

    def test_missing_event_date(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_event_window_step(event_date=""))

    def test_negative_days_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_event_window_step(before_days=-1))

    def test_non_int_days_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_event_window_step(after_days="7"))

    def test_grain_week_not_supported_in_r0(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_event_window_step(grain="week"))


def _top_n_step(**param_overrides):
    params = {
        "input": "clauses",
        "group_by": ["aspect"],
        "metric": "count",
        "filters": [{"column": "sentiment", "op": "=", "value": "negative"}],
        "sort": {"column": "count", "direction": "desc"},
        "limit": 10,
        "title": "부정 후기가 많은 aspect",
    }
    params.update(param_overrides)
    return {"id": "negative_aspect_top_n", "skill": "top_n", "params": params}


class TopNLoweringTests(unittest.TestCase):
    def test_lowers_to_expected_atomic_steps(self) -> None:
        steps = lower_recipe(_top_n_step())
        self.assertEqual(steps, [
            {
                "id": "negative_aspect_top_n_filter1",
                "skill": "filter",
                "params": {"input": "clauses", "column": "sentiment", "operator": "eq", "value": "negative"},
            },
            {
                "id": "negative_aspect_top_n_agg",
                "skill": "aggregate",
                "params": {
                    "input": "negative_aspect_top_n_filter1",
                    "group_by": ["aspect"],
                    "metrics": [{"name": "count", "function": "count", "column": "*"}],
                },
            },
            {
                "id": "negative_aspect_top_n_sorted",
                "skill": "sort",
                "params": {"input": "negative_aspect_top_n_agg", "by": ["count"], "order": "desc", "limit": 10},
            },
            {
                "id": "negative_aspect_top_n_present",
                "skill": "present",
                "params": {
                    "input": "negative_aspect_top_n_sorted",
                    "format": "table",
                    "columns": ["aspect", "count"],
                    "title": "부정 후기가 많은 aspect",
                },
            },
        ])

    def test_no_filters_lowers_agg_sort_present(self) -> None:
        steps = lower_recipe(_top_n_step(filters=[]))
        self.assertEqual([s["skill"] for s in steps], ["aggregate", "sort", "present"])
        self.assertEqual(steps[0]["params"]["input"], "clauses")  # aggregate가 input 직접

    def test_limit_on_sort_step(self) -> None:
        steps = lower_recipe(_top_n_step(limit=5))
        sort_step = next(s for s in steps if s["skill"] == "sort")
        self.assertEqual(sort_step["params"]["limit"], 5)

    def test_limit_default_10(self) -> None:
        step = _top_n_step()
        del step["params"]["limit"]
        steps = lower_recipe(step)
        sort_step = next(s for s in steps if s["skill"] == "sort")
        self.assertEqual(sort_step["params"]["limit"], 10)

    def test_sort_defaults_to_count_desc(self) -> None:
        step = _top_n_step()
        del step["params"]["sort"]
        steps = lower_recipe(step)
        sort_step = next(s for s in steps if s["skill"] == "sort")
        self.assertEqual(sort_step["params"]["by"], ["count"])
        self.assertEqual(sort_step["params"]["order"], "desc")

    def test_deterministic(self) -> None:
        self.assertEqual(lower_recipe(_top_n_step()), lower_recipe(_top_n_step()))

    def test_lowered_plan_passes_validator(self) -> None:
        for variant in (_top_n_step(), _top_n_step(filters=[])):
            with self.subTest():
                self.assertEqual(collect_plan_issues({"plan_version": "v2", "steps": lower_recipe(variant)}), [])

    def test_multiple_filters_chain(self) -> None:
        steps = lower_recipe(_top_n_step(filters=[
            {"column": "sentiment", "op": "=", "value": "negative"},
            {"column": "aspect", "op": "!=", "value": "etc"},
        ]))
        filters = [s for s in steps if s["skill"] == "filter"]
        self.assertEqual(len(filters), 2)
        self.assertEqual(filters[1]["params"]["input"], filters[0]["id"])
        self.assertEqual(filters[1]["params"]["operator"], "neq")

    def test_limit_zero_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_top_n_step(limit=0))

    def test_limit_negative_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_top_n_step(limit=-3))

    def test_unsupported_metric_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_top_n_step(metric="sum"))

    def test_missing_group_by_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_top_n_step(group_by=[]))

    def test_unsupported_filter_op_error(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe(_top_n_step(filters=[{"column": "sentiment", "op": "~=", "value": "x"}]))

    def test_title_optional(self) -> None:
        steps = lower_top_n(_top_n_step(title=None))
        self.assertNotIn("title", steps[-1]["params"])


class ExpandRecipesTests(unittest.TestCase):
    """expand_recipes: runtime 활성 recipe 치환, atomic no-op."""

    def test_distribution_recipe_expanded_and_valid(self) -> None:
        plan = {"plan_version": "v2", "steps": [_distribution_step()]}
        out = expand_recipes(plan)
        self.assertEqual([s["skill"] for s in out["steps"]], ["aggregate", "calculate", "present"])
        self.assertEqual(collect_plan_issues(out), [])
        # 원본 plan 비파괴
        self.assertEqual(plan["steps"][0]["skill"], "distribution")

    def test_atomic_only_plan_is_noop(self) -> None:
        plan = {
            "plan_version": "v2",
            "steps": [
                {"id": "f", "skill": "filter", "params": {"input": "clauses", "column": "sentiment", "operator": "eq", "value": "positive"}},
                {"id": "out", "skill": "present", "params": {"input": "f", "format": "table"}},
            ],
        }
        out = expand_recipes(plan)
        self.assertIs(out, plan)  # recipe 없음 → 동일 객체(완전 no-op)

    def test_mixed_recipe_and_atomic_preserves_order(self) -> None:
        plan = {"plan_version": "v2", "steps": [
            _distribution_step(),
            {"id": "tail", "skill": "summarize", "params": {"input": "x", "focus": "y"}},
        ]}
        out = expand_recipes(plan)
        self.assertEqual([s["skill"] for s in out["steps"]], ["aggregate", "calculate", "present", "summarize"])

    def test_top_n_recipe_expanded_and_valid(self) -> None:
        plan = {"plan_version": "v2", "steps": [_top_n_step()]}
        out = expand_recipes(plan)
        self.assertEqual([s["skill"] for s in out["steps"]], ["filter", "aggregate", "sort", "present"])
        self.assertEqual(collect_plan_issues(out), [])
        self.assertEqual(plan["steps"][0]["skill"], "top_n")

    def test_event_window_count_recipe_expanded_and_valid(self) -> None:
        plan = {"plan_version": "v2", "steps": [_event_window_step()]}
        out = expand_recipes(plan)
        self.assertEqual([s["skill"] for s in out["steps"]], ["filter", "aggregate", "sort", "present"])
        self.assertEqual(collect_plan_issues(out), [])
        self.assertEqual(plan["steps"][0]["skill"], "event_window_count")

    def test_bad_distribution_params_raises(self) -> None:
        plan = {"plan_version": "v2", "steps": [_distribution_step(group_by=[])]}
        with self.assertRaises(RecipeError):
            expand_recipes(plan)

    def test_bad_top_n_params_raises(self) -> None:
        plan = {"plan_version": "v2", "steps": [_top_n_step(limit=0)]}
        with self.assertRaises(RecipeError):
            expand_recipes(plan)

    def test_bad_event_window_count_params_raises(self) -> None:
        plan = {"plan_version": "v2", "steps": [_event_window_step(event_date="2026-99-99")]}
        with self.assertRaises(RecipeError):
            expand_recipes(plan)


def _pcc_step(**param_overrides):
    params = {
        "input": "docs",
        "period_a": {"start": "2024-08-01", "end": "2024-08-14"},
        "period_b": {"start": "2024-08-15", "end": "2024-08-28"},
    }
    params.update(param_overrides)
    return {"id": "pcc", "skill": "period_compare_count", "params": params}


def _wrap_one(step):
    return {"plan_version": "v2", "answerable": True, "steps": [step]}


def _codes(plan):
    return [i.code for i in collect_plan_issues(plan)]


class PeriodCompareCountLoweringTests(unittest.TestCase):
    def test_total_mode_lowers_to_expected_atomic_steps(self) -> None:
        steps = lower_recipe(_pcc_step())
        self.assertEqual(
            [s["skill"] for s in steps],
            ["filter", "aggregate", "filter", "aggregate", "compare", "calculate", "present"],
        )
        # 두 aggregate는 group_by=[] (total)
        aggs = [s for s in steps if s["skill"] == "aggregate"]
        self.assertEqual(aggs[0]["params"]["group_by"], [])
        self.assertEqual(aggs[1]["params"]["group_by"], [])
        cmp = next(s for s in steps if s["skill"] == "compare")
        self.assertEqual(cmp["params"]["join_key"], [])
        self.assertEqual(cmp["params"]["left_label"], "a")
        self.assertEqual(cmp["params"]["right_label"], "b")
        calc = next(s for s in steps if s["skill"] == "calculate")
        names = {e["name"] for e in calc["params"]["expressions"]}
        self.assertEqual(names, {"delta_count", "delta_rate"})
        present = next(s for s in steps if s["skill"] == "present")
        self.assertEqual(
            present["params"]["columns"], ["a_count", "b_count", "delta_count", "delta_rate"]
        )

    def test_group_mode_uses_group_by_as_join_key(self) -> None:
        steps = lower_recipe(_pcc_step(group_by=["channel"]))
        aggs = [s for s in steps if s["skill"] == "aggregate"]
        self.assertEqual(aggs[0]["params"]["group_by"], ["channel"])
        cmp = next(s for s in steps if s["skill"] == "compare")
        self.assertEqual(cmp["params"]["join_key"], ["channel"])
        present = next(s for s in steps if s["skill"] == "present")
        self.assertEqual(
            present["params"]["columns"],
            ["channel", "a_count", "b_count", "delta_count", "delta_rate"],
        )

    def test_filter_windows_use_between_bounds(self) -> None:
        steps = lower_recipe(_pcc_step())
        filters = [s for s in steps if s["skill"] == "filter"]
        self.assertEqual(filters[0]["params"]["operator"], "between")
        self.assertEqual(filters[0]["params"]["value"], ["2024-08-01", "2024-08-14"])
        self.assertEqual(filters[1]["params"]["value"], ["2024-08-15", "2024-08-28"])
        self.assertEqual(filters[0]["params"]["column"], "created_at")

    def test_deterministic(self) -> None:
        self.assertEqual(lower_period_compare_count(_pcc_step()), lower_period_compare_count(_pcc_step()))

    def test_lowered_total_plan_passes_validator(self) -> None:
        steps = lower_recipe(_pcc_step())
        self.assertEqual(collect_plan_issues({"plan_version": "v2", "steps": steps}), [])

    def test_lowered_group_plan_passes_validator(self) -> None:
        steps = lower_recipe(_pcc_step(group_by=["channel"]))
        self.assertEqual(collect_plan_issues({"plan_version": "v2", "steps": steps}), [])

    def test_bad_date_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_period_compare_count(_pcc_step(period_a={"start": "2024/08/01", "end": "2024-08-14"}))

    def test_start_after_end_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_period_compare_count(_pcc_step(period_b={"start": "2024-08-28", "end": "2024-08-15"}))

    def test_metric_other_than_count_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_period_compare_count(_pcc_step(metric="sum"))


class PeriodCompareCountValidatorTests(unittest.TestCase):
    def test_valid_total_passes(self) -> None:
        self.assertEqual(collect_plan_issues(_wrap_one(_pcc_step())), [])

    def test_valid_group_passes(self) -> None:
        self.assertEqual(collect_plan_issues(_wrap_one(_pcc_step(group_by=["channel"]))), [])

    def test_missing_period_b(self) -> None:
        step = _pcc_step()
        del step["params"]["period_b"]
        self.assertIn("params.missing_keys", _codes(_wrap_one(step)))

    def test_bad_period_shape(self) -> None:
        self.assertIn(
            "params.recipe_period_invalid",
            _codes(_wrap_one(_pcc_step(period_a={"start": "2024-08-01"}))),
        )

    def test_bad_period_date_format(self) -> None:
        self.assertIn(
            "params.recipe_period_invalid",
            _codes(_wrap_one(_pcc_step(period_a={"start": "08-01-2024", "end": "2024-08-14"}))),
        )

    def test_group_by_invalid_type(self) -> None:
        self.assertIn(
            "params.recipe_group_by_invalid",
            _codes(_wrap_one(_pcc_step(group_by="channel"))),
        )

    def test_unsupported_metric(self) -> None:
        self.assertIn(
            "params.recipe_metric_unsupported",
            _codes(_wrap_one(_pcc_step(metric="sum"))),
        )

    def test_unknown_input(self) -> None:
        self.assertIn("params.input_unknown", _codes(_wrap_one(_pcc_step(input="nope"))))


def _pcd_step(**param_overrides):
    params = {
        "input": "docs",
        "period_a": {"start": "2024-08-01", "end": "2024-08-14"},
        "period_b": {"start": "2024-08-15", "end": "2024-08-28"},
        "group_by": ["sentiment"],
    }
    params.update(param_overrides)
    return {"id": "pcd", "skill": "period_compare_distribution", "params": params}


class PeriodCompareDistributionLoweringTests(unittest.TestCase):
    def test_lowers_to_expected_atomic_steps(self) -> None:
        steps = lower_recipe(_pcd_step())
        self.assertEqual(
            [s["skill"] for s in steps],
            ["filter", "aggregate", "calculate", "filter", "aggregate", "calculate", "compare", "calculate", "present"],
        )

    def test_each_period_computes_share_of_total(self) -> None:
        steps = lower_recipe(_pcd_step())
        shares = [
            s for s in steps
            if s["skill"] == "calculate"
            and any(e.get("operation") == "share_of_total" for e in s["params"]["expressions"])
        ]
        self.assertEqual(len(shares), 2)
        for s in shares:
            expr = s["params"]["expressions"][0]
            self.assertEqual(expr["operation"], "share_of_total")
            self.assertEqual(expr["value"], "count")
            self.assertEqual(expr["name"], "ratio")

    def test_compare_joins_on_group_by_and_labels_ab(self) -> None:
        cmp = next(s for s in lower_recipe(_pcd_step()) if s["skill"] == "compare")
        self.assertEqual(cmp["params"]["join_key"], ["sentiment"])
        self.assertEqual(cmp["params"]["left_label"], "a")
        self.assertEqual(cmp["params"]["right_label"], "b")

    def test_delta_expressions_and_present_columns(self) -> None:
        steps = lower_recipe(_pcd_step())
        delta = steps[-2]
        self.assertEqual(delta["skill"], "calculate")
        names = {e["name"] for e in delta["params"]["expressions"]}
        self.assertEqual(names, {"delta_count", "delta_ratio"})
        present = steps[-1]
        self.assertEqual(
            present["params"]["columns"],
            ["sentiment", "a_count", "a_ratio", "b_count", "b_ratio", "delta_count", "delta_ratio"],
        )

    def test_aspect_group_by(self) -> None:
        steps = lower_recipe(_pcd_step(group_by=["aspect"]))
        cmp = next(s for s in steps if s["skill"] == "compare")
        self.assertEqual(cmp["params"]["join_key"], ["aspect"])
        self.assertEqual(steps[-1]["params"]["columns"][0], "aspect")

    def test_custom_count_ratio_column(self) -> None:
        steps = lower_recipe(_pcd_step(count_column="cnt", ratio_column="share"))
        present = steps[-1]
        self.assertEqual(
            present["params"]["columns"],
            ["sentiment", "a_cnt", "a_share", "b_cnt", "b_share", "delta_count", "delta_ratio"],
        )

    def test_deterministic(self) -> None:
        self.assertEqual(
            lower_period_compare_distribution(_pcd_step()),
            lower_period_compare_distribution(_pcd_step()),
        )

    def test_lowered_plan_passes_validator(self) -> None:
        # 전후 감성 구성비는 created_at(docs) + sentiment(clauses)가 함께 필요하므로
        # 먼저 join한 step을 recipe input으로 준다. validator schema-lineage는 join
        # 없이 docs에 sentiment를 group_by하면 column_not_in_input으로 잡는다.
        join_step = {
            "id": "joined",
            "skill": "join",
            "params": {
                "left": "clauses",
                "right": "docs",
                "on": ["doc_id"],
                "how": "inner",
            },
        }
        steps = lower_recipe(_pcd_step(input="joined"))
        plan = {"plan_version": "v2", "steps": [join_step, *steps]}
        self.assertEqual(collect_plan_issues(plan), [])

    def test_group_by_clause_column_on_docs_only_flagged(self) -> None:
        # silverone 2026-06-09 — join 없이 docs lineage에 clause-level group_by →
        # lineage가 column_not_in_input으로 잡아 planner self-correct를 유도.
        steps = lower_recipe(_pcd_step(input="docs", group_by=["sentiment"]))
        codes = [i.code for i in collect_plan_issues({"plan_version": "v2", "steps": steps})]
        self.assertIn("params.column_not_in_input", codes)

    def test_group_by_required_raises(self) -> None:
        step = _pcd_step()
        del step["params"]["group_by"]
        with self.assertRaises(RecipeError):
            lower_period_compare_distribution(step)

    def test_bad_date_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_period_compare_distribution(_pcd_step(period_a={"start": "2024/08/01", "end": "2024-08-14"}))

    def test_start_after_end_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_period_compare_distribution(_pcd_step(period_b={"start": "2024-08-28", "end": "2024-08-15"}))

    def test_metric_other_than_count_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_period_compare_distribution(_pcd_step(metric="sum"))


class PeriodCompareDistributionValidatorTests(unittest.TestCase):
    def test_valid_sentiment_passes(self) -> None:
        self.assertEqual(collect_plan_issues(_wrap_one(_pcd_step())), [])

    def test_valid_aspect_passes(self) -> None:
        self.assertEqual(collect_plan_issues(_wrap_one(_pcd_step(group_by=["aspect"]))), [])

    def test_group_by_required(self) -> None:
        step = _pcd_step()
        del step["params"]["group_by"]
        self.assertIn("params.missing_keys", _codes(_wrap_one(step)))

    def test_group_by_empty_invalid(self) -> None:
        self.assertIn("params.recipe_group_by_invalid", _codes(_wrap_one(_pcd_step(group_by=[]))))

    def test_missing_period_b(self) -> None:
        step = _pcd_step()
        del step["params"]["period_b"]
        self.assertIn("params.missing_keys", _codes(_wrap_one(step)))

    def test_bad_period_date_format(self) -> None:
        self.assertIn(
            "params.recipe_period_invalid",
            _codes(_wrap_one(_pcd_step(period_a={"start": "08-01-2024", "end": "2024-08-14"}))),
        )

    def test_unsupported_metric(self) -> None:
        self.assertIn(
            "params.recipe_metric_unsupported",
            _codes(_wrap_one(_pcd_step(metric="sum"))),
        )

    def test_unknown_input(self) -> None:
        self.assertIn("params.input_unknown", _codes(_wrap_one(_pcd_step(input="nope"))))


class RecipeRegistryTests(unittest.TestCase):
    def test_specs_present(self) -> None:
        self.assertEqual(
            set(RECIPE_SPECS),
            {
                "distribution",
                "event_window_count",
                "top_n",
                "sample_rows",
                "period_compare_count",
                "period_compare_distribution",
            },
        )

    def test_all_recipes_implemented(self) -> None:
        self.assertTrue(DISTRIBUTION_SPEC.implemented)
        self.assertTrue(EVENT_WINDOW_COUNT_SPEC.implemented)
        self.assertTrue(TOP_N_SPEC.implemented)

    def test_all_recipes_runtime_enabled(self) -> None:
        self.assertEqual(RUNTIME_ENABLED_RECIPES, frozenset(RECIPE_SPECS))

    def test_unknown_recipe_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe({"id": "x", "skill": "nope", "params": {}})


if __name__ == "__main__":
    unittest.main()
