"""Composite recipe R0 — distribution lowering 잠금 (silverone 2026-06-04).

검증: (1) lowering 결과가 기대 atomic steps와 동일, (2) lowered plan이 기존
validator를 통과, (3) 같은 params → 항상 같은 lowered plan(결정성), (4) edge/
미구현 recipe 처리. recipe는 아직 planner/executor에 연결 안 됨(runtime 변화 0).
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner.recipes import (
    DISTRIBUTION_SPEC,
    EVENT_WINDOW_COUNT_SPEC,
    RECIPE_SPECS,
    RecipeError,
    TOP_N_SPEC,
    lower_distribution,
    lower_event_window_count,
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


class RecipeRegistryTests(unittest.TestCase):
    def test_specs_present(self) -> None:
        self.assertEqual(set(RECIPE_SPECS), {"distribution", "event_window_count", "top_n"})

    def test_all_r0_recipes_implemented(self) -> None:
        self.assertTrue(DISTRIBUTION_SPEC.implemented)
        self.assertTrue(EVENT_WINDOW_COUNT_SPEC.implemented)
        self.assertTrue(TOP_N_SPEC.implemented)

    def test_unknown_recipe_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe({"id": "x", "skill": "nope", "params": {}})


if __name__ == "__main__":
    unittest.main()
