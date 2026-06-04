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
    lower_recipe,
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


class RecipeRegistryTests(unittest.TestCase):
    def test_specs_present(self) -> None:
        self.assertEqual(set(RECIPE_SPECS), {"distribution", "event_window_count", "top_n"})

    def test_only_distribution_implemented_in_r0(self) -> None:
        self.assertTrue(DISTRIBUTION_SPEC.implemented)
        self.assertFalse(EVENT_WINDOW_COUNT_SPEC.implemented)
        self.assertFalse(TOP_N_SPEC.implemented)

    def test_unimplemented_recipe_lowering_raises(self) -> None:
        for skill in ("event_window_count", "top_n"):
            with self.subTest(skill=skill):
                with self.assertRaises(RecipeError):
                    lower_recipe({"id": "x", "skill": skill, "params": {}})

    def test_unknown_recipe_raises(self) -> None:
        with self.assertRaises(RecipeError):
            lower_recipe({"id": "x", "skill": "nope", "params": {}})


if __name__ == "__main__":
    unittest.main()
