"""sample_rows recipe — lowering / validator / prompt routing lock.

silverone 2026-06-05. 집계 없이 원문 근거 row를 결정적으로 보여주는 가장 얇은 recipe.
filter*/sort(limit)/present 로 lower. random sampling 없음, 내부 join 없음.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from python_ai_worker.planner.recipes import (
    RECIPE_SPECS,
    RUNTIME_ENABLED_RECIPES,
    SAMPLE_ROWS_MAX_LIMIT,
    SAMPLE_ROWS_SPEC,
    RecipeError,
    expand_recipes,
    lower_recipe,
    lower_sample_rows,
)
from python_ai_worker.planner.validator import collect_plan_issues


def _sample_step(**overrides):
    params = {
        "input": "clauses",
        "columns": ["clause", "aspect"],
        "filters": [{"column": "sentiment", "op": "=", "value": "negative"}],
        "limit": 10,
    }
    params.update(overrides)
    return {"id": "neg_examples", "skill": "sample_rows", "params": params}


def _wrap(steps):
    return {"plan_version": "v2", "answerable": True, "steps": steps}


def _codes(plan):
    return [i.code for i in collect_plan_issues(plan)]


class SampleRowsLoweringTest(unittest.TestCase):
    def test_registered(self):
        self.assertIn("sample_rows", RECIPE_SPECS)
        self.assertIn("sample_rows", RUNTIME_ENABLED_RECIPES)
        self.assertEqual(SAMPLE_ROWS_SPEC.name, "sample_rows")

    def test_lowers_to_filter_sort_present(self):
        steps = lower_recipe(_sample_step())
        self.assertEqual(
            steps,
            [
                {
                    "id": "neg_examples_filter1",
                    "skill": "filter",
                    "params": {
                        "input": "clauses",
                        "column": "sentiment",
                        "operator": "eq",
                        "value": "negative",
                    },
                },
                {
                    "id": "neg_examples_sorted",
                    "skill": "sort",
                    "params": {
                        "input": "neg_examples_filter1",
                        "by": ["doc_id", "clause", "aspect"],
                        "order": "asc",
                        "limit": 10,
                    },
                },
                {
                    "id": "neg_examples_present",
                    "skill": "present",
                    "params": {
                        "input": "neg_examples_sorted",
                        "format": "table",
                        "columns": ["clause", "aspect"],
                    },
                },
            ],
        )

    def test_lowered_plan_passes_validator(self):
        steps = lower_recipe(_sample_step())
        self.assertEqual(collect_plan_issues(_wrap(steps)), [])

    def test_deterministic(self):
        self.assertEqual(lower_sample_rows(_sample_step()), lower_sample_rows(_sample_step()))

    def test_default_limit_10(self):
        steps = lower_sample_rows(_sample_step(limit=None))
        # limit 미지정(None) → 기본 10. (params에서 limit 키 제거 효과)
        s = _sample_step()
        del s["params"]["limit"]
        steps2 = lower_sample_rows(s)
        sort_step = next(x for x in steps2 if x["skill"] == "sort")
        self.assertEqual(sort_step["params"]["limit"], 10)

    def test_no_filters_drops_filter_step(self):
        s = _sample_step()
        del s["params"]["filters"]
        steps = lower_sample_rows(s)
        self.assertEqual([x["skill"] for x in steps], ["sort", "present"])

    def test_explicit_sort_respected(self):
        steps = lower_sample_rows(_sample_step(sort={"by": ["created_at"], "direction": "desc"}))
        sort_step = next(x for x in steps if x["skill"] == "sort")
        self.assertEqual(sort_step["params"]["by"], ["created_at"])
        self.assertEqual(sort_step["params"]["order"], "desc")

    def test_limit_cap_and_nonpositive(self):
        with self.assertRaises(RecipeError):
            lower_sample_rows(_sample_step(limit=SAMPLE_ROWS_MAX_LIMIT + 1))
        with self.assertRaises(RecipeError):
            lower_sample_rows(_sample_step(limit=0))
        with self.assertRaises(RecipeError):
            lower_sample_rows(_sample_step(limit=-1))

    def test_columns_required(self):
        s = _sample_step()
        del s["params"]["columns"]
        with self.assertRaises(RecipeError):
            lower_sample_rows(s)
        with self.assertRaises(RecipeError):
            lower_sample_rows(_sample_step(columns=[]))

    def test_korean_column_quoted_in_lowered_sql(self):
        # silverone 2026-06-05 — 한글 컬럼(제목)이 projection/filter/sort에서 double-quote로
        # 안전하게 렌더돼야 한다("unsafe SQL identifier" 회귀 방지). build_sql은 filter에서만
        # context를 쓰고 실패 시 fallback하므로 context=None으로 호출 가능.
        from python_ai_worker.executor.skills import filter as filter_skill
        from python_ai_worker.executor.skills import present as present_skill
        from python_ai_worker.executor.skills import sort as sort_skill

        step = _sample_step(
            columns=["제목", "aspect"],
            filters=[{"column": "제목", "op": "contains", "value": "야행"}],
            sort={"by": ["제목"], "direction": "asc"},
        )
        steps = lower_recipe(step)
        fsql, _ = filter_skill.build_sql(next(s for s in steps if s["skill"] == "filter")["params"], None)
        ssql, _ = sort_skill.build_sql(next(s for s in steps if s["skill"] == "sort")["params"], None)
        psql, _ = present_skill.build_sql(next(s for s in steps if s["skill"] == "present")["params"], None)
        self.assertIn('"제목"', fsql)
        self.assertIn('"제목"', ssql)
        self.assertIn('"제목"', psql)


class SampleRowsValidatorTest(unittest.TestCase):
    def test_valid_passes(self):
        self.assertEqual(collect_plan_issues(_wrap([_sample_step()])), [])

    def test_minimal_passes(self):
        plan = _wrap([{"id": "s", "skill": "sample_rows", "params": {"input": "clauses", "columns": ["clause"]}}])
        self.assertEqual(collect_plan_issues(plan), [])

    def test_missing_columns(self):
        plan = _wrap([{"id": "s", "skill": "sample_rows", "params": {"input": "clauses"}}])
        self.assertIn("params.missing_keys", _codes(plan))

    def test_empty_columns(self):
        self.assertIn("params.recipe_columns_invalid", _codes(_wrap([_sample_step(columns=[])])))

    def test_limit_over_cap(self):
        self.assertIn("params.recipe_limit_invalid", _codes(_wrap([_sample_step(limit=SAMPLE_ROWS_MAX_LIMIT + 1)])))

    def test_bad_filter_op(self):
        bad = _sample_step(filters=[{"column": "sentiment", "op": "like", "value": "x"}])
        self.assertIn("params.recipe_filter_invalid", _codes(_wrap([bad])))

    def test_bad_sort_direction(self):
        bad = _sample_step(sort={"by": ["doc_id"], "direction": "sideways"})
        self.assertIn("params.recipe_sort_invalid", _codes(_wrap([bad])))

    def test_unknown_input(self):
        self.assertIn("params.input_unknown", _codes(_wrap([_sample_step(input="nope")])))


class SampleRowsPromptRoutingLockTest(unittest.TestCase):
    def test_sample_rows_section_rendered_in_catalog(self):
        # C 이후: recipe 상세는 prompt md가 아니라 render_recipe_catalog()에서 렌더.
        from python_ai_worker.planner.prompt import render_recipe_catalog

        cat = render_recipe_catalog()
        self.assertIn("### sample_rows", cat)

    def test_prompt_template_keeps_sample_rows_routing_policy(self):
        # silverone 2026-06-09 — 라우팅 정책은 prompt md(규칙)에 하드코딩하지 않고
        # RecipeSpec.use_when/avoid_when/examples → render_recipe_catalog로 렌더한다.
        # manual few-shot 제거 후, 정책이 spec→catalog 경로로 살아있는지 잠근다.
        from python_ai_worker.planner.prompt import render_recipe_catalog

        catalog = render_recipe_catalog()
        self.assertIn("### sample_rows", catalog)
        self.assertIn("절대 쓰지 않는다", catalog)
        self.assertIn("- 예시 질문:", catalog)


if __name__ == "__main__":
    unittest.main()
