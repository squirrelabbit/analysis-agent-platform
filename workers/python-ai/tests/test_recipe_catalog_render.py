"""recipe catalog 자동 렌더 + drift 잠금 (silverone 2026-06-05, C).

recipe 상세(params/use_when/avoid_when)의 single source는 recipes.py. prompt md는
{{recipe_catalog}} placeholder만 두고 prompt.py render_recipe_catalog()가 렌더한다.
recipe 추가 시 prompt md를 손대지 않아도 되게 + drift를 막는다.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from python_ai_worker.planner.recipes import RECIPE_SPECS, RUNTIME_ENABLED_RECIPES
from python_ai_worker.planner.prompt import render_recipe_catalog
from python_ai_worker.planner.validator import collect_plan_issues

_TEMPLATE = (
    Path(__file__).resolve().parents[3] / "config/prompts/planner-v2-anthropic-v1.md"
)


def _wrap(step):
    return {"plan_version": "v2", "answerable": True, "steps": [step]}


class RecipeCatalogRenderTest(unittest.TestCase):
    def test_template_has_placeholder_not_hardcoded_catalog(self):
        tpl = _TEMPLATE.read_text(encoding="utf-8")
        self.assertIn("{{recipe_catalog}}", tpl)
        # recipe 상세 섹션(### <name>)을 prompt md에 하드코딩하지 않는다.
        for name in RECIPE_SPECS:
            self.assertNotIn(f"### {name}", tpl, f"하드코딩된 recipe 섹션 발견: ### {name}")

    def test_all_runtime_enabled_recipes_exposed(self):
        cat = render_recipe_catalog()
        for name in RUNTIME_ENABLED_RECIPES:
            self.assertIn(f"### {name}", cat, f"enabled recipe 미노출: {name}")
        # enabled == 노출 개수
        exposed = cat.count("### ")
        self.assertEqual(exposed, len(RUNTIME_ENABLED_RECIPES))

    def test_disabled_recipe_not_exposed(self):
        cat = render_recipe_catalog()
        for name in RECIPE_SPECS:
            if name not in RUNTIME_ENABLED_RECIPES:
                self.assertNotIn(f"### {name}", cat)

    def test_catalog_renders_params_and_use_avoid_from_spec(self):
        cat = render_recipe_catalog()
        self.assertIn("- 쓰는 경우:", cat)
        self.assertIn("- 쓰지 않는 경우:", cat)
        self.assertIn("- params:", cat)
        # spec의 use_when/avoid_when 문자열이 그대로 렌더됨(= recipes.py가 source).
        for name in RUNTIME_ENABLED_RECIPES:
            spec = RECIPE_SPECS[name]
            if spec.use_when:
                self.assertIn(spec.use_when, cat)
            if spec.avoid_when:
                self.assertIn(spec.avoid_when, cat)
            # 첫 param 이름이 렌더됨
            if spec.params:
                self.assertIn(f"`{spec.params[0].name}`", cat)

    def test_minimal_plans_pass_validator(self):
        minimal = {
            "distribution": {"input": "clauses", "group_by": ["sentiment"]},
            "event_window_count": {"input": "docs", "event_date": "2024-08-15"},
            "top_n": {"input": "clauses", "group_by": ["aspect"]},
            "sample_rows": {"input": "clauses", "columns": ["clause"]},
        }
        for name in RUNTIME_ENABLED_RECIPES:
            self.assertIn(name, minimal, f"minimal plan 누락: {name}")
            step = {"id": "s", "skill": name, "params": minimal[name]}
            issues = collect_plan_issues(_wrap(step))
            self.assertEqual(issues, [], f"{name} minimal plan validator 실패: {issues}")


if __name__ == "__main__":
    unittest.main()
