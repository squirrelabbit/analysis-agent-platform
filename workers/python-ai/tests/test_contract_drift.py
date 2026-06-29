"""계약 drift detector (ADR-031 1단계).

task / skill 계약이 여러 곳에 흩어져 수작업 동기화되는 데서 오는 drift를 잠근다.
- task: task_registry.json(dataset_build) == task_router handler == capability
- skill: SKILL_CATALOG == prompt._ORDERED_SKILL_NAMES == step_display._BUILDERS
         == executor.SKILL_BUILDERS

배경: dataset_clause_keywords가 코드엔 있는데 문서엔 "3종"으로 남은 drift(2026-06-29),
summarize 1개 제거에 7곳을 손대야 했던 일(MR !255)이 모두 "single source 미강제" 탓.
이 테스트가 새 task/skill 추가·제거 시 동기화 누락을 CI에서 fail-loud로 잡는다.
순수 단위 테스트(외부 의존 0).
"""
from __future__ import annotations

import unittest

from python_ai_worker.registries.task_registry import internal_task_names
from python_ai_worker.task_router import task_handlers, capability_names
from python_ai_worker.planner.schema import SKILL_CATALOG
from python_ai_worker.planner.prompt import _ORDERED_SKILL_NAMES
from python_ai_worker.planner.step_display import _BUILDERS as STEP_DISPLAY_BUILDERS
from python_ai_worker.executor.skills import SKILL_BUILDERS


class TaskContractDriftTests(unittest.TestCase):
    """dataset_build task가 registry / handler / capability에서 정합해야 한다."""

    def test_registry_matches_handlers(self) -> None:
        # registry는 dataset_build task만 담는다(task_handlers엔 taxonomy/prompt_options 등
        # 보조 task도 있으므로 dataset_* 범위로 대조). 모든 dataset_* task는 양쪽 정합 필수.
        registry = set(internal_task_names())
        dataset_handlers = {k for k in task_handlers() if k.startswith("dataset_")}
        self.assertEqual(
            registry,
            dataset_handlers,
            "task_registry.json(dataset_build)와 task_router.task_handlers의 dataset_* 가 "
            "어긋남 — dataset_build task 추가/제거 시 양쪽을 함께 갱신해야 한다.",
        )

    def test_registry_tasks_exposed_as_capabilities(self) -> None:
        registry = set(internal_task_names())
        caps = set(capability_names())
        missing = registry - caps
        self.assertEqual(
            missing,
            set(),
            f"registry task가 capability에 노출 안 됨: {missing} — supported_capabilities 갱신 필요.",
        )


class SkillContractDriftTests(unittest.TestCase):
    """plan_v2 skill이 catalog / prompt / display / executor에서 정합해야 한다."""

    def test_skill_catalog_consistent_across_layers(self) -> None:
        catalog = set(SKILL_CATALOG.keys())
        ordered = set(_ORDERED_SKILL_NAMES)
        display = set(STEP_DISPLAY_BUILDERS.keys())
        executor = set(SKILL_BUILDERS.keys())

        self.assertEqual(
            catalog, ordered,
            "SKILL_CATALOG와 prompt._ORDERED_SKILL_NAMES가 어긋남(프롬프트 노출 drift).",
        )
        self.assertEqual(
            catalog, display,
            "SKILL_CATALOG와 step_display._BUILDERS가 어긋남(화면 display drift).",
        )
        self.assertEqual(
            catalog, executor,
            "SKILL_CATALOG와 executor.SKILL_BUILDERS가 어긋남 — catalog에 advertise한 skill에 "
            "executor 빌더가 없으면 plan 실행 시 hard-fail한다(summarize 사고 재발 방지).",
        )

    def test_ordered_skill_names_no_duplicates(self) -> None:
        self.assertEqual(
            len(_ORDERED_SKILL_NAMES),
            len(set(_ORDERED_SKILL_NAMES)),
            "_ORDERED_SKILL_NAMES에 중복이 있다.",
        )


if __name__ == "__main__":
    unittest.main()
