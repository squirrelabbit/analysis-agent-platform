from __future__ import annotations

"""plan_v2 runner — orchestration only.

각 skill의 SQL 생성은 ``executor.skills`` 하위 모듈이 담당하고, runner는
step 순회 + dispatch + step view 등록 + result 수집만 책임진다.
"""

from dataclasses import dataclass, field
from typing import Any

from ..planner import validate_plan
from .context import ExecutorContext
from .skills import SKILL_BUILDERS, ExecutorError


@dataclass(frozen=True)
class ExecutionStepResult:
    step_id: str
    skill: str
    row_count: int
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


def execute_plan(
    context: ExecutorContext,
    plan: dict[str, Any],
    *,
    sample_limit: int = 5,
) -> dict[str, ExecutionStepResult]:
    """validator → step dispatch → step output 적재 + 결과 수집.

    raise:
      - ``planner.PlanValidationError``: validator 통과 실패
      - ``ExecutorError``: SQL 생성/실행 오류 또는 지원하지 않는 skill
    """

    validate_plan(plan)
    results: dict[str, ExecutionStepResult] = {}
    for step in plan["steps"]:
        step_id = str(step["id"]).strip()
        skill = str(step["skill"]).strip()
        params = dict(step["params"])

        builder = SKILL_BUILDERS.get(skill)
        if builder is None:
            raise ExecutorError(
                f"step '{step_id}': skill '{skill}' is not supported in executor first cut "
                f"(supported: {sorted(SKILL_BUILDERS.keys())})"
            )

        try:
            sql, extra = builder(params, context)
            context.register_step_view(step_id, sql)
        except ExecutorError:
            raise
        except Exception as exc:
            raise ExecutorError(
                f"step '{step_id}' ({skill}) failed to build SQL: {exc}"
            ) from exc

        try:
            row_count = context.count_rows(step_id)
            # silverone 2026-05-26 (SQL-4) — present step만 max_rows까지 실제 데이터를
            # 적재한다. 다른 intermediate step은 기존대로 sample_limit(=5)건만 디버그용.
            if skill == "present":
                max_rows = int(extra.get("max_rows", 0) or 0)
                if max_rows <= 0:
                    max_rows = 1000
                sample_rows = context.fetch_rows(step_id, limit=max_rows)
            else:
                sample_rows = (
                    context.fetch_rows(step_id, limit=sample_limit)
                    if sample_limit > 0
                    else []
                )
        except Exception as exc:
            raise ExecutorError(
                f"step '{step_id}' ({skill}) failed to fetch result: {exc}"
            ) from exc

        results[step_id] = ExecutionStepResult(
            step_id=step_id,
            skill=skill,
            row_count=row_count,
            sample_rows=sample_rows,
            extra=extra,
        )
    return results


__all__ = [
    "ExecutionStepResult",
    "ExecutorError",
    "execute_plan",
]
