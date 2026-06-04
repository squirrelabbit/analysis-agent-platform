from __future__ import annotations

"""analyze service — POST /tasks/analyze (canonical) 진입점.
옛 POST /tasks/analyze_v2 URL은 backward-compatible alias로 task_router에서 dispatch.

silverone 2026-05-21 4단계 결정:
- payload: ``{ dataset_version_id, plan, artifact_paths? }``
- 처리 우선순위:
  1. payload에 ``artifact_paths``가 있으면 그대로 사용
  2. 없으면 ``_resolve_artifact_paths(dataset_version_id)`` 호출 (이번 단계 stub)
  3. resolve 실패 시 명확한 validation/runtime error

(b) production 흐름은 후속 ``plan_and_execute_analyze`` 추가 시 별도 함수로
분리. 현재는 plan을 그대로 받아 실행하는 smoke path만 제공.
"""

from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..composer import compose_answer
from ..planner.recipes import expand_recipes
from ..planner.step_display import plan_with_step_display
from ..planner import (
    DatasetSpecificColumn,
    PlanValidationError,
    PlannerCallError,
    PlannerResult,
    generate_plan,
)
from .context import ArtifactPaths, ExecutorContext, ExecutorContextError
from .runner import ExecutionStepResult, ExecutorError, execute_plan


class ArtifactPathResolutionError(RuntimeError):
    """``dataset_version_id``로 artifact path를 알아낼 수 없는 상태.

    4단계 smoke path에서는 payload에 ``artifact_paths``를 명시하지 않으면
    이 오류가 raise된다. control plane 통합은 (b) production path 단계에서.
    """


def execute_analyze_plan(
    dataset_version_id: str,
    plan: dict[str, Any],
    *,
    artifact_paths: ArtifactPaths | None = None,
    sample_limit: int = 5,
    user_question: str | None = None,
    reuse_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """plan_v2 plan을 실행해 step 결과 + present step 응답을 만든다.

    Args:
        dataset_version_id: dataset 식별자. 감사/log용 + ``artifact_paths``가
            없을 때 ``_resolve_artifact_paths`` 입력.
        plan: validator 통과 가능한 plan_v2 객체.
        artifact_paths: payload에 직접 inject된 path. None이면 resolver 호출.
        sample_limit: 각 step 결과에서 미리보기로 들고 올 row 개수.

    Raises:
        ArtifactPathResolutionError: artifact path 결정 실패.
        PlanValidationError: plan invariant 위반.
        ExecutorContextError: artifact 파일 누락 / docs.created_at 표준화 안 됨.
        ExecutorError: SQL 생성/실행 실패 / 지원하지 않는 skill.
    """

    # silverone 2026-06-01 (PR1) — answerable=false 거절 plan은 artifact/DuckDB
    # 없이 short-circuit. step이 없으므로 실행하지 않고 composer가 reason별
    # 메시지를 렌더한다 (display=null). raw row를 절대 만들지 않는다.
    if isinstance(plan, dict) and plan.get("answerable") is False:
        return _build_reject_response(
            dataset_version_id=dataset_version_id,
            plan=plan,
            user_question=user_question,
            reuse_metadata=reuse_metadata,
        )

    # Skill Contract v2 — runtime-enabled recipe(distribution/top_n)를
    # 실행 직전 deterministic 하게 atomic step으로 expand. recipe가 없으면 no-op
    # (기존 atomic plan 무영향). 미활성 recipe(event_window_count)는 RecipeError
    # → 400. expand 후 execute_plan의 기존 validator가 atomic plan을 재검증한다.
    plan = expand_recipes(plan)

    if artifact_paths is None:
        artifact_paths = _resolve_artifact_paths(dataset_version_id)

    with ExecutorContext(artifact_paths) as context:
        step_results = execute_plan(context, plan, sample_limit=sample_limit)

    return _build_response(
        dataset_version_id=dataset_version_id,
        plan=plan,
        artifact_paths=artifact_paths,
        step_results=step_results,
        user_question=user_question,
        reuse_metadata=reuse_metadata,
    )


def _resolve_artifact_paths(dataset_version_id: str) -> ArtifactPaths:
    """control plane lookup stub. 4단계에서는 미구현 — payload에 ``artifact_paths``를
    명시적으로 보내야 한다.

    실제 lookup은 (b) production path 단계에서 control plane 통합 시점에 구현한다.
    그때 control plane이 ``dataset_version_id``로 path를 resolve한 결과를
    payload에 inject하는 (1) 방식으로 정리한다.
    """

    raise ArtifactPathResolutionError(
        f"_resolve_artifact_paths(dataset_version_id={dataset_version_id!r}) is not implemented "
        "in the 4단계 smoke path — payload must include explicit 'artifact_paths' "
        "(see silverone 2026-05-21 4단계 결정)."
    )


def _build_response(
    *,
    dataset_version_id: str,
    plan: dict[str, Any],
    artifact_paths: ArtifactPaths,
    step_results: dict[str, ExecutionStepResult],
    user_question: str | None = None,
    reuse_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    steps_payload: list[dict[str, Any]] = []
    present_payload: dict[str, Any] | None = None
    for step in plan["steps"]:
        step_id = str(step["id"]).strip()
        result = step_results[step_id]
        step_payload = {
            "step_id": result.step_id,
            "skill": result.skill,
            "row_count": result.row_count,
            "sample_rows": result.sample_rows,
            "extra": result.extra,
        }
        steps_payload.append(step_payload)
        if result.skill == "present":
            # silverone 2026-05-26 (SQL-4, audit M7) — present 한도 메타 정리.
            #   total_rows    = 전체 결과 row 수 (= 기존 row_count, 호환 유지)
            #   returned_rows = 응답에 담은 rows 길이
            #   max_rows      = 적용된 한도 (user limit 또는 default 1000)
            #   truncated     = returned_rows < total_rows
            total_rows = result.row_count
            returned_rows = len(result.sample_rows)
            max_rows = int(result.extra.get("max_rows", 0) or 0) or 1000
            present_payload = {
                "step_id": result.step_id,
                "format": result.extra.get("format"),
                "title": result.extra.get("title"),
                "row_count": total_rows,  # 호환 필드 — total_rows와 동일.
                "total_rows": total_rows,
                "returned_rows": returned_rows,
                "max_rows": max_rows,
                "truncated": returned_rows < total_rows,
                "rows": result.sample_rows,
            }

    # silverone 2026-05-26 (ADR-020 PR-A) — deterministic answer composer.
    # executor 결과를 사용자-facing assistant_content / display / context_summary
    # 으로 변환. LLM 호출 없음, 5 단순 템플릿. composer는 raise하지 않으므로
    # response 흐름을 깨지 않는다.
    composer_output = compose_answer(
        user_question=user_question,
        present=present_payload,
        plan=plan,
        steps=steps_payload,
        reuse_metadata=reuse_metadata,
        error_metadata=None,
    )

    return {
        "dataset_version_id": dataset_version_id,
        "plan_version": str(plan.get("plan_version") or "").strip(),
        # silverone 2026-05-26 (plan reuse POC-1) — 후속 follow-up 질의에서
        # 이전 successful run의 plan을 patch해 LLM 호출 없이 재실행하려면 plan
        # 본문이 응답에 명시적으로 노출돼야 한다. planner.attempts[].raw는
        # 디버그용이라 안정 path가 아니다. caller(Go control plane)는 run.result_json.plan
        # 경로로 plan을 조회한다.
        "plan": plan_with_step_display(plan),
        "artifact_paths": {
            "docs": str(artifact_paths.docs),
            "clauses": str(artifact_paths.clauses),
            "genuineness": str(artifact_paths.genuineness),
        },
        "steps": steps_payload,
        "present": present_payload,
        "composer": composer_output,
    }


def _build_reject_response(
    *,
    dataset_version_id: str,
    plan: dict[str, Any],
    user_question: str | None = None,
    reuse_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """answerable=false plan의 응답 (silverone 2026-06-01, PR1).

    step 실행 없음 → present=None, artifact_paths=None. composer가 plan.reason /
    message를 보고 거절 메시지를 렌더(display=null)한다. capability_gap(있으면)은
    composer.metadata로 전달되어 PR2(rejection event 저장)에서 사용한다.
    """
    composer_output = compose_answer(
        user_question=user_question,
        present=None,
        plan=plan,
        steps=[],
        reuse_metadata=reuse_metadata,
        error_metadata=None,
    )
    return {
        "dataset_version_id": dataset_version_id,
        "plan_version": str(plan.get("plan_version") or "").strip(),
        "plan": plan_with_step_display(plan),
        "artifact_paths": None,
        "steps": [],
        "present": None,
        "composer": composer_output,
    }


def plan_from_question(
    dataset_version_id: str,
    user_question: str,
    *,
    docs_extra_columns: list[DatasetSpecificColumn] | None = None,
    conversation_context: list[dict[str, Any]] | None = None,
    anthropic_client: Any = None,
    prompt_version: str = "",
) -> dict[str, Any]:
    """user_question으로 plan_v2를 생성한다 (실행하지 않음).

    silverone 2026-05-21 6단계 — debug용 ``POST /tasks/plan`` (옛 alias ``/tasks/plan_v2``)의 service 진입점.
    LLM 응답 → JSON parse → validator까지 끝낸 plan을 dict로 돌려준다.

    Raises:
        PlannerCallError / PlannerParseError / PlannerValidationError: planner.llm
            에서 발생한 흐름 오류 그대로 노출.
    """

    client = anthropic_client if anthropic_client is not None else _default_anthropic_client()
    result = generate_plan(
        user_question=user_question,
        anthropic_client=client,
        docs_extra_columns=docs_extra_columns,
        conversation_context=conversation_context,
        prompt_version=prompt_version,
    )
    return _build_plan_response(dataset_version_id=dataset_version_id, planner_result=result)


def plan_and_execute_analyze(
    dataset_version_id: str,
    user_question: str,
    *,
    artifact_paths: ArtifactPaths | None = None,
    docs_extra_columns: list[DatasetSpecificColumn] | None = None,
    conversation_context: list[dict[str, Any]] | None = None,
    anthropic_client: Any = None,
    prompt_version: str = "",
    sample_limit: int = 5,
) -> dict[str, Any]:
    """user_question → plan_v2 생성 → executor 실행. silverone 2026-05-21 6단계.

    흐름: planner.llm.generate_plan → execute_analyze_plan.
    planner metadata (attempts, usage, prompt_version)를 응답에 함께 노출.
    """

    client = anthropic_client if anthropic_client is not None else _default_anthropic_client()
    planner_result = generate_plan(
        user_question=user_question,
        anthropic_client=client,
        docs_extra_columns=docs_extra_columns,
        conversation_context=conversation_context,
        prompt_version=prompt_version,
    )
    execution_response = execute_analyze_plan(
        dataset_version_id,
        planner_result.plan,
        artifact_paths=artifact_paths,
        sample_limit=sample_limit,
        user_question=user_question,
    )
    execution_response["planner"] = _planner_metadata(planner_result, user_question=user_question)
    return execution_response


def _default_anthropic_client() -> Any:
    """기존 planner 기본 client. test에서는 anthropic_client= 인자로 mock."""
    from ..runtime.llm import _anthropic_client

    return _anthropic_client()


def _planner_metadata(result: PlannerResult, *, user_question: str) -> dict[str, Any]:
    return {
        "user_question": user_question,
        "prompt_version": result.prompt_version,
        "attempts": result.attempts,
        "usage": result.usage,
    }


def _build_plan_response(
    *,
    dataset_version_id: str,
    planner_result: PlannerResult,
) -> dict[str, Any]:
    """plan_from_question 응답. plan 본문 + planner metadata."""

    return {
        "dataset_version_id": dataset_version_id,
        "plan_version": str(planner_result.plan.get("plan_version") or "").strip(),
        "plan": plan_with_step_display(planner_result.plan),
        "planner": {
            "prompt_version": planner_result.prompt_version,
            "attempts": planner_result.attempts,
            "usage": planner_result.usage,
        },
    }


def coerce_artifact_paths_payload(payload: Any) -> ArtifactPaths | None:
    """task_router가 payload['artifact_paths'] 를 ``ArtifactPaths``로 변환.

    None/falsy → None (resolver 호출 흐름). 잘못된 형태 → ValueError.
    """

    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError("artifact_paths must be an object with docs/clauses/genuineness keys")

    missing = [key for key in ("docs", "clauses", "genuineness") if not str(payload.get(key) or "").strip()]
    if missing:
        raise ValueError(f"artifact_paths missing keys: {', '.join(missing)}")
    return ArtifactPaths(
        docs=Path(str(payload["docs"])).expanduser(),
        clauses=Path(str(payload["clauses"])).expanduser(),
        genuineness=Path(str(payload["genuineness"])).expanduser(),
    )


__all__ = [
    "ArtifactPathResolutionError",
    "coerce_artifact_paths_payload",
    "execute_analyze_plan",
    "plan_and_execute_analyze",
    "plan_from_question",
]
