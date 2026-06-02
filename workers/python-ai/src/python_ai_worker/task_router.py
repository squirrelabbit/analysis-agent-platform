from __future__ import annotations

"""task_router — Python worker HTTP entrypoint dispatch.

silverone 2026-05-21 δ (old plan layer 제거): rule trigger/sequence 기반 planner,
13 skill, validator/policy 카탈로그 모두 삭제. 남은 task:
  - dataset_build 3종: dataset_clean / dataset_doc_genuineness / dataset_clause_label
  - plan_v2 흐름 2종: analyze / plan (executor.service)
    - silverone 2026-06-01: canonical task name은 `analyze` / `plan`. 옛
      `analyze_v2` / `plan_v2`는 backward-compatible alias로 dispatch 됨.
      응답 body의 `plan_version: "v2"`는 wire version 의미라 유지.
"""

from dataclasses import dataclass
from typing import Any

from .dataset_build import (
    run_dataset_clause_label,
    run_dataset_clean,
    run_dataset_doc_genuineness,
)
from .executor import (
    ArtifactPathResolutionError,
    coerce_artifact_paths_payload,
    execute_analyze_plan,
    plan_and_execute_analyze,
    plan_from_question,
)
from .obs import get
from .planner import (
    DatasetSpecificColumn,
    PlanValidationError,
    PlannerCallError,
)
from .prompt_options import list_prompt_options
from .taxonomies import (
    TaxonomyMismatchError,
    check_taxonomy_compatibility,
    load_taxonomy,
)

_LOG = get("task_router")


# silverone 2026-06-01 (rename PR A) — canonical task name은 `analyze` / `plan`.
# 옛 `analyze_v2` / `plan_v2`는 backward-compatible alias로 dispatch에서
# 정규 이름으로 redirect된다. 응답 body의 `plan_version: "v2"`는 wire version
# 식별자라 변경 대상 아님 (CLAUDE.md 정책).
_ANALYZE_TASK_NAME = "analyze"
_PLAN_TASK_NAME = "plan"
_LEGACY_ANALYZE_TASK_NAME = "analyze_v2"
_LEGACY_PLAN_TASK_NAME = "plan_v2"


# taxonomy-driven config Phase 3-B (silverone 2026-05-27) — analyze 시
# clause_label artifact의 taxonomy_id/hash와 비교할 planner active taxonomy.
# Phase 3-A에서 planner schema description이 이 taxonomy에서 derive되므로
# 동일 source. Phase 3-B 후속에서 dataset_version metadata 기반 동적 lookup.
_PLANNER_TAXONOMY = load_taxonomy("festival-v2")


@dataclass(frozen=True)
class TaskCapability:
    name: str
    description: str


def capability_names() -> list[str]:
    return [item.name for item in supported_capabilities()]


def capability_payload() -> dict[str, Any]:
    return {
        "capabilities": [
            {"name": c.name, "description": c.description}
            for c in supported_capabilities()
        ],
    }


def supported_capabilities() -> list[TaskCapability]:
    # canonical capability만 노출. alias(`analyze_v2` / `plan_v2`)는 dispatch
    # 단계에서만 받아주고 /health capability 목록에는 노출하지 않는다.
    return [
        TaskCapability(name="dataset_clean", description="Clean uploaded dataset rows via deterministic regex + noise scrub."),
        TaskCapability(name="dataset_doc_genuineness", description="LLOA-based doc-level 3-tier genuineness classification."),
        TaskCapability(name="dataset_clause_label", description="LLOA-based clause split + sentiment + aspect labelling."),
        TaskCapability(name=_PLAN_TASK_NAME, description="plan_v2 LLM planner — generate plan from user_question (debug entrypoint)."),
        TaskCapability(name=_ANALYZE_TASK_NAME, description="plan_v2 executor — plan or user_question + artifact_paths → result."),
        TaskCapability(name="prompt_options", description="List prompt versions/default/label for a task-folder prompt (read-only)."),
    ]


def task_handlers() -> dict[str, Any]:
    return {
        "dataset_clean": run_dataset_clean,
        "dataset_doc_genuineness": run_dataset_doc_genuineness,
        "dataset_clause_label": run_dataset_clause_label,
        "prompt_options": _run_prompt_options,
    }


def _run_prompt_options(payload: dict[str, Any]) -> dict[str, Any]:
    """prompt_options task — task별 prompt 선택지(version/default/label) 반환.

    Go control-plane이 ``GET /prompt_options?task=<task>``를 이 task로 proxy한다.
    Go는 파일을 직접 읽지 않는다. invalid task / index.yaml 오류는
    PromptOptionsError(ValueError) → main.py에서 HTTP 400.
    """
    task = str(payload.get("task") or "").strip()
    if not task:
        raise ValueError("prompt_options requires 'task'")
    return list_prompt_options(task)


def run_task(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    # silverone 2026-06-01 (rename PR A) — canonical `analyze` / `plan` 정식,
    # 옛 `analyze_v2` / `plan_v2`는 backward-compatible alias로 같은 handler에
    # 위임. alias 호출은 obs warning을 남겨 옛 client 제거 시점을 가늠한다.
    if name in (_ANALYZE_TASK_NAME, _LEGACY_ANALYZE_TASK_NAME):
        if name == _LEGACY_ANALYZE_TASK_NAME:
            _LOG.warning(
                "task.dispatch.legacy_alias",
                requested=name,
                canonical=_ANALYZE_TASK_NAME,
            )
        return _run_analyze(payload)
    if name in (_PLAN_TASK_NAME, _LEGACY_PLAN_TASK_NAME):
        if name == _LEGACY_PLAN_TASK_NAME:
            _LOG.warning(
                "task.dispatch.legacy_alias",
                requested=name,
                canonical=_PLAN_TASK_NAME,
            )
        return _run_plan(payload)
    handler = task_handlers().get(name)
    if handler is None:
        _LOG.error("task.dispatch.unsupported", skill_name=name)
        raise ValueError(f"unsupported capability: {name}")
    _LOG.info("task.dispatch.started", skill_name=name)
    try:
        result = handler(payload)
    except Exception as exc:
        _LOG.error(
            "task.dispatch.failed",
            skill_name=name,
            error_category=type(exc).__name__,
        )
        raise
    _LOG.info(
        "task.dispatch.completed",
        skill_name=name,
        response_keys=sorted(result.keys()) if isinstance(result, dict) else [],
    )
    return result


def _run_analyze(payload: dict[str, Any]) -> dict[str, Any]:
    """analyze endpoint handler — silverone 4단계 + 6단계 통합.

    payload는 두 형태 중 정확히 하나:
    - 4단계 (replay/debug): ``{dataset_version_id, plan, artifact_paths?}``
    - 6단계 (production): ``{dataset_version_id, user_question, artifact_paths?,
      docs_extra_columns?}``

    ``plan``과 ``user_question``이 동시에 오면 ambiguous로 fail.
    """

    if not isinstance(payload, dict):
        raise ValueError("analyze payload must be an object")
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    if not dataset_version_id:
        raise ValueError("analyze payload requires 'dataset_version_id'")

    plan = payload.get("plan")
    user_question = str(payload.get("user_question") or "").strip()
    if plan is not None and user_question:
        raise ValueError(
            "analyze payload must include exactly one of 'plan' or 'user_question', not both"
        )
    if plan is None and not user_question:
        raise ValueError("analyze payload requires 'plan' or 'user_question'")
    if plan is not None and not isinstance(plan, dict):
        raise ValueError("analyze payload 'plan' must be an object")

    artifact_paths = coerce_artifact_paths_payload(payload.get("artifact_paths"))
    docs_extra_columns = _coerce_docs_extra_columns(payload.get("docs_extra_columns"))
    conversation_context = _coerce_conversation_context(payload.get("conversation_context"))

    # taxonomy-driven config Phase 3-B (silverone 2026-05-27) — clause_label
    # artifact metadata와 planner taxonomy 정합성 체크. control plane이 wire
    # 확장 전까지는 ``clause_label_metadata``가 None이라 ``legacy_missing``로
    # 떨어진다. id_mismatch면 TaxonomyMismatchError가 raise되어 아래 except
    # 블록에서 ValueError로 잡힌다 (subclass).
    clause_label_metadata = payload.get("clause_label_metadata")
    if not isinstance(clause_label_metadata, dict):
        clause_label_metadata = None
    artifact_taxonomy_id = (
        clause_label_metadata.get("taxonomy_id") if clause_label_metadata else None
    )
    artifact_taxonomy_hash = (
        clause_label_metadata.get("taxonomy_hash") if clause_label_metadata else None
    )
    taxonomy_check = check_taxonomy_compatibility(
        planner_taxonomy=_PLANNER_TAXONOMY,
        artifact_taxonomy_id=artifact_taxonomy_id,
        artifact_taxonomy_hash=artifact_taxonomy_hash,
    )
    if taxonomy_check["status"] == "hash_mismatch":
        _LOG.warning(
            "analyze.taxonomy_hash_mismatch",
            skill_name=_ANALYZE_TASK_NAME,
            dataset_version_id=dataset_version_id,
            planner_taxonomy_id=taxonomy_check["planner_taxonomy_id"],
            planner_taxonomy_hash=taxonomy_check["planner_taxonomy_hash"],
            artifact_taxonomy_hash=taxonomy_check["artifact_taxonomy_hash"],
        )
    elif taxonomy_check["status"] == "legacy_missing":
        _LOG.info(
            "analyze.taxonomy_legacy_missing",
            skill_name=_ANALYZE_TASK_NAME,
            dataset_version_id=dataset_version_id,
            planner_taxonomy_id=taxonomy_check["planner_taxonomy_id"],
        )
    # silverone 2026-05-26 (ADR-020 PR-A) — reuse 분기 hint를 composer로 전달.
    reuse_metadata = payload.get("reuse_metadata") if isinstance(payload.get("reuse_metadata"), dict) else None
    # silverone 2026-05-26 (cost-2 A/B) — opt-in planner prompt variant. payload에
    # prompt_version이 명시되면 그대로 planner에 전달. user-question mode에서만
    # 효과 (plan 모드는 LLM 호출 없음).
    prompt_version = str(payload.get("prompt_version") or "").strip()

    mode = "plan" if plan is not None else "user_question"
    _LOG.info(
        "task.dispatch.started",
        skill_name=_ANALYZE_TASK_NAME,
        dataset_version_id=dataset_version_id,
        artifact_paths_inline=bool(artifact_paths),
        mode=mode,
    )
    try:
        if plan is not None:
            result = execute_analyze_plan(
                dataset_version_id,
                plan,
                artifact_paths=artifact_paths,
                user_question=user_question or None,
                reuse_metadata=reuse_metadata,
            )
        else:
            result = plan_and_execute_analyze(
                dataset_version_id,
                user_question,
                artifact_paths=artifact_paths,
                docs_extra_columns=docs_extra_columns,
                conversation_context=conversation_context,
                prompt_version=prompt_version,
            )
    except (
        PlanValidationError,
        ArtifactPathResolutionError,
        PlannerCallError,
        ValueError,
    ) as exc:
        _LOG.warning(
            "task.dispatch.failed",
            skill_name=_ANALYZE_TASK_NAME,
            dataset_version_id=dataset_version_id,
            mode=mode,
            error_category=type(exc).__name__,
        )
        raise
    except Exception as exc:
        _LOG.error(
            "task.dispatch.failed",
            skill_name=_ANALYZE_TASK_NAME,
            dataset_version_id=dataset_version_id,
            mode=mode,
            error_category=type(exc).__name__,
        )
        raise

    # Phase 3-B — analyze 결과에 taxonomy_check diagnostic을 그대로 노출.
    # 운영자/operator audit log용. 사용자 화면 직접 노출은 안 함 (display.
    # warnings와 별도 슬롯).
    result["taxonomy_check"] = taxonomy_check

    _LOG.info(
        "task.dispatch.completed",
        skill_name=_ANALYZE_TASK_NAME,
        dataset_version_id=dataset_version_id,
        mode=mode,
        step_count=len(result.get("steps") or []),
        taxonomy_check_status=taxonomy_check["status"],
    )
    return result


def _run_plan(payload: dict[str, Any]) -> dict[str, Any]:
    """plan debug endpoint — user_question → plan 생성만 (wire body는 plan_v2)."""

    if not isinstance(payload, dict):
        raise ValueError("plan payload must be an object")
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    if not dataset_version_id:
        raise ValueError("plan payload requires 'dataset_version_id'")
    user_question = str(payload.get("user_question") or "").strip()
    if not user_question:
        raise ValueError("plan payload requires 'user_question'")
    docs_extra_columns = _coerce_docs_extra_columns(payload.get("docs_extra_columns"))
    conversation_context = _coerce_conversation_context(payload.get("conversation_context"))

    _LOG.info(
        "task.dispatch.started",
        skill_name=_PLAN_TASK_NAME,
        dataset_version_id=dataset_version_id,
    )
    try:
        result = plan_from_question(
            dataset_version_id,
            user_question,
            docs_extra_columns=docs_extra_columns,
            conversation_context=conversation_context,
        )
    except (PlannerCallError, PlanValidationError, ValueError) as exc:
        _LOG.warning(
            "task.dispatch.failed",
            skill_name=_PLAN_TASK_NAME,
            dataset_version_id=dataset_version_id,
            error_category=type(exc).__name__,
        )
        raise
    except Exception as exc:
        _LOG.error(
            "task.dispatch.failed",
            skill_name=_PLAN_TASK_NAME,
            dataset_version_id=dataset_version_id,
            error_category=type(exc).__name__,
        )
        raise

    _LOG.info(
        "task.dispatch.completed",
        skill_name=_PLAN_TASK_NAME,
        dataset_version_id=dataset_version_id,
        step_count=len((result.get("plan") or {}).get("steps") or []),
    )
    return result


def _coerce_docs_extra_columns(payload: Any) -> list[DatasetSpecificColumn] | None:
    if payload is None:
        return None
    if not isinstance(payload, list):
        raise ValueError("docs_extra_columns must be a list of {name, type, description} objects")
    result: list[DatasetSpecificColumn] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"docs_extra_columns[{index}] must be an object")
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError(f"docs_extra_columns[{index}].name is required")
        result.append(
            DatasetSpecificColumn(
                name=name,
                type=str(item.get("type") or "string"),
                description=str(item.get("description") or ""),
            )
        )
    return result


def _coerce_conversation_context(payload: Any) -> list[dict[str, Any]] | None:
    if payload is None:
        return None
    if not isinstance(payload, list):
        raise ValueError("conversation_context must be a list of summary objects")
    result: list[dict[str, Any]] = []
    for index, item in enumerate(payload[-3:]):
        if not isinstance(item, dict):
            raise ValueError(f"conversation_context[{index}] must be an object")
        compact: dict[str, Any] = {}
        for key in ("question", "answer_summary", "present_title", "row_count", "columns", "key_filters", "key_dimensions"):
            if key in item and item[key] is not None:
                compact[key] = item[key]
        if compact:
            result.append(compact)
    return result
