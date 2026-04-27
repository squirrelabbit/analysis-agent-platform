from __future__ import annotations

from typing import Any

from . import runtime as rt
from ._migration_targets import canonical_skill_name
from .skills._contract_models import (
    validate_execution_final_answer,
    validate_issue_evidence_artifact,
    validate_ranked_issue_summary_artifact,
)
from .skill_bundle import skill_definition


class SkillContractError(ValueError):
    """Raised when a task violates a declared skill contract."""


class SkillPreconditionError(SkillContractError):
    """Raised when a task is missing required inputs or prior artifacts."""


class SkillOutputError(SkillContractError):
    """Raised when a task returns a malformed output payload."""


_PRESENTER_SKILL_META = {
    "execution_final_answer": {
        "fallback_policy": "strict_fail",
        "quality_tier": "llm_dependent",
        "result_kind": "summary_narrative",
        "result_scope": "document_subset",
        "result_scope_policy": "dynamic",
        "allowed_runtime_result_scopes": [
            "full_dataset",
            "document_subset",
            "cluster_subset",
            "partial_build",
        ],
    }
}


def validate_task_payload(name: str, payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise SkillPreconditionError(f"{name} payload must be a JSON object")

    definition = skill_definition(name)
    if not definition:
        return

    prior_artifacts = payload.get("prior_artifacts")
    required_prior_skills = _string_list(definition.get("requires_prior_skills"))
    missing_prior_skills = [
        skill_name
        for skill_name in required_prior_skills
        if rt._find_prior_artifact(prior_artifacts, skill_name) is None
    ]
    if missing_prior_skills:
        raise SkillPreconditionError(
            f"{name} requires prior artifacts from: {', '.join(missing_prior_skills)}"
        )

    required_any_prior_skills = _string_list(definition.get("requires_any_prior_skills"))
    if required_any_prior_skills and not any(
        rt._find_prior_artifact(prior_artifacts, skill_name) is not None
        for skill_name in required_any_prior_skills
    ):
        raise SkillPreconditionError(
            f"{name} requires at least one prior artifact from: {', '.join(required_any_prior_skills)}"
        )


def validate_task_result(name: str, payload: dict[str, Any], result: dict[str, Any]) -> None:
    if not isinstance(result, dict):
        raise SkillOutputError(f"{name} must return a JSON object")

    if name == "planner":
        plan = result.get("plan")
        if not isinstance(plan, dict):
            raise SkillOutputError("planner result must contain a plan object")
        steps = plan.get("steps")
        if not isinstance(steps, list):
            raise SkillOutputError("planner result must contain plan.steps")
        metadata = plan.get("metadata")
        if not isinstance(metadata, dict):
            raise SkillOutputError("planner result must contain plan.metadata")
        return

    artifact = result.get("artifact")
    if not isinstance(artifact, dict):
        raise SkillOutputError(f"{name} result must contain an artifact object")

    artifact_skill_name = str(artifact.get("skill_name") or "").strip()
    if not artifact_skill_name:
        raise SkillOutputError(f"{name} artifact must contain skill_name")
    # ADR-009 F1: during the deprecation period, treat deprecated and canonical
    # skill names as equivalent for artifact identity comparison. The handler
    # itself is not yet renamed (Phase 4/5 territory), so an artifact produced
    # via the term_frequency route may still report skill_name=keyword_frequency
    # and vice versa — both resolve to the same canonical identity.
    if canonical_skill_name(artifact_skill_name) != canonical_skill_name(name):
        raise SkillOutputError(
            f"{name} artifact skill_name mismatch: {artifact_skill_name}"
        )

    definition = _skill_contract_meta(name)
    _apply_declared_result_scope(name, artifact, definition)
    _validate_result_scope_policy(name, payload, artifact, definition)
    fallback_policy = str(definition.get("fallback_policy") or "").strip()
    if fallback_policy == "strict_fail" and _looks_gracefully_empty(name, result, artifact):
        raise SkillOutputError(f"{name} returned an empty result despite strict_fail contract")

    canonical_name = canonical_skill_name(name)

    if canonical_name in {"unstructured_issue_summary", "issue_cluster_summary"}:
        validate_ranked_issue_summary_artifact(artifact)
    elif canonical_name == "issue_evidence_summary":
        validate_issue_evidence_artifact(artifact)
    elif name == "execution_final_answer":
        answer = result.get("answer")
        if not isinstance(answer, dict):
            raise SkillOutputError("execution_final_answer result must contain an answer object")
        _apply_declared_result_scope(name, answer, definition)
        runtime_result_scope = rt._normalize_result_scope(answer.get("runtime_result_scope"))
        if runtime_result_scope:
            artifact["runtime_result_scope"] = runtime_result_scope
        declared_result_scope = rt._normalize_result_scope(answer.get("result_scope"))
        if declared_result_scope:
            artifact["result_scope"] = declared_result_scope
        validate_execution_final_answer(answer)


def _apply_declared_result_scope(
    name: str,
    artifact: dict[str, Any],
    definition: dict[str, Any],
) -> None:
    declared_result_scope = rt._normalize_result_scope(definition.get("result_scope"))
    if not declared_result_scope:
        return
    current_result_scope = rt._normalize_result_scope(artifact.get("result_scope"))
    if not current_result_scope:
        artifact["result_scope"] = declared_result_scope
        current_result_scope = declared_result_scope
    else:
        artifact["result_scope"] = current_result_scope
    if current_result_scope != declared_result_scope:
        raise SkillOutputError(
            f"{name} declared result_scope mismatch: {current_result_scope} != {declared_result_scope}"
        )


def _validate_result_scope_policy(
    name: str,
    payload: dict[str, Any],
    artifact: dict[str, Any],
    definition: dict[str, Any],
) -> None:
    policy = str(definition.get("result_scope_policy") or "").strip()
    declared_result_scope = rt._normalize_result_scope(definition.get("result_scope"))
    runtime_result_scope = rt._normalize_result_scope(artifact.get("runtime_result_scope"))
    if runtime_result_scope:
        artifact["runtime_result_scope"] = runtime_result_scope
    if not policy:
        return
    if policy == "static":
        if not runtime_result_scope:
            if not declared_result_scope:
                raise SkillOutputError(f"{name} static result_scope is missing")
            artifact["runtime_result_scope"] = declared_result_scope
            runtime_result_scope = declared_result_scope
        if runtime_result_scope != declared_result_scope:
            raise SkillOutputError(
                f"{name} runtime_result_scope mismatch for static policy: {runtime_result_scope}"
            )
        return
    if policy == "inherits_from_input":
        expected_runtime_scope = rt.infer_runtime_scope_from_prior(
            payload,
            declared_result_scope=declared_result_scope,
        )
        if not runtime_result_scope:
            raise SkillOutputError(f"{name} runtime_result_scope is required for inherits_from_input policy")
        if runtime_result_scope != expected_runtime_scope:
            raise SkillOutputError(
                f"{name} runtime_result_scope mismatch for inherits_from_input policy: "
                f"{runtime_result_scope} != {expected_runtime_scope}"
            )
        return
    if policy == "dynamic":
        allowed_runtime_scopes = {
            normalized
            for item in _string_list(definition.get("allowed_runtime_result_scopes"))
            if (normalized := rt._normalize_result_scope(item))
        }
        if not runtime_result_scope:
            raise SkillOutputError(f"{name} runtime_result_scope is required for dynamic policy")
        if allowed_runtime_scopes and runtime_result_scope not in allowed_runtime_scopes:
            raise SkillOutputError(
                f"{name} runtime_result_scope {runtime_result_scope} is outside allowed_runtime_result_scopes"
            )
        return
    raise SkillOutputError(f"{name} has unsupported result_scope_policy: {policy}")


def _skill_contract_meta(name: str) -> dict[str, Any]:
    definition = skill_definition(name)
    if definition:
        return definition
    return dict(_PRESENTER_SKILL_META.get(name) or {})


def _looks_gracefully_empty(name: str, result: dict[str, Any], artifact: dict[str, Any]) -> bool:
    canonical_name = canonical_skill_name(name)
    if canonical_name in {"cluster_label_candidates", "embedding_cluster"}:
        return len(list(artifact.get("clusters") or [])) == 0
    if canonical_name in {"unstructured_issue_summary", "issue_cluster_summary"}:
        return len(list(artifact.get("ranked_issues") or [])) == 0
    if canonical_name == "issue_evidence_summary":
        return (
            not str(artifact.get("summary") or "").strip()
            or len(list(artifact.get("evidence") or [])) == 0
        )
    if name == "execution_final_answer":
        answer = result.get("answer") or {}
        return (
            not str(answer.get("answer_text") or "").strip()
            or len(list(answer.get("evidence") or [])) == 0
        )
    return False


def _string_list(value: Any) -> list[str]:
    values: list[str] = []
    if not isinstance(value, list):
        return values
    for item in value:
        normalized = str(item or "").strip()
        if normalized:
            values.append(normalized)
    return values


__all__ = [
    "SkillContractError",
    "SkillOutputError",
    "SkillPreconditionError",
    "validate_task_payload",
    "validate_task_result",
]
