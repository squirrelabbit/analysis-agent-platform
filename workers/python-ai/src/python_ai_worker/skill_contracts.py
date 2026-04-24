from __future__ import annotations

from typing import Any

from . import runtime as rt
from .skill_bundle import skill_definition


class SkillContractError(ValueError):
    """Raised when a task violates a declared skill contract."""


class SkillPreconditionError(SkillContractError):
    """Raised when a task is missing required inputs or prior artifacts."""


class SkillOutputError(SkillContractError):
    """Raised when a task returns a malformed output payload."""


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


def validate_task_result(name: str, result: dict[str, Any]) -> None:
    if not isinstance(result, dict):
        raise SkillOutputError(f"{name} must return a JSON object")

    if name == "planner":
        plan = result.get("plan")
        if not isinstance(plan, dict):
            raise SkillOutputError("planner result must contain a plan object")
        steps = plan.get("steps")
        if not isinstance(steps, list):
            raise SkillOutputError("planner result must contain plan.steps")
        return

    artifact = result.get("artifact")
    if not isinstance(artifact, dict):
        raise SkillOutputError(f"{name} result must contain an artifact object")

    artifact_skill_name = str(artifact.get("skill_name") or "").strip()
    if not artifact_skill_name:
        raise SkillOutputError(f"{name} artifact must contain skill_name")
    if artifact_skill_name != name:
        raise SkillOutputError(
            f"{name} artifact skill_name mismatch: {artifact_skill_name}"
        )

    if name == "execution_final_answer" and not isinstance(result.get("answer"), dict):
        raise SkillOutputError("execution_final_answer result must contain an answer object")


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
