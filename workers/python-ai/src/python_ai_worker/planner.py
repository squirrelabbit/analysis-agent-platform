from __future__ import annotations

"""Planner entrypoints for the python-ai worker."""

import time
from typing import Any

from . import runtime as rt
from .obs import get
from .skill_bundle import default_inputs_for_skill, planner_sequence, skill_definition

_LOG = get("planner")

def run_planner(payload: dict[str, Any]) -> dict[str, Any]:
    started_at = time.monotonic()
    goal = str(payload.get("goal") or "").strip()
    data_type = str(payload.get("data_type") or "").strip().lower()
    _LOG.info(
        "planner.started",
        goal=goal,
        data_type=data_type,
        dataset_name=str(payload.get("dataset_name") or "dataset_from_version").strip(),
    )
    client = rt._anthropic_client()
    if client and client.is_enabled():
        try:
            result = _attach_plan_metadata(
                rt._run_planner_with_llm(client, payload, fallback_planner=_run_rule_based_planner)
            )
            _log_planner_completed("llm", result, started_at)
            return result
        except Exception as exc:
            fallback = _run_rule_based_planner(payload)
            fallback["planner_type"] = "python-ai-fallback"
            fallback["planner_model"] = "rule-based-v1"
            fallback["planner_prompt_version"] = "planner-fallback-v1"
            fallback["notes"] = [f"anthropic planner fallback: {exc}"]
            _LOG.warning(
                "planner.fallback",
                planner_model=client._config.model,
                error_category=type(exc).__name__,
                duration_ms=int((time.monotonic() - started_at) * 1000),
            )
            _log_planner_completed("fallback", fallback, started_at)
            return fallback
    result = _run_rule_based_planner(payload)
    _log_planner_completed("rule", result, started_at)
    return result

def _run_rule_based_planner(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "dataset_from_version").strip()
    data_type = str(payload.get("data_type") or "").strip().lower()
    goal_raw = str(payload.get("goal") or "").strip()
    goal = goal_raw.lower()

    if data_type in {"mixed", "both"}:
        sequence_name = "mixed_default"
    elif data_type == "unstructured" and rt._looks_sentence_split_goal(goal):
        sequence_name = "unstructured_sentence_split"
    elif data_type == "unstructured" and rt._looks_noun_frequency_goal(goal):
        sequence_name = "unstructured_noun_frequency"
    elif data_type == "unstructured" and rt._looks_cluster_goal(goal):
        if rt._looks_cluster_subset_goal(goal):
            sequence_name = "unstructured_cluster_subset"
        else:
            sequence_name = "unstructured_cluster_materialized"
    elif data_type == "unstructured" and rt._looks_taxonomy_goal(goal):
        sequence_name = "unstructured_taxonomy"
    elif data_type == "unstructured" and rt._looks_duplicate_goal(goal):
        sequence_name = "unstructured_duplicate"
    elif data_type == "unstructured" and rt._looks_sentiment_goal(goal):
        sequence_name = "unstructured_sentiment"
    elif data_type == "unstructured" and rt._looks_semantic_search_goal(goal):
        sequence_name = "unstructured_semantic_search"
    elif data_type == "unstructured" and rt._looks_compare_goal(goal):
        sequence_name = "unstructured_compare"
    elif data_type == "unstructured" and rt._looks_breakdown_goal(goal):
        sequence_name = "unstructured_breakdown"
    elif data_type == "unstructured" and rt._looks_trend_goal(goal):
        sequence_name = "unstructured_trend"
    elif data_type == "unstructured" or rt._looks_unstructured(goal):
        sequence_name = "unstructured_default"
    else:
        sequence_name = "structured_default"

    skills = planner_sequence(sequence_name)

    steps = []
    for skill_name in skills:
        step = {
            "skill_name": skill_name,
            "dataset_name": dataset_name,
            "inputs": default_inputs_for_skill(skill_name, goal=goal_raw),
        }
        steps.append(step)
    metadata = _plan_metadata(skills)

    return {
        "plan": {
            "steps": steps,
            "notes": "planned by python-ai worker",
            "metadata": metadata,
        },
        "planner_type": "python-ai",
        "planner_model": "rule-based-v1",
        "planner_prompt_version": "planner-http-v1",
    }


def _plan_metadata(skills: list[str]) -> dict[str, Any]:
    llm_stages: list[str] = []
    for skill_name in skills:
        definition = skill_definition(skill_name) or {}
        if str(definition.get("quality_tier") or "").strip() == "llm_dependent":
            llm_stages.append(skill_name)
    return {
        "contains_llm_stage": bool(llm_stages),
        "llm_stages": llm_stages,
    }


def _attach_plan_metadata(result: dict[str, Any]) -> dict[str, Any]:
    plan = result.get("plan")
    if not isinstance(plan, dict):
        return result
    steps = list(plan.get("steps") or [])
    skill_names = [
        str(step.get("skill_name") or "").strip()
        for step in steps
        if isinstance(step, dict) and str(step.get("skill_name") or "").strip()
    ]
    plan["metadata"] = _plan_metadata(skill_names)
    return result


def _log_planner_completed(mode: str, result: dict[str, Any], started_at: float) -> None:
    plan = result.get("plan") if isinstance(result, dict) else {}
    steps = list(plan.get("steps") or []) if isinstance(plan, dict) else []
    _LOG.info(
        "planner.completed",
        planner_mode=mode,
        planner_type=str(result.get("planner_type") or "").strip() if isinstance(result, dict) else "",
        step_count=len(steps),
        duration_ms=int((time.monotonic() - started_at) * 1000),
    )
