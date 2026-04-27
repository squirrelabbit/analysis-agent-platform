from __future__ import annotations

"""Planner entrypoints for the python-ai worker."""

import time
from typing import Any

from . import runtime as rt
from .obs import get
from .planner_meta import MetaPlanResult, select_active_layers
from .skill_bundle import default_inputs_for_skill, planner_layer_hints, planner_sequence, skill_definition

_LOG = get("planner")

def run_planner(payload: dict[str, Any]) -> dict[str, Any]:
    started_at = time.monotonic()
    goal = str(payload.get("goal") or "").strip()
    data_type = str(payload.get("data_type") or "").strip().lower()
    client = rt._anthropic_client()
    meta_plan = _select_meta_plan(payload, anthropic_client=client)
    planner_payload = dict(payload)
    planner_payload["active_layers"] = sorted(meta_plan.active_layers)
    planner_payload["meta_planner_confidence"] = meta_plan.confidence
    planner_payload["meta_planner_trigger_matches"] = list(meta_plan.trigger_matches)
    _LOG.info(
        "planner.started",
        goal=goal,
        data_type=data_type,
        dataset_name=str(payload.get("dataset_name") or "dataset_from_version").strip(),
        active_layers=sorted(meta_plan.active_layers),
        meta_planner_confidence=meta_plan.confidence,
    )
    if client and client.is_enabled():
        try:
            result = _attach_plan_metadata(
                rt._run_planner_with_llm(client, planner_payload, fallback_planner=_run_rule_based_planner),
                meta_plan=meta_plan,
            )
            _log_planner_completed("llm", result, started_at)
            return result
        except Exception as exc:
            fallback = _run_rule_based_planner(planner_payload)
            fallback["planner_type"] = "python-ai-fallback"
            fallback["planner_model"] = "rule-based-v1"
            fallback["planner_prompt_version"] = "planner-fallback-v1"
            fallback["notes"] = [f"anthropic planner fallback: {exc}"]
            _attach_plan_metadata(fallback, meta_plan=meta_plan)
            _LOG.warning(
                "planner.fallback",
                planner_model=client._config.model,
                error_category=type(exc).__name__,
                duration_ms=int((time.monotonic() - started_at) * 1000),
            )
            _log_planner_completed("fallback", fallback, started_at)
            return fallback
    result = _run_rule_based_planner(planner_payload)
    _attach_plan_metadata(result, meta_plan=meta_plan)
    _log_planner_completed("rule", result, started_at)
    return result

def _run_rule_based_planner(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "dataset_from_version").strip()
    data_type = str(payload.get("data_type") or "").strip().lower()
    goal_raw = str(payload.get("goal") or "").strip()
    goal = goal_raw.lower()

    if data_type in {"mixed", "both"}:
        sequence_name = "mixed_default"
    else:
        sequence_name = _sequence_name_from_rule_hints(data_type, goal)

    skills = planner_sequence(sequence_name)

    steps = []
    for skill_name in skills:
        step = {
            "skill_name": skill_name,
            "dataset_name": dataset_name,
            "inputs": default_inputs_for_skill(skill_name, goal=goal_raw),
        }
        steps.append(step)
    metadata = _plan_metadata(
        skills,
        active_layers=payload.get("active_layers"),
        meta_planner_confidence=str(payload.get("meta_planner_confidence") or "").strip(),
        trigger_matches=payload.get("meta_planner_trigger_matches"),
    )

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


def _plan_metadata(
    skills: list[str],
    *,
    active_layers: Any = None,
    meta_planner_confidence: str = "",
    trigger_matches: Any = None,
) -> dict[str, Any]:
    llm_stages: list[str] = []
    for skill_name in skills:
        definition = skill_definition(skill_name) or {}
        if str(definition.get("quality_tier") or "").strip() == "llm_dependent":
            llm_stages.append(skill_name)
    normalized_layers = sorted(
        {
            str(layer or "").strip()
            for layer in list(active_layers or [])
            if str(layer or "").strip()
        }
    )
    normalized_matches = [
        str(item or "").strip()
        for item in list(trigger_matches or [])
        if str(item or "").strip()
    ]
    return {
        "contains_llm_stage": bool(llm_stages),
        "llm_stages": llm_stages,
        "active_layers": normalized_layers,
        "meta_planner_confidence": meta_planner_confidence,
        "meta_planner_trigger_matches": normalized_matches,
    }


def _sequence_name_from_rule_hints(data_type: str, goal: str) -> str:
    if data_type == "unstructured" or rt._looks_unstructured(goal):
        normalized_goal = str(goal or "").strip().lower()
        for hint in planner_layer_hints():
            triggers = [str(trigger or "").strip().lower() for trigger in list(hint.get("trigger") or [])]
            if not triggers:
                continue
            if any(trigger and trigger in normalized_goal for trigger in triggers):
                sequence_name = str(hint.get("sequence_name") or "").strip()
                if sequence_name:
                    return sequence_name
        return "unstructured_default"
    return "structured_default"


def _attach_plan_metadata(result: dict[str, Any], *, meta_plan: MetaPlanResult | None = None) -> dict[str, Any]:
    plan = result.get("plan")
    if not isinstance(plan, dict):
        return result
    steps = list(plan.get("steps") or [])
    skill_names = [
        str(step.get("skill_name") or "").strip()
        for step in steps
        if isinstance(step, dict) and str(step.get("skill_name") or "").strip()
    ]
    plan["metadata"] = _plan_metadata(
        skill_names,
        active_layers=meta_plan.active_layers if meta_plan is not None else None,
        meta_planner_confidence=meta_plan.confidence if meta_plan is not None else "",
        trigger_matches=meta_plan.trigger_matches if meta_plan is not None else None,
    )
    return result


def _select_meta_plan(payload: dict[str, Any], *, anthropic_client=None) -> MetaPlanResult:
    goal = str(payload.get("goal") or "").strip()
    result = select_active_layers(goal, anthropic_client=anthropic_client)
    data_type = str(payload.get("data_type") or "").strip().lower()
    active_layers = set(result.active_layers)
    if data_type in {"structured"}:
        active_layers.add("structured")
    elif data_type in {"mixed", "both"}:
        active_layers.add("structured")
    return MetaPlanResult(
        active_layers=frozenset(sorted(active_layers)),
        confidence=result.confidence,
        trigger_matches=result.trigger_matches,
    )


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
