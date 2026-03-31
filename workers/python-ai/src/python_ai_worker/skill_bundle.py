from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any


def bundle_version() -> str:
    return str(skill_bundle().get("version") or "").strip()


def capability_skills() -> list[dict[str, Any]]:
    skills = []
    for skill in list(skill_bundle().get("skills") or []):
        if isinstance(skill, dict):
            skills.append(skill)
    return skills


def plan_skill_names() -> list[str]:
    names: list[str] = []
    for skill in capability_skills():
        if bool(skill.get("plan_enabled")):
            name = str(skill.get("name") or "").strip()
            if name:
                names.append(name)
    return names


def skill_definition(name: str) -> dict[str, Any] | None:
    return dict(skills_by_name().get(str(name).strip()) or {}) or None


def task_path_for_skill(name: str) -> str | None:
    skill = skill_definition(name)
    if not skill:
        return None
    task_path = str(skill.get("task_path") or "").strip()
    return task_path or None


def default_plan_skills(data_type: str) -> list[str]:
    default_plans = skill_bundle().get("default_plans") or {}
    selected = list(default_plans.get(str(data_type).strip()) or [])
    if not selected:
        selected = list(default_plans.get("structured") or [])
    return [str(name).strip() for name in selected if str(name).strip()]


def planner_sequence(name: str) -> list[str]:
    sequences = skill_bundle().get("planner_sequences") or {}
    selected = list(sequences.get(str(name).strip()) or [])
    return [str(skill_name).strip() for skill_name in selected if str(skill_name).strip()]


def default_inputs_for_skill(skill_name: str, *, goal: str = "") -> dict[str, Any]:
    skill = skill_definition(skill_name)
    if not skill:
        return {}
    inputs = dict(skill.get("default_inputs") or {})
    goal_input = str(skill.get("goal_input") or "").strip()
    if goal_input and goal:
        inputs[goal_input] = goal
    return inputs


def capability_payload_data() -> dict[str, Any]:
    return {
        "skill_bundle_version": bundle_version(),
        "capabilities": [
            {
                "name": str(skill.get("name") or "").strip(),
                "description": str(skill.get("description") or "").strip(),
            }
            for skill in capability_skills()
            if str(skill.get("name") or "").strip()
        ],
    }


def skill_bundle() -> dict[str, Any]:
    path = resolve_bundle_path()
    return _load_bundle(str(path))


@lru_cache(maxsize=None)
def _load_bundle(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        parsed = json.load(handle)
    if not isinstance(parsed, dict):
        raise ValueError(f"invalid skill bundle: {path}")
    return parsed


@lru_cache(maxsize=None)
def skills_by_name() -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for skill in capability_skills():
        name = str(skill.get("name") or "").strip()
        if not name:
            continue
        mapping[name] = dict(skill)
    return mapping


def resolve_bundle_path() -> Path:
    override = os.getenv("SKILL_BUNDLE_PATH", "").strip()
    root = detect_workspace_root()
    if not override:
        return root / "config" / "skill_bundle.json"
    path = Path(override)
    if path.is_absolute():
        return path
    return root / path


def detect_workspace_root() -> Path:
    candidates = [Path.cwd(), Path(__file__).resolve().parent]
    for start in candidates:
        current = start
        while True:
            if (current / "config" / "skill_bundle.json").is_file():
                return current
            if (current / "compose.dev.yml").is_file() or (current / "AGENTS.md").is_file():
                return current
            if current.parent == current:
                break
            current = current.parent
    return Path.cwd()
