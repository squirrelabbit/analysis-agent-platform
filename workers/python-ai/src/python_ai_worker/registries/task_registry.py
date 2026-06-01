from __future__ import annotations

"""task_registry.json loader — control plane 내부 실행 task 카탈로그.

ADR-017에 따라 *내부 실행 task* 카탈로그(`task_registry.json`)만 담당.
δ-4 (5/21)로 plan skill 카탈로그(`skill_bundle.json`)는 삭제됐다 —
plan은 planner가 LLM으로 생성하므로 고정 카탈로그가 필요하지 않다.
"""

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any


def task_registry() -> dict[str, Any]:
    path = resolve_registry_path()
    return _load_registry(str(path))


def registry_version() -> str:
    return str(task_registry().get("version") or "").strip()


def internal_tasks() -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for task in list(task_registry().get("tasks") or []):
        if isinstance(task, dict):
            tasks.append(task)
    return tasks


def internal_task_names() -> list[str]:
    names: list[str] = []
    for task in internal_tasks():
        name = str(task.get("task_name") or "").strip()
        if name:
            names.append(name)
    return names


def task_definition(name: str) -> dict[str, Any] | None:
    return dict(_tasks_by_name().get(str(name).strip()) or {}) or None


def task_path_for(name: str) -> str | None:
    task = task_definition(name)
    if not task:
        return None
    path = str(task.get("task_path") or "").strip()
    return path or None


@lru_cache(maxsize=None)
def _load_registry(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        parsed = json.load(handle)
    if not isinstance(parsed, dict):
        raise ValueError(f"invalid task registry: {path}")
    return parsed


@lru_cache(maxsize=None)
def _tasks_by_name() -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for task in internal_tasks():
        name = str(task.get("task_name") or "").strip()
        if not name:
            continue
        mapping[name] = dict(task)
    return mapping


def resolve_registry_path() -> Path:
    override = os.getenv("TASK_REGISTRY_PATH", "").strip()
    root = detect_workspace_root()
    if not override:
        return root / "config" / "task_registry.json"
    path = Path(override)
    if path.is_absolute():
        return path
    return root / path


def detect_workspace_root() -> Path:
    """Find repo root that contains ``config/task_registry.json``.

    Falls back to compose.dev.yml / AGENTS.md markers when running from
    a nested working directory.
    """
    cwd = Path.cwd()
    current = cwd
    while True:
        if (
            (current / "config" / "task_registry.json").is_file()
            or (current / "compose.dev.yml").is_file()
            or (current / "AGENTS.md").is_file()
        ):
            return current
        if current.parent == current:
            return cwd
        current = current.parent
