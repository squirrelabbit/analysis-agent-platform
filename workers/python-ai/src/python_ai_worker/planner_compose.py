from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ._migration_targets import canonical_skill_name
from .skill_bundle import default_inputs_for_skill, layer_for_skill, skill_definition


@dataclass(frozen=True)
class ComposedPlan:
    steps: list[dict[str, Any]]
    active_layers: frozenset[str]


class PlannerComposeError(ValueError):
    """Raised when planner steps cannot be composed into a valid ordered plan."""


def compose_plan(
    steps: list[dict[str, Any]],
    active_layers: frozenset[str],
    *,
    goal: str = "",
) -> ComposedPlan:
    ordered = [_normalize_step(step) for step in steps if isinstance(step, dict)]
    present = {step["skill_name"] for step in ordered}
    expanded_layers = set(active_layers)

    index = 0
    while index < len(ordered):
        step = ordered[index]
        missing_dependencies = _missing_dependencies(step["skill_name"], present)
        if not missing_dependencies:
            index += 1
            continue
        inserted: list[dict[str, Any]] = []
        for dependency_name in missing_dependencies:
            inserted.extend(
                _build_dependency_chain(
                    dependency_name,
                    step,
                    goal=goal,
                    present=present,
                    depth=0,
                )
            )
        for item in inserted:
            present.add(item["skill_name"])
            layer = str(layer_for_skill(item["skill_name"]) or "").strip()
            if layer:
                expanded_layers.add(layer)
        ordered[index:index] = inserted
        index += len(inserted) + 1

    dependency_edges = _dependency_edges(ordered)
    _assert_no_cycles(dependency_edges)
    sorted_steps = _sort_by_precedence(ordered, dependency_edges)
    return ComposedPlan(
        steps=sorted_steps,
        active_layers=frozenset(sorted(expanded_layers)),
    )


def _normalize_step(step: dict[str, Any]) -> dict[str, Any]:
    return {
        "skill_name": canonical_skill_name(str(step.get("skill_name") or "").strip()),
        "dataset_name": str(step.get("dataset_name") or "dataset_from_version").strip() or "dataset_from_version",
        "inputs": dict(step.get("inputs") or {}),
    }


def _missing_dependencies(skill_name: str, present: set[str]) -> list[str]:
    definition = skill_definition(skill_name) or {}
    required = _string_list(definition.get("requires_prior_skills"))
    missing = [name for name in required if canonical_skill_name(name) not in present]
    if missing:
        return missing

    required_any = _string_list(definition.get("requires_any_prior_skills"))
    if required_any and not any(canonical_skill_name(name) in present for name in required_any):
        return [required_any[0]]
    return []


def _build_dependency_chain(
    skill_name: str,
    source_step: dict[str, Any],
    *,
    goal: str,
    present: set[str],
    depth: int,
) -> list[dict[str, Any]]:
    if depth > 1:
        raise PlannerComposeError(f"dependency chain is deeper than one level for {skill_name}")
    canonical_name = canonical_skill_name(skill_name)
    if canonical_name in present:
        return []
    if not skill_definition(canonical_name):
        raise PlannerComposeError(f"unknown dependency skill: {canonical_name}")

    dependency_step = {
        "skill_name": canonical_name,
        "dataset_name": source_step["dataset_name"],
        "inputs": default_inputs_for_skill(canonical_name, goal=goal),
    }

    nested_missing = _missing_dependencies(canonical_name, present)
    chain: list[dict[str, Any]] = []
    for nested in nested_missing:
        chain.extend(
            _build_dependency_chain(
                nested,
                dependency_step,
                goal=goal,
                present=present | {item["skill_name"] for item in chain},
                depth=depth + 1,
            )
        )
    chain.append(dependency_step)
    return chain


def _dependency_edges(steps: list[dict[str, Any]]) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = {step["skill_name"]: set() for step in steps}
    present = {step["skill_name"] for step in steps}
    for step in steps:
        skill_name = step["skill_name"]
        definition = skill_definition(skill_name) or {}
        for dependency_name in _string_list(definition.get("requires_prior_skills")):
            canonical_dependency = canonical_skill_name(dependency_name)
            if canonical_dependency in present:
                edges[canonical_dependency].add(skill_name)
        required_any = _string_list(definition.get("requires_any_prior_skills"))
        for dependency_name in required_any:
            canonical_dependency = canonical_skill_name(dependency_name)
            if canonical_dependency in present:
                edges[canonical_dependency].add(skill_name)
                break
    return edges


def _assert_no_cycles(edges: dict[str, set[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise PlannerComposeError(f"planner dependency cycle detected at {node}")
        visiting.add(node)
        for target in edges.get(node, set()):
            visit(target)
        visiting.remove(node)
        visited.add(node)

    for node in edges:
        visit(node)


def _sort_by_precedence(
    steps: list[dict[str, Any]],
    dependency_edges: dict[str, set[str]],
) -> list[dict[str, Any]]:
    original_index = {step["skill_name"]: index for index, step in enumerate(steps)}
    incoming: dict[str, set[str]] = {step["skill_name"]: set() for step in steps}
    for source, targets in dependency_edges.items():
        for target in targets:
            incoming.setdefault(target, set()).add(source)
    queue = [step["skill_name"] for step in steps if not incoming.get(step["skill_name"])]
    ordered_names: list[str] = []

    def sort_key(skill_name: str) -> int:
        # Preserve the canonical sequence family unless a hard dependency forces reordering.
        return original_index[skill_name]

    while queue:
        queue.sort(key=sort_key)
        current = queue.pop(0)
        ordered_names.append(current)
        for target in dependency_edges.get(current, set()):
            blockers = incoming.get(target)
            if blockers is None:
                continue
            blockers.discard(current)
            if not blockers:
                queue.append(target)

    if len(ordered_names) != len(steps):
        raise PlannerComposeError("planner compose could not produce a complete order")
    by_name = {step["skill_name"]: step for step in steps}
    return [by_name[name] for name in ordered_names]


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
    "ComposedPlan",
    "PlannerComposeError",
    "compose_plan",
]
