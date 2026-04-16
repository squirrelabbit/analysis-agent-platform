from __future__ import annotations

from typing import Any


def requested_policy_version(payload: dict[str, Any], *keys: str) -> str:
    step = payload.get("step") or {}
    inputs = step.get("inputs") or {}
    for key in keys:
        value = str(inputs.get(key) or payload.get(key) or "").strip()
        if value:
            return value
    return ""


def payload_has_explicit_value(payload: dict[str, Any], key: str) -> bool:
    step = payload.get("step") or {}
    inputs = step.get("inputs") or {}
    if key in inputs and inputs.get(key) not in (None, ""):
        return True
    return key in payload and payload.get(key) not in (None, "")


def with_payload_defaults(payload: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    step = payload.get("step") or {}
    step_copy = dict(step)
    inputs = step.get("inputs") or {}
    inputs_copy = dict(inputs)
    changed = False
    for key, value in defaults.items():
        if payload_has_explicit_value(payload, key):
            continue
        inputs_copy[key] = value
        changed = True
    if changed:
        step_copy["inputs"] = inputs_copy
        updated["step"] = step_copy
    return updated


def annotate_result_policy(
    result: dict[str, Any],
    policy: dict[str, Any],
    *,
    policy_snapshot: dict[str, Any] | None = None,
    note_prefix: str = "policy",
) -> dict[str, Any]:
    artifact = result.get("artifact") or {}
    if isinstance(artifact, dict):
        artifact["policy_version"] = str(policy.get("version") or "").strip()
        artifact["policy_hash"] = str(policy.get("policy_hash") or "").strip()
        if policy_snapshot:
            artifact["policy_snapshot"] = dict(policy_snapshot)
    notes = result.get("notes")
    if isinstance(notes, list):
        notes.append(f"{note_prefix}_version: {policy.get('version')}")
        notes.append(f"{note_prefix}_hash: {policy.get('policy_hash')}")
    return result

