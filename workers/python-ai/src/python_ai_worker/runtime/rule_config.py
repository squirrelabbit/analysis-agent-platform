from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..config import load_config
from .constants import (
    DEFAULT_GARBAGE_RULE_NAMES,
    DEFAULT_PREPARE_REGEX_RULE_NAMES,
    DEFAULT_TAXONOMY_RULES,
    GARBAGE_RULES,
    PREPARE_REGEX_RULES,
)


def rule_config_status() -> dict[str, Any]:
    config = load_config()
    return {
        "rule_config_path": config.rule_config_path,
        "rule_config_inline": bool(config.rule_config_json),
    }


def resolve_prepare_regex_rules() -> dict[str, dict[str, Any]]:
    overlay = _load_layered_rule_config()
    merged: dict[str, dict[str, Any]] = {
        name: {
            "description": str(rule.get("description") or "").strip(),
            "patterns": [str(pattern) for pattern in list(rule.get("patterns") or []) if str(pattern).strip()],
            "replacement": str(rule.get("replacement") or " "),
        }
        for name, rule in PREPARE_REGEX_RULES.items()
    }
    for name, rule in _normalize_prepare_rule_map(overlay.get("prepare_regex_rules")).items():
        merged[name] = rule
    return merged


def resolve_default_prepare_regex_rule_names() -> list[str]:
    rules = resolve_prepare_regex_rules()
    configured = _normalize_rule_name_list(_load_layered_rule_config().get("default_prepare_regex_rule_names"), rules)
    return configured or list(DEFAULT_PREPARE_REGEX_RULE_NAMES)


def resolve_garbage_rules() -> dict[str, dict[str, Any]]:
    overlay = _load_layered_rule_config()
    merged: dict[str, dict[str, Any]] = {
        name: {
            "description": str(rule.get("description") or "").strip(),
            "patterns": [str(pattern) for pattern in list(rule.get("patterns") or []) if str(pattern).strip()],
        }
        for name, rule in GARBAGE_RULES.items()
    }
    for name, rule in _normalize_garbage_rule_map(overlay.get("garbage_rules")).items():
        merged[name] = rule
    return merged


def resolve_default_garbage_rule_names() -> list[str]:
    rules = resolve_garbage_rules()
    configured = _normalize_rule_name_list(_load_layered_rule_config().get("default_garbage_rule_names"), rules)
    return configured or list(DEFAULT_GARBAGE_RULE_NAMES)


def resolve_taxonomy_rules() -> dict[str, dict[str, Any]]:
    normalized_defaults = _normalize_taxonomy_rule_map(DEFAULT_TAXONOMY_RULES)
    overlay = _normalize_taxonomy_rule_map(_load_layered_rule_config().get("taxonomy_rules"))
    merged = dict(normalized_defaults)
    merged.update(overlay)
    return merged or dict(normalized_defaults)


def _load_layered_rule_config() -> dict[str, Any]:
    config = load_config()
    merged: dict[str, Any] = {}
    if config.rule_config_path:
        merged = _merge_nested_dicts(merged, _read_rule_config_file(config.rule_config_path))
    if config.rule_config_json:
        merged = _merge_nested_dicts(merged, _parse_rule_config_json(config.rule_config_json))
    return merged


def _read_rule_config_file(path: str) -> dict[str, Any]:
    if not path:
        return {}
    content = Path(path).read_text(encoding="utf-8")
    return _parse_rule_config_json(content)


def _parse_rule_config_json(raw: str) -> dict[str, Any]:
    if not str(raw or "").strip():
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("rule config must be a JSON object")
    return parsed


def _merge_nested_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_nested_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def _normalize_prepare_rule_map(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for rule_name, raw_rule in value.items():
        name = str(rule_name or "").strip()
        if not name or not isinstance(raw_rule, dict):
            continue
        patterns = [str(pattern) for pattern in list(raw_rule.get("patterns") or []) if str(pattern).strip()]
        if not patterns:
            continue
        normalized[name] = {
            "description": str(raw_rule.get("description") or name).strip() or name,
            "patterns": patterns,
            "replacement": str(raw_rule.get("replacement") or " "),
        }
    return normalized


def _normalize_garbage_rule_map(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for rule_name, raw_rule in value.items():
        name = str(rule_name or "").strip()
        if not name or not isinstance(raw_rule, dict):
            continue
        patterns = [str(pattern) for pattern in list(raw_rule.get("patterns") or []) if str(pattern).strip()]
        normalized[name] = {
            "description": str(raw_rule.get("description") or name).strip() or name,
            "patterns": patterns,
        }
    return normalized


def _normalize_taxonomy_rule_map(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for taxonomy_id, raw_rule in value.items():
        taxonomy_key = str(taxonomy_id).strip()
        if not taxonomy_key:
            continue
        label = taxonomy_key
        patterns: list[str] = []
        if isinstance(raw_rule, dict):
            label = str(raw_rule.get("label") or taxonomy_key).strip() or taxonomy_key
            patterns = [str(item).strip().lower() for item in list(raw_rule.get("patterns") or []) if str(item).strip()]
        elif isinstance(raw_rule, list):
            patterns = [str(item).strip().lower() for item in raw_rule if str(item).strip()]
        if not patterns:
            continue
        normalized[taxonomy_key] = {
            "label": label,
            "patterns": patterns,
        }
    return normalized


def _normalize_rule_name_list(value: Any, available_rules: dict[str, dict[str, Any]]) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        name = str(item or "").strip()
        if not name or name not in available_rules or name in normalized:
            continue
        normalized.append(name)
    return normalized


__all__ = [
    "resolve_default_garbage_rule_names",
    "resolve_default_prepare_regex_rule_names",
    "resolve_garbage_rules",
    "resolve_prepare_regex_rules",
    "resolve_taxonomy_rules",
    "rule_config_status",
]
