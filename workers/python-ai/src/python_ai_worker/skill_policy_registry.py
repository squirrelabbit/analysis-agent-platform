from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

DEFAULT_EMBEDDING_CLUSTER_POLICY_VERSION = "embedding-cluster-v1"
DEFAULT_CLUSTER_LABEL_POLICY_VERSION = "cluster-label-candidates-v1"
DEFAULT_ISSUE_EVIDENCE_SUMMARY_POLICY_VERSION = "issue-evidence-summary-v1"
SKILL_POLICIES_DIR_ENV = "PYTHON_AI_SKILL_POLICIES_DIR"

_POLICY_DEFAULT_GROUPS = {
    DEFAULT_EMBEDDING_CLUSTER_POLICY_VERSION: ["embedding_cluster"],
    DEFAULT_CLUSTER_LABEL_POLICY_VERSION: ["cluster_label_candidates"],
    DEFAULT_ISSUE_EVIDENCE_SUMMARY_POLICY_VERSION: ["issue_evidence_summary"],
}
_POLICY_DIR_EXCLUDE = {"README", "CHANGELOG"}
_VALID_SELECTION_SOURCES = {"semantic_search", "cluster_membership", "document_sample", "lexical_overlap"}


def _skill_policies_dir() -> Path:
    override = os.getenv(SKILL_POLICIES_DIR_ENV, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[4] / "config" / "skill_policies"


def available_skill_policy_versions() -> list[str]:
    directory = _skill_policies_dir()
    if not directory.exists():
        return []
    return sorted(
        path.stem
        for path in directory.glob("*.json")
        if path.is_file() and path.stem not in _POLICY_DIR_EXCLUDE
    )


def skill_policy_catalog() -> list[dict[str, object]]:
    catalog: list[dict[str, object]] = []
    for version in available_skill_policy_versions():
        payload = _read_policy_file(version)
        if payload is None:
            continue
        catalog.append(
            {
                "version": version,
                "skill_name": str(payload.get("skill_name") or "").strip(),
                "status": str(payload.get("status") or "active").strip(),
                "summary": str(payload.get("summary") or "").strip(),
                "default_groups": list(_POLICY_DEFAULT_GROUPS.get(version, [])),
                "policy_hash": _policy_hash(payload),
            }
        )
    return catalog


def skill_policy_status() -> dict[str, Any]:
    return {
        "source_path": str(_skill_policies_dir()),
        "available_versions": available_skill_policy_versions(),
        "default_versions": {
            "embedding_cluster": DEFAULT_EMBEDDING_CLUSTER_POLICY_VERSION,
            "cluster_label_candidates": DEFAULT_CLUSTER_LABEL_POLICY_VERSION,
            "issue_evidence_summary": DEFAULT_ISSUE_EVIDENCE_SUMMARY_POLICY_VERSION,
        },
    }


def validate_skill_policies() -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    catalog = skill_policy_catalog()
    available_versions = {item["version"] for item in catalog}
    for skill_name, version in {
        "embedding_cluster": DEFAULT_EMBEDDING_CLUSTER_POLICY_VERSION,
        "cluster_label_candidates": DEFAULT_CLUSTER_LABEL_POLICY_VERSION,
        "issue_evidence_summary": DEFAULT_ISSUE_EVIDENCE_SUMMARY_POLICY_VERSION,
    }.items():
        if version not in available_versions:
            issues.append(
                {
                    "severity": "error",
                    "code": "default_policy_missing",
                    "message": f"default policy version {version!r} for {skill_name} 이(가) 없습니다.",
                    "scope": "skill_policy",
                    "resource_ref": version,
                }
            )
            continue
        try:
            load_skill_policy(skill_name, version)
        except ValueError as exc:
            issues.append(
                {
                    "severity": "error",
                    "code": "policy_invalid",
                    "message": str(exc),
                    "scope": "skill_policy",
                    "resource_ref": version,
                }
            )
    return {
        "source_path": str(_skill_policies_dir()),
        "valid": len(issues) == 0,
        "issues": issues,
        "catalog": catalog,
    }


def load_embedding_cluster_policy(version: str = "") -> dict[str, Any]:
    return load_skill_policy("embedding_cluster", version)


def load_cluster_label_policy(version: str = "") -> dict[str, Any]:
    return load_skill_policy("cluster_label_candidates", version)


def load_issue_evidence_summary_policy(version: str = "") -> dict[str, Any]:
    return load_skill_policy("issue_evidence_summary", version)


def load_skill_policy(skill_name: str, version: str = "") -> dict[str, Any]:
    normalized_skill_name = str(skill_name or "").strip()
    if not normalized_skill_name:
        raise ValueError("skill_name is required")
    resolved_version = version.strip() or _default_policy_version(normalized_skill_name)
    payload = _read_policy_file(resolved_version)
    if payload is None:
        available = ", ".join(available_skill_policy_versions())
        raise ValueError(f"unsupported skill policy version: {resolved_version} (available: {available})")
    file_skill_name = str(payload.get("skill_name") or "").strip()
    if file_skill_name != normalized_skill_name:
        raise ValueError(f"skill policy {resolved_version} targets {file_skill_name!r}, expected {normalized_skill_name!r}")
    policy = _normalize_policy(normalized_skill_name, payload.get("policy") or {})
    return {
        "version": resolved_version,
        "skill_name": normalized_skill_name,
        "status": str(payload.get("status") or "active").strip(),
        "summary": str(payload.get("summary") or "").strip(),
        "policy": policy,
        "policy_hash": _policy_hash({"version": resolved_version, "skill_name": normalized_skill_name, "policy": policy}),
    }


def _default_policy_version(skill_name: str) -> str:
    normalized = str(skill_name or "").strip()
    if normalized == "embedding_cluster":
        return DEFAULT_EMBEDDING_CLUSTER_POLICY_VERSION
    if normalized == "cluster_label_candidates":
        return DEFAULT_CLUSTER_LABEL_POLICY_VERSION
    if normalized == "issue_evidence_summary":
        return DEFAULT_ISSUE_EVIDENCE_SUMMARY_POLICY_VERSION
    raise ValueError(f"unsupported skill policy target: {normalized}")


def _read_policy_file(version: str) -> dict[str, Any] | None:
    version_text = str(version or "").strip()
    if not version_text:
        return None
    path = _skill_policies_dir() / f"{version_text}.json"
    if not path.is_file():
        return None
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"skill policy {version_text} must be a JSON object")
    declared_version = str(parsed.get("version") or "").strip()
    if declared_version != version_text:
        raise ValueError(f"skill policy {version_text} has mismatched version field {declared_version!r}")
    return parsed


def _policy_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _normalize_policy(skill_name: str, raw_policy: Any) -> dict[str, Any]:
    if not isinstance(raw_policy, dict):
        raise ValueError(f"{skill_name} policy must be a JSON object")
    if skill_name == "embedding_cluster":
        return _normalize_embedding_cluster_policy(raw_policy)
    if skill_name == "cluster_label_candidates":
        return _normalize_cluster_label_policy(raw_policy)
    if skill_name == "issue_evidence_summary":
        return _normalize_issue_evidence_summary_policy(raw_policy)
    raise ValueError(f"unsupported skill policy target: {skill_name}")


def _normalize_embedding_cluster_policy(raw_policy: dict[str, Any]) -> dict[str, Any]:
    materialized_preference = str(raw_policy.get("materialized_preference") or "prefer_full_dataset").strip()
    if materialized_preference not in {"prefer_full_dataset", "always_try", "disabled"}:
        raise ValueError("embedding_cluster policy.materialized_preference must be one of prefer_full_dataset, always_try, disabled")
    subset_fallback_policy = str(raw_policy.get("subset_fallback_policy") or "on_demand_subset").strip()
    if subset_fallback_policy not in {"on_demand_subset", "disabled"}:
        raise ValueError("embedding_cluster policy.subset_fallback_policy must be one of on_demand_subset, disabled")
    return {
        "default_cluster_similarity_threshold": round(
            max(0.0, min(1.0, float(raw_policy.get("default_cluster_similarity_threshold") or 0.3))),
            4,
        ),
        "default_top_n": max(1, int(raw_policy.get("default_top_n") or 10)),
        "default_sample_n": max(1, int(raw_policy.get("default_sample_n") or 3)),
        "materialized_preference": materialized_preference,
        "subset_fallback_policy": subset_fallback_policy,
    }


def _normalize_cluster_label_policy(raw_policy: dict[str, Any]) -> dict[str, Any]:
    primary_joiner = raw_policy.get("primary_joiner")
    secondary_joiner = raw_policy.get("secondary_joiner")
    return {
        "default_top_n": max(1, int(raw_policy.get("default_top_n") or 10)),
        "default_sample_n": max(1, int(raw_policy.get("default_sample_n") or 3)),
        "max_candidate_labels": max(1, int(raw_policy.get("max_candidate_labels") or 3)),
        "ignore_terms": _normalize_unique_terms(raw_policy.get("ignore_terms")),
        "generic_terms": _normalize_unique_terms(raw_policy.get("generic_terms")),
        "primary_joiner": str(primary_joiner) if isinstance(primary_joiner, str) and primary_joiner != "" else " / ",
        "secondary_joiner": str(secondary_joiner) if isinstance(secondary_joiner, str) and secondary_joiner != "" else ", ",
        "fallback_label": str(raw_policy.get("fallback_label") or "기타 이슈").strip() or "기타 이슈",
    }


def _normalize_issue_evidence_summary_policy(raw_policy: dict[str, Any]) -> dict[str, Any]:
    priority = _normalize_selection_priority(raw_policy.get("selection_source_priority"))
    if "lexical_overlap" not in priority:
        priority.append("lexical_overlap")
    return {
        "selection_source_priority": priority,
        "max_selected_documents": max(1, int(raw_policy.get("max_selected_documents") or 3)),
    }


def _normalize_unique_terms(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def _normalize_selection_priority(value: Any) -> list[str]:
    if not isinstance(value, list):
        return ["semantic_search", "cluster_membership", "document_sample", "lexical_overlap"]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        source = str(item or "").strip()
        if source not in _VALID_SELECTION_SOURCES or source in seen:
            continue
        normalized.append(source)
        seen.add(source)
    return normalized or ["semantic_search", "cluster_membership", "document_sample", "lexical_overlap"]


__all__ = [
    "DEFAULT_CLUSTER_LABEL_POLICY_VERSION",
    "DEFAULT_EMBEDDING_CLUSTER_POLICY_VERSION",
    "DEFAULT_ISSUE_EVIDENCE_SUMMARY_POLICY_VERSION",
    "SKILL_POLICIES_DIR_ENV",
    "available_skill_policy_versions",
    "load_cluster_label_policy",
    "load_embedding_cluster_policy",
    "load_issue_evidence_summary_policy",
    "load_skill_policy",
    "skill_policy_catalog",
    "skill_policy_status",
    "validate_skill_policies",
]
