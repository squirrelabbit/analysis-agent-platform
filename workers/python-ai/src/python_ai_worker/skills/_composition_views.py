from __future__ import annotations

from typing import Any

from ._contract_models import (
    validate_cluster_overview_view,
    validate_issue_overview_view,
)


def build_issue_overview_view(
    issue_summary_artifact: dict[str, Any],
    issue_evidence_artifact: dict[str, Any],
) -> dict[str, Any]:
    coverage = dict(issue_summary_artifact.get("coverage") or {})
    view = {
        "view_name": "issue_overview",
        "ranked_issues": list(issue_summary_artifact.get("ranked_issues") or []),
        "coverage": coverage,
        "summary": str(issue_evidence_artifact.get("summary") or "").strip(),
        "key_findings": list(issue_evidence_artifact.get("key_findings") or []),
        "evidence": list(issue_evidence_artifact.get("evidence") or []),
        "selection_source": str(issue_evidence_artifact.get("selection_source") or "").strip(),
        "quality_tier": str(issue_evidence_artifact.get("quality_tier") or "").strip(),
        "llm_output_parsed_strictly": bool(issue_evidence_artifact.get("llm_output_parsed_strictly")),
    }
    return validate_issue_overview_view(view).model_dump()


def build_cluster_overview_view(
    issue_cluster_summary_artifact: dict[str, Any],
    cluster_label_candidates_artifact: dict[str, Any],
    issue_evidence_artifact: dict[str, Any],
) -> dict[str, Any]:
    cluster_labels = []
    for item in list(cluster_label_candidates_artifact.get("clusters") or []):
        if not isinstance(item, dict):
            continue
        cluster_labels.append(
            {
                "cluster_id": str(item.get("cluster_id") or "").strip(),
                "label": str(item.get("label") or "").strip(),
                "candidate_labels": list(item.get("candidate_labels") or []),
            }
        )
    view = {
        "view_name": "cluster_overview",
        "ranked_issues": list(issue_cluster_summary_artifact.get("ranked_issues") or []),
        "coverage": dict(issue_cluster_summary_artifact.get("coverage") or {}),
        "summary": str(issue_evidence_artifact.get("summary") or "").strip(),
        "key_findings": list(issue_evidence_artifact.get("key_findings") or []),
        "evidence": list(issue_evidence_artifact.get("evidence") or []),
        "cluster_labels": cluster_labels,
        "selection_source": str(issue_evidence_artifact.get("selection_source") or "").strip(),
        "quality_tier": str(issue_evidence_artifact.get("quality_tier") or "").strip(),
        "llm_output_parsed_strictly": bool(issue_evidence_artifact.get("llm_output_parsed_strictly")),
    }
    return validate_cluster_overview_view(view).model_dump()


__all__ = [
    "build_cluster_overview_view",
    "build_issue_overview_view",
]
