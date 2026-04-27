from __future__ import annotations

"""Summarize-layer skill handlers."""

from contextlib import contextmanager
from typing import Any, Iterator

from ..config import load_config
from ..obs import skill_handler
from ..runtime import artifacts as rt_artifacts
from ..skill_policy_registry import (
    load_cluster_label_policy,
    load_embedding_cluster_policy,
    load_issue_evidence_summary_policy,
)
from . import _summarize_impl as _impl
from ._policy_utils import annotate_result_policy, requested_policy_version
from .retrieve import _cluster_label_candidates_from_terms, _cluster_label_rationale

run_issue_breakdown_summary = skill_handler("python-ai")(_impl.run_issue_breakdown_summary)
run_issue_period_compare = skill_handler("python-ai")(_impl.run_issue_period_compare)
run_issue_sentiment_summary = skill_handler("python-ai")(_impl.run_issue_sentiment_summary)
run_issue_taxonomy_summary = skill_handler("python-ai")(_impl.run_issue_taxonomy_summary)
run_issue_trend_summary = skill_handler("python-ai")(_impl.run_issue_trend_summary)
run_unstructured_issue_summary = skill_handler("python-ai")(_impl.run_unstructured_issue_summary)


@contextmanager
def _bind_cluster_label_policy(policy: dict[str, Any]) -> Iterator[None]:
    policy_config = dict(policy.get("policy") or {})
    original_label_fn = _impl.rt._cluster_candidate_labels
    original_rationale_fn = _impl.rt._cluster_label_rationale
    try:
        _impl.rt._cluster_candidate_labels = lambda top_terms: _cluster_label_candidates_from_terms(top_terms, policy_config)
        _impl.rt._cluster_label_rationale = lambda top_terms: _cluster_label_rationale(top_terms, policy_config)
        yield
    finally:
        _impl.rt._cluster_candidate_labels = original_label_fn
        _impl.rt._cluster_label_rationale = original_rationale_fn


@contextmanager
def _bind_embedding_cluster_policy(policy: dict[str, Any]) -> Iterator[None]:
    original_threshold = _impl.rt.DEFAULT_CLUSTER_SIMILARITY_THRESHOLD
    try:
        _impl.rt.DEFAULT_CLUSTER_SIMILARITY_THRESHOLD = float(
            (policy.get("policy") or {}).get("default_cluster_similarity_threshold") or original_threshold
        )
        yield
    finally:
        _impl.rt.DEFAULT_CLUSTER_SIMILARITY_THRESHOLD = original_threshold


def _policy_select_evidence_candidates(policy_config: dict[str, Any], payload: dict[str, Any], normalized: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    limit = max(1, min(int(normalized.get("sample_n") or 3), int(policy_config.get("max_selected_documents") or 3)))
    priority = list(policy_config.get("selection_source_priority") or [])
    for source in priority:
        if source == "semantic_search":
            semantic_candidates = rt_artifacts._extract_semantic_candidates(payload.get("prior_artifacts"))
            if semantic_candidates:
                selected: list[dict[str, Any]] = []
                for rank, item in enumerate(semantic_candidates[:limit], start=1):
                    selected_item = {
                        "rank": rank,
                        "source_index": int(item.get("source_index") or 0),
                        "score": float(item.get("score") or 0),
                        "text": str(item.get("text") or ""),
                    }
                    rt_artifacts._copy_citation_fields(item, selected_item)
                    selected.append(selected_item)
                return selected, "semantic_search"
        elif source == "cluster_membership":
            candidates = rt_artifacts._extract_cluster_membership_candidates(payload.get("prior_artifacts"), normalized)
            if candidates:
                return list(candidates[:limit]), "cluster_membership"
        elif source == "document_sample":
            samples = rt_artifacts._extract_document_samples(payload.get("prior_artifacts"))
            if samples:
                selected = []
                for rank, item in enumerate(samples[:limit], start=1):
                    selected_item = {
                        "rank": rank,
                        "source_index": int(item.get("source_index") or 0),
                        "score": float(item.get("score") or 0),
                        "text": str(item.get("text") or ""),
                    }
                    rt_artifacts._copy_citation_fields(item, selected_item)
                    selected.append(selected_item)
                return selected, "document_sample"
        elif source == "lexical_overlap":
            documents = [item for item in rt_artifacts._iter_documents(normalized["dataset_name"], normalized["text_column"]) if item]
            ranked_documents = rt_artifacts._rank_documents(documents, normalized["query"])
            return ranked_documents[:limit], "lexical_overlap"
    documents = [item for item in rt_artifacts._iter_documents(normalized["dataset_name"], normalized["text_column"]) if item]
    ranked_documents = rt_artifacts._rank_documents(documents, normalized["query"])
    return ranked_documents[:limit], "lexical_overlap"


@contextmanager
def _bind_issue_evidence_policy(policy: dict[str, Any]) -> Iterator[None]:
    original_selector = _impl.rt._select_evidence_candidates
    policy_config = dict(policy.get("policy") or {})
    try:
        _impl.rt._select_evidence_candidates = lambda payload, normalized: _policy_select_evidence_candidates(policy_config, payload, normalized)
        yield
    finally:
        _impl.rt._select_evidence_candidates = original_selector


@skill_handler("python-ai")
def run_issue_cluster_summary(payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config()
    label_policy = load_cluster_label_policy(
        requested_policy_version(payload, "label_policy_version", "policy_version")
        or config.cluster_label_policy_version
    )
    embedding_policy = load_embedding_cluster_policy(
        requested_policy_version(payload, "cluster_policy_version")
        or config.embedding_cluster_policy_version
    )
    with _bind_cluster_label_policy(label_policy), _bind_embedding_cluster_policy(embedding_policy):
        result = _impl.run_issue_cluster_summary(payload)
    artifact = result.get("artifact") or {}
    if isinstance(artifact, dict):
        artifact["cluster_label_policy_version"] = label_policy["version"]
        artifact["cluster_label_policy_hash"] = label_policy["policy_hash"]
        artifact["embedding_cluster_policy_version"] = embedding_policy["version"]
        artifact["embedding_cluster_policy_hash"] = embedding_policy["policy_hash"]
    return result


@skill_handler("python-ai")
def run_issue_evidence_summary(payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config()
    policy = load_issue_evidence_summary_policy(
        requested_policy_version(payload, "evidence_policy_version", "policy_version")
        or config.issue_evidence_summary_policy_version
    )
    with _bind_issue_evidence_policy(policy):
        result = _impl.run_issue_evidence_summary(payload)
    artifact = result.get("artifact") or {}
    if isinstance(artifact, dict):
        artifact["policy_scope"] = "issue_evidence_summary"
    return annotate_result_policy(
        result,
        policy,
        policy_snapshot={
            "selection_source_priority": list((policy.get("policy") or {}).get("selection_source_priority") or []),
            "max_selected_documents": int((policy.get("policy") or {}).get("max_selected_documents") or 3),
        },
        note_prefix="issue_evidence_policy",
    )


@skill_handler("python-ai")
def run_evidence_pack(payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config()
    policy = load_issue_evidence_summary_policy(
        requested_policy_version(payload, "evidence_policy_version", "policy_version")
        or config.issue_evidence_summary_policy_version
    )
    with _bind_issue_evidence_policy(policy):
        result = _impl.run_evidence_pack(payload)
    artifact = result.get("artifact") or {}
    if isinstance(artifact, dict):
        artifact["policy_scope"] = "issue_evidence_summary"
    return annotate_result_policy(
        result,
        policy,
        policy_snapshot={
            "selection_source_priority": list((policy.get("policy") or {}).get("selection_source_priority") or []),
            "max_selected_documents": int((policy.get("policy") or {}).get("max_selected_documents") or 3),
        },
        note_prefix="issue_evidence_policy",
    )


__all__ = [
    "run_evidence_pack",
    "run_issue_breakdown_summary",
    "run_issue_cluster_summary",
    "run_issue_evidence_summary",
    "run_issue_period_compare",
    "run_issue_sentiment_summary",
    "run_issue_taxonomy_summary",
    "run_issue_trend_summary",
    "run_unstructured_issue_summary",
]
