from __future__ import annotations

"""Retrieve-layer skill handlers and retrieval helpers.

This module is the public import surface for retrieval/search/cluster skills.
Implementation bodies live in the private ``_retrieve_impl`` module so tests
and callers can patch this module without importing a catch-all legacy file.
"""

from contextlib import contextmanager
from typing import Any, Iterator

from ..config import load_config
from ..skill_policy_registry import load_cluster_label_policy, load_embedding_cluster_policy
from . import _retrieve_impl as _impl
from ._policy_utils import annotate_result_policy, requested_policy_version, with_payload_defaults

rt = _impl.rt

_base_semantic_match = _impl._base_semantic_match
_chunk_rows_by_id = _impl._chunk_rows_by_id
_coerce_json_dict = _impl._coerce_json_dict
_dataset_version_id_from_index_ref = _impl._dataset_version_id_from_index_ref
_embedding_cluster_records = _impl._embedding_cluster_records
_embedding_cluster_records_from_pgvector = _impl._embedding_cluster_records_from_pgvector
_fnv1a_64 = _impl._fnv1a_64
_load_precomputed_cluster_artifact = _impl._load_precomputed_cluster_artifact
_lookup_pgvector_index_metadata = _impl._lookup_pgvector_index_metadata
_parse_pgvector_literal = _impl._parse_pgvector_literal
_pgvector_literal = _impl._pgvector_literal
_precomputed_cluster_matches_request = _impl._precomputed_cluster_matches_request
_project_token_counts_to_dense_vector = _impl._project_token_counts_to_dense_vector
_query_pgvector_cluster_rows = _impl._query_pgvector_cluster_rows
_query_pgvector_rows = _impl._query_pgvector_rows
_semantic_dataset_version_id = _impl._semantic_dataset_version_id
_semantic_matches_from_pgvector = _impl._semantic_matches_from_pgvector
_semantic_matches_from_sidecar = _impl._semantic_matches_from_sidecar
_semantic_query_vector = _impl._semantic_query_vector

_RETRIEVE_HELPER_NAMES = [
    "_base_semantic_match",
    "_chunk_rows_by_id",
    "_coerce_json_dict",
    "_dataset_version_id_from_index_ref",
    "_embedding_cluster_records",
    "_embedding_cluster_records_from_pgvector",
    "_fnv1a_64",
    "_load_precomputed_cluster_artifact",
    "_lookup_pgvector_index_metadata",
    "_parse_pgvector_literal",
    "_pgvector_literal",
    "_precomputed_cluster_matches_request",
    "_project_token_counts_to_dense_vector",
    "_query_pgvector_cluster_rows",
    "_query_pgvector_rows",
    "_semantic_dataset_version_id",
    "_semantic_matches_from_pgvector",
    "_semantic_matches_from_sidecar",
    "_semantic_query_vector",
]


@contextmanager
def _bind_retrieve_helpers() -> Iterator[None]:
    original = {name: getattr(_impl, name) for name in _RETRIEVE_HELPER_NAMES}
    try:
        for name in _RETRIEVE_HELPER_NAMES:
            setattr(_impl, name, globals()[name])
        yield
    finally:
        for name, value in original.items():
            setattr(_impl, name, value)


def _cluster_label_candidates_from_terms(top_terms: list[dict[str, Any]], policy_config: dict[str, Any]) -> list[str]:
    ignore_terms = set(str(item).strip() for item in list(policy_config.get("ignore_terms") or []) if str(item).strip())
    generic_terms = set(str(item).strip() for item in list(policy_config.get("generic_terms") or []) if str(item).strip())
    terms = []
    seen_terms: set[str] = set()
    for item in top_terms:
        term = str(item.get("term") or "").strip()
        if not term or term in ignore_terms or term in generic_terms or term in seen_terms:
            continue
        terms.append(term)
        seen_terms.add(term)
    labels: list[str] = []
    if len(terms) >= 2:
        labels.append(f"{terms[0]}{policy_config['primary_joiner']}{terms[1]}")
    if len(terms) >= 3:
        labels.append(policy_config["secondary_joiner"].join(terms[:3]))
    if terms:
        labels.append(terms[0])
    if not labels:
        labels.append(str(policy_config.get("fallback_label") or "기타 이슈"))
    unique: list[str] = []
    seen_labels: set[str] = set()
    max_candidate_labels = max(1, int(policy_config.get("max_candidate_labels") or 3))
    for label in labels:
        if label in seen_labels:
            continue
        unique.append(label)
        seen_labels.add(label)
        if len(unique) >= max_candidate_labels:
            break
    return unique


def _cluster_label_rationale(top_terms: list[dict[str, Any]], policy_config: dict[str, Any]) -> str:
    terms = [str(item.get("term") or "").strip() for item in top_terms if str(item.get("term") or "").strip()]
    if not terms:
        return f"대표 용어가 부족해 policy fallback label {policy_config.get('fallback_label')!r} 을(를) 사용했습니다."
    return (
        f"상위 용어 {', '.join(terms[:3])} 에서 ignore/generic term을 제거한 뒤 "
        f"policy joiner로 label 후보를 만들었습니다."
    )


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


def run_embedding_cluster(payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config()
    policy = load_embedding_cluster_policy(
        requested_policy_version(payload, "cluster_policy_version", "policy_version")
        or config.embedding_cluster_policy_version
    )
    policy_config = dict(policy.get("policy") or {})
    payload_with_defaults = with_payload_defaults(
        payload,
        {
            "cluster_similarity_threshold": policy_config["default_cluster_similarity_threshold"],
            "top_n": policy_config["default_top_n"],
            "sample_n": policy_config["default_sample_n"],
        },
    )
    with _bind_retrieve_helpers():
        result = _impl.run_embedding_cluster(payload_with_defaults)
    artifact = result.get("artifact") or {}
    if isinstance(artifact, dict):
        artifact["policy_scope"] = "embedding_cluster"
    return annotate_result_policy(
        result,
        policy,
        policy_snapshot={
            "default_cluster_similarity_threshold": policy_config["default_cluster_similarity_threshold"],
            "materialized_preference": policy_config["materialized_preference"],
            "subset_fallback_policy": policy_config["subset_fallback_policy"],
        },
        note_prefix="embedding_cluster_policy",
    )


def run_cluster_label_candidates(payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config()
    policy = load_cluster_label_policy(
        requested_policy_version(payload, "label_policy_version", "policy_version")
        or config.cluster_label_policy_version
    )
    policy_config = dict(policy.get("policy") or {})
    payload_with_defaults = with_payload_defaults(
        payload,
        {
            "top_n": policy_config["default_top_n"],
            "sample_n": policy_config["default_sample_n"],
        },
    )
    with _bind_retrieve_helpers(), _bind_cluster_label_policy(policy):
        result = _impl.run_cluster_label_candidates(payload_with_defaults)
    artifact = result.get("artifact") or {}
    if isinstance(artifact, dict):
        artifact["policy_scope"] = "cluster_label_candidates"
    return annotate_result_policy(
        result,
        policy,
        policy_snapshot={
            "fallback_label": policy_config["fallback_label"],
            "max_candidate_labels": policy_config["max_candidate_labels"],
        },
        note_prefix="cluster_label_policy",
    )


def run_semantic_search(payload: dict[str, Any]) -> dict[str, Any]:
    with _bind_retrieve_helpers():
        return _impl.run_semantic_search(payload)


__all__ = [
    "_base_semantic_match",
    "_chunk_rows_by_id",
    "_coerce_json_dict",
    "_dataset_version_id_from_index_ref",
    "_embedding_cluster_records",
    "_embedding_cluster_records_from_pgvector",
    "_fnv1a_64",
    "_load_precomputed_cluster_artifact",
    "_lookup_pgvector_index_metadata",
    "_parse_pgvector_literal",
    "_pgvector_literal",
    "_precomputed_cluster_matches_request",
    "_project_token_counts_to_dense_vector",
    "_query_pgvector_cluster_rows",
    "_query_pgvector_rows",
    "_semantic_dataset_version_id",
    "_semantic_matches_from_pgvector",
    "_semantic_matches_from_sidecar",
    "_semantic_query_vector",
    "rt",
    "run_cluster_label_candidates",
    "run_embedding_cluster",
    "run_semantic_search",
]
