from __future__ import annotations

"""Retrieve-layer skill handlers and retrieval helpers.

This module is the primary import surface for retrieval/search/cluster skills.
Legacy implementation bodies still live in ``_legacy_support_impl`` during the
refactor, but helper globals are rebound on each call so tests and future
patching can target this module instead of the legacy file.
"""

from contextlib import contextmanager
from typing import Any, Iterator

from . import _legacy_support_impl as _impl

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

_LEGACY_HELPER_NAMES = [
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
def _bind_legacy_helpers() -> Iterator[None]:
    original = {name: getattr(_impl, name) for name in _LEGACY_HELPER_NAMES}
    try:
        for name in _LEGACY_HELPER_NAMES:
            setattr(_impl, name, globals()[name])
        yield
    finally:
        for name, value in original.items():
            setattr(_impl, name, value)


def run_embedding_cluster(payload: dict[str, Any]) -> dict[str, Any]:
    with _bind_legacy_helpers():
        return _impl.run_embedding_cluster(payload)


def run_cluster_label_candidates(payload: dict[str, Any]) -> dict[str, Any]:
    with _bind_legacy_helpers():
        return _impl.run_cluster_label_candidates(payload)


def run_semantic_search(payload: dict[str, Any]) -> dict[str, Any]:
    with _bind_legacy_helpers():
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
