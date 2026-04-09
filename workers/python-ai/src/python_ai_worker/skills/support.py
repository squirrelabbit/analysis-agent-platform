from __future__ import annotations

"""Compatibility shim for legacy support imports.

Primary runtime imports should use preprocess/aggregate/retrieve modules.
"""

from .aggregate import (
    run_dictionary_tagging,
    run_keyword_frequency,
    run_meta_group_count,
    run_noun_frequency,
    run_time_bucket_count,
)
from .preprocess import (
    run_deduplicate_documents,
    run_document_filter,
    run_document_sample,
    run_garbage_filter,
    run_sentence_split,
)
from .retrieve import (
    _base_semantic_match,
    _chunk_rows_by_id,
    _coerce_json_dict,
    _dataset_version_id_from_index_ref,
    _embedding_cluster_records,
    _embedding_cluster_records_from_pgvector,
    _fnv1a_64,
    _load_precomputed_cluster_artifact,
    _lookup_pgvector_index_metadata,
    _parse_pgvector_literal,
    _pgvector_literal,
    _precomputed_cluster_matches_request,
    _project_token_counts_to_dense_vector,
    _query_pgvector_cluster_rows,
    _query_pgvector_rows,
    _semantic_dataset_version_id,
    _semantic_matches_from_pgvector,
    _semantic_matches_from_sidecar,
    _semantic_query_vector,
    run_cluster_label_candidates,
    run_embedding_cluster,
    run_semantic_search,
)

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
    "run_cluster_label_candidates",
    "run_deduplicate_documents",
    "run_dictionary_tagging",
    "run_document_filter",
    "run_document_sample",
    "run_embedding_cluster",
    "run_garbage_filter",
    "run_keyword_frequency",
    "run_meta_group_count",
    "run_noun_frequency",
    "run_semantic_search",
    "run_sentence_split",
    "run_time_bucket_count",
]
