from __future__ import annotations

"""Private retrieve-layer skill implementations and helpers."""

import json
import math
import os
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .. import runtime as rt

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional for local unit test fallback
    psycopg = None
    dict_row = None

def run_embedding_cluster(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_embedding_cluster_payload(payload)
    inputs = normalized["step"].get("inputs") or {}
    prior_artifacts = payload.get("prior_artifacts")
    prior_artifact_items = rt._iter_prior_artifacts(prior_artifacts)
    has_prior_artifacts = len(prior_artifact_items) > 0
    precomputed = None
    if normalized["cluster_ref"] and not has_prior_artifacts:
        precomputed = _load_precomputed_cluster_artifact(normalized["cluster_ref"])
        if precomputed is not None and _precomputed_cluster_matches_request(precomputed, normalized):
            artifact = dict(precomputed)
            artifact["step_id"] = normalized["step"].get("step_id")
            artifact["dataset_name"] = normalized["dataset_name"]
            artifact["cluster_execution_mode"] = str(artifact.get("cluster_execution_mode") or "materialized_full_dataset").strip() or "materialized_full_dataset"
            artifact["cluster_materialization_scope"] = str(artifact.get("cluster_materialization_scope") or "full_dataset").strip() or "full_dataset"
            artifact["cluster_materialized_ref_used"] = True
            artifact["cluster_fallback_reason"] = ""
            artifact["cluster_ref"] = normalized["cluster_ref"]
            artifact["cluster_format"] = normalized["cluster_format"] or "json"
            summary = dict(artifact.get("summary") or {})
            summary["cluster_similarity_threshold"] = normalized["cluster_similarity_threshold"]
            summary["top_n"] = normalized["top_n"]
            summary["sample_n"] = normalized["sample_n"]
            artifact["summary"] = summary
            return {
                "notes": [
                    f"embedding_cluster loaded precomputed cluster artifact",
                    f"cluster_ref: {normalized['cluster_ref']}",
                ],
                "artifact": artifact,
            }
    cluster_execution_mode = "on_demand_full_dataset"
    cluster_materialization_scope = "full_dataset"
    cluster_materialized_ref_used = False
    cluster_fallback_reason = ""
    if has_prior_artifacts:
        cluster_execution_mode = "on_demand_subset_fallback"
        cluster_materialization_scope = "subset_selection"
        cluster_fallback_reason = "prior_artifacts_present"
    elif normalized["cluster_ref"]:
        cluster_fallback_reason = "cluster_request_mismatch" if precomputed is not None else "cluster_artifact_unavailable"
    records, source_backend, source_ref = _embedding_cluster_records(
        dataset_version_id=_semantic_dataset_version_id(payload, inputs),
        embedding_index_ref=normalized["embedding_index_ref"],
        embedding_uri=normalized["embedding_uri"],
        prior_artifacts=prior_artifacts,
        chunk_ref=normalized["chunk_ref"],
        chunk_format=normalized["chunk_format"],
    )
    clusters = rt._cluster_embedding_records(records, normalized["cluster_similarity_threshold"], normalized["sample_n"], normalized["top_n"])
    similarity_backends = {
        str(cluster.get("similarity_backend") or "").strip()
        for cluster in clusters
        if str(cluster.get("similarity_backend") or "").strip()
    }
    similarity_backend = "mixed" if len(similarity_backends) > 1 else next(iter(similarity_backends), "token-overlap")
    noise_count = len([cluster for cluster in clusters if int(cluster["document_count"]) == 1])
    notes = [
        f"embedding_cluster built {len(clusters)} clusters",
        f"embedding source backend: {source_backend}",
        f"embedding source: {source_ref}",
        f"similarity_backend: {similarity_backend}",
        f"cluster_similarity_threshold: {normalized['cluster_similarity_threshold']}",
        f"cluster_execution_mode: {cluster_execution_mode}",
    ]
    if cluster_fallback_reason:
        notes.append(f"cluster_fallback_reason: {cluster_fallback_reason}")
    return {
        "notes": notes,
        "artifact": {
            "skill_name": "embedding_cluster",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": normalized["embedding_uri"],
            "embedding_index_ref": normalized["embedding_index_ref"],
            "embedding_source_backend": source_backend,
            "cluster_execution_mode": cluster_execution_mode,
            "cluster_materialization_scope": cluster_materialization_scope,
            "cluster_materialized_ref_used": cluster_materialized_ref_used,
            "cluster_fallback_reason": cluster_fallback_reason,
            "cluster_ref": normalized["cluster_ref"],
            "cluster_format": normalized["cluster_format"],
            "cluster_summary_ref": normalized["cluster_ref"],
            "cluster_summary_format": normalized["cluster_format"],
            "cluster_membership_ref": "",
            "cluster_membership_format": "",
            "chunk_ref": normalized["chunk_ref"],
            "chunk_format": normalized["chunk_format"],
            "summary": {
                "cluster_count": len(clusters),
                "clustered_document_count": len(records),
                "noise_count": noise_count,
                "similarity_backend": similarity_backend,
                "cluster_similarity_threshold": normalized["cluster_similarity_threshold"],
                "top_n": normalized["top_n"],
                "sample_n": normalized["sample_n"],
                "embedding_source_backend": source_backend,
            },
            "clusters": clusters,
        },
    }

def _load_precomputed_cluster_artifact(cluster_ref: str) -> dict[str, Any] | None:
    path = Path(cluster_ref)
    if not path.exists():
        return None
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def _precomputed_cluster_matches_request(artifact: dict[str, Any], normalized: dict[str, Any]) -> bool:
    summary = artifact.get("summary") or {}
    if not isinstance(summary, dict):
        return False
    threshold = float(summary.get("cluster_similarity_threshold") or 0.3)
    top_n = int(summary.get("top_n") or normalized.get("top_n") or 10)
    sample_n = int(summary.get("sample_n") or normalized.get("sample_n") or 3)
    return round(threshold, 4) == round(float(normalized["cluster_similarity_threshold"]), 4) and top_n == int(normalized["top_n"]) and sample_n == int(normalized["sample_n"])


def run_cluster_label_candidates(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_cluster_label_payload(payload)
    prior = rt._find_prior_artifact(payload.get("prior_artifacts"), "embedding_cluster")
    if prior is None:
        raise ValueError("cluster_label_candidates requires embedding_cluster prior artifact")
    clusters = []
    for cluster in list((prior or {}).get("clusters") or []):
        if not isinstance(cluster, dict):
            continue
        top_terms = list(cluster.get("top_terms") or [])
        candidate_labels = rt._cluster_candidate_labels(top_terms)
        clusters.append(
            {
                "cluster_id": cluster.get("cluster_id"),
                "document_count": int(cluster.get("document_count") or 0),
                "label": candidate_labels[0] if candidate_labels else "기타 이슈",
                "candidate_labels": candidate_labels,
                "top_terms": top_terms[: normalized["top_n"]],
                "samples": list(cluster.get("sample_documents") or [])[: normalized["sample_n"]],
                "rationale": rt._cluster_label_rationale(top_terms),
            }
        )

    return {
        "notes": [
            f"cluster_label_candidates generated labels for {len(clusters)} clusters",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": {
            "skill_name": "cluster_label_candidates",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "cluster_execution_mode": str((prior or {}).get("cluster_execution_mode") or "").strip(),
            "cluster_materialization_scope": str((prior or {}).get("cluster_materialization_scope") or "").strip(),
            "cluster_materialized_ref_used": bool((prior or {}).get("cluster_materialized_ref_used")),
            "cluster_fallback_reason": str((prior or {}).get("cluster_fallback_reason") or "").strip(),
            "cluster_ref": str((prior or {}).get("cluster_ref") or "").strip(),
            "cluster_format": str((prior or {}).get("cluster_format") or "").strip(),
            "cluster_summary_ref": str((prior or {}).get("cluster_summary_ref") or "").strip(),
            "cluster_summary_format": str((prior or {}).get("cluster_summary_format") or "").strip(),
            "cluster_membership_ref": str((prior or {}).get("cluster_membership_ref") or "").strip(),
            "cluster_membership_format": str((prior or {}).get("cluster_membership_format") or "").strip(),
            "summary": {
                "cluster_count": len(clusters),
                "label_rule": "top_terms",
            },
            "clusters": clusters,
        },
    }


def run_semantic_search(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    inputs = (normalized["step"].get("inputs") or {})
    dataset_version_id = _semantic_dataset_version_id(payload, inputs)
    embedding_index_ref = str(inputs.get("embedding_index_ref") or payload.get("embedding_index_ref") or "").strip()
    embedding_uri = str(inputs.get("embedding_uri") or payload.get("embedding_uri") or "").strip()
    embedding_model = str(inputs.get("embedding_model") or payload.get("embedding_model") or "").strip()
    if not embedding_uri and not embedding_index_ref:
        raise ValueError("embedding_uri or embedding_index_ref is required")
    chunk_ref = str(inputs.get("chunk_ref") or payload.get("chunk_ref") or "").strip()
    if not chunk_ref and embedding_uri.endswith(".jsonl"):
        chunk_ref = f"{embedding_uri[:-len('.jsonl')]}.chunks.parquet"
    chunk_format = str(inputs.get("chunk_format") or payload.get("chunk_format") or "").strip()
    if not chunk_format and chunk_ref.endswith(".parquet"):
        chunk_format = "parquet"

    query_counts = Counter(rt._tokenize(normalized["query"]))
    retrieval_backend = "jsonl-sidecar"
    note_prefix = "semantic search executed with precomputed embeddings"
    matches = _semantic_matches_from_pgvector(
        dataset_version_id=dataset_version_id,
        embedding_index_ref=embedding_index_ref,
        embedding_model=embedding_model,
        query=normalized["query"],
        query_counts=query_counts,
        sample_n=normalized["sample_n"],
        fallback_chunk_ref=chunk_ref,
        fallback_chunk_format=chunk_format,
    )
    if matches is not None:
        retrieval_backend = "pgvector"
        note_prefix = "semantic search executed with pgvector index"
    else:
        matches = _semantic_matches_from_sidecar(
            embedding_uri=embedding_uri,
            query_counts=query_counts,
            prior_artifacts=payload.get("prior_artifacts"),
            chunk_ref=chunk_ref,
            chunk_format=chunk_format,
        )

    matches.sort(key=lambda item: (-item["score"], item["source_index"]))
    limited = matches[: normalized["sample_n"]]
    for rank, item in enumerate(limited, start=1):
        item["rank"] = rank
    citation_mode = "chunk" if any(str(item.get("chunk_id") or "").strip() for item in limited) else "row"
    source_ref = embedding_uri
    if retrieval_backend == "pgvector":
        source_ref = embedding_index_ref or f"pgvector://embedding_index_chunks?dataset_version_id={dataset_version_id}"

    return {
        "notes": [
            note_prefix,
            f"embedding source: {source_ref}",
            f"retrieval_backend: {retrieval_backend}",
            f"citation_mode: {citation_mode}",
        ],
        "artifact": {
            "skill_name": "semantic_search",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": embedding_uri,
            "embedding_index_ref": embedding_index_ref,
            "retrieval_backend": retrieval_backend,
            "query": normalized["query"],
            "citation_mode": citation_mode,
            "chunk_ref": chunk_ref,
            "chunk_format": chunk_format,
            "summary": {
                "candidate_count": len(matches),
                "match_count": len(limited),
                "chunk_match_count": len([item for item in limited if str(item.get("chunk_id") or "").strip()]),
                "retrieval_backend": retrieval_backend,
                "citation_mode": citation_mode,
            },
            "matches": limited,
        },
    }


def _embedding_cluster_records(
    *,
    dataset_version_id: str,
    embedding_index_ref: str,
    embedding_uri: str,
    prior_artifacts: Any,
    chunk_ref: str,
    chunk_format: str,
) -> tuple[list[dict[str, Any]], str, str]:
    pgvector_records = _embedding_cluster_records_from_pgvector(
        dataset_version_id=dataset_version_id,
        embedding_index_ref=embedding_index_ref,
        prior_artifacts=prior_artifacts,
        fallback_chunk_ref=chunk_ref,
        fallback_chunk_format=chunk_format,
    )
    if pgvector_records is not None:
        source_ref = embedding_index_ref or f"pgvector://embedding_index_chunks?dataset_version_id={dataset_version_id}"
        return pgvector_records, "pgvector", source_ref
    if embedding_uri:
        return rt._selected_embedding_records(embedding_uri, prior_artifacts), "jsonl-sidecar", embedding_uri
    return [], "pgvector", embedding_index_ref or dataset_version_id


def _embedding_cluster_records_from_pgvector(
    *,
    dataset_version_id: str,
    embedding_index_ref: str,
    prior_artifacts: Any,
    fallback_chunk_ref: str,
    fallback_chunk_format: str,
) -> list[dict[str, Any]] | None:
    if not dataset_version_id:
        dataset_version_id = _dataset_version_id_from_index_ref(embedding_index_ref)
    if not dataset_version_id:
        return None
    rows = _query_pgvector_cluster_rows(dataset_version_id)
    if rows is None:
        return None
    if not rows:
        return []
    selected_indices = rt._selected_source_indices(prior_artifacts)
    chunk_lookup = _chunk_rows_by_id(rows, fallback_chunk_ref)
    records: list[dict[str, Any]] = []
    for row in rows:
        source_index = int(row.get("source_row_index") or 0)
        if selected_indices is not None and source_index not in selected_indices:
            continue
        chunk_id = str(row.get("chunk_id") or "").strip()
        chunk_row = chunk_lookup.get(chunk_id, {})
        chunk_text = str(chunk_row.get("chunk_text") or "").strip()
        token_counts = Counter(rt._tokenize(chunk_text))
        embedding_model = str(row.get("embedding_model") or "").strip()
        dense_vector: list[float] = []
        if rt._looks_dense_embedding_model(embedding_model):
            dense_vector = _parse_pgvector_literal(row.get("embedding_literal"))
        if not token_counts and not dense_vector:
            continue
        record = {
            "source_index": source_index,
            "row_id": str(row.get("row_id") or chunk_row.get("row_id") or "").strip(),
            "chunk_id": chunk_id or str(chunk_row.get("chunk_id") or "").strip(),
            "chunk_index": int(row.get("chunk_index") or chunk_row.get("chunk_index") or 0),
            "text": chunk_text,
            "token_counts": dict(token_counts),
            "norm": rt._vector_norm(token_counts),
            "chunk_ref": str(row.get("chunk_ref") or fallback_chunk_ref or "").strip(),
            "chunk_format": fallback_chunk_format or ("parquet" if str(row.get("chunk_ref") or fallback_chunk_ref or "").endswith(".parquet") else ""),
        }
        if dense_vector:
            record["embedding"] = dense_vector
            record["embedding_dim"] = int(row.get("vector_dim") or len(dense_vector))
            record["embedding_provider"] = "pgvector"
        records.append(record)
    return records or None


def _semantic_matches_from_sidecar(
    *,
    embedding_uri: str,
    query_counts: Counter[str],
    prior_artifacts: Any,
    chunk_ref: str,
    chunk_format: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for record in rt._selected_embedding_records(embedding_uri, prior_artifacts):
        score = rt._cosine_similarity(query_counts, record.get("token_counts") or {}, float(record.get("norm") or 0))
        matches.append(
            _base_semantic_match(
                source_index=record.get("source_index"),
                score=score,
                text=str(record.get("text") or "")[:240],
                row_id=record.get("row_id"),
                chunk_id=record.get("chunk_id"),
                chunk_index=record.get("chunk_index"),
                char_start=record.get("char_start"),
                char_end=record.get("char_end"),
                chunk_ref=chunk_ref,
                chunk_format=chunk_format,
            )
        )
    return matches


def _semantic_matches_from_pgvector(
    *,
    dataset_version_id: str,
    embedding_index_ref: str,
    embedding_model: str,
    query: str,
    query_counts: Counter[str],
    sample_n: int,
    fallback_chunk_ref: str,
    fallback_chunk_format: str,
) -> list[dict[str, Any]] | None:
    if not dataset_version_id:
        dataset_version_id = _dataset_version_id_from_index_ref(embedding_index_ref)
    if not dataset_version_id or not str(query or "").strip():
        return None
    index_metadata = _lookup_pgvector_index_metadata(dataset_version_id)
    resolved_embedding_model = str(index_metadata.get("embedding_model") or embedding_model or "").strip()
    vector_dim = int(index_metadata.get("vector_dim") or 0)
    if not resolved_embedding_model:
        resolved_embedding_model = rt.TOKEN_OVERLAP_EMBEDDING_MODEL
    query_vector = _semantic_query_vector(
        query,
        query_counts,
        embedding_model=resolved_embedding_model,
        vector_dim=vector_dim,
    )
    if not query_vector:
        return None
    rows = _query_pgvector_rows(dataset_version_id, query_vector, sample_n)
    if rows is None:
        return None
    if not rows:
        return []
    chunk_lookup = _chunk_rows_by_id(rows, fallback_chunk_ref)
    matches: list[dict[str, Any]] = []
    for row in rows:
        chunk_id = str(row.get("chunk_id") or "").strip()
        chunk_row = chunk_lookup.get(chunk_id, {})
        metadata = _coerce_json_dict(row.get("metadata"))
        resolved_chunk_ref = str(row.get("chunk_ref") or fallback_chunk_ref or "").strip()
        resolved_chunk_format = fallback_chunk_format or ("parquet" if resolved_chunk_ref.endswith(".parquet") else "")
        matches.append(
            _base_semantic_match(
                source_index=row.get("source_row_index"),
                score=row.get("score"),
                text=str(chunk_row.get("chunk_text") or "")[:240],
                row_id=row.get("row_id") or chunk_row.get("row_id"),
                chunk_id=chunk_id,
                chunk_index=row.get("chunk_index") if row.get("chunk_index") is not None else chunk_row.get("chunk_index"),
                char_start=metadata.get("char_start") if metadata else chunk_row.get("char_start"),
                char_end=metadata.get("char_end") if metadata else chunk_row.get("char_end"),
                chunk_ref=resolved_chunk_ref,
                chunk_format=resolved_chunk_format,
            )
        )
    return matches


def _semantic_dataset_version_id(payload: dict[str, Any], inputs: dict[str, Any]) -> str:
    for candidate in (payload.get("dataset_version_id"), inputs.get("dataset_version_id")):
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _dataset_version_id_from_index_ref(embedding_index_ref: str) -> str:
    if not embedding_index_ref:
        return ""
    values = parse_qs(urlparse(embedding_index_ref).query).get("dataset_version_id") or []
    return str(values[0] or "").strip() if values else ""


def _lookup_pgvector_index_metadata(dataset_version_id: str) -> dict[str, Any]:
    database_url = str(os.getenv("DATABASE_URL") or "").strip()
    if not database_url or psycopg is None or dict_row is None:
        return {}
    query = """
        SELECT embedding_model, vector_dim
        FROM embedding_index_chunks
        WHERE dataset_version_id = %s
        LIMIT 1
    """
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (dataset_version_id,))
                row = cursor.fetchone()
                return dict(row) if row is not None else {}
    except Exception:
        return {}


def _semantic_query_vector(
    query: str,
    query_counts: Counter[str],
    *,
    embedding_model: str,
    vector_dim: int,
) -> list[float]:
    if rt._looks_dense_embedding_model(embedding_model):
        dense_vector = rt._generate_query_embedding(query, model=embedding_model, dimensions=vector_dim)
        return list(dense_vector or [])
    return _project_token_counts_to_dense_vector(query_counts, dim=vector_dim or 64)


def _query_pgvector_rows(dataset_version_id: str, query_vector: list[float], sample_n: int) -> list[dict[str, Any]]:
    database_url = str(os.getenv("DATABASE_URL") or "").strip()
    if not database_url or psycopg is None or dict_row is None:
        return []
    if not query_vector or all(abs(value) <= 1e-9 for value in query_vector):
        return []
    literal = _pgvector_literal(query_vector)
    query = """
        SELECT
            chunk_id,
            row_id,
            source_row_index,
            chunk_index,
            chunk_ref,
            metadata,
            1 - (embedding <=> %s::vector) AS score
        FROM embedding_index_chunks
        WHERE dataset_version_id = %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (literal, dataset_version_id, literal, sample_n))
                return [dict(row) for row in cursor.fetchall()]
    except Exception:
        return []


def _query_pgvector_cluster_rows(dataset_version_id: str) -> list[dict[str, Any]]:
    database_url = str(os.getenv("DATABASE_URL") or "").strip()
    if not database_url or psycopg is None or dict_row is None:
        return []
    query = """
        SELECT
            chunk_id,
            row_id,
            source_row_index,
            chunk_index,
            chunk_ref,
            embedding_model,
            vector_dim,
            embedding::text AS embedding_literal,
            metadata
        FROM embedding_index_chunks
        WHERE dataset_version_id = %s
        ORDER BY source_row_index, chunk_index, chunk_id
    """
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (dataset_version_id,))
                return [dict(row) for row in cursor.fetchall()]
    except Exception:
        return []


def _chunk_rows_by_id(rows: list[dict[str, Any]], fallback_chunk_ref: str) -> dict[str, dict[str, Any]]:
    refs: dict[str, set[str]] = {}
    for row in rows:
        chunk_id = str(row.get("chunk_id") or "").strip()
        chunk_ref = str(row.get("chunk_ref") or fallback_chunk_ref or "").strip()
        if not chunk_id or not chunk_ref:
            continue
        refs.setdefault(chunk_ref, set()).add(chunk_id)
    lookup: dict[str, dict[str, Any]] = {}
    for chunk_ref, chunk_ids in refs.items():
        path = Path(chunk_ref)
        if not path.exists():
            continue
        for row in rt._iter_rows(chunk_ref):
            chunk_id = str(row.get("chunk_id") or "").strip()
            if chunk_id in chunk_ids:
                lookup[chunk_id] = row
    return lookup


def _base_semantic_match(
    *,
    source_index: Any,
    score: Any,
    text: str,
    row_id: Any,
    chunk_id: Any,
    chunk_index: Any,
    char_start: Any,
    char_end: Any,
    chunk_ref: str,
    chunk_format: str,
) -> dict[str, Any]:
    match = {
        "rank": 0,
        "source_index": int(source_index or 0),
        "score": round(float(score or 0), 6),
        "text": text[:240],
    }
    row_id_text = str(row_id or "").strip()
    if row_id_text:
        match["row_id"] = row_id_text
    chunk_id_text = str(chunk_id or "").strip()
    if chunk_id_text:
        match["chunk_id"] = chunk_id_text
    for field, value in (("chunk_index", chunk_index), ("char_start", char_start), ("char_end", char_end)):
        if value is None or value == "":
            continue
        try:
            match[field] = int(value)
        except (TypeError, ValueError):
            continue
    if chunk_ref:
        match["chunk_ref"] = chunk_ref
    if chunk_format:
        match["chunk_format"] = chunk_format
    return match


def _coerce_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _parse_pgvector_literal(value: Any) -> list[float]:
    text = str(value or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if not text:
        return []
    vector: list[float] = []
    for item in text.split(","):
        piece = item.strip()
        if not piece:
            continue
        try:
            vector.append(float(piece))
        except ValueError:
            return []
    norm = math.sqrt(sum(component * component for component in vector))
    if norm <= 0:
        return []
    return [component / norm for component in vector]


def _project_token_counts_to_dense_vector(token_counts: Counter[str], *, dim: int) -> list[float]:
    if dim <= 0:
        return []
    vector = [0.0] * dim
    for token, count in token_counts.items():
        token = str(token).strip()
        if not token or int(count) == 0:
            continue
        hashed = _fnv1a_64(token)
        index = int(hashed % dim)
        sign = -1.0 if ((hashed >> 63) & 1) == 1 else 1.0
        vector[index] += sign * float(count)
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 0:
        return vector
    return [value / norm for value in vector]


def _fnv1a_64(value: str) -> int:
    hashed = 14695981039346656037
    for byte in value.encode("utf-8"):
        hashed ^= byte
        hashed = (hashed * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return hashed


def _pgvector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in vector) + "]"
