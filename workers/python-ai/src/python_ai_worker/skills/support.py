from __future__ import annotations

"""Support and retrieval-oriented skill handlers."""

from collections import Counter
from typing import Any

from .. import runtime as rt

def run_document_filter(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    indexed_rows = rt._indexed_rows(normalized["dataset_name"])
    query_tokens = set(rt._tokenize(normalized["query"]))
    matches: list[dict[str, Any]] = []

    if query_tokens:
        for item in indexed_rows:
            text = str(item["row"].get(normalized["text_column"]) or "").strip()
            if not text:
                continue
            score = sum(1 for token in rt._tokenize(text) if token in query_tokens)
            if score <= 0:
                continue
            matches.append(
                {
                    "rank": 0,
                    "source_index": item["source_index"],
                    "score": score,
                    "text": text[:240],
                }
            )
        matches.sort(key=lambda item: (-int(item["score"]), int(item["source_index"])))
        selection_mode = "lexical_overlap"
    else:
        selection_mode = "all_rows"

    if not matches:
        for item in indexed_rows:
            text = str(item["row"].get(normalized["text_column"]) or "").strip()
            if not text:
                continue
            matches.append(
                {
                    "rank": 0,
                    "source_index": item["source_index"],
                    "score": 0,
                    "text": text[:240],
                }
            )
        if query_tokens:
            selection_mode = "fallback_all_rows"

    for rank, item in enumerate(matches, start=1):
        item["rank"] = rank

    filtered_indices = [int(item["source_index"]) for item in matches]
    artifact_matches = matches[: normalized["sample_n"]]
    notes = [
        f"document_filter selected {len(filtered_indices)} rows",
        f"dataset source: {normalized['dataset_name']}",
        f"selection_mode: {selection_mode}",
    ]

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "document_filter",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "query": normalized["query"],
            "summary": {
                "input_row_count": len(indexed_rows),
                "filtered_row_count": len(filtered_indices),
                "selection_mode": selection_mode,
                "query_token_count": len(query_tokens),
            },
            "matched_indices": filtered_indices,
            "matches": artifact_matches,
        },
    }


def run_deduplicate_documents(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_deduplicate_payload(payload)
    selected_rows = rt._selected_text_rows(
        normalized["dataset_name"],
        normalized["text_column"],
        payload.get("prior_artifacts"),
        apply_dedup=False,
    )
    canonical_documents: list[dict[str, Any]] = []
    duplicate_records: list[dict[str, Any]] = []
    groups: dict[int, dict[str, Any]] = {}

    for item in selected_rows:
        text = item["text"]
        if not text:
            continue
        normalized_text = rt._normalize_prepared_text(text).lower()
        token_set = set(rt._tokenize(text))
        best_match: dict[str, Any] | None = None
        best_score = 0.0
        for canonical in canonical_documents:
            score = rt._duplicate_similarity(normalized_text, token_set, canonical["normalized_text"], canonical["token_set"])
            if score > best_score:
                best_score = score
                best_match = canonical
        if best_match is not None and best_score >= normalized["duplicate_threshold"]:
            group = groups[int(best_match["source_index"])]
            group["duplicate_source_indices"].append(int(item["source_index"]))
            group["member_count"] = 1 + len(group["duplicate_source_indices"])
            if len(group["samples"]) < normalized["sample_n"]:
                group["samples"].append(text[:240])
            duplicate_records.append(
                {
                    "source_index": int(item["source_index"]),
                    "canonical_source_index": int(best_match["source_index"]),
                    "similarity": round(best_score, 4),
                    "text": text[:240],
                }
            )
            continue

        canonical = {
            "source_index": int(item["source_index"]),
            "normalized_text": normalized_text,
            "token_set": token_set,
            "text": text[:240],
        }
        canonical_documents.append(canonical)
        groups[canonical["source_index"]] = {
            "group_id": "",
            "canonical_source_index": canonical["source_index"],
            "duplicate_source_indices": [],
            "member_count": 1,
            "samples": [text[:240]],
        }

    sorted_groups = sorted(
        groups.values(),
        key=lambda item: (-int(item["member_count"]), int(item["canonical_source_index"])),
    )
    duplicate_groups = []
    for rank, group in enumerate(sorted_groups, start=1):
        group["group_id"] = f"duplicate-{rank:02d}"
        duplicate_groups.append(group)

    return {
        "notes": [
            f"deduplicate_documents reduced {len(selected_rows)} rows to {len(canonical_documents)} canonical documents",
            f"dataset source: {normalized['dataset_name']}",
            f"duplicate_threshold: {normalized['duplicate_threshold']}",
        ],
        "artifact": {
            "skill_name": "deduplicate_documents",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "summary": {
                "input_row_count": len([item for item in selected_rows if item["text"]]),
                "canonical_row_count": len(canonical_documents),
                "duplicate_row_count": len(duplicate_records),
                "duplicate_group_count": len([group for group in duplicate_groups if group["duplicate_source_indices"]]),
                "duplicate_threshold": normalized["duplicate_threshold"],
            },
            "canonical_indices": [int(item["source_index"]) for item in canonical_documents],
            "duplicate_records": duplicate_records[: max(1, normalized["sample_n"] * 4)],
            "duplicate_groups": duplicate_groups,
        },
    }


def run_keyword_frequency(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    tokens = Counter()
    total_terms = 0
    document_count = 0
    for item in selected_rows:
        if not item["text"]:
            continue
        row_tokens = rt._tokenize(item["text"])
        total_terms += len(row_tokens)
        tokens.update(row_tokens)
        document_count += 1

    return {
        "notes": [
            f"keyword_frequency counted tokens across {document_count} rows",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": {
            "skill_name": "keyword_frequency",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "summary": {
                "document_count": document_count,
                "unique_terms": len(tokens),
                "total_terms": total_terms,
            },
            "top_terms": [
                {"term": term, "count": count}
                for term, count in tokens.most_common(normalized["top_n"])
            ],
        },
    }


def run_time_bucket_count(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_trend_task_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_time_bucket_artifact(normalized, selected_rows)
    artifact["skill_name"] = "time_bucket_count"
    return {
        "notes": [
            f"time_bucket_count built {normalized['bucket']} buckets",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_meta_group_count(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_breakdown_task_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_meta_group_artifact(normalized, selected_rows)
    artifact["skill_name"] = "meta_group_count"
    return {
        "notes": [
            f"meta_group_count grouped rows by {normalized['dimension_column']}",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_document_sample(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    samples = rt._rank_sample_rows(selected_rows, normalized["query"], normalized["sample_n"])
    selection_source = "query_overlap" if normalized["query"] else "source_order"
    if normalized["query"] and samples and float(samples[0].get("score") or 0) <= 0:
        selection_source = "source_order"

    return {
        "notes": [
            f"document_sample selected {len(samples)} representative rows",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": {
            "skill_name": "document_sample",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "query": normalized["query"],
            "selection_source": selection_source,
            "summary": {
                "document_count": len([item for item in selected_rows if item["text"]]),
                "sample_count": len(samples),
            },
            "samples": samples,
        },
    }


def run_dictionary_tagging(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_dictionary_tagging_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_dictionary_tagging_artifact(normalized, selected_rows)
    artifact["skill_name"] = "dictionary_tagging"
    return {
        "notes": [
            f"dictionary_tagging assigned tags to {artifact['summary']['tagged_row_count']} rows",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_embedding_cluster(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_embedding_cluster_payload(payload)
    records = rt._selected_embedding_records(normalized["embedding_uri"], payload.get("prior_artifacts"))
    clusters = rt._cluster_embedding_records(records, normalized["cluster_similarity_threshold"], normalized["sample_n"], normalized["top_n"])
    noise_count = len([cluster for cluster in clusters if int(cluster["document_count"]) == 1])
    return {
        "notes": [
            f"embedding_cluster built {len(clusters)} clusters",
            f"embedding source: {normalized['embedding_uri']}",
            f"cluster_similarity_threshold: {normalized['cluster_similarity_threshold']}",
        ],
        "artifact": {
            "skill_name": "embedding_cluster",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": normalized["embedding_uri"],
            "summary": {
                "cluster_count": len(clusters),
                "clustered_document_count": len(records),
                "noise_count": noise_count,
                "cluster_similarity_threshold": normalized["cluster_similarity_threshold"],
            },
            "clusters": clusters,
        },
    }


def run_cluster_label_candidates(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_cluster_label_payload(payload)
    prior = rt._find_prior_artifact(payload.get("prior_artifacts"), "embedding_cluster")
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
            "summary": {
                "cluster_count": len(clusters),
                "label_rule": "top_terms",
            },
            "clusters": clusters,
        },
    }


def run_semantic_search(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    embedding_uri = str(
        (normalized["step"].get("inputs") or {}).get("embedding_uri")
        or payload.get("embedding_uri")
        or f"{normalized['dataset_name']}.embeddings.jsonl"
    ).strip()
    if not embedding_uri:
        raise ValueError("embedding_uri is required")

    query_counts = Counter(rt._tokenize(normalized["query"]))
    matches = []
    for record in rt._selected_embedding_records(embedding_uri, payload.get("prior_artifacts")):
        score = rt._cosine_similarity(query_counts, record.get("token_counts") or {}, float(record.get("norm") or 0))
        match = {
            "rank": 0,
            "source_index": int(record.get("source_index") or 0),
            "score": round(score, 6),
            "text": str(record.get("text") or "")[:240],
        }
        row_id = str(record.get("row_id") or "").strip()
        if row_id:
            match["row_id"] = row_id
        chunk_id = str(record.get("chunk_id") or "").strip()
        if chunk_id:
            match["chunk_id"] = chunk_id
        matches.append(match)

    matches.sort(key=lambda item: (-item["score"], item["source_index"]))
    limited = matches[: normalized["sample_n"]]
    for rank, item in enumerate(limited, start=1):
        item["rank"] = rank

    return {
        "notes": [
            "semantic search executed with precomputed embeddings",
            f"embedding source: {embedding_uri}",
        ],
        "artifact": {
            "skill_name": "semantic_search",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": embedding_uri,
            "query": normalized["query"],
            "summary": {
                "candidate_count": len(matches),
                "match_count": len(limited),
            },
            "matches": limited,
        },
    }
