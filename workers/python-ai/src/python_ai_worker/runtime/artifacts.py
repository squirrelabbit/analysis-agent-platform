from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .common import (
    _bucket_label,
    _cosine_similarity,
    _duplicate_similarity,
    _iter_documents,
    _iter_embedding_records,
    _iter_rows,
    _match_taxonomies,
    _normalize_prepared_text,
    _parse_timestamp,
    _rank_documents,
    _token_counter,
    _tokenize,
    _vector_norm,
)


def _select_evidence_candidates(
    payload: dict[str, Any],
    normalized: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    semantic_candidates = _extract_semantic_candidates(payload.get("prior_artifacts"))
    if semantic_candidates:
        selected = []
        for rank, item in enumerate(semantic_candidates[: normalized["sample_n"]], start=1):
            selected.append(
                {
                    "rank": rank,
                    "source_index": int(item.get("source_index") or 0),
                    "score": float(item.get("score") or 0),
                    "text": str(item.get("text") or ""),
                }
            )
        return selected, "semantic_search"

    document_samples = _extract_document_samples(payload.get("prior_artifacts"))
    if document_samples:
        selected = []
        for rank, item in enumerate(document_samples[: normalized["sample_n"]], start=1):
            selected.append(
                {
                    "rank": rank,
                    "source_index": int(item.get("source_index") or 0),
                    "score": float(item.get("score") or 0),
                    "text": str(item.get("text") or ""),
                }
            )
        return selected, "document_sample"

    documents = [item for item in _iter_documents(normalized["dataset_name"], normalized["text_column"]) if item]
    ranked_documents = _rank_documents(documents, normalized["query"])
    return ranked_documents[: normalized["sample_n"]], "lexical_overlap"


def _extract_semantic_candidates(prior_artifacts: Any) -> list[dict[str, Any]]:
    if not isinstance(prior_artifacts, dict):
        return []

    candidates: list[dict[str, Any]] = []
    for artifact in prior_artifacts.values():
        normalized = artifact
        if isinstance(normalized, str):
            try:
                normalized = json.loads(normalized)
            except json.JSONDecodeError:
                continue
        if not isinstance(normalized, dict):
            continue
        if normalized.get("skill_name") != "semantic_search":
            continue
        matches = normalized.get("matches")
        if not isinstance(matches, list):
            continue
        for item in matches:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            candidates.append(item)
    return candidates


def _extract_document_samples(prior_artifacts: Any) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for artifact in _iter_prior_artifacts(prior_artifacts):
        if artifact.get("skill_name") != "document_sample":
            continue
        for item in artifact.get("samples") or []:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            samples.append(item)
    return samples


def _iter_prior_artifacts(prior_artifacts: Any) -> list[dict[str, Any]]:
    if not isinstance(prior_artifacts, dict):
        return []
    artifacts: list[dict[str, Any]] = []
    for artifact in prior_artifacts.values():
        normalized = artifact
        if isinstance(normalized, str):
            try:
                normalized = json.loads(normalized)
            except json.JSONDecodeError:
                continue
        if isinstance(normalized, dict):
            artifacts.append(normalized)
    return artifacts


def _find_prior_artifact(prior_artifacts: Any, skill_name: str) -> dict[str, Any] | None:
    for artifact in reversed(_iter_prior_artifacts(prior_artifacts)):
        if str(artifact.get("skill_name") or "").strip() == skill_name:
            return artifact
    return None


def _copy_artifact_fields(artifact: dict[str, Any], skill_name: str, step_id: Any) -> dict[str, Any]:
    copied = dict(artifact)
    copied["skill_name"] = skill_name
    copied["step_id"] = step_id
    return copied


def _indexed_rows(dataset_name: str) -> list[dict[str, Any]]:
    indexed: list[dict[str, Any]] = []
    for fallback_index, row in enumerate(_iter_rows(dataset_name)):
        indexed.append(
            {
                "source_index": _row_source_index(row, fallback_index),
                "row": row,
            }
        )
    return indexed


def _selected_text_rows(
    dataset_name: str,
    text_column: str,
    prior_artifacts: Any,
    *,
    apply_dedup: bool = True,
) -> list[dict[str, Any]]:
    selected_indices = _selected_source_indices(prior_artifacts, apply_dedup=apply_dedup)
    selected_rows: list[dict[str, Any]] = []
    for item in _indexed_rows(dataset_name):
        source_index = int(item["source_index"])
        if selected_indices is not None and source_index not in selected_indices:
            continue
        row = item["row"]
        selected_rows.append(
            {
                "source_index": source_index,
                "row": row,
                "text": str(row.get(text_column) or "").strip(),
            }
        )
    return selected_rows


def _selected_source_indices(prior_artifacts: Any, *, apply_dedup: bool = True) -> set[int] | None:
    selected_indices = _extract_document_filter_indices(prior_artifacts)
    if not apply_dedup:
        return selected_indices
    deduplicated_indices = _extract_deduplicated_indices(prior_artifacts)
    if deduplicated_indices is None:
        return selected_indices
    if selected_indices is None:
        return deduplicated_indices
    return selected_indices & deduplicated_indices


def _extract_document_filter_indices(prior_artifacts: Any) -> set[int] | None:
    artifact = _find_prior_artifact(prior_artifacts, "document_filter")
    if artifact is None:
        return None
    indices: set[int] = set()
    for item in artifact.get("matched_indices") or []:
        try:
            indices.add(int(item))
        except (TypeError, ValueError):
            continue
    return indices


def _extract_deduplicated_indices(prior_artifacts: Any) -> set[int] | None:
    artifact = _find_prior_artifact(prior_artifacts, "deduplicate_documents")
    if artifact is None:
        return None
    indices: set[int] = set()
    for item in artifact.get("canonical_indices") or []:
        try:
            indices.add(int(item))
        except (TypeError, ValueError):
            continue
    return indices


def _row_source_index(row: dict[str, Any], fallback_index: int) -> int:
    value = row.get("source_row_index")
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback_index


def _rank_sample_rows(rows: list[dict[str, Any]], query: str, sample_n: int) -> list[dict[str, Any]]:
    query_tokens = set(_tokenize(query))
    ranked = []
    for item in rows:
        text = item["text"]
        if not text:
            continue
        tokens = _tokenize(text)
        overlap = sum(1 for token in tokens if token in query_tokens) if query_tokens else 0
        ranked.append(
            {
                "rank": 0,
                "source_index": int(item["source_index"]),
                "score": overlap,
                "text": text[:240],
            }
        )
    if query_tokens:
        ranked.sort(key=lambda item: (-int(item["score"]), int(item["source_index"])))
    else:
        ranked.sort(key=lambda item: int(item["source_index"]))
    limited = ranked[:sample_n]
    for rank, item in enumerate(limited, start=1):
        item["rank"] = rank
    return limited


def _selected_embedding_records(embedding_uri: str, prior_artifacts: Any) -> list[dict[str, Any]]:
    selected_indices = _selected_source_indices(prior_artifacts)
    records = []
    for record in _iter_embedding_records(Path(embedding_uri)):
        try:
            source_index = int(record.get("source_index") or 0)
        except (TypeError, ValueError):
            continue
        if selected_indices is not None and source_index not in selected_indices:
            continue
        records.append(record)
    return records


def _build_time_bucket_artifact(normalized: dict[str, Any], selected_rows: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_counts: Counter[str] = Counter()
    bucket_terms: dict[str, Counter[str]] = {}
    bucket_samples: dict[str, list[str]] = {}
    skipped_rows = 0

    for item in selected_rows:
        row = item["row"]
        text = item["text"]
        raw_timestamp = str(row.get(normalized["time_column"]) or "").strip()
        if not text or not raw_timestamp:
            skipped_rows += 1
            continue
        parsed_at = _parse_timestamp(raw_timestamp)
        if parsed_at is None:
            skipped_rows += 1
            continue
        bucket_label = _bucket_label(parsed_at, normalized["bucket"])
        bucket_counts.update([bucket_label])
        bucket_terms.setdefault(bucket_label, Counter()).update(_tokenize(text))
        bucket_samples.setdefault(bucket_label, [])
        if len(bucket_samples[bucket_label]) < normalized["sample_n"]:
            bucket_samples[bucket_label].append(text[:240])

    series = [
        {"bucket": bucket_label, "count": count}
        for bucket_label, count in sorted(bucket_counts.items())
    ]
    busiest = sorted(bucket_counts.items(), key=lambda item: (-item[1], item[0]))
    highlights = []
    for rank, (bucket_label, count) in enumerate(busiest[: normalized["sample_n"]], start=1):
        highlights.append(
            {
                "rank": rank,
                "bucket": bucket_label,
                "count": count,
                "top_terms": [
                    {"term": term, "count": term_count}
                    for term, term_count in bucket_terms.get(bucket_label, Counter()).most_common(normalized["top_n"])
                ],
                "samples": bucket_samples.get(bucket_label, []),
            }
        )

    summary = {
        "document_count": sum(bucket_counts.values()),
        "bucket_count": len(series),
        "bucket_type": normalized["bucket"],
        "time_column": normalized["time_column"],
        "skipped_rows": skipped_rows,
    }
    if series:
        summary["first_bucket"] = series[0]["bucket"]
        summary["last_bucket"] = series[-1]["bucket"]
        summary["peak_bucket"] = busiest[0][0]
        summary["peak_count"] = busiest[0][1]

    return {
        "step_id": normalized["step"].get("step_id"),
        "dataset_name": normalized["dataset_name"],
        "time_column": normalized["time_column"],
        "bucket": normalized["bucket"],
        "summary": summary,
        "series": series,
        "highlights": highlights,
    }


def _build_meta_group_artifact(normalized: dict[str, Any], selected_rows: list[dict[str, Any]]) -> dict[str, Any]:
    group_counts: Counter[str] = Counter()
    group_terms: dict[str, Counter[str]] = {}
    group_samples: dict[str, list[str]] = {}

    for item in selected_rows:
        row = item["row"]
        text = item["text"]
        group_value = str(row.get(normalized["dimension_column"]) or "(missing)").strip() or "(missing)"
        if not text:
            continue
        group_counts.update([group_value])
        group_terms.setdefault(group_value, Counter()).update(_tokenize(text))
        group_samples.setdefault(group_value, [])
        if len(group_samples[group_value]) < normalized["sample_n"]:
            group_samples[group_value].append(text[:240])

    ranked_groups = sorted(group_counts.items(), key=lambda item: (-item[1], item[0]))
    breakdown = []
    for rank, (group_value, count) in enumerate(ranked_groups[: normalized["top_n"]], start=1):
        breakdown.append(
            {
                "rank": rank,
                "dimension_value": group_value,
                "count": count,
                "top_terms": [
                    {"term": term, "count": term_count}
                    for term, term_count in group_terms.get(group_value, Counter()).most_common(normalized["top_n"])
                ],
                "samples": group_samples.get(group_value, []),
            }
        )

    summary = {
        "group_count": len(group_counts),
        "dimension_column": normalized["dimension_column"],
    }
    if breakdown:
        summary["top_group"] = breakdown[0]["dimension_value"]
        summary["top_group_count"] = breakdown[0]["count"]

    return {
        "step_id": normalized["step"].get("step_id"),
        "dataset_name": normalized["dataset_name"],
        "dimension_column": normalized["dimension_column"],
        "summary": summary,
        "breakdown": breakdown,
    }


def _build_dictionary_tagging_artifact(normalized: dict[str, Any], selected_rows: list[dict[str, Any]]) -> dict[str, Any]:
    taxonomy_counts: Counter[str] = Counter()
    taxonomy_terms: dict[str, Counter[str]] = {}
    taxonomy_samples: dict[str, list[str]] = {}
    multi_tagged_count = 0
    uncovered_row_count = 0
    tagged_row_count = 0

    for item in selected_rows:
        text = item["text"]
        if not text:
            continue
        matched = _match_taxonomies(text, normalized["taxonomy_rules"], normalized["max_tags_per_document"])
        if not matched:
            uncovered_row_count += 1
            continue
        tagged_row_count += 1
        if len(matched) > 1:
            multi_tagged_count += 1
        row_tokens = _tokenize(text)
        for taxonomy_id in matched:
            taxonomy_counts.update([taxonomy_id])
            taxonomy_terms.setdefault(taxonomy_id, Counter()).update(row_tokens)
            taxonomy_samples.setdefault(taxonomy_id, [])
            if len(taxonomy_samples[taxonomy_id]) < normalized["sample_n"]:
                taxonomy_samples[taxonomy_id].append(text[:240])

    total_documents = tagged_row_count
    breakdown = []
    ranked = sorted(taxonomy_counts.items(), key=lambda item: (-item[1], item[0]))
    for rank, (taxonomy_id, count) in enumerate(ranked, start=1):
        rule = normalized["taxonomy_rules"].get(taxonomy_id, {})
        breakdown.append(
            {
                "rank": rank,
                "taxonomy_id": taxonomy_id,
                "label": str(rule.get("label") or taxonomy_id),
                "count": count,
                "ratio_pct": round((count / total_documents) * 100, 2) if total_documents > 0 else 0.0,
                "top_terms": [
                    {"term": term, "count": term_count}
                    for term, term_count in taxonomy_terms.get(taxonomy_id, Counter()).most_common(normalized["top_n"])
                ],
                "samples": taxonomy_samples.get(taxonomy_id, []),
            }
        )

    summary = {
        "document_count": len([item for item in selected_rows if item["text"]]),
        "tagged_row_count": tagged_row_count,
        "uncovered_row_count": uncovered_row_count,
        "multi_tagged_row_count": multi_tagged_count,
        "taxonomy_count": len(taxonomy_counts),
    }

    return {
        "step_id": normalized["step"].get("step_id"),
        "dataset_name": normalized["dataset_name"],
        "summary": summary,
        "taxonomy_breakdown": breakdown,
    }


def _build_embedding_records_from_rows(selected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    for item in selected_rows:
        text = item["text"]
        if not text:
            continue
        token_counts = Counter(_tokenize(text))
        records.append(
            {
                "source_index": int(item["source_index"]),
                "text": text,
                "token_counts": dict(token_counts),
                "norm": _vector_norm(token_counts),
            }
        )
    return records


def _cluster_embedding_records(
    records: list[dict[str, Any]],
    similarity_threshold: float,
    sample_n: int,
    top_n: int,
) -> list[dict[str, Any]]:
    working_clusters: list[dict[str, Any]] = []
    ordered_records = sorted(records, key=lambda item: int(item.get("source_index") or 0))
    for record in ordered_records:
        token_counts = _token_counter(record.get("token_counts") or {})
        if not token_counts:
            continue
        best_cluster: dict[str, Any] | None = None
        best_score = 0.0
        for cluster in working_clusters:
            score = _cosine_similarity(token_counts, dict(cluster["aggregate_counts"]), float(cluster["aggregate_norm"]))
            if score > best_score:
                best_score = score
                best_cluster = cluster
        member = {
            "source_index": int(record.get("source_index") or 0),
            "text": str(record.get("text") or "")[:240],
            "token_counts": token_counts,
        }
        if best_cluster is None or best_score < similarity_threshold:
            working_clusters.append(
                {
                    "members": [member],
                    "aggregate_counts": Counter(token_counts),
                    "aggregate_norm": _vector_norm(token_counts),
                }
            )
            continue
        best_cluster["members"].append(member)
        best_cluster["aggregate_counts"].update(token_counts)
        best_cluster["aggregate_norm"] = _vector_norm(best_cluster["aggregate_counts"])

    payload_clusters = []
    sorted_clusters = sorted(
        working_clusters,
        key=lambda item: (-len(item["members"]), min(member["source_index"] for member in item["members"])),
    )
    for rank, cluster in enumerate(sorted_clusters, start=1):
        members = sorted(cluster["members"], key=lambda item: int(item["source_index"]))
        payload_clusters.append(
            {
                "cluster_id": f"cluster-{rank:02d}",
                "document_count": len(members),
                "member_source_indices": [int(member["source_index"]) for member in members],
                "top_terms": [
                    {"term": term, "count": count}
                    for term, count in cluster["aggregate_counts"].most_common(top_n)
                ],
                "sample_documents": [
                    {
                        "source_index": int(member["source_index"]),
                        "text": str(member["text"])[:240],
                    }
                    for member in members[:sample_n]
                ],
            }
        )
    return payload_clusters


__all__ = [
    "_build_dictionary_tagging_artifact",
    "_build_embedding_records_from_rows",
    "_build_meta_group_artifact",
    "_build_time_bucket_artifact",
    "_cluster_embedding_records",
    "_copy_artifact_fields",
    "_extract_deduplicated_indices",
    "_extract_document_filter_indices",
    "_extract_document_samples",
    "_extract_semantic_candidates",
    "_find_prior_artifact",
    "_indexed_rows",
    "_iter_prior_artifacts",
    "_rank_sample_rows",
    "_row_source_index",
    "_select_evidence_candidates",
    "_selected_embedding_records",
    "_selected_source_indices",
    "_selected_text_rows",
]
