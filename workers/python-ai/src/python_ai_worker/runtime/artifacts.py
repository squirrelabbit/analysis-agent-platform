from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

from .common import (
    _bucket_label,
    _duplicate_similarity,
    _iter_documents,
    _iter_embedding_records,
    _iter_rows,
    _load_cluster_membership_rows,
    _looks_cluster_goal,
    _match_taxonomies,
    _normalize_prepared_text,
    _parse_timestamp,
    _rank_documents,
    _token_counter,
    _tokenize,
    _vector_norm,
)

_RESULT_SCOPE_ALIASES = {
    "subset_filtered": "document_subset",
    "sample_n": "document_subset",
    "single_record": "document_subset",
    "subset_selection": "cluster_subset",
}


def _copy_citation_fields(source: dict[str, Any], target: dict[str, Any]) -> None:
    for key in ("row_id", "chunk_id", "chunk_ref", "chunk_format"):
        value = str(source.get(key) or "").strip()
        if value:
            target[key] = value
    for key in ("chunk_index", "char_start", "char_end"):
        value = source.get(key)
        if value is None or value == "":
            continue
        try:
            target[key] = int(value)
        except (TypeError, ValueError):
            continue


def _select_evidence_candidates(
    payload: dict[str, Any],
    normalized: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    semantic_candidates = _extract_semantic_candidates(payload.get("prior_artifacts"))
    if semantic_candidates:
        selected = []
        for rank, item in enumerate(semantic_candidates[: normalized["sample_n"]], start=1):
            selected_item = {
                "rank": rank,
                "source_index": int(item.get("source_index") or 0),
                "score": float(item.get("score") or 0),
                "text": str(item.get("text") or ""),
            }
            _copy_citation_fields(item, selected_item)
            selected.append(selected_item)
        return selected, "semantic_search"

    cluster_candidates = _extract_cluster_membership_candidates(payload.get("prior_artifacts"), normalized)
    if cluster_candidates:
        return cluster_candidates, "cluster_membership"

    document_samples = _extract_document_samples(payload.get("prior_artifacts"))
    if document_samples:
        selected = []
        for rank, item in enumerate(document_samples[: normalized["sample_n"]], start=1):
            selected_item = {
                "rank": rank,
                "source_index": int(item.get("source_index") or 0),
                "score": float(item.get("score") or 0),
                "text": str(item.get("text") or ""),
            }
            _copy_citation_fields(item, selected_item)
            selected.append(selected_item)
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


def _extract_cluster_membership_candidates(prior_artifacts: Any, normalized: dict[str, Any]) -> list[dict[str, Any]]:
    query = str(normalized.get("query") or "").strip().lower()
    if not _looks_cluster_goal(query):
        return []

    summary_artifact = _find_prior_artifact(prior_artifacts, "issue_cluster_summary")
    if summary_artifact is not None:
        candidates = _cluster_candidates_from_artifact(summary_artifact, normalized)
        if candidates:
            return candidates

    embedding_artifact = _find_prior_artifact(prior_artifacts, "embedding_cluster")
    if embedding_artifact is not None:
        candidates = _cluster_candidates_from_artifact(embedding_artifact, normalized)
        if candidates:
            return candidates
    return []


def _cluster_candidates_from_artifact(artifact: dict[str, Any], normalized: dict[str, Any]) -> list[dict[str, Any]]:
    clusters = artifact.get("clusters")
    if not isinstance(clusters, list) or not clusters:
        return []
    first_cluster = None
    for item in clusters:
        if not isinstance(item, dict):
            continue
        first_cluster = item
        break
    if first_cluster is None:
        return []

    cluster_id = str(first_cluster.get("cluster_id") or "").strip()
    cluster_membership_ref = str(artifact.get("cluster_membership_ref") or "").strip()
    if cluster_membership_ref and cluster_id:
        rows = _load_cluster_membership_rows(cluster_membership_ref, cluster_id, limit=normalized["sample_n"])
        selected: list[dict[str, Any]] = []
        for rank, item in enumerate(rows, start=1):
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            selected_item = {
                "rank": rank,
                "source_index": int(item.get("source_index") or 0),
                "score": float(max(0, len(rows)-rank+1)),
                "text": text,
                "row_id": str(item.get("row_id") or "").strip(),
                "chunk_id": str(item.get("chunk_id") or "").strip(),
                "chunk_index": int(item.get("chunk_index") or 0),
            }
            selected.append(selected_item)
        if selected:
            return selected

    fallback_samples = first_cluster.get("samples") or first_cluster.get("sample_documents") or []
    selected = []
    for rank, item in enumerate(list(fallback_samples)[: normalized["sample_n"]], start=1):
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        selected_item = {
            "rank": rank,
            "source_index": int(item.get("source_index") or 0),
            "score": float(max(0, len(fallback_samples)-rank+1)),
            "text": text,
        }
        _copy_citation_fields(item, selected_item)
        selected.append(selected_item)
    return selected


def _iter_prior_artifacts(prior_artifacts: Any) -> list[dict[str, Any]]:
    if isinstance(prior_artifacts, list):
        artifacts: list[dict[str, Any]] = []
        for artifact in prior_artifacts:
            if isinstance(artifact, dict):
                artifacts.append(artifact)
        return artifacts
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


def _analysis_context_entries(prior_artifacts: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for artifact in (
        _first_prior_artifact(prior_artifacts, "issue_trend_summary", "time_bucket_count"),
        _first_prior_artifact(prior_artifacts, "issue_breakdown_summary", "meta_group_count"),
        _first_prior_artifact(prior_artifacts, "issue_period_compare"),
        _first_prior_artifact(prior_artifacts, "issue_cluster_summary", "cluster_label_candidates", "embedding_cluster"),
        _first_prior_artifact(prior_artifacts, "issue_taxonomy_summary", "dictionary_tagging"),
        _first_prior_artifact(prior_artifacts, "issue_sentiment_summary"),
        _first_prior_artifact(prior_artifacts, "unstructured_issue_summary"),
        _first_prior_artifact(prior_artifacts, "keyword_frequency"),
    ):
        if artifact is None:
            continue
        entry = _analysis_context_entry(artifact)
        if entry is not None:
            entries.append(entry)
    return entries


def _first_prior_artifact(prior_artifacts: Any, *skill_names: str) -> dict[str, Any] | None:
    for skill_name in skill_names:
        artifact = _find_prior_artifact(prior_artifacts, skill_name)
        if artifact is not None:
            return artifact
    return None


def _analysis_context_entry(artifact: dict[str, Any]) -> dict[str, Any] | None:
    skill_name = str(artifact.get("skill_name") or "").strip()
    if not skill_name:
        return None
    summary = _analysis_context_summary(artifact)
    if not summary:
        return None
    return {
        "source_skill": skill_name,
        "summary": summary,
    }


def _analysis_context_summary(artifact: dict[str, Any]) -> str:
    skill_name = str(artifact.get("skill_name") or "").strip()
    summary = artifact.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}

    if skill_name in {"issue_trend_summary", "time_bucket_count"}:
        peak_bucket = str(summary.get("peak_bucket") or "").strip()
        peak_count = int(summary.get("peak_count") or 0)
        bucket = str(artifact.get("bucket") or summary.get("bucket_type") or "").strip()
        if peak_bucket and peak_count > 0:
            prefix = f"{bucket} 기준 " if bucket else ""
            return f"{prefix}피크 구간은 {peak_bucket}({peak_count}건)이다."
        return ""

    if skill_name in {"issue_breakdown_summary", "meta_group_count"}:
        top_group = str(summary.get("top_group") or "").strip()
        top_group_count = int(summary.get("top_group_count") or 0)
        dimension = str(summary.get("dimension_column") or artifact.get("dimension_column") or "").strip()
        if top_group and top_group_count > 0:
            prefix = f"{dimension} 기준 " if dimension else ""
            return f"{prefix}최다 그룹은 {top_group}({top_group_count}건)이다."
        return ""

    if skill_name == "issue_period_compare":
        current_count = int(summary.get("current_count") or 0)
        previous_count = int(summary.get("previous_count") or 0)
        count_delta = int(summary.get("count_delta") or 0)
        if count_delta > 0:
            return f"현재 기간 {current_count}건, 이전 기간 {previous_count}건으로 {count_delta}건 증가했다."
        if count_delta < 0:
            return f"현재 기간 {current_count}건, 이전 기간 {previous_count}건으로 {abs(count_delta)}건 감소했다."
        if current_count or previous_count:
            return f"현재 기간과 이전 기간이 모두 {current_count}건으로 동일하다."
        return ""

    if skill_name == "issue_cluster_summary":
        label = str(summary.get("dominant_cluster_label") or "").strip()
        count = int(summary.get("dominant_cluster_count") or 0)
        if label and count > 0:
            return f"가장 큰 군집은 {label}이며 {count}건이다."
        return ""

    if skill_name == "cluster_label_candidates":
        clusters = artifact.get("clusters") or []
        if isinstance(clusters, list):
            for cluster in clusters:
                if not isinstance(cluster, dict):
                    continue
                label = str(cluster.get("label") or "").strip()
                count = int(cluster.get("document_count") or 0)
                if label and count > 0:
                    return f"가장 큰 군집 후보 라벨은 {label}이며 {count}건이다."
        return ""

    if skill_name == "embedding_cluster":
        clusters = artifact.get("clusters") or []
        if isinstance(clusters, list):
            for cluster in clusters:
                if not isinstance(cluster, dict):
                    continue
                count = int(cluster.get("document_count") or 0)
                terms = [str(item.get("term") or "").strip() for item in list(cluster.get("top_terms") or []) if isinstance(item, dict)]
                terms = [term for term in terms if term]
                if count > 0 and terms:
                    return f"대표 군집 top term은 {', '.join(terms[:2])}이고 {count}건이다."
        return ""

    if skill_name in {"issue_taxonomy_summary", "dictionary_tagging"}:
        label = str(summary.get("dominant_taxonomy_label") or "").strip()
        count = int(summary.get("dominant_taxonomy_count") or 0)
        if not label:
            breakdown = artifact.get("taxonomy_breakdown") or []
            if isinstance(breakdown, list) and breakdown:
                first = breakdown[0]
                if isinstance(first, dict):
                    label = str(first.get("label") or first.get("taxonomy_id") or "").strip()
                    count = int(first.get("count") or 0)
        if label and count > 0:
            return f"가장 큰 taxonomy는 {label}이며 {count}건이다."
        return ""

    if skill_name == "issue_sentiment_summary":
        label = str(summary.get("dominant_label") or "").strip()
        count = int(summary.get("dominant_label_count") or 0)
        if label and count > 0:
            return f"지배적인 감성은 {label}이며 {count}건이다."
        return ""

    if skill_name == "unstructured_issue_summary":
        top_terms = [str(item.get("term") or "").strip() for item in list(artifact.get("top_terms") or []) if isinstance(item, dict)]
        top_terms = [term for term in top_terms if term]
        document_count = int(summary.get("document_count") or 0)
        if top_terms:
            return f"주요 키워드는 {', '.join(top_terms[:3])}이며 문서 수는 {document_count}건이다."
        return ""

    if skill_name == "keyword_frequency":
        top_terms = [str(item.get("term") or "").strip() for item in list(artifact.get("top_terms") or []) if isinstance(item, dict)]
        top_terms = [term for term in top_terms if term]
        if top_terms:
            return f"상위 키워드는 {', '.join(top_terms[:3])}이다."
        return ""

    return ""


def _copy_artifact_fields(artifact: dict[str, Any], skill_name: str, step_id: Any) -> dict[str, Any]:
    copied = dict(artifact)
    copied["skill_name"] = skill_name
    copied["step_id"] = step_id
    return copied


def _normalize_result_scope(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    return _RESULT_SCOPE_ALIASES.get(normalized, normalized)


def infer_runtime_scope_from_prior(
    payload: dict[str, Any],
    *,
    declared_result_scope: str | None = None,
) -> str:
    for artifact in reversed(_iter_prior_artifacts(payload.get("prior_artifacts"))):
        runtime_result_scope = _normalize_result_scope(artifact.get("runtime_result_scope"))
        if runtime_result_scope:
            return runtime_result_scope
        result_scope = _normalize_result_scope(artifact.get("result_scope"))
        if result_scope:
            return result_scope
    normalized_declared_result_scope = _normalize_result_scope(
        declared_result_scope or payload.get("_declared_result_scope")
    )
    if normalized_declared_result_scope:
        return normalized_declared_result_scope
    raise ValueError("runtime_result_scope could not be inferred from prior artifacts")


def _set_scope_fields(
    artifact: dict[str, Any],
    *,
    declared_result_scope: str,
    runtime_result_scope: str | None = None,
) -> dict[str, Any]:
    normalized_declared_result_scope = _normalize_result_scope(declared_result_scope)
    if not normalized_declared_result_scope:
        raise ValueError("declared_result_scope is required")
    normalized_runtime_result_scope = _normalize_result_scope(
        runtime_result_scope or normalized_declared_result_scope
    )
    if not normalized_runtime_result_scope:
        raise ValueError("runtime_result_scope is required")
    artifact["result_scope"] = normalized_declared_result_scope
    artifact["runtime_result_scope"] = normalized_runtime_result_scope
    return artifact


def _inherit_scope_fields(
    artifact: dict[str, Any],
    payload: dict[str, Any],
    *,
    declared_result_scope: str = "document_subset",
) -> dict[str, Any]:
    runtime_result_scope = infer_runtime_scope_from_prior(
        payload,
        declared_result_scope=declared_result_scope,
    )
    return _set_scope_fields(
        artifact,
        declared_result_scope=declared_result_scope,
        runtime_result_scope=runtime_result_scope,
    )


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
        row_id = str(row.get("row_id") or "").strip()
        selected_rows.append(
            {
                "source_index": source_index,
                "row_id": row_id,
                "row": row,
                "text": str(row.get(text_column) or "").strip(),
            }
        )
    return selected_rows


def _selected_source_indices(prior_artifacts: Any, *, apply_dedup: bool = True) -> set[int] | None:
    selected_indices = _extract_garbage_filter_indices(prior_artifacts)
    document_filter_indices = _extract_document_filter_indices(prior_artifacts)
    if document_filter_indices is not None:
        if selected_indices is None:
            selected_indices = document_filter_indices
        else:
            selected_indices = selected_indices & document_filter_indices
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
    if not indices:
        indices = _sidecar_source_indices(artifact)
    return indices


def _extract_garbage_filter_indices(prior_artifacts: Any) -> set[int] | None:
    artifact = _find_prior_artifact(prior_artifacts, "garbage_filter")
    if artifact is None:
        return None
    indices: set[int] = set()
    for item in artifact.get("retained_indices") or []:
        try:
            indices.add(int(item))
        except (TypeError, ValueError):
            continue
    if not indices:
        indices = _sidecar_source_indices(artifact, status_column="filter_status", include_values={"retained"})
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
    if not indices:
        indices = _sidecar_source_indices(artifact, status_column="dedup_status", include_values={"canonical"})
    return indices


def _sidecar_source_indices(
    artifact: dict[str, Any],
    *,
    status_column: str = "",
    include_values: set[str] | None = None,
) -> set[int]:
    artifact_ref = str(artifact.get("artifact_ref") or "").strip()
    source_index_column = str(artifact.get("source_index_column") or "source_index").strip() or "source_index"
    if not artifact_ref:
        return set()
    values = {str(item).strip() for item in set(include_values or set()) if str(item).strip()}
    indices: set[int] = set()
    for row in _iter_rows(artifact_ref):
        if status_column and values:
            status = str(row.get(status_column) or "").strip()
            if status not in values:
                continue
        try:
            indices.add(int(row.get(source_index_column) or 0))
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


def _dense_embedding_vector(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    vector: list[float] = []
    for item in value:
        try:
            vector.append(float(item))
        except (TypeError, ValueError):
            return []
    norm = math.sqrt(sum(component * component for component in vector))
    if norm <= 0:
        return []
    return [component / norm for component in vector]


def _dense_cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return sum(left[index] * right[index] for index in range(len(left)))


def _cluster_token_idf(records: list[dict[str, Any]]) -> dict[str, float]:
    if not records:
        return {}
    document_frequency: Counter[str] = Counter()
    for record in records:
        token_counts = _token_counter(record.get("token_counts") or {})
        document_frequency.update(set(token_counts.keys()))
    total_documents = max(len(records), 1)
    return {
        token: 1.0 + math.log((total_documents + 1.0) / (float(frequency) + 1.0))
        for token, frequency in document_frequency.items()
        if token
    }


def _weighted_token_counts(token_counts: Counter[str], token_idf: dict[str, float]) -> dict[str, float]:
    weighted: dict[str, float] = {}
    for token, count in token_counts.items():
        weight = float(count) * float(token_idf.get(token, 1.0))
        if weight > 0:
            weighted[token] = weight
    return weighted


def _sparse_vector_norm(values: dict[str, float]) -> float:
    if not values:
        return 0.0
    return math.sqrt(sum(value * value for value in values.values()))


def _sparse_cosine_similarity(left: dict[str, float], right: dict[str, float], right_norm: float) -> float:
    if not left or not right or right_norm <= 0:
        return 0.0
    left_norm = _sparse_vector_norm(left)
    if left_norm <= 0:
        return 0.0
    dot_product = 0.0
    for token, left_value in left.items():
        dot_product += left_value * float(right.get(token) or 0.0)
    return dot_product / (left_norm * right_norm)


def _leading_anchor_token(text: Any) -> str:
    tokens = _tokenize(str(text or ""))
    if not tokens:
        return ""
    return str(tokens[0]).strip()


def _cluster_similarity_backend(value: Any) -> str:
    if not isinstance(value, set):
        return "token-overlap"
    normalized = {str(item).strip() for item in value if str(item).strip()}
    if normalized == {"dense-only"}:
        return "dense-only"
    if normalized == {"dense-hybrid"}:
        return "dense-hybrid"
    if normalized == {"token-overlap"} or not normalized:
        return "token-overlap"
    return "mixed"


def _normalize_cluster_similarity_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "dense-only":
        return "dense-only"
    if normalized == "token-overlap":
        return "token-overlap"
    return "dense-hybrid"


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
                "row_id": str(item.get("row_id") or "").strip(),
                "chunk_id": str(item.get("chunk_id") or item.get("row_id") or "").strip(),
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
    similarity_mode: str = "dense-hybrid",
    *,
    include_members: bool = False,
) -> list[dict[str, Any]]:
    working_clusters: list[dict[str, Any]] = []
    ordered_records = sorted(records, key=lambda item: int(item.get("source_index") or 0))
    token_idf = _cluster_token_idf(ordered_records)
    resolved_similarity_mode = _normalize_cluster_similarity_mode(similarity_mode)
    for record in ordered_records:
        token_counts = _token_counter(record.get("token_counts") or {})
        weighted_counts = _weighted_token_counts(token_counts, token_idf)
        dense_vector = _dense_embedding_vector(record.get("embedding"))
        leading_anchor = _leading_anchor_token(record.get("text"))
        if not token_counts and not dense_vector:
            continue
        best_cluster: dict[str, Any] | None = None
        best_score = 0.0
        best_backend = "token-overlap"
        for cluster in working_clusters:
            score = 0.0
            backend = "token-overlap"
            aggregate_embedding = list(cluster.get("aggregate_embedding") or [])
            if (
                resolved_similarity_mode == "dense-only"
                and dense_vector
                and aggregate_embedding
                and len(dense_vector) == len(aggregate_embedding)
            ):
                score = _dense_cosine_similarity(dense_vector, aggregate_embedding)
                backend = "dense-only"
            elif dense_vector and aggregate_embedding and len(dense_vector) == len(aggregate_embedding):
                dense_score = _dense_cosine_similarity(dense_vector, aggregate_embedding)
                token_score = _sparse_cosine_similarity(
                    weighted_counts,
                    dict(cluster.get("aggregate_weighted_counts") or {}),
                    float(cluster.get("aggregate_weighted_norm") or 0.0),
                )
                cluster_anchor_counts = Counter(cluster.get("leading_anchor_counts") or {})
                anchor_match = bool(leading_anchor and cluster_anchor_counts.get(leading_anchor, 0) > 0)
                cluster_tokens = set(_token_counter(cluster.get("aggregate_counts") or {}).keys())
                shared_token_count = len(set(token_counts.keys()) & cluster_tokens)
                generic_overlap_penalty = not anchor_match and shared_token_count >= 2
                if resolved_similarity_mode == "dense-hybrid":
                    lexical_guard = token_score * 0.25 if generic_overlap_penalty else max(token_score, 0.1)
                    score = dense_score * lexical_guard
                    backend = "dense-hybrid"
                else:
                    score = token_score
            elif token_counts:
                score = _sparse_cosine_similarity(
                    weighted_counts,
                    dict(cluster.get("aggregate_weighted_counts") or {}),
                    float(cluster.get("aggregate_weighted_norm") or 0.0),
                )
            if score > best_score:
                best_score = score
                best_cluster = cluster
                best_backend = backend
        member = {
            "source_index": int(record.get("source_index") or 0),
            "row_id": str(record.get("row_id") or "").strip(),
            "chunk_id": str(record.get("chunk_id") or "").strip(),
            "chunk_index": int(record.get("chunk_index") or 0),
            "text": str(record.get("text") or "")[:240],
            "token_counts": token_counts,
            "leading_anchor": leading_anchor,
        }
        if best_cluster is None or best_score < similarity_threshold:
            if dense_vector and resolved_similarity_mode == "dense-only":
                backends = {"dense-only"}
            elif dense_vector and resolved_similarity_mode == "dense-hybrid":
                backends = {"dense-hybrid"}
            else:
                backends = {"token-overlap"}
            working_clusters.append(
                {
                    "members": [member],
                    "aggregate_counts": Counter(token_counts),
                    "aggregate_norm": _vector_norm(token_counts),
                    "aggregate_weighted_counts": Counter(weighted_counts),
                    "aggregate_weighted_norm": _sparse_vector_norm(weighted_counts),
                    "aggregate_embedding": list(dense_vector),
                    "dense_member_count": 1 if dense_vector else 0,
                    "leading_anchor_counts": Counter([leading_anchor]) if leading_anchor else Counter(),
                    "similarity_backends": backends,
                }
            )
            continue
        best_cluster["members"].append(member)
        best_cluster["aggregate_counts"].update(token_counts)
        best_cluster["aggregate_norm"] = _vector_norm(best_cluster["aggregate_counts"])
        best_cluster.setdefault("aggregate_weighted_counts", Counter()).update(weighted_counts)
        best_cluster["aggregate_weighted_norm"] = _sparse_vector_norm(dict(best_cluster["aggregate_weighted_counts"]))
        if leading_anchor:
            best_cluster.setdefault("leading_anchor_counts", Counter()).update([leading_anchor])
        best_cluster.setdefault("similarity_backends", set()).add(best_backend)
        if dense_vector:
            aggregate_embedding = list(best_cluster.get("aggregate_embedding") or [])
            dense_member_count = int(best_cluster.get("dense_member_count") or 0)
            if not aggregate_embedding or len(aggregate_embedding) != len(dense_vector):
                best_cluster["aggregate_embedding"] = list(dense_vector)
                best_cluster["dense_member_count"] = 1
            else:
                merged = [
                    ((aggregate_embedding[index] * dense_member_count) + dense_vector[index]) / float(dense_member_count + 1)
                    for index in range(len(dense_vector))
                ]
                best_cluster["aggregate_embedding"] = _dense_embedding_vector(merged)
                best_cluster["dense_member_count"] = dense_member_count + 1

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
                "similarity_backend": _cluster_similarity_backend(cluster.get("similarity_backends")),
                "member_source_indices": [int(member["source_index"]) for member in members],
                "top_terms": [
                    {"term": term, "count": count}
                    for term, count in cluster["aggregate_counts"].most_common(top_n)
                ],
                "sample_documents": [
                    {
                        "source_index": int(member["source_index"]),
                        "row_id": str(member.get("row_id") or ""),
                        "chunk_id": str(member.get("chunk_id") or ""),
                        "text": str(member["text"])[:240],
                    }
                    for member in members[:sample_n]
                ],
                **(
                    {
                        "members": [
                            {
                                "source_index": int(member["source_index"]),
                                "row_id": str(member.get("row_id") or ""),
                                "chunk_id": str(member.get("chunk_id") or ""),
                                "chunk_index": int(member.get("chunk_index") or 0),
                                "text": str(member.get("text") or "")[:240],
                            }
                            for member in members
                        ]
                    }
                    if include_members
                    else {}
                ),
            }
        )
    return payload_clusters


__all__ = [
    "_analysis_context_entries",
    "_build_dictionary_tagging_artifact",
    "_build_embedding_records_from_rows",
    "_build_meta_group_artifact",
    "_build_time_bucket_artifact",
    "_cluster_embedding_records",
    "_copy_artifact_fields",
    "_inherit_scope_fields",
    "_normalize_result_scope",
    "_set_scope_fields",
    "_extract_deduplicated_indices",
    "_extract_document_filter_indices",
    "_extract_garbage_filter_indices",
    "_extract_document_samples",
    "_extract_semantic_candidates",
    "_find_prior_artifact",
    "infer_runtime_scope_from_prior",
    "_indexed_rows",
    "_iter_prior_artifacts",
    "_rank_sample_rows",
    "_row_source_index",
    "_select_evidence_candidates",
    "_selected_embedding_records",
    "_selected_source_indices",
    "_selected_text_rows",
]
