from __future__ import annotations

from typing import Any

from ..skill_bundle import default_inputs_for_skill
from .common import _normalize_taxonomy_rules
from .constants import (
    DEFAULT_CLUSTER_SIMILARITY_THRESHOLD,
    DEFAULT_DUPLICATE_THRESHOLD,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_MAX_TAGS_PER_DOCUMENT,
    DEFAULT_PREPARE_BATCH_SIZE,
)


def _normalize_text_task_payload(payload: dict[str, Any]) -> dict[str, Any]:
    step = payload.get("step") or {}
    inputs = step.get("inputs") or {}
    dataset_name = str(
        step.get("dataset_name") or payload.get("dataset_name") or ""
    ).strip()
    text_column = str(
        inputs.get("text_column") or payload.get("text_column") or "text"
    ).strip()
    top_n = max(1, int(inputs.get("top_n") or payload.get("top_n") or 10))
    sample_n = max(1, int(inputs.get("sample_n") or payload.get("sample_n") or 3))
    query = str(inputs.get("query") or payload.get("query") or payload.get("goal") or "").strip()

    if not dataset_name:
        raise ValueError("dataset_name is required")

    return {
        "step": step,
        "dataset_name": dataset_name,
        "text_column": text_column,
        "top_n": top_n,
        "sample_n": sample_n,
        "query": query,
    }


def _normalize_breakdown_task_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["dimension_column"] = str(inputs.get("dimension_column") or payload.get("dimension_column") or "channel").strip()
    return normalized


def _normalize_trend_task_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    time_column = str(inputs.get("time_column") or payload.get("time_column") or "occurred_at").strip()
    bucket = str(inputs.get("bucket") or payload.get("bucket") or "day").strip().lower()
    if bucket not in {"day", "week", "month"}:
        raise ValueError("bucket must be one of day, week, month")
    normalized["time_column"] = time_column
    normalized["bucket"] = bucket
    return normalized


def _normalize_compare_task_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_trend_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["window_size"] = max(1, int(inputs.get("window_size") or payload.get("window_size") or 1))
    normalized["current_start_bucket"] = str(inputs.get("current_start_bucket") or payload.get("current_start_bucket") or "").strip()
    normalized["current_end_bucket"] = str(inputs.get("current_end_bucket") or payload.get("current_end_bucket") or "").strip()
    normalized["previous_start_bucket"] = str(inputs.get("previous_start_bucket") or payload.get("previous_start_bucket") or "").strip()
    normalized["previous_end_bucket"] = str(inputs.get("previous_end_bucket") or payload.get("previous_end_bucket") or "").strip()
    return normalized


def _normalize_sentiment_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["sentiment_column"] = str(inputs.get("sentiment_column") or payload.get("sentiment_column") or "sentiment_label").strip()
    return normalized


def _normalize_deduplicate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["duplicate_threshold"] = round(
        max(0.0, min(1.0, float(inputs.get("duplicate_threshold") or payload.get("duplicate_threshold") or DEFAULT_DUPLICATE_THRESHOLD))),
        4,
    )
    return normalized


def _normalize_dictionary_tagging_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["max_tags_per_document"] = max(
        1,
        int(inputs.get("max_tags_per_document") or payload.get("max_tags_per_document") or DEFAULT_MAX_TAGS_PER_DOCUMENT),
    )
    normalized["taxonomy_rules"] = _normalize_taxonomy_rules(inputs.get("taxonomy_rules") or payload.get("taxonomy_rules"))
    return normalized


def _normalize_embedding_cluster_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    embedding_uri = str(
        inputs.get("embedding_uri")
        or payload.get("embedding_uri")
        or f"{normalized['dataset_name']}.embeddings.jsonl"
    ).strip()
    if not embedding_uri:
        raise ValueError("embedding_uri is required")
    normalized["embedding_uri"] = embedding_uri
    normalized["cluster_similarity_threshold"] = round(
        max(
            0.0,
            min(
                1.0,
                float(
                    inputs.get("cluster_similarity_threshold")
                    or payload.get("cluster_similarity_threshold")
                    or DEFAULT_CLUSTER_SIMILARITY_THRESHOLD
                ),
            ),
        ),
        4,
    )
    return normalized


def _normalize_cluster_label_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_text_task_payload(payload)


def _normalize_prepare_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    output_path = str(payload.get("output_path") or f"{dataset_name}.prepared.jsonl").strip()
    if not output_path:
        raise ValueError("output_path is required")
    text_column = str(payload.get("text_column") or "text").strip()
    model = str(payload.get("model") or "").strip()
    prepare_batch_size = max(1, int(payload.get("prepare_batch_size") or DEFAULT_PREPARE_BATCH_SIZE))
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "model": model,
        "prepare_batch_size": prepare_batch_size,
    }


def _normalize_embedding_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    text_column = str(payload.get("text_column") or "text").strip()
    output_path = str(payload.get("output_path") or f"{dataset_name}.embeddings.jsonl").strip()
    embedding_model = str(payload.get("embedding_model") or DEFAULT_EMBEDDING_MODEL).strip()
    return {
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "embedding_model": embedding_model,
    }


def _normalize_sentiment_build_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    output_path = str(payload.get("output_path") or f"{dataset_name}.sentiment.jsonl").strip()
    if not output_path:
        raise ValueError("output_path is required")
    text_column = str(payload.get("text_column") or "normalized_text").strip()
    model = str(payload.get("model") or "").strip()
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "model": model,
    }


def _default_inputs(skill_name: str, *, goal: str = "") -> dict[str, Any]:
    return default_inputs_for_skill(skill_name, goal=goal)


def _normalize_inputs(skill_name: str, inputs: dict[str, Any], *, goal: str = "") -> dict[str, Any]:
    defaults = _default_inputs(skill_name, goal=goal)
    normalized = dict(defaults)
    for key, value in inputs.items():
        normalized[key] = value
    return normalized


__all__ = [
    "_default_inputs",
    "_normalize_breakdown_task_payload",
    "_normalize_cluster_label_payload",
    "_normalize_compare_task_payload",
    "_normalize_deduplicate_payload",
    "_normalize_dictionary_tagging_payload",
    "_normalize_embedding_cluster_payload",
    "_normalize_embedding_payload",
    "_normalize_inputs",
    "_normalize_prepare_payload",
    "_normalize_sentiment_build_payload",
    "_normalize_sentiment_summary_payload",
    "_normalize_text_task_payload",
    "_normalize_trend_task_payload",
]
