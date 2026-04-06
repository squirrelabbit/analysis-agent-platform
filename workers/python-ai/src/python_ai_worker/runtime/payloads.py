from __future__ import annotations

from typing import Any

from ..skill_bundle import default_inputs_for_skill
from .common import (
    _normalize_garbage_rule_names,
    _normalize_pos_prefixes,
    _normalize_prepare_regex_rule_names,
    _normalize_stopwords,
    _normalize_taxonomy_rules,
)
from .constants import (
    DEFAULT_CLUSTER_SIMILARITY_THRESHOLD,
    DEFAULT_DUPLICATE_THRESHOLD,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_GARBAGE_RULE_NAMES,
    DEFAULT_MAX_TAGS_PER_DOCUMENT,
    DEFAULT_NOUN_ALLOWED_POS_PREFIXES,
    DEFAULT_NOUN_MIN_TOKEN_LENGTH,
    DEFAULT_PREPARE_BATCH_SIZE,
    DEFAULT_SENTENCE_PREVIEW_PER_ROW,
    DEFAULT_SENTENCE_SPLIT_LANGUAGE,
)

DEFAULT_EMBEDDING_CHUNK_MAX_CHARS = 400
DEFAULT_EMBEDDING_CHUNK_OVERLAP_CHARS = 40


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
        "artifact_output_path": str(
            inputs.get("artifact_output_path") or payload.get("artifact_output_path") or ""
        ).strip(),
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
    normalized["prepared_dataset_name"] = str(
        inputs.get("prepared_dataset_name") or payload.get("prepared_dataset_name") or ""
    ).strip()
    normalized["row_id_column"] = str(inputs.get("row_id_column") or payload.get("row_id_column") or "row_id").strip()
    normalized["source_row_index_column"] = str(
        inputs.get("source_row_index_column") or payload.get("source_row_index_column") or "source_row_index"
    ).strip()
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


def _normalize_garbage_filter_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["garbage_rule_names"] = _normalize_garbage_rule_names(
        inputs.get("garbage_rule_names") or payload.get("garbage_rule_names") or DEFAULT_GARBAGE_RULE_NAMES
    )
    normalized["artifact_output_path"] = str(
        inputs.get("artifact_output_path") or payload.get("artifact_output_path") or ""
    ).strip()
    return normalized


def _normalize_noun_frequency_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["stopwords"] = _normalize_stopwords(inputs.get("stopwords") or payload.get("stopwords") or [])
    normalized["user_dictionary_path"] = str(
        inputs.get("user_dictionary_path") or payload.get("user_dictionary_path") or ""
    ).strip()
    normalized["min_token_length"] = max(
        1,
        int(inputs.get("min_token_length") or payload.get("min_token_length") or DEFAULT_NOUN_MIN_TOKEN_LENGTH),
    )
    normalized["allowed_pos_prefixes"] = _normalize_pos_prefixes(
        inputs.get("allowed_pos_prefixes") or payload.get("allowed_pos_prefixes") or DEFAULT_NOUN_ALLOWED_POS_PREFIXES
    ) or list(DEFAULT_NOUN_ALLOWED_POS_PREFIXES)
    return normalized


def _normalize_sentence_split_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    normalized["language"] = str(
        inputs.get("language") or payload.get("language") or DEFAULT_SENTENCE_SPLIT_LANGUAGE
    ).strip().lower() or DEFAULT_SENTENCE_SPLIT_LANGUAGE
    normalized["preview_sentences_per_row"] = max(
        1,
        int(
            inputs.get("preview_sentences_per_row")
            or payload.get("preview_sentences_per_row")
            or DEFAULT_SENTENCE_PREVIEW_PER_ROW
        ),
    )
    normalized["artifact_output_path"] = str(
        inputs.get("artifact_output_path") or payload.get("artifact_output_path") or ""
    ).strip()
    return normalized


def _normalize_embedding_cluster_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    step = normalized["step"]
    inputs = step.get("inputs") or {}
    embedding_index_ref = str(inputs.get("embedding_index_ref") or payload.get("embedding_index_ref") or "").strip()
    embedding_uri = str(inputs.get("embedding_uri") or payload.get("embedding_uri") or "").strip()
    chunk_ref = str(inputs.get("chunk_ref") or payload.get("chunk_ref") or "").strip()
    if not chunk_ref and embedding_uri.endswith(".jsonl"):
        chunk_ref = f"{embedding_uri[:-len('.jsonl')]}.chunks.parquet"
    chunk_format = str(inputs.get("chunk_format") or payload.get("chunk_format") or "").strip()
    if not chunk_format and chunk_ref.endswith(".parquet"):
        chunk_format = "parquet"
    if not embedding_uri and not embedding_index_ref:
        raise ValueError("embedding_uri or embedding_index_ref is required")
    normalized["embedding_uri"] = embedding_uri
    normalized["embedding_index_ref"] = embedding_index_ref
    normalized["chunk_ref"] = chunk_ref
    normalized["chunk_format"] = chunk_format
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
    output_path = str(payload.get("output_path") or f"{dataset_name}.prepared.parquet").strip()
    if not output_path:
        raise ValueError("output_path is required")
    text_column = str(payload.get("text_column") or "text").strip()
    model = str(payload.get("model") or "").strip()
    prepare_batch_size = max(1, int(payload.get("prepare_batch_size") or DEFAULT_PREPARE_BATCH_SIZE))
    regex_rule_names = _normalize_prepare_regex_rule_names(payload.get("regex_rule_names"))
    prepare_prompt_version = str(payload.get("prepare_prompt_version") or "").strip()
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "model": model,
        "prepare_batch_size": prepare_batch_size,
        "regex_rule_names": regex_rule_names,
        "prepare_prompt_version": prepare_prompt_version,
    }


def _normalize_embedding_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    text_column = str(payload.get("text_column") or "text").strip()
    output_path = str(payload.get("output_path") or "").strip()
    index_output_path = str(payload.get("index_output_path") or "").strip()
    if not index_output_path:
        if output_path.endswith(".jsonl"):
            index_output_path = f"{output_path[:-len('.jsonl')]}.index.parquet"
        else:
            index_output_path = f"{dataset_name}.embeddings.index.parquet"
    chunk_output_path = str(payload.get("chunk_output_path") or "").strip()
    embedding_model = str(payload.get("embedding_model") or DEFAULT_EMBEDDING_MODEL).strip()
    embedding_dimensions = max(0, int(payload.get("embedding_dimensions") or 0))
    chunk_max_chars = max(80, int(payload.get("chunk_max_chars") or DEFAULT_EMBEDDING_CHUNK_MAX_CHARS))
    chunk_overlap_chars = max(0, int(payload.get("chunk_overlap_chars") or DEFAULT_EMBEDDING_CHUNK_OVERLAP_CHARS))
    if chunk_overlap_chars >= chunk_max_chars:
        chunk_overlap_chars = max(0, chunk_max_chars // 4)
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "index_output_path": index_output_path,
        "chunk_output_path": chunk_output_path,
        "chunk_max_chars": chunk_max_chars,
        "chunk_overlap_chars": chunk_overlap_chars,
        "embedding_model": embedding_model,
        "embedding_dimensions": embedding_dimensions,
    }


def _normalize_sentiment_build_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    output_path = str(payload.get("output_path") or f"{dataset_name}.sentiment.parquet").strip()
    if not output_path:
        raise ValueError("output_path is required")
    text_column = str(payload.get("text_column") or "normalized_text").strip()
    model = str(payload.get("model") or "").strip()
    sentiment_prompt_version = str(payload.get("sentiment_prompt_version") or "").strip()
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "model": model,
        "sentiment_prompt_version": sentiment_prompt_version,
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
    "_normalize_garbage_filter_payload",
    "_normalize_embedding_cluster_payload",
    "_normalize_embedding_payload",
    "_normalize_inputs",
    "_normalize_noun_frequency_payload",
    "_normalize_prepare_payload",
    "_normalize_sentiment_build_payload",
    "_normalize_sentiment_summary_payload",
    "_normalize_sentence_split_payload",
    "_normalize_text_task_payload",
    "_normalize_trend_task_payload",
]
