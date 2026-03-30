from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .anthropic_client import AnthropicClient, AnthropicConfig
from .config import load_config


@dataclass(frozen=True)
class TaskCapability:
    name: str
    description: str


SUPPORTED_CAPABILITIES = [
    TaskCapability(name="planner", description="Generate replayable skill plans."),
    TaskCapability(name="dataset_prepare", description="Normalize and filter raw text rows into a prepared dataset artifact."),
    TaskCapability(name="sentiment_label", description="Attach deterministic sentiment labels to prepared text rows."),
    TaskCapability(name="embedding", description="Produce embedding vectors for unstructured rows."),
    TaskCapability(name="document_filter", description="Filter documents before downstream analysis steps."),
    TaskCapability(name="keyword_frequency", description="Count top keywords from filtered unstructured rows."),
    TaskCapability(name="time_bucket_count", description="Aggregate filtered issue rows by time bucket."),
    TaskCapability(name="meta_group_count", description="Aggregate filtered issue rows by metadata group."),
    TaskCapability(name="document_sample", description="Select representative documents for downstream summaries."),
    TaskCapability(name="issue_breakdown_summary", description="Break down issue volume by a metadata dimension."),
    TaskCapability(name="issue_trend_summary", description="Summarize issue volume trends over time buckets."),
    TaskCapability(name="issue_period_compare", description="Compare issue volume between two time windows."),
    TaskCapability(name="issue_sentiment_summary", description="Summarize sentiment label distribution and representative samples."),
    TaskCapability(name="semantic_search", description="Find evidence candidates for a request."),
    TaskCapability(name="issue_evidence_summary", description="Build user-facing evidence summaries for text analysis."),
    TaskCapability(name="evidence_pack", description="Build evidence bundles for final outputs."),
    TaskCapability(name="unstructured_issue_summary", description="Summarize top issues from unstructured text rows."),
]

TOKEN_PATTERN = re.compile(r"[0-9A-Za-z가-힣]{2,}")
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "this",
    "that",
    "from",
    "have",
    "were",
    "will",
    "about",
    "error",
    "issue",
    "please",
    "there",
    "있습니다",
    "합니다",
    "문의",
    "내용",
    "확인",
    "처리",
    "대한",
    "관련",
}
DEFAULT_EMBEDDING_MODEL = "token-overlap-v1"
SENTIMENT_LABELS = {"positive", "negative", "neutral", "mixed", "unknown"}
POSITIVE_SENTIMENT_TERMS = {
    "good",
    "great",
    "excellent",
    "fast",
    "resolved",
    "fixed",
    "satisfied",
    "thanks",
    "좋다",
    "만족",
    "편리",
    "빠르",
    "정상",
    "해결",
    "감사",
    "원활",
}
NEGATIVE_SENTIMENT_TERMS = {
    "bad",
    "issue",
    "error",
    "fail",
    "failed",
    "broken",
    "slow",
    "delay",
    "refund",
    "complaint",
    "문제",
    "오류",
    "실패",
    "불만",
    "불편",
    "지연",
    "환불",
    "반복",
    "안됨",
    "안돼",
    "끊김",
}


def capability_names() -> list[str]:
    return [item.name for item in SUPPORTED_CAPABILITIES]


def capability_payload() -> dict[str, Any]:
    return {
        "capabilities": [
            {"name": item.name, "description": item.description}
            for item in SUPPORTED_CAPABILITIES
        ]
    }


def run_task(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if name == "planner":
        return run_planner(payload)
    if name == "dataset_prepare":
        return run_dataset_prepare(payload)
    if name == "sentiment_label":
        return run_sentiment_label(payload)
    if name == "embedding":
        return run_embedding(payload)
    if name == "document_filter":
        return run_document_filter(payload)
    if name == "keyword_frequency":
        return run_keyword_frequency(payload)
    if name == "time_bucket_count":
        return run_time_bucket_count(payload)
    if name == "meta_group_count":
        return run_meta_group_count(payload)
    if name == "document_sample":
        return run_document_sample(payload)
    if name == "issue_breakdown_summary":
        return run_issue_breakdown_summary(payload)
    if name == "issue_trend_summary":
        return run_issue_trend_summary(payload)
    if name == "issue_period_compare":
        return run_issue_period_compare(payload)
    if name == "issue_sentiment_summary":
        return run_issue_sentiment_summary(payload)
    if name == "semantic_search":
        return run_semantic_search(payload)
    if name == "issue_evidence_summary":
        return run_issue_evidence_summary(payload)
    if name == "evidence_pack":
        return run_evidence_pack(payload)
    if name == "unstructured_issue_summary":
        return run_unstructured_issue_summary(payload)
    raise ValueError(f"unsupported capability: {name}")


def run_planner(payload: dict[str, Any]) -> dict[str, Any]:
    client = _anthropic_client()
    if client and client.is_enabled():
        try:
            return _run_planner_with_llm(client, payload)
        except Exception as exc:
            fallback = _run_rule_based_planner(payload)
            fallback["planner_type"] = "python-ai-fallback"
            fallback["planner_model"] = "rule-based-v1"
            fallback["planner_prompt_version"] = "planner-fallback-v1"
            fallback["notes"] = [f"anthropic planner fallback: {exc}"]
            return fallback
    return _run_rule_based_planner(payload)


def run_dataset_prepare(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_prepare_payload(payload)
    rows = _iter_rows(normalized["dataset_name"])
    output_path = Path(normalized["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    client = _anthropic_prepare_client(normalized["model"])
    kept_count = 0
    review_count = 0
    dropped_count = 0
    skipped_rows = 0

    with output_path.open("w", encoding="utf-8") as handle:
        for source_index, row in enumerate(rows):
            raw_text = str(row.get(normalized["text_column"]) or "").strip()
            if not raw_text:
                skipped_rows += 1
                continue

            prepared = _prepare_row(raw_text, client=client, model=normalized["model"])
            disposition = prepared["disposition"]
            if disposition == "drop":
                dropped_count += 1
                continue
            if disposition == "review":
                review_count += 1
            else:
                kept_count += 1

            prepared_row = dict(row)
            prepared_row["source_row_index"] = source_index
            prepared_row["raw_text"] = raw_text
            prepared_row["normalized_text"] = prepared["normalized_text"]
            prepared_row["prepare_disposition"] = disposition
            prepared_row["prepare_reason"] = prepared["reason"]
            prepared_row["quality_flags"] = prepared["quality_flags"]
            prepared_row["prepare_prompt_version"] = prepared["prompt_version"]
            handle.write(json.dumps(prepared_row, ensure_ascii=False))
            handle.write("\n")

    notes = [
        "dataset prepare artifact generated by python-ai worker",
        f"dataset source: {normalized['dataset_name']}",
        f"prepared output: {output_path}",
    ]
    prepare_model = "fallback-normalizer-v1"
    if client and client.is_enabled():
        prepare_model = client._config.model
        notes.append(f"prepare model: {prepare_model}")
    else:
        notes.append(f"prepare model: {prepare_model}")
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")

    prompt_version = "dataset-prepare-fallback-v1"
    if client and client.is_enabled():
        prompt_version = "dataset-prepare-anthropic-v1"

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "dataset_prepare",
            "dataset_version_id": normalized["dataset_version_id"],
            "source_dataset_name": normalized["dataset_name"],
            "prepare_uri": str(output_path),
            "prepare_model": prepare_model,
            "prepare_prompt_version": prompt_version,
            "prepared_text_column": "normalized_text",
            "summary": {
                "input_row_count": len(rows),
                "output_row_count": kept_count + review_count,
                "kept_count": kept_count,
                "review_count": review_count,
                "dropped_count": dropped_count,
                "text_column": normalized["text_column"],
            },
        },
    }


def run_embedding(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_embedding_payload(payload)
    documents = [item for item in _iter_documents(normalized["dataset_name"], normalized["text_column"]) if item]
    embedding_path = Path(normalized["output_path"])
    embedding_path.parent.mkdir(parents=True, exist_ok=True)

    with embedding_path.open("w", encoding="utf-8") as handle:
        for index, document in enumerate(documents):
            token_counts = Counter(_tokenize(document))
            record = {
                "source_index": index,
                "text": document,
                "token_counts": dict(token_counts),
                "norm": _vector_norm(token_counts),
            }
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")

    return {
        "notes": [
            "embedding sidecar generated by python-ai worker",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": {
            "skill_name": "embedding",
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": str(embedding_path),
            "embedding_model": normalized["embedding_model"],
            "document_count": len(documents),
        },
    }


def run_sentiment_label(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_sentiment_build_payload(payload)
    rows = _iter_rows(normalized["dataset_name"])
    output_path = Path(normalized["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    client = _anthropic_prepare_client(normalized["model"])
    label_counts: Counter[str] = Counter()
    skipped_rows = 0
    labeled_count = 0

    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            text = str(row.get(normalized["text_column"]) or "").strip()
            if not text:
                skipped_rows += 1
                continue
            labeled = _label_sentiment(text, client=client)
            labeled_row = dict(row)
            labeled_row["sentiment_label"] = labeled["label"]
            labeled_row["sentiment_confidence"] = labeled["confidence"]
            labeled_row["sentiment_reason"] = labeled["reason"]
            labeled_row["sentiment_prompt_version"] = labeled["prompt_version"]
            handle.write(json.dumps(labeled_row, ensure_ascii=False))
            handle.write("\n")
            label_counts.update([labeled["label"]])
            labeled_count += 1

    sentiment_model = "sentiment-fallback-v1"
    prompt_version = "sentiment-fallback-v1"
    if client and client.is_enabled():
        sentiment_model = client._config.model
        prompt_version = "sentiment-anthropic-v1"

    notes = [
        "sentiment label artifact generated by python-ai worker",
        f"dataset source: {normalized['dataset_name']}",
        f"sentiment output: {output_path}",
        f"sentiment model: {sentiment_model}",
    ]
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "sentiment_label",
            "dataset_version_id": normalized["dataset_version_id"],
            "source_dataset_name": normalized["dataset_name"],
            "sentiment_uri": str(output_path),
            "sentiment_model": sentiment_model,
            "sentiment_prompt_version": prompt_version,
            "sentiment_label_column": "sentiment_label",
            "sentiment_confidence_column": "sentiment_confidence",
            "sentiment_reason_column": "sentiment_reason",
            "summary": {
                "input_row_count": len(rows),
                "labeled_row_count": labeled_count,
                "text_column": normalized["text_column"],
                "label_counts": dict(sorted(label_counts.items())),
            },
        },
    }


def run_document_filter(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    indexed_rows = _indexed_rows(normalized["dataset_name"])
    query_tokens = set(_tokenize(normalized["query"]))
    matches: list[dict[str, Any]] = []

    if query_tokens:
        for item in indexed_rows:
            text = str(item["row"].get(normalized["text_column"]) or "").strip()
            if not text:
                continue
            score = sum(1 for token in _tokenize(text) if token in query_tokens)
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


def run_keyword_frequency(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    selected_rows = _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    tokens = Counter()
    total_terms = 0
    document_count = 0
    for item in selected_rows:
        if not item["text"]:
            continue
        row_tokens = _tokenize(item["text"])
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
    normalized = _normalize_trend_task_payload(payload)
    selected_rows = _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = _build_time_bucket_artifact(normalized, selected_rows)
    artifact["skill_name"] = "time_bucket_count"
    return {
        "notes": [
            f"time_bucket_count built {normalized['bucket']} buckets",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_meta_group_count(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_breakdown_task_payload(payload)
    selected_rows = _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = _build_meta_group_artifact(normalized, selected_rows)
    artifact["skill_name"] = "meta_group_count"
    return {
        "notes": [
            f"meta_group_count grouped rows by {normalized['dimension_column']}",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_document_sample(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    selected_rows = _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    samples = _rank_sample_rows(selected_rows, normalized["query"], normalized["sample_n"])
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


def run_semantic_search(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    embedding_uri = str(
        (normalized["step"].get("inputs") or {}).get("embedding_uri")
        or payload.get("embedding_uri")
        or f"{normalized['dataset_name']}.embeddings.jsonl"
    ).strip()
    if not embedding_uri:
        raise ValueError("embedding_uri is required")

    query_counts = Counter(_tokenize(normalized["query"]))
    matches = []
    for record in _iter_embedding_records(Path(embedding_uri)):
        score = _cosine_similarity(query_counts, record.get("token_counts") or {}, float(record.get("norm") or 0))
        matches.append(
            {
                "rank": 0,
                "source_index": int(record.get("source_index") or 0),
                "score": round(score, 6),
                "text": str(record.get("text") or "")[:240],
            }
        )

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


def run_issue_trend_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_trend_task_payload(payload)
    prior = _find_prior_artifact(payload.get("prior_artifacts"), "time_bucket_count")
    if prior:
        return {
            "notes": [
                "issue_trend_summary reused time_bucket_count artifact",
                f"dataset source: {normalized['dataset_name']}",
            ],
            "artifact": _copy_artifact_fields(prior, "issue_trend_summary", normalized["step"].get("step_id")),
        }

    selected_rows = _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = _build_time_bucket_artifact(normalized, selected_rows)
    artifact["skill_name"] = "issue_trend_summary"
    return {
        "notes": [
            f"python-ai built {normalized['bucket']} trend",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_issue_breakdown_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_breakdown_task_payload(payload)
    prior = _find_prior_artifact(payload.get("prior_artifacts"), "meta_group_count")
    if prior:
        return {
            "notes": [
                "issue_breakdown_summary reused meta_group_count artifact",
                f"dataset source: {normalized['dataset_name']}",
            ],
            "artifact": _copy_artifact_fields(prior, "issue_breakdown_summary", normalized["step"].get("step_id")),
        }

    selected_rows = _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = _build_meta_group_artifact(normalized, selected_rows)
    artifact["skill_name"] = "issue_breakdown_summary"
    return {
        "notes": [
            f"python-ai grouped rows by {normalized['dimension_column']}",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_issue_period_compare(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_compare_task_payload(payload)
    bucket_documents: dict[str, list[str]] = {}
    skipped_rows = 0

    for item in _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts")):
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
        bucket_documents.setdefault(bucket_label, []).append(text[:240])

    bucket_labels = sorted(bucket_documents)
    current_buckets, previous_buckets = _resolve_compare_periods(bucket_labels, normalized)
    current_documents = _collect_bucket_documents(bucket_documents, current_buckets)
    previous_documents = _collect_bucket_documents(bucket_documents, previous_buckets)
    current_terms = Counter()
    previous_terms = Counter()
    for document in current_documents:
        current_terms.update(_tokenize(document))
    for document in previous_documents:
        previous_terms.update(_tokenize(document))

    current_count = len(current_documents)
    previous_count = len(previous_documents)
    count_delta = current_count - previous_count
    count_delta_ratio_pct = None
    if previous_count > 0:
        count_delta_ratio_pct = round((count_delta / previous_count) * 100, 2)

    notes = [
        f"python-ai compared {normalized['window_size']} {normalized['bucket']} bucket(s)",
        f"dataset source: {normalized['dataset_name']}",
    ]
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")
    if not current_buckets:
        notes.append("current period could not be resolved")

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "issue_period_compare",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "time_column": normalized["time_column"],
            "bucket": normalized["bucket"],
            "window_size": normalized["window_size"],
            "summary": {
                "current_count": current_count,
                "previous_count": previous_count,
                "count_delta": count_delta,
                "count_delta_ratio_pct": count_delta_ratio_pct,
                "current_period_start": _period_start(current_buckets),
                "current_period_end": _period_end(current_buckets),
                "previous_period_start": _period_start(previous_buckets),
                "previous_period_end": _period_end(previous_buckets),
            },
            "periods": {
                "current": _build_period_payload(current_buckets, current_documents, current_terms, normalized["top_n"], normalized["sample_n"]),
                "previous": _build_period_payload(previous_buckets, previous_documents, previous_terms, normalized["top_n"], normalized["sample_n"]),
            },
            "top_term_deltas": _build_term_deltas(current_terms, previous_terms, normalized["top_n"]),
        },
    }


def run_issue_sentiment_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_sentiment_summary_payload(payload)
    label_counts: Counter[str] = Counter()
    label_samples: dict[str, list[str]] = {}
    unlabeled_rows = 0

    for item in _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts")):
        row = item["row"]
        text = item["text"]
        label = str(row.get(normalized["sentiment_column"]) or "").strip().lower()
        if not text or label not in SENTIMENT_LABELS:
            unlabeled_rows += 1
            continue
        label_counts.update([label])
        label_samples.setdefault(label, [])
        if len(label_samples[label]) < normalized["sample_n"]:
            label_samples[label].append(text[:240])

    total_labeled = sum(label_counts.values())
    ranked_labels = sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
    breakdown = []
    for rank, (label, count) in enumerate(ranked_labels, start=1):
        ratio_pct = round((count / total_labeled) * 100, 2) if total_labeled > 0 else 0.0
        breakdown.append(
            {
                "rank": rank,
                "sentiment_label": label,
                "count": count,
                "ratio_pct": ratio_pct,
                "samples": label_samples.get(label, []),
            }
        )

    notes = [
        f"python-ai summarized sentiment labels across {total_labeled} rows",
        f"dataset source: {normalized['dataset_name']}",
    ]
    if unlabeled_rows > 0:
        notes.append(f"unlabeled_rows={unlabeled_rows}")
    if not breakdown:
        notes.append("no labeled rows found")

    summary = {
        "document_count": total_labeled,
        "sentiment_column": normalized["sentiment_column"],
        "label_count": len(label_counts),
    }
    if breakdown:
        summary["dominant_label"] = breakdown[0]["sentiment_label"]
        summary["dominant_label_count"] = breakdown[0]["count"]
    for label in ("positive", "negative", "neutral", "mixed", "unknown"):
        count = label_counts.get(label, 0)
        summary[f"{label}_count"] = count
        summary[f"{label}_ratio_pct"] = round((count / total_labeled) * 100, 2) if total_labeled > 0 else 0.0

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "issue_sentiment_summary",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "text_column": normalized["text_column"],
            "sentiment_column": normalized["sentiment_column"],
            "summary": summary,
            "breakdown": breakdown,
        },
    }


def run_issue_evidence_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return _run_evidence_summary(payload, "issue_evidence_summary")


def run_evidence_pack(payload: dict[str, Any]) -> dict[str, Any]:
    return _run_evidence_summary(payload, "evidence_pack")


def _run_evidence_summary(payload: dict[str, Any], artifact_skill_name: str) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    selected, selection_source = _select_evidence_candidates(payload, normalized)
    client = _anthropic_client()
    if client and client.is_enabled():
        try:
            return _run_evidence_pack_with_llm(client, normalized, selected, selection_source, artifact_skill_name)
        except Exception as exc:
            fallback = _run_evidence_pack_fallback(normalized, selected, selection_source, artifact_skill_name)
            fallback["notes"].append(f"anthropic evidence fallback: {exc}")
            return fallback
    return _run_evidence_pack_fallback(normalized, selected, selection_source, artifact_skill_name)


def _run_rule_based_planner(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "dataset_from_version").strip()
    data_type = str(payload.get("data_type") or "").strip().lower()
    goal_raw = str(payload.get("goal") or "").strip()
    goal = goal_raw.lower()

    if data_type in {"mixed", "both"}:
        skills = ["structured_kpi_summary", "document_filter", "keyword_frequency", "document_sample", "unstructured_issue_summary", "issue_evidence_summary"]
    elif data_type == "unstructured" and _looks_sentiment_goal(goal):
        skills = ["document_filter", "document_sample", "issue_sentiment_summary", "issue_evidence_summary"]
    elif data_type == "unstructured" and _looks_semantic_search_goal(goal):
        skills = ["semantic_search", "issue_evidence_summary"]
    elif data_type == "unstructured" and _looks_compare_goal(goal):
        skills = ["document_filter", "document_sample", "issue_period_compare", "issue_evidence_summary"]
    elif data_type == "unstructured" and _looks_breakdown_goal(goal):
        skills = ["document_filter", "meta_group_count", "document_sample", "issue_breakdown_summary", "issue_evidence_summary"]
    elif data_type == "unstructured" and _looks_trend_goal(goal):
        skills = ["document_filter", "time_bucket_count", "document_sample", "issue_trend_summary", "issue_evidence_summary"]
    elif data_type == "unstructured" or _looks_unstructured(goal):
        skills = ["document_filter", "keyword_frequency", "document_sample", "unstructured_issue_summary", "issue_evidence_summary"]
    else:
        skills = ["structured_kpi_summary"]

    steps = []
    for skill_name in skills:
        step = {
            "skill_name": skill_name,
            "dataset_name": dataset_name,
            "inputs": _default_inputs(skill_name, goal=goal_raw),
        }
        steps.append(step)

    return {
        "plan": {
            "steps": steps,
            "notes": "planned by python-ai worker",
        },
        "planner_type": "python-ai",
        "planner_model": "rule-based-v1",
        "planner_prompt_version": "planner-http-v1",
    }


def run_unstructured_issue_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_text_task_payload(payload)
    keyword_artifact = _find_prior_artifact(payload.get("prior_artifacts"), "keyword_frequency")
    sample_artifact = _find_prior_artifact(payload.get("prior_artifacts"), "document_sample")
    if keyword_artifact or sample_artifact:
        summary = {
            "document_count": int((((keyword_artifact or {}).get("summary") or {}).get("document_count") or 0)),
            "unique_terms": int((((keyword_artifact or {}).get("summary") or {}).get("unique_terms") or 0)),
            "total_terms": int((((keyword_artifact or {}).get("summary") or {}).get("total_terms") or 0)),
        }
        top_terms = list((keyword_artifact or {}).get("top_terms") or [])
        samples = []
        for item in list((sample_artifact or {}).get("samples") or []):
            samples.append(
                {
                    "rank": int(item.get("rank") or 0),
                    "text": str(item.get("text") or "")[:240],
                }
            )
        return {
            "notes": [
                "unstructured_issue_summary reused support skill artifacts",
                f"dataset source: {normalized['dataset_name']}",
            ],
            "artifact": {
                "skill_name": "unstructured_issue_summary",
                "step_id": normalized["step"].get("step_id"),
                "dataset_name": normalized["dataset_name"],
                "summary": summary,
                "top_terms": top_terms,
                "samples": samples,
            },
        }

    documents = [item["text"] for item in _selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts")) if item["text"]]
    tokens = Counter()
    samples: list[dict[str, Any]] = []
    total_terms = 0
    for index, document in enumerate(documents):
        row_tokens = _tokenize(document)
        total_terms += len(row_tokens)
        tokens.update(row_tokens)
        if index < normalized["sample_n"]:
            samples.append({"rank": index + 1, "text": document[:240]})

    top_terms = [
        {"term": term, "count": count}
        for term, count in tokens.most_common(normalized["top_n"])
    ]
    notes = [
        f"python-ai analyzed {len(documents)} documents",
        f"dataset source: {normalized['dataset_name']}",
    ]
    if not documents:
        notes.append("no non-empty documents found")

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "unstructured_issue_summary",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "summary": {
                "document_count": len(documents),
                "unique_terms": len(tokens),
                "total_terms": total_terms,
            },
            "top_terms": top_terms,
            "samples": samples,
        },
    }


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


def _normalize_prepare_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    output_path = str(payload.get("output_path") or f"{dataset_name}.prepared.jsonl").strip()
    if not output_path:
        raise ValueError("output_path is required")
    text_column = str(payload.get("text_column") or "text").strip()
    model = str(payload.get("model") or "").strip()
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "output_path": output_path,
        "model": model,
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


def _selected_text_rows(dataset_name: str, text_column: str, prior_artifacts: Any) -> list[dict[str, Any]]:
    selected_indices = _extract_document_filter_indices(prior_artifacts)
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


def _iter_documents(dataset_name: str, text_column: str) -> list[str]:
    return [str(row.get(text_column) or "").strip() for row in _iter_rows(dataset_name)]


def _iter_rows(dataset_name: str) -> list[dict[str, Any]]:
    path = Path(dataset_name)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv_rows(path)
    if suffix == ".jsonl":
        return _read_jsonl_rows(path)
    if suffix == ".txt":
        return [{"text": line.strip()} for line in path.read_text(encoding="utf-8").splitlines()]
    raise ValueError("dataset_name must point to a .csv, .jsonl, or .txt file")


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _normalize_prepared_text(text: str) -> str:
    normalized = text.strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[!?.]{2,}", ".", normalized)
    normalized = re.sub(r"[_\-=/]{3,}", " ", normalized)
    return normalized.strip()


def _looks_noise_only(text: str) -> bool:
    if not text:
        return True
    tokens = TOKEN_PATTERN.findall(text.lower())
    if tokens:
        return False
    return True


def _tokenize(text: str) -> list[str]:
    tokens = []
    for match in TOKEN_PATTERN.findall(text.lower()):
        if match in STOPWORDS:
            continue
        tokens.append(match)
    return tokens


def _looks_unstructured(goal: str) -> bool:
    keywords = ("issue", "voc", "text", "document", "review", "이슈", "문의", "리뷰", "문서", "텍스트")
    return any(keyword in goal for keyword in keywords)


def _looks_semantic_search_goal(goal: str) -> bool:
    keywords = ("search", "evidence", "find", "relevant", "근거", "찾아", "검색", "관련 문서")
    return any(keyword in goal for keyword in keywords)


def _looks_trend_goal(goal: str) -> bool:
    keywords = ("trend", "increase", "decrease", "change", "recent", "over time", "추세", "증가", "감소", "변화", "급증", "최근")
    return any(keyword in goal for keyword in keywords)


def _looks_compare_goal(goal: str) -> bool:
    keywords = ("compare", "versus", "vs", "difference", "period compare", "전주", "전월", "지난주", "지난달", "대비", "비교", "달라졌", "얼마나 달라")
    return any(keyword in goal for keyword in keywords)


def _looks_breakdown_goal(goal: str) -> bool:
    keywords = ("breakdown", "group by", "channel", "source", "product", "region", "채널별", "제품별", "상태별", "분해", "어디서", "어느 채널")
    return any(keyword in goal for keyword in keywords)


def _looks_sentiment_goal(goal: str) -> bool:
    keywords = ("sentiment", "positive", "negative", "neutral", "긍정", "부정", "중립", "감성", "감정", "호감", "불만", "만족")
    return any(keyword in goal for keyword in keywords)


def _default_inputs(skill_name: str, *, goal: str = "") -> dict[str, Any]:
    if skill_name == "structured_kpi_summary":
        return {
            "time_column": "date",
            "metric_column": "value",
        }
    if skill_name == "document_filter":
        inputs = {
            "text_column": "text",
            "sample_n": 5,
        }
        if goal:
            inputs["query"] = goal
        return inputs
    if skill_name == "keyword_frequency":
        return {
            "text_column": "text",
            "top_n": 10,
        }
    if skill_name == "time_bucket_count":
        return {
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "top_n": 5,
            "sample_n": 3,
        }
    if skill_name == "meta_group_count":
        return {
            "text_column": "text",
            "dimension_column": "channel",
            "top_n": 5,
            "sample_n": 3,
        }
    if skill_name == "document_sample":
        inputs = {
            "text_column": "text",
            "sample_n": 3,
        }
        if goal:
            inputs["query"] = goal
        return inputs
    if skill_name == "unstructured_issue_summary":
        return {
            "text_column": "text",
            "top_n": 10,
            "sample_n": 3,
        }
    if skill_name == "issue_breakdown_summary":
        return {
            "text_column": "text",
            "dimension_column": "channel",
            "top_n": 5,
            "sample_n": 3,
        }
    if skill_name == "issue_trend_summary":
        return {
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "top_n": 5,
            "sample_n": 3,
        }
    if skill_name == "issue_period_compare":
        return {
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "window_size": 1,
            "top_n": 5,
            "sample_n": 3,
        }
    if skill_name == "issue_sentiment_summary":
        return {
            "text_column": "text",
            "sentiment_column": "sentiment_label",
            "sample_n": 3,
        }
    if skill_name == "issue_evidence_summary":
        inputs = {
            "text_column": "text",
            "sample_n": 3,
        }
        if goal:
            inputs["query"] = goal
        return inputs
    if skill_name == "semantic_search":
        inputs = {
            "text_column": "text",
            "sample_n": 5,
        }
        if goal:
            inputs["query"] = goal
        return inputs
    if skill_name == "evidence_pack":
        inputs = {
            "text_column": "text",
            "sample_n": 3,
        }
        if goal:
            inputs["query"] = goal
        return inputs
    return {}


def _anthropic_client() -> AnthropicClient | None:
    config = load_config()
    if config.llm_provider.lower() != "anthropic":
        return None
    return AnthropicClient(
        AnthropicConfig(
            api_key=config.anthropic_api_key,
            model=config.anthropic_model,
            api_url=config.anthropic_api_url,
            version=config.anthropic_version,
            max_tokens=config.anthropic_max_tokens,
            timeout_sec=config.anthropic_timeout_sec,
        )
    )


def _anthropic_prepare_client(model_override: str = "") -> AnthropicClient | None:
    config = load_config()
    if config.llm_provider.lower() != "anthropic":
        return None
    model = model_override.strip() or config.anthropic_prepare_model.strip() or config.anthropic_model
    return AnthropicClient(
        AnthropicConfig(
            api_key=config.anthropic_api_key,
            model=model,
            api_url=config.anthropic_api_url,
            version=config.anthropic_version,
            max_tokens=config.anthropic_max_tokens,
            timeout_sec=config.anthropic_timeout_sec,
        )
    )


def _run_planner_with_llm(client: AnthropicClient, payload: dict[str, Any]) -> dict[str, Any]:
    prompt = "\n".join(
        [
            "You are an analysis planner for a deterministic execution platform.",
            "Choose the smallest valid skill plan for the request.",
            "Allowed skills: structured_kpi_summary, document_filter, keyword_frequency, time_bucket_count, meta_group_count, document_sample, unstructured_issue_summary, issue_breakdown_summary, issue_trend_summary, issue_period_compare, issue_sentiment_summary, semantic_search, issue_evidence_summary.",
            "Use structured_kpi_summary for numeric KPI/tabular analysis.",
            "Use document_filter first for replayable lexical narrowing before downstream text analysis.",
            "Use keyword_frequency to count top terms after document filtering.",
            "Use time_bucket_count to aggregate filtered rows by time bucket.",
            "Use meta_group_count to aggregate filtered rows by metadata dimension.",
            "Use document_sample to select representative documents for downstream summaries.",
            "Use unstructured_issue_summary for VOC/document/text analysis.",
            "Use issue_breakdown_summary when the user asks which channel, product, region, or metadata group has more issues.",
            "Use issue_trend_summary when the user asks about changes, increases, decreases, or time-based trends in text issues.",
            "Use issue_period_compare when the user asks to compare current vs previous periods in text issues.",
            "Use issue_sentiment_summary when the user asks about positive, negative, neutral, or sentiment distribution.",
            "Use semantic_search when the user asks to find relevant evidence or related documents.",
            "Use issue_evidence_summary to return representative snippets and follow-up questions for text analysis.",
            "For general unstructured text analysis, prefer unstructured_issue_summary followed by issue_evidence_summary.",
            "For general unstructured text analysis, prefer document_filter, keyword_frequency, document_sample, unstructured_issue_summary, then issue_evidence_summary.",
            "For breakdown analysis, prefer document_filter, meta_group_count, document_sample, issue_breakdown_summary, then issue_evidence_summary.",
            "For trend analysis, prefer document_filter, time_bucket_count, document_sample, issue_trend_summary, then issue_evidence_summary.",
            "For period comparison, prefer document_filter, document_sample, issue_period_compare, then issue_evidence_summary.",
            "For sentiment analysis, prefer document_filter, document_sample, issue_sentiment_summary, then issue_evidence_summary.",
            "For evidence lookup, prefer semantic_search followed by issue_evidence_summary.",
            "When issue_evidence_summary is used, set inputs.query to the user goal.",
            "When semantic_search is used, set inputs.query to the user goal.",
            "Return only a plan that can be replayed without extra reasoning.",
            "",
            f"dataset_name: {payload.get('dataset_name') or 'dataset_from_version'}",
            f"dataset_version_id: {payload.get('dataset_version_id') or ''}",
            f"data_type: {payload.get('data_type') or ''}",
            f"goal: {payload.get('goal') or ''}",
            f"constraints: {json.dumps(payload.get('constraints') or [], ensure_ascii=False)}",
            f"context: {json.dumps(payload.get('context') or {}, ensure_ascii=False)}",
        ]
    )
    response = client.create_json(prompt=prompt, schema=_planner_schema(), max_tokens=1200)
    return _normalize_planner_response(response, payload, planner_model=client._config.model)


def _run_evidence_pack_with_llm(
    client: AnthropicClient,
    normalized: dict[str, Any],
    selected_documents: list[dict[str, Any]],
    selection_source: str,
    artifact_skill_name: str,
) -> dict[str, Any]:
    prompt = "\n".join(
        [
            "You are generating an evidence pack for an analysis execution.",
            "Summarize the issue briefly and cite the most relevant snippets using the provided source_index values.",
            "Do not invent evidence outside the provided snippets.",
            "",
            f"dataset_name: {normalized['dataset_name']}",
            f"query: {normalized['query']}",
            "documents:",
            json.dumps(selected_documents, ensure_ascii=False),
        ]
    )
    response = client.create_json(prompt=prompt, schema=_evidence_schema(), max_tokens=1400)
    artifact = {
        "skill_name": artifact_skill_name,
        "step_id": normalized["step"].get("step_id"),
        "dataset_name": normalized["dataset_name"],
        "query": normalized["query"],
        "selection_source": selection_source,
        "summary": response.get("summary") or "",
        "key_findings": response.get("key_findings") or [],
        "evidence": response.get("evidence") or [],
        "follow_up_questions": response.get("follow_up_questions") or [],
    }
    return {
        "notes": [
            f"{artifact_skill_name} generated by anthropic",
            f"model: {client._config.model}",
            f"selection source: {selection_source}",
        ],
        "artifact": artifact,
    }


def _run_evidence_pack_fallback(
    normalized: dict[str, Any],
    selected_documents: list[dict[str, Any]],
    selection_source: str,
    artifact_skill_name: str,
) -> dict[str, Any]:
    snippets = []
    for item in selected_documents:
        snippets.append(
            {
                "rank": item["rank"],
                "source_index": item["source_index"],
                "snippet": item["text"][:240],
                "rationale": _evidence_rationale(item, selection_source),
            }
        )

    top_terms = Counter()
    for item in selected_documents:
        top_terms.update(_tokenize(item["text"]))

    artifact = {
        "skill_name": artifact_skill_name,
        "step_id": normalized["step"].get("step_id"),
        "dataset_name": normalized["dataset_name"],
        "query": normalized["query"],
        "selection_source": selection_source,
        "summary": _fallback_evidence_summary(normalized["query"], snippets, top_terms),
        "key_findings": [
            f"selected_documents={len(selected_documents)}",
            f"top_terms={[term for term, _ in top_terms.most_common(5)]}",
        ],
        "evidence": snippets,
        "follow_up_questions": _fallback_follow_up_questions(normalized["query"]),
    }
    return {
        "notes": [
            f"{artifact_skill_name} generated by fallback summarizer",
            f"dataset source: {normalized['dataset_name']}",
            f"selection source: {selection_source}",
        ],
        "artifact": artifact,
    }


def _prepare_row(
    raw_text: str,
    *,
    client: AnthropicClient | None,
    model: str,
) -> dict[str, Any]:
    if client and client.is_enabled():
        try:
            return _prepare_row_with_llm(client, raw_text)
        except Exception as exc:
            fallback = _prepare_row_fallback(raw_text)
            fallback["quality_flags"] = list(fallback["quality_flags"]) + [f"llm_fallback:{exc}"]
            return fallback
    return _prepare_row_fallback(raw_text)


def _prepare_row_with_llm(client: AnthropicClient, raw_text: str) -> dict[str, Any]:
    prompt = "\n".join(
        [
            "You are preparing raw VOC or issue text for deterministic downstream analysis.",
            "Keep the original meaning. Remove only obvious noise, duplicated punctuation, and boilerplate.",
            "Do not summarize beyond a short normalization. Do not invent facts.",
            "Choose disposition keep, review, or drop.",
            "Use drop only for empty, unreadable noise, or clear non-content rows.",
            "Use review when the text is partially readable but low quality or mixed.",
            "",
            f"raw_text: {raw_text}",
        ]
    )
    response = client.create_json(prompt=prompt, schema=_prepare_schema(), max_tokens=600)
    disposition = str(response.get("disposition") or "review").strip().lower()
    if disposition not in {"keep", "review", "drop"}:
        disposition = "review"
    normalized_text = _normalize_prepared_text(str(response.get("normalized_text") or raw_text))
    if disposition != "drop" and not normalized_text:
        disposition = "review"
        normalized_text = _normalize_prepared_text(raw_text)
    return {
        "disposition": disposition,
        "normalized_text": normalized_text,
        "reason": str(response.get("reason") or "").strip(),
        "quality_flags": _coerce_string_list(response.get("quality_flags")),
        "prompt_version": "dataset-prepare-anthropic-v1",
    }


def _prepare_row_fallback(raw_text: str) -> dict[str, Any]:
    normalized_text = _normalize_prepared_text(raw_text)
    disposition = "keep"
    flags: list[str] = []
    reason = "text kept after deterministic normalization"

    if not normalized_text:
        disposition = "drop"
        reason = "empty after normalization"
    elif len(normalized_text) < 4:
        disposition = "review"
        flags.append("short_text")
        reason = "text is very short"
    elif _looks_noise_only(normalized_text):
        disposition = "drop"
        flags.append("noise_only")
        reason = "text is mostly noise"
    elif normalized_text != raw_text.strip():
        flags.append("normalized")

    return {
        "disposition": disposition,
        "normalized_text": normalized_text,
        "reason": reason,
        "quality_flags": flags,
        "prompt_version": "dataset-prepare-fallback-v1",
    }


def _label_sentiment(text: str, *, client: AnthropicClient | None) -> dict[str, Any]:
    if client and client.is_enabled():
        try:
            return _label_sentiment_with_llm(client, text)
        except Exception as exc:
            fallback = _label_sentiment_fallback(text)
            fallback["reason"] = f"{fallback['reason']} (llm_fallback: {exc})"
            return fallback
    return _label_sentiment_fallback(text)


def _label_sentiment_with_llm(client: AnthropicClient, text: str) -> dict[str, Any]:
    prompt = "\n".join(
        [
            "You are labeling sentiment for customer feedback or issue text.",
            "Classify one label only: positive, negative, neutral, mixed, or unknown.",
            "negative: complaint, failure, error, dissatisfaction, delay, refund, or blocked experience.",
            "positive: satisfaction, appreciation, successful resolution, or clearly favorable experience.",
            "neutral: factual report without clear positive or negative sentiment.",
            "mixed: both positive and negative signals are explicit in the same text.",
            "unknown: the text is too ambiguous or too short to classify reliably.",
            "Do not invent context beyond the text.",
            "",
            f"text: {text}",
        ]
    )
    response = client.create_json(prompt=prompt, schema=_sentiment_schema(), max_tokens=400)
    label = str(response.get("label") or "unknown").strip().lower()
    if label not in SENTIMENT_LABELS:
        label = "unknown"
    confidence = float(response.get("confidence") or 0.0)
    confidence = max(0.0, min(1.0, round(confidence, 4)))
    return {
        "label": label,
        "confidence": confidence,
        "reason": str(response.get("reason") or "").strip(),
        "prompt_version": "sentiment-anthropic-v1",
    }


def _label_sentiment_fallback(text: str) -> dict[str, Any]:
    tokens = _tokenize(text)
    if not tokens:
        return {
            "label": "unknown",
            "confidence": 0.2,
            "reason": "no meaningful tokens detected",
            "prompt_version": "sentiment-fallback-v1",
        }

    positive_score = sum(1 for token in tokens if _matches_sentiment_term(token, POSITIVE_SENTIMENT_TERMS))
    negative_score = sum(1 for token in tokens if _matches_sentiment_term(token, NEGATIVE_SENTIMENT_TERMS))

    if positive_score > 0 and negative_score > 0:
        label = "mixed"
        confidence = 0.72
        reason = "both positive and negative sentiment markers were detected"
    elif negative_score > positive_score and negative_score > 0:
        label = "negative"
        confidence = 0.85 if negative_score >= 2 else 0.68
        reason = "negative sentiment markers were dominant"
    elif positive_score > negative_score and positive_score > 0:
        label = "positive"
        confidence = 0.85 if positive_score >= 2 else 0.68
        reason = "positive sentiment markers were dominant"
    else:
        label = "neutral"
        confidence = 0.55
        reason = "no strong positive or negative marker was detected"

    return {
        "label": label,
        "confidence": confidence,
        "reason": reason,
        "prompt_version": "sentiment-fallback-v1",
    }


def _matches_sentiment_term(token: str, lexicon: set[str]) -> bool:
    for term in lexicon:
        if token == term or term in token or token in term:
            return True
    return False


def _planner_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "plan": {
                "type": "object",
                "properties": {
                    "notes": {"type": "string"},
                    "steps": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "skill_name": {"type": "string"},
                                "dataset_name": {"type": "string"},
                                "inputs": {
                                    "type": "object",
                                    "properties": {
                                        "time_column": {"type": "string"},
                                        "metric_column": {"type": "string"},
                                        "text_column": {"type": "string"},
                                        "dimension_column": {"type": "string"},
                                        "bucket": {"type": "string"},
                                        "window_size": {"type": "integer"},
                                        "current_start_bucket": {"type": "string"},
                                        "current_end_bucket": {"type": "string"},
                                        "previous_start_bucket": {"type": "string"},
                                        "previous_end_bucket": {"type": "string"},
                                        "sentiment_column": {"type": "string"},
                                        "embedding_uri": {"type": "string"},
                                        "query": {"type": "string"},
                                        "top_n": {"type": "integer"},
                                        "sample_n": {"type": "integer"},
                                    },
                                    "additionalProperties": True,
                                },
                            },
                            "required": ["skill_name", "dataset_name", "inputs"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["notes", "steps"],
                "additionalProperties": False,
            }
        },
        "required": ["plan"],
        "additionalProperties": False,
    }


def _evidence_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "key_findings": {
                "type": "array",
                "items": {"type": "string"},
            },
            "evidence": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "rank": {"type": "integer"},
                        "source_index": {"type": "integer"},
                        "snippet": {"type": "string"},
                        "rationale": {"type": "string"},
                    },
                    "required": ["rank", "source_index", "snippet", "rationale"],
                    "additionalProperties": False,
                },
            },
            "follow_up_questions": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["summary", "key_findings", "evidence", "follow_up_questions"],
        "additionalProperties": False,
    }


def _prepare_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "disposition": {"type": "string"},
            "normalized_text": {"type": "string"},
            "reason": {"type": "string"},
            "quality_flags": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["disposition", "normalized_text", "reason", "quality_flags"],
        "additionalProperties": False,
    }


def _sentiment_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "label": {"type": "string"},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
        },
        "required": ["label", "confidence", "reason"],
        "additionalProperties": False,
    }


def _normalize_planner_response(
    response: dict[str, Any],
    payload: dict[str, Any],
    *,
    planner_model: str,
) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "dataset_from_version").strip()
    raw_plan = response.get("plan") or {}
    raw_steps = raw_plan.get("steps") or []
    steps = []
    for raw_step in raw_steps:
        skill_name = str(raw_step.get("skill_name") or "").strip()
        if skill_name not in {"structured_kpi_summary", "document_filter", "keyword_frequency", "time_bucket_count", "meta_group_count", "document_sample", "unstructured_issue_summary", "issue_breakdown_summary", "issue_trend_summary", "issue_period_compare", "issue_sentiment_summary", "semantic_search", "issue_evidence_summary", "evidence_pack"}:
            continue
        inputs = raw_step.get("inputs") or {}
        steps.append(
            {
                "skill_name": skill_name,
                "dataset_name": str(raw_step.get("dataset_name") or dataset_name).strip() or dataset_name,
                "inputs": _normalize_inputs(
                    skill_name,
                    inputs,
                    goal=str(payload.get("goal") or "").strip(),
                ),
            }
        )

    if not steps:
        return _run_rule_based_planner(payload)

    return {
        "plan": {
            "steps": steps,
            "notes": str(raw_plan.get("notes") or "planned by anthropic").strip(),
        },
        "planner_type": "anthropic",
        "planner_model": planner_model,
        "planner_prompt_version": "planner-anthropic-v1",
    }


def _normalize_inputs(skill_name: str, inputs: dict[str, Any], *, goal: str = "") -> dict[str, Any]:
    defaults = _default_inputs(skill_name, goal=goal)
    normalized = dict(defaults)
    for key, value in inputs.items():
        normalized[key] = value
    return normalized


def _iter_embedding_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ValueError(f"embedding_uri does not exist: {path}")
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def _vector_norm(token_counts: Counter[str]) -> float:
    total = sum(value * value for value in token_counts.values())
    return math.sqrt(total)


def _cosine_similarity(query_counts: Counter[str], doc_counts: dict[str, int], doc_norm: float) -> float:
    if not query_counts or not doc_counts or doc_norm <= 0:
        return 0.0
    query_norm = _vector_norm(query_counts)
    if query_norm <= 0:
        return 0.0
    dot_product = 0
    for token, query_value in query_counts.items():
        dot_product += query_value * int(doc_counts.get(token) or 0)
    return dot_product / (query_norm * doc_norm)


def _rank_documents(documents: list[str], query: str) -> list[dict[str, Any]]:
    query_tokens = set(_tokenize(query))
    ranked = []
    for index, document in enumerate(documents):
        tokens = _tokenize(document)
        overlap = sum(1 for token in tokens if token in query_tokens)
        ranked.append(
            {
                "rank": 0,
                "source_index": index,
                "score": overlap,
                "text": document,
            }
        )

    ranked.sort(key=lambda item: (-item["score"], item["source_index"]))
    for order, item in enumerate(ranked, start=1):
        item["rank"] = order
    return ranked


def _fallback_evidence_summary(query: str, snippets: list[dict[str, Any]], top_terms: Counter[str]) -> str:
    if not snippets:
        return "선택된 근거 문서가 없습니다."
    top_term_list = ", ".join(term for term, _ in top_terms.most_common(5))
    if query:
        return f"질문 '{query}' 기준으로 관련 문서를 추렸고, 주요 용어는 {top_term_list} 입니다."
    return f"대표 문서를 추렸고, 주요 용어는 {top_term_list} 입니다."


def _fallback_follow_up_questions(query: str) -> list[str]:
    if query:
        return [
            f"'{query}'와 직접 연결되는 메타 컬럼은 무엇인가?",
            "기간별 변화도 같이 비교할 것인가?",
        ]
    return [
        "기간별 변화도 같이 비교할 것인가?",
        "제품/채널별 분해가 필요한가?",
    ]


def _evidence_rationale(item: dict[str, Any], selection_source: str) -> str:
    if selection_source == "semantic_search":
        score = float(item.get("score") or 0)
        return f"selected by semantic similarity (score={score:.3f})"
    if selection_source == "document_sample":
        score = float(item.get("score") or 0)
        if score > 0:
            return f"selected by document_sample support skill (score={score:.3f})"
        return "selected by document_sample support skill"
    if float(item.get("score") or 0) > 0:
        return "selected by lexical overlap"
    return "selected by source order"


def _resolve_compare_periods(bucket_labels: list[str], normalized: dict[str, Any]) -> tuple[list[str], list[str]]:
    current_start = normalized["current_start_bucket"]
    current_end = normalized["current_end_bucket"]
    previous_start = normalized["previous_start_bucket"]
    previous_end = normalized["previous_end_bucket"]
    if current_start or current_end or previous_start or previous_end:
        current = [label for label in bucket_labels if _in_bucket_range(label, current_start, current_end)]
        previous = [label for label in bucket_labels if _in_bucket_range(label, previous_start, previous_end)]
        return current, previous

    window_size = normalized["window_size"]
    if not bucket_labels:
        return [], []
    current = bucket_labels[-window_size:]
    previous_end_index = max(0, len(bucket_labels) - window_size)
    previous_start_index = max(0, previous_end_index - window_size)
    previous = bucket_labels[previous_start_index:previous_end_index]
    return current, previous


def _in_bucket_range(label: str, start: str, end: str) -> bool:
    if start and label < start:
        return False
    if end and label > end:
        return False
    return True


def _collect_bucket_documents(bucket_documents: dict[str, list[str]], buckets: list[str]) -> list[str]:
    documents: list[str] = []
    for bucket in buckets:
        documents.extend(bucket_documents.get(bucket, []))
    return documents


def _build_period_payload(
    buckets: list[str],
    documents: list[str],
    terms: Counter[str],
    top_n: int,
    sample_n: int,
) -> dict[str, Any]:
    return {
        "start_bucket": _period_start(buckets),
        "end_bucket": _period_end(buckets),
        "bucket_count": len(buckets),
        "document_count": len(documents),
        "top_terms": [
            {"term": term, "count": count}
            for term, count in terms.most_common(top_n)
        ],
        "samples": documents[:sample_n],
    }


def _build_term_deltas(current_terms: Counter[str], previous_terms: Counter[str], top_n: int) -> list[dict[str, Any]]:
    candidates = set(current_terms.keys()) | set(previous_terms.keys())
    rows = []
    for term in candidates:
        current_count = current_terms.get(term, 0)
        previous_count = previous_terms.get(term, 0)
        delta = current_count - previous_count
        rows.append(
            {
                "term": term,
                "current_count": current_count,
                "previous_count": previous_count,
                "delta": delta,
            }
        )
    rows.sort(key=lambda item: (-abs(item["delta"]), -item["current_count"], item["term"]))
    return rows[:top_n]


def _period_start(buckets: list[str]) -> str | None:
    if not buckets:
        return None
    return buckets[0]


def _period_end(buckets: list[str]) -> str | None:
    if not buckets:
        return None
    return buckets[-1]


def _parse_timestamp(raw: str) -> datetime | None:
    value = raw.strip()
    if not value:
        return None

    candidates = [value]
    if value.endswith("Z"):
        candidates.insert(0, value[:-1] + "+00:00")

    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue

    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(value, pattern)
        except ValueError:
            continue
    return None


def _bucket_label(timestamp: datetime, bucket: str) -> str:
    if bucket == "week":
        week_start = timestamp - timedelta(days=timestamp.weekday())
        return week_start.date().isoformat()
    if bucket == "month":
        return f"{timestamp.year:04d}-{timestamp.month:02d}"
    return timestamp.date().isoformat()
