from __future__ import annotations

import json
from collections import Counter
from typing import Any, Callable

from ..anthropic_client import AnthropicClient, AnthropicConfig
from ..config import load_config
from ..skill_bundle import plan_skill_names
from .common import (
    _coerce_string_list,
    _evidence_rationale,
    _fallback_evidence_summary,
    _fallback_follow_up_questions,
    _looks_noise_only,
    _normalize_prepared_text,
    _tokenize,
)
from .constants import NEGATIVE_SENTIMENT_TERMS, POSITIVE_SENTIMENT_TERMS, SENTIMENT_LABELS
from .payloads import _normalize_inputs


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


def _run_planner_with_llm(
    client: AnthropicClient,
    payload: dict[str, Any],
    *,
    fallback_planner: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    allowed_skills = ", ".join(plan_skill_names())
    prompt = "\n".join(
        [
            "You are an analysis planner for a deterministic execution platform.",
            "Choose the smallest valid skill plan for the request.",
            f"Allowed skills: {allowed_skills}.",
            "Use structured_kpi_summary for numeric KPI/tabular analysis.",
            "Use document_filter first for replayable lexical narrowing before downstream text analysis.",
            "Use deduplicate_documents to collapse repeated or near-identical documents.",
            "Use keyword_frequency to count top terms after document filtering.",
            "Use time_bucket_count to aggregate filtered rows by time bucket.",
            "Use meta_group_count to aggregate filtered rows by metadata dimension.",
            "Use document_sample to select representative documents for downstream summaries.",
            "Use dictionary_tagging when the user asks for category or taxonomy-based classification.",
            "Use embedding_cluster to group similar issues with precomputed embeddings.",
            "Use cluster_label_candidates after embedding_cluster to propose deterministic cluster labels.",
            "Use unstructured_issue_summary for VOC/document/text analysis.",
            "Use issue_breakdown_summary when the user asks which channel, product, region, or metadata group has more issues.",
            "Use issue_cluster_summary when the user asks for major themes, clusters, or grouped issues.",
            "Use issue_trend_summary when the user asks about changes, increases, decreases, or time-based trends in text issues.",
            "Use issue_period_compare when the user asks to compare current vs previous periods in text issues.",
            "Use issue_sentiment_summary when the user asks about positive, negative, neutral, or sentiment distribution.",
            "Use issue_taxonomy_summary when the user asks for tagged categories or taxonomy distribution.",
            "Use semantic_search when the user asks to find relevant evidence or related documents.",
            "Use issue_evidence_summary to return representative snippets and follow-up questions for text analysis.",
            "For general unstructured text analysis, prefer unstructured_issue_summary followed by issue_evidence_summary.",
            "For general unstructured text analysis, prefer document_filter, keyword_frequency, document_sample, unstructured_issue_summary, then issue_evidence_summary.",
            "For cluster analysis, prefer document_filter, deduplicate_documents, embedding_cluster, cluster_label_candidates, issue_cluster_summary, then issue_evidence_summary.",
            "For taxonomy analysis, prefer document_filter, dictionary_tagging, issue_taxonomy_summary, then issue_evidence_summary.",
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
    return _normalize_planner_response(
        response,
        payload,
        planner_model=client._config.model,
        fallback_planner=fallback_planner,
    )


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
    fallback_planner: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dataset_name = str(payload.get("dataset_name") or "dataset_from_version").strip()
    raw_plan = response.get("plan") or {}
    raw_steps = raw_plan.get("steps") or []
    allowed_skills = set(plan_skill_names())
    steps = []
    for raw_step in raw_steps:
        skill_name = str(raw_step.get("skill_name") or "").strip()
        if skill_name not in allowed_skills:
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
        if fallback_planner is not None:
            return fallback_planner(payload)
        raise ValueError("planner did not return any valid steps")

    return {
        "plan": {
            "steps": steps,
            "notes": str(raw_plan.get("notes") or "planned by anthropic").strip(),
        },
        "planner_type": "anthropic",
        "planner_model": planner_model,
        "planner_prompt_version": "planner-anthropic-v1",
    }


__all__ = [
    "_anthropic_client",
    "_anthropic_prepare_client",
    "_evidence_schema",
    "_label_sentiment",
    "_label_sentiment_fallback",
    "_label_sentiment_with_llm",
    "_matches_sentiment_term",
    "_normalize_planner_response",
    "_planner_schema",
    "_prepare_row",
    "_prepare_row_fallback",
    "_prepare_row_with_llm",
    "_prepare_schema",
    "_run_evidence_pack_fallback",
    "_run_evidence_pack_with_llm",
    "_run_planner_with_llm",
    "_sentiment_schema",
]
