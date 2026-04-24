from __future__ import annotations

"""Private aggregate-layer skill implementations."""

from collections import Counter

from .. import runtime as rt

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
        "artifact": rt._inherit_scope_fields(
            {
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
            payload,
        ),
    }

def run_noun_frequency(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_noun_frequency_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    term_frequency: Counter[str] = Counter()
    document_frequency: Counter[str] = Counter()
    analyzer_backend_counts: Counter[str] = Counter()
    sample_rows: list[dict[str, Any]] = []
    total_terms = 0
    document_count = 0

    for item in selected_rows:
        text = item["text"]
        if not text:
            continue
        noun_tokens, analyzer_backend = rt._extract_noun_tokens(
            text,
            stopwords=normalized["stopwords"],
            user_dictionary_path=normalized["user_dictionary_path"],
            min_token_length=normalized["min_token_length"],
            allowed_pos_prefixes=normalized["allowed_pos_prefixes"],
        )
        analyzer_backend_counts.update([analyzer_backend])
        if not noun_tokens:
            continue
        unique_terms = set(noun_tokens)
        term_frequency.update(noun_tokens)
        document_frequency.update(unique_terms)
        total_terms += len(noun_tokens)
        document_count += 1
        if len(sample_rows) < normalized["sample_n"]:
            sample_rows.append(
                {
                    "row_id": str(item.get("row_id") or "").strip(),
                    "source_index": int(item["source_index"]),
                    "text": text[:240],
                    "noun_tokens": noun_tokens[: normalized["top_n"]],
                }
            )

    analyzer_backend = analyzer_backend_counts.most_common(1)[0][0] if analyzer_backend_counts else "empty"
    top_nouns = []
    for term, count in term_frequency.most_common(normalized["top_n"]):
        top_nouns.append(
            {
                "term": term,
                "term_frequency": count,
                "document_frequency": int(document_frequency.get(term) or 0),
            }
        )

    return {
        "notes": [
            f"noun_frequency counted noun tokens across {document_count} rows",
            f"dataset source: {normalized['dataset_name']}",
            f"analyzer_backend: {analyzer_backend}",
        ],
        "artifact": rt._inherit_scope_fields(
            {
                "skill_name": "noun_frequency",
                "step_id": normalized["step"].get("step_id"),
                "dataset_name": normalized["dataset_name"],
                "user_dictionary_path": normalized["user_dictionary_path"],
                "stopwords": list(normalized["stopwords"]),
                "allowed_pos_prefixes": list(normalized["allowed_pos_prefixes"]),
                "summary": {
                    "document_count": document_count,
                    "unique_terms": len(term_frequency),
                    "total_terms": total_terms,
                    "min_token_length": normalized["min_token_length"],
                    "analyzer_backend": analyzer_backend,
                },
                "top_nouns": top_nouns,
                "sample_rows": sample_rows,
            },
            payload,
        ),
    }

def run_time_bucket_count(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_trend_task_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_time_bucket_artifact(normalized, selected_rows)
    artifact["skill_name"] = "time_bucket_count"
    rt._inherit_scope_fields(artifact, payload)
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
    rt._inherit_scope_fields(artifact, payload)
    return {
        "notes": [
            f"meta_group_count grouped rows by {normalized['dimension_column']}",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }

def run_dictionary_tagging(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_dictionary_tagging_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_dictionary_tagging_artifact(normalized, selected_rows)
    artifact["skill_name"] = "dictionary_tagging"
    rt._inherit_scope_fields(artifact, payload)
    return {
        "notes": [
            f"dictionary_tagging assigned tags to {artifact['summary']['tagged_row_count']} rows",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }
