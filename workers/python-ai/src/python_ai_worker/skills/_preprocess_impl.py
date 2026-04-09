from __future__ import annotations

"""Private preprocess-layer skill implementations."""

from collections import Counter
from pathlib import Path
from typing import Any

from .. import runtime as rt

def run_garbage_filter(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_garbage_filter_payload(payload)
    selected_rows = rt._selected_text_rows(
        normalized["dataset_name"],
        normalized["text_column"],
        payload.get("prior_artifacts"),
        apply_dedup=False,
    )
    retained_indices: list[int] = []
    removed_indices: list[int] = []
    removed_samples: list[dict[str, Any]] = []
    artifact_rows: list[dict[str, Any]] = []
    rule_hits: Counter[str] = Counter()

    for item in selected_rows:
        text = item["text"]
        source_index = int(item["source_index"])
        row_id = str(item.get("row_id") or "").strip()
        matched_rules = rt._match_garbage_rules(text, normalized["garbage_rule_names"])
        if not matched_rules:
            retained_indices.append(source_index)
            artifact_rows.append(
                {
                    "row_id": row_id,
                    "source_index": source_index,
                    "filter_status": "retained",
                    "matched_rules": [],
                }
            )
            continue
        removed_indices.append(source_index)
        rule_hits.update(matched_rules)
        artifact_rows.append(
            {
                "row_id": row_id,
                "source_index": source_index,
                "filter_status": "removed",
                "matched_rules": list(matched_rules),
            }
        )
        if len(removed_samples) < normalized["sample_n"]:
            removed_samples.append(
                {
                    "row_id": row_id,
                    "source_index": source_index,
                    "matched_rules": matched_rules,
                    "text": text[:240],
                }
            )

    artifact_ref = ""
    artifact_format = ""
    if normalized["artifact_output_path"]:
        artifact_path = Path(normalized["artifact_output_path"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        rt._write_parquet_rows(artifact_path, artifact_rows)
        artifact_ref = str(artifact_path)
        artifact_format = "parquet"

    return {
        "notes": [
            f"garbage_filter removed {len(removed_indices)} rows",
            f"dataset source: {normalized['dataset_name']}",
            f"garbage rules: {', '.join(normalized['garbage_rule_names'])}",
        ],
        "artifact": {
            "skill_name": "garbage_filter",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "garbage_rule_names": list(normalized["garbage_rule_names"]),
            "artifact_storage_mode": "sidecar_ref" if artifact_ref else "inline",
            "summary": {
                "input_row_count": len(selected_rows),
                "retained_row_count": len(retained_indices),
                "removed_row_count": len(removed_indices),
                "garbage_rule_hits": dict(rule_hits),
            },
            "retained_indices": retained_indices,
            "removed_indices": removed_indices,
            "removed_samples": removed_samples,
            "row_id_column": "row_id",
            "source_index_column": "source_index",
            "status_column": "filter_status",
            "matched_rules_column": "matched_rules",
            "artifact_ref": artifact_ref,
            "artifact_format": artifact_format,
        },
    }

def run_document_filter(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    selected_rows = rt._selected_text_rows(
        normalized["dataset_name"],
        normalized["text_column"],
        payload.get("prior_artifacts"),
        apply_dedup=False,
    )
    query_tokens = set(rt._tokenize(normalized["query"]))
    matches: list[dict[str, Any]] = []

    if query_tokens:
        for item in selected_rows:
            text = item["text"]
            if not text:
                continue
            text_tokens = rt._tokenize(text)
            matched_tokens = {token for token in query_tokens if token in text_tokens}
            if normalized["match_mode"] == "all" and len(matched_tokens) < len(query_tokens):
                continue
            score = sum(1 for token in text_tokens if token in query_tokens)
            if score <= 0:
                continue
            matches.append(
                {
                    "rank": 0,
                    "source_index": int(item["source_index"]),
                    "score": score,
                    "matched_token_count": len(matched_tokens),
                    "text": text[:240],
                }
            )
        matches.sort(key=lambda item: (-int(item["score"]), int(item["source_index"])))
        selection_mode = "lexical_overlap_all" if normalized["match_mode"] == "all" else "lexical_overlap"
    else:
        selection_mode = "all_rows"

    if not matches:
        for item in selected_rows:
            text = item["text"]
            if not text:
                continue
            matches.append(
                {
                    "rank": 0,
                    "source_index": int(item["source_index"]),
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
    artifact_rows = []
    if normalized["artifact_output_path"]:
        row_by_source_index = {int(item["source_index"]): item for item in selected_rows}
        for item in matches:
            source_index = int(item["source_index"])
            row = row_by_source_index.get(source_index) or {}
            artifact_rows.append(
                {
                    "row_id": str(row.get("row_id") or "").strip(),
                    "source_index": source_index,
                    "rank": int(item["rank"]),
                    "score": int(item["score"]),
                }
            )
    artifact_ref = ""
    artifact_format = ""
    if normalized["artifact_output_path"]:
        artifact_path = Path(normalized["artifact_output_path"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        rt._write_parquet_rows(artifact_path, artifact_rows)
        artifact_ref = str(artifact_path)
        artifact_format = "parquet"
    notes = [
        f"document_filter selected {len(filtered_indices)} rows",
        f"dataset source: {normalized['dataset_name']}",
        f"selection_mode: {selection_mode}",
    ]
    if query_tokens:
        notes.append(f"match_mode: {normalized['match_mode']}")

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "document_filter",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "query": normalized["query"],
            "match_mode": normalized["match_mode"],
            "summary": {
                "input_row_count": len(selected_rows),
                "filtered_row_count": len(filtered_indices),
                "selection_mode": selection_mode,
                "query_token_count": len(query_tokens),
            },
            "artifact_storage_mode": "sidecar_ref" if artifact_ref else "inline",
            "matched_indices": filtered_indices,
            "matches": artifact_matches,
            "artifact_ref": artifact_ref,
            "artifact_format": artifact_format,
            "row_id_column": "row_id",
            "source_index_column": "source_index",
            "rank_column": "rank",
            "score_column": "score",
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
        row_id = str(item.get("row_id") or "").strip()
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
                    "row_id": row_id,
                    "source_index": int(item["source_index"]),
                    "canonical_row_id": str(best_match.get("row_id") or "").strip(),
                    "canonical_source_index": int(best_match["source_index"]),
                    "similarity": round(best_score, 4),
                    "text": text[:240],
                }
            )
            continue

        canonical = {
            "row_id": row_id,
            "source_index": int(item["source_index"]),
            "normalized_text": normalized_text,
            "token_set": token_set,
            "text": text[:240],
        }
        canonical_documents.append(canonical)
        groups[canonical["source_index"]] = {
            "group_id": "",
            "canonical_row_id": canonical["row_id"],
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

    artifact_rows: list[dict[str, Any]] = []
    if normalized["artifact_output_path"]:
        group_by_canonical = {
            int(group["canonical_source_index"]): {
                "group_id": str(group["group_id"]),
                "member_count": int(group["member_count"]),
                "canonical_row_id": str(group.get("canonical_row_id") or "").strip(),
            }
            for group in duplicate_groups
        }
        for item in canonical_documents:
            group = group_by_canonical.get(int(item["source_index"])) or {}
            artifact_rows.append(
                {
                    "row_id": str(item.get("row_id") or "").strip(),
                    "source_index": int(item["source_index"]),
                    "canonical_row_id": str(item.get("row_id") or "").strip(),
                    "canonical_source_index": int(item["source_index"]),
                    "group_id": str(group.get("group_id") or ""),
                    "dedup_status": "canonical",
                    "similarity": 1.0,
                    "member_count": int(group.get("member_count") or 1),
                }
            )
        for item in duplicate_records:
            group = group_by_canonical.get(int(item["canonical_source_index"])) or {}
            artifact_rows.append(
                {
                    "row_id": str(item.get("row_id") or "").strip(),
                    "source_index": int(item["source_index"]),
                    "canonical_row_id": str(item.get("canonical_row_id") or "").strip(),
                    "canonical_source_index": int(item["canonical_source_index"]),
                    "group_id": str(group.get("group_id") or ""),
                    "dedup_status": "duplicate",
                    "similarity": float(item["similarity"]),
                    "member_count": int(group.get("member_count") or 1),
                }
            )
        artifact_rows.sort(key=lambda item: int(item["source_index"]))

    artifact_ref = ""
    artifact_format = ""
    if normalized["artifact_output_path"]:
        artifact_path = Path(normalized["artifact_output_path"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        rt._write_parquet_rows(artifact_path, artifact_rows)
        artifact_ref = str(artifact_path)
        artifact_format = "parquet"

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
            "artifact_storage_mode": "sidecar_ref" if artifact_ref else "inline",
            "canonical_indices": [int(item["source_index"]) for item in canonical_documents],
            "duplicate_records": duplicate_records[: max(1, normalized["sample_n"] * 4)],
            "duplicate_groups": duplicate_groups,
            "artifact_ref": artifact_ref,
            "artifact_format": artifact_format,
            "row_id_column": "row_id",
            "source_index_column": "source_index",
            "canonical_row_id_column": "canonical_row_id",
            "canonical_source_index_column": "canonical_source_index",
            "group_id_column": "group_id",
            "status_column": "dedup_status",
            "similarity_column": "similarity",
            "member_count_column": "member_count",
        },
    }

def run_sentence_split(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_sentence_split_payload(payload)
    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact_rows: list[dict[str, Any]] = []
    sample_documents: list[dict[str, Any]] = []
    splitter_backend_counts: Counter[str] = Counter()
    document_count = 0
    sentence_count = 0
    max_sentences_per_document = 0

    for item in selected_rows:
        text = item["text"]
        if not text:
            continue
        sentence_spans, splitter_backend = rt._sentence_spans(text, language=normalized["language"])
        splitter_backend_counts.update([splitter_backend])
        sentence_count += len(sentence_spans)
        max_sentences_per_document = max(max_sentences_per_document, len(sentence_spans))
        document_count += 1

        preview_sentences = []
        for sentence in sentence_spans:
            row = {
                "row_id": str(item.get("row_id") or "").strip(),
                "source_index": int(item["source_index"]),
                "sentence_index": int(sentence["sentence_index"]),
                "sentence_text": str(sentence["sentence_text"]),
                "char_start": int(sentence["char_start"]),
                "char_end": int(sentence["char_end"]),
            }
            artifact_rows.append(row)
            if len(preview_sentences) < normalized["preview_sentences_per_row"]:
                preview_sentences.append(dict(row))

        if preview_sentences and len(sample_documents) < normalized["sample_n"]:
            sample_documents.append(
                {
                    "row_id": str(item.get("row_id") or "").strip(),
                    "source_index": int(item["source_index"]),
                    "sentence_count": len(sentence_spans),
                    "sentences": preview_sentences,
                }
            )

    artifact_ref = ""
    artifact_format = ""
    if normalized["artifact_output_path"]:
        artifact_path = Path(normalized["artifact_output_path"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        rt._write_parquet_rows(artifact_path, artifact_rows)
        artifact_ref = str(artifact_path)
        artifact_format = "parquet"

    splitter_backend = splitter_backend_counts.most_common(1)[0][0] if splitter_backend_counts else "empty"
    average_sentences_per_document = round(sentence_count / document_count, 2) if document_count > 0 else 0.0

    return {
        "notes": [
            f"sentence_split produced {sentence_count} sentences from {document_count} rows",
            f"dataset source: {normalized['dataset_name']}",
            f"splitter_backend: {splitter_backend}",
        ],
        "artifact": {
            "skill_name": "sentence_split",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "language": normalized["language"],
            "artifact_storage_mode": "sidecar_ref" if artifact_ref else "inline_preview",
            "artifact_ref": artifact_ref,
            "artifact_format": artifact_format,
            "row_id_column": "row_id",
            "source_index_column": "source_index",
            "sentence_index_column": "sentence_index",
            "sentence_text_column": "sentence_text",
            "char_start_column": "char_start",
            "char_end_column": "char_end",
            "summary": {
                "document_count": document_count,
                "sentence_count": sentence_count,
                "average_sentences_per_document": average_sentences_per_document,
                "max_sentences_per_document": max_sentences_per_document,
                "splitter_backend": splitter_backend,
            },
            "sample_documents": sample_documents,
        },
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

