from __future__ import annotations

"""Dataset preparation and enrichment skill handlers."""

import json
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..obs import get, skill_handler

LOGGER = get(__name__)


def _stable_source_index(row: dict[str, Any], fallback_index: int) -> int:
    try:
        return int(row.get("source_row_index") or fallback_index)
    except (TypeError, ValueError):
        return fallback_index


def _row_id(row: dict[str, Any], fallback_index: int, dataset_version_id: str) -> str:
    existing = str(row.get("row_id") or "").strip()
    if existing:
        return existing
    source_index = _stable_source_index(row, fallback_index)
    prefix = dataset_version_id or "dataset"
    return f"{prefix}:row:{source_index}"


def _chunk_id(row_id: str, chunk_index: int = 0) -> str:
    return f"{row_id}:chunk:{chunk_index}"


def _unique_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        result.append(normalized)
        seen.add(normalized)
    return result


def _prepare_llm_fallback_summary(rows: list[dict[str, Any]]) -> tuple[int, str, list[str]]:
    reasons: list[str] = []
    count = 0
    for row in rows:
        flags = row.get("quality_flags") or []
        if isinstance(flags, str):
            flags = [flags]
        row_has_fallback = False
        for flag in flags:
            text = str(flag or "").strip()
            for prefix in ("llm_fallback:", "llm_batch_fallback:"):
                if text.startswith(prefix):
                    row_has_fallback = True
                    reasons.append(text.removeprefix(prefix).strip())
        if row_has_fallback:
            count += 1
    unique_reasons = _unique_strings(reasons)
    first_reason = unique_reasons[0] if unique_reasons else ""
    return count, first_reason, unique_reasons


def _sentiment_llm_fallback_summary(rows: list[dict[str, Any]]) -> tuple[int, str, list[str]]:
    reasons: list[str] = []
    count = 0
    marker = "llm_fallback:"
    for row in rows:
        reason_text = str(row.get("sentiment_reason") or row.get("reason") or "").strip()
        if marker not in reason_text:
            continue
        count += 1
        fallback_reason = reason_text.split(marker, 1)[1].strip()
        if fallback_reason.endswith(")"):
            fallback_reason = fallback_reason[:-1].strip()
        reasons.append(fallback_reason)
    unique_reasons = _unique_strings(reasons)
    first_reason = unique_reasons[0] if unique_reasons else ""
    return count, first_reason, unique_reasons


def _write_progress(
    progress_path: str,
    *,
    processed_rows: int,
    total_rows: int,
    started_at: float,
    message: str,
) -> None:
    if not progress_path:
        return
    total = max(0, int(total_rows))
    processed = min(max(0, int(processed_rows)), total) if total > 0 else max(0, int(processed_rows))
    elapsed = max(0.0, time.monotonic() - started_at)
    percent = 100.0 if total == 0 else round((processed / total) * 100.0, 2)
    eta_seconds = None
    if processed > 0 and total > processed and elapsed > 0:
        rows_per_second = processed / elapsed
        if rows_per_second > 0:
            eta_seconds = round((total - processed) / rows_per_second, 2)
    payload = {
        "percent": percent,
        "processed_rows": processed,
        "total_rows": total,
        "elapsed_seconds": round(elapsed, 2),
        "eta_seconds": eta_seconds,
        "message": message,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    path = Path(progress_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _prepare_output_format(path: Path) -> str:
    return _artifact_output_format(path, "prepare")


def _artifact_output_format(path: Path, artifact_name: str) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".jsonl":
        return "jsonl"
    raise ValueError(f"{artifact_name} output_path must end with .parquet or .jsonl")


def _joined_text(row: dict[str, Any], text_columns: list[str], text_joiner: str) -> str:
    parts: list[str] = []
    for column in text_columns:
        value = str(row.get(column) or "").strip()
        if value:
            parts.append(value)
    return text_joiner.join(parts).strip()


def _derive_chunk_output_path(embedding_path: Path) -> Path:
    name = embedding_path.name
    if name.endswith(".index.parquet"):
        return embedding_path.with_name(name[: -len(".index.parquet")] + ".chunks.parquet")
    if name.endswith(".embeddings.jsonl"):
        return embedding_path.with_name(name[: -len(".embeddings.jsonl")] + ".chunks.parquet")
    if name.endswith(".jsonl"):
        return embedding_path.with_name(name[: -len(".jsonl")] + ".chunks.parquet")
    return embedding_path.with_name(name + ".chunks.parquet")


def _derive_embedding_index_output_path(embedding_path: Path) -> Path:
    name = embedding_path.name
    if name.endswith(".jsonl"):
        return embedding_path.with_name(name[: -len(".jsonl")] + ".index.parquet")
    return embedding_path.with_name(name + ".index.parquet")


def _derive_cluster_membership_output_path(summary_path: Path) -> Path:
    name = summary_path.name
    if name.endswith(".json"):
        return summary_path.with_name(name[: -len(".json")] + ".memberships.parquet")
    return summary_path.with_name(name + ".memberships.parquet")


def _build_chunk_rows(
    rows: list[dict[str, Any]],
    *,
    text_column: str,
    dataset_version_id: str,
    chunk_max_chars: int,
    chunk_overlap_chars: int,
) -> tuple[list[dict[str, Any]], int]:
    chunk_rows: list[dict[str, Any]] = []
    source_row_count = 0
    for index, row in enumerate(rows):
        document = str(row.get(text_column) or "").strip()
        if not document:
            continue
        source_row_count += 1
        source_index = _stable_source_index(row, index)
        row_identifier = _row_id(row, source_index, dataset_version_id)
        for chunk_index, (chunk_text, char_start, char_end) in enumerate(
            _split_text_chunks(document, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars)
        ):
            chunk_rows.append(
                {
                    "source_row_index": source_index,
                    "row_id": row_identifier,
                    "chunk_id": _chunk_id(row_identifier, chunk_index),
                    "chunk_index": chunk_index,
                    "chunk_text": chunk_text,
                    "char_start": char_start,
                    "char_end": char_end,
                }
            )
    return chunk_rows, source_row_count


def _prepare_output_schema() -> Any:
    arrow, _ = rt._require_pyarrow()
    return arrow.schema(
        [
            ("source_row_index", arrow.int64()),
            ("row_id", arrow.string()),
            ("raw_text", arrow.string()),
            ("normalized_text", arrow.string()),
            ("prepare_disposition", arrow.string()),
            ("prepare_reason", arrow.string()),
            ("quality_flags", arrow.list_(arrow.string())),
            ("prepare_prompt_version", arrow.string()),
            ("prepare_regex_applied_rules", arrow.list_(arrow.string())),
        ]
    )


def _clean_output_schema() -> Any:
    arrow, _ = rt._require_pyarrow()
    return arrow.schema(
        [
            ("source_row_index", arrow.int64()),
            ("row_id", arrow.string()),
            ("raw_text", arrow.string()),
            ("cleaned_text", arrow.string()),
            ("clean_disposition", arrow.string()),
            ("clean_reason", arrow.string()),
            ("clean_flags", arrow.list_(arrow.string())),
            ("clean_regex_applied_rules", arrow.list_(arrow.string())),
        ]
    )


def _chunk_output_schema() -> Any:
    arrow, _ = rt._require_pyarrow()
    return arrow.schema(
        [
            ("source_row_index", arrow.int64()),
            ("row_id", arrow.string()),
            ("chunk_id", arrow.string()),
            ("chunk_index", arrow.int64()),
            ("chunk_text", arrow.string()),
            ("char_start", arrow.int64()),
            ("char_end", arrow.int64()),
        ]
    )


def _cluster_membership_output_schema() -> Any:
    arrow, _ = rt._require_pyarrow()
    return arrow.schema(
        [
            ("cluster_id", arrow.string()),
            ("cluster_rank", arrow.int64()),
            ("cluster_document_count", arrow.int64()),
            ("source_index", arrow.int64()),
            ("row_id", arrow.string()),
            ("chunk_id", arrow.string()),
            ("chunk_index", arrow.int64()),
            ("text", arrow.string()),
            ("is_sample", arrow.bool_()),
        ]
    )


def _embedding_index_output_schema() -> Any:
    arrow, _ = rt._require_pyarrow()
    return arrow.schema(
        [
            ("source_index", arrow.int64()),
            ("row_id", arrow.string()),
            ("chunk_id", arrow.string()),
            ("chunk_index", arrow.int64()),
            ("char_start", arrow.int64()),
            ("char_end", arrow.int64()),
            ("embedding_json", arrow.string()),
            ("embedding_dim", arrow.int64()),
            ("embedding_provider", arrow.string()),
            ("token_counts_json", arrow.string()),
        ]
    )


def _sentiment_output_schema() -> Any:
    arrow, _ = rt._require_pyarrow()
    return arrow.schema(
        [
            ("source_row_index", arrow.int64()),
            ("row_id", arrow.string()),
            ("raw_text", arrow.string()),
            ("normalized_text", arrow.string()),
            ("prepare_disposition", arrow.string()),
            ("prepare_reason", arrow.string()),
            ("quality_flags", arrow.list_(arrow.string())),
            ("prepare_prompt_version", arrow.string()),
            ("prepare_regex_applied_rules", arrow.list_(arrow.string())),
            ("sentiment_label", arrow.string()),
            ("sentiment_confidence", arrow.float64()),
            ("sentiment_reason", arrow.string()),
            ("sentiment_prompt_version", arrow.string()),
        ]
    )


def _split_text_chunks(
    text: str,
    *,
    chunk_max_chars: int,
    chunk_overlap_chars: int,
) -> list[tuple[str, int, int]]:
    normalized = text.strip()
    if not normalized:
        return []
    if len(normalized) <= chunk_max_chars:
        return [(normalized, 0, len(normalized))]

    chunks: list[tuple[str, int, int]] = []
    start = 0
    text_length = len(normalized)
    while start < text_length:
        hard_end = min(text_length, start + chunk_max_chars)
        end = hard_end
        if hard_end < text_length:
            boundary = normalized.rfind(" ", start + max(1, chunk_max_chars // 2), hard_end)
            if boundary > start:
                end = boundary
        chunk_text = normalized[start:end].strip()
        if not chunk_text:
            start = hard_end
            continue
        chunk_start = start
        chunk_end = min(text_length, chunk_start + len(chunk_text))
        chunks.append((chunk_text, chunk_start, chunk_end))
        if chunk_end >= text_length:
            break
        next_start = max(chunk_end - chunk_overlap_chars, start + 1)
        if next_start <= start:
            next_start = chunk_end
        while next_start < text_length and normalized[next_start].isspace():
            next_start += 1
        start = next_start
    return chunks


@skill_handler("python-ai")
def run_dataset_clean(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_dataset_clean_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    source_row_count = len(rows)
    output_path = Path(normalized["output_path"])
    output_format = _artifact_output_format(output_path, "clean")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = normalized["progress_path"]
    started_at = time.monotonic()
    _write_progress(
        progress_path,
        processed_rows=0,
        total_rows=source_row_count,
        started_at=started_at,
        message="clean queued",
    )

    kept_count = 0
    dropped_count = 0
    skipped_rows = 0
    regex_rule_hits: Counter[str] = Counter()
    source_input_char_count = 0
    cleaned_input_char_count = 0
    cleaned_rows: list[dict[str, Any]] = []

    handle = output_path.open("w", encoding="utf-8") if output_format == "jsonl" else None
    try:
        for source_index, row in enumerate(rows):
            raw_text = _joined_text(row, normalized["text_columns"], normalized["text_joiner"])
            if not raw_text:
                skipped_rows += 1
                dropped_count += 1
                _write_progress(
                    progress_path,
                    processed_rows=source_index + 1,
                    total_rows=source_row_count,
                    started_at=started_at,
                    message="clean scanning rows",
                )
                continue

            regex_cleaned_text, applied_regex_rules = rt._apply_prepare_regex_rules(raw_text, normalized["regex_rule_names"])
            regex_rule_hits.update(applied_regex_rules)
            cleaned_text = rt._prepare_preprocess_text(regex_cleaned_text, normalized["preprocess_options"])
            source_input_char_count += len(raw_text)
            cleaned_input_char_count += len(cleaned_text)
            if not cleaned_text:
                dropped_count += 1
                continue

            kept_count += 1
            cleaned_row = dict(row)
            cleaned_row["source_row_index"] = source_index
            cleaned_row["row_id"] = _row_id(cleaned_row, source_index, normalized["dataset_version_id"])
            cleaned_row["raw_text"] = raw_text
            cleaned_row["cleaned_text"] = cleaned_text
            cleaned_row["clean_disposition"] = "keep"
            cleaned_row["clean_reason"] = "text kept after deterministic cleaning"
            cleaned_row["clean_flags"] = ["cleaned"] if cleaned_text != raw_text.strip() else []
            cleaned_row["clean_regex_applied_rules"] = list(applied_regex_rules)
            cleaned_rows.append(cleaned_row)
            if handle is not None:
                handle.write(json.dumps(cleaned_row, ensure_ascii=False))
                handle.write("\n")

            _write_progress(
                progress_path,
                processed_rows=source_index + 1,
                total_rows=source_row_count,
                started_at=started_at,
                message="clean processing rows",
            )
    except Exception:
        _write_progress(
            progress_path,
            processed_rows=len(cleaned_rows),
            total_rows=source_row_count,
            started_at=started_at,
            message="clean failed",
        )
        raise
    finally:
        if handle is not None:
            handle.close()

    if output_format == "parquet":
        rt._write_parquet_rows(output_path, cleaned_rows, schema=_clean_output_schema())
    _write_progress(
        progress_path,
        processed_rows=source_row_count,
        total_rows=source_row_count,
        started_at=started_at,
        message="clean completed",
    )

    summary = {
        "input_row_count": source_row_count,
        "output_row_count": kept_count,
        "kept_count": kept_count,
        "dropped_count": dropped_count,
        "skipped_row_count": skipped_rows,
        "text_column": normalized["text_column"],
        "text_columns": list(normalized["text_columns"]),
        "text_joiner": normalized["text_joiner"],
        "preprocess_options": dict(normalized["preprocess_options"]),
        "source_input_char_count": source_input_char_count,
        "cleaned_input_char_count": cleaned_input_char_count,
        "clean_reduced_char_count": max(0, source_input_char_count - cleaned_input_char_count),
        "clean_regex_rule_names": list(normalized["regex_rule_names"]),
        "clean_regex_rule_hits": dict(regex_rule_hits),
    }

    return {
        "notes": [
            "dataset clean artifact generated by python-ai worker",
            f"dataset source: {normalized['dataset_name']}",
            f"cleaned output: {output_path}",
            f"clean regex rules: {', '.join(normalized['regex_rule_names'])}",
        ],
        "artifact": rt._set_scope_fields({
            "skill_name": "dataset_clean",
            "dataset_version_id": normalized["dataset_version_id"],
            "source_dataset_name": normalized["dataset_name"],
            "clean_uri": str(output_path),
            "cleaned_ref": str(output_path),
            "clean_format": output_format,
            "progress_ref": progress_path,
            "text_column": normalized["text_column"],
            "text_columns": list(normalized["text_columns"]),
            "text_joiner": normalized["text_joiner"],
            "raw_text_column": normalized["text_column"],
            "raw_text_columns": list(normalized["text_columns"]),
            "cleaned_text_column": "cleaned_text",
            "row_id_column": "row_id",
            "preprocess_options": dict(normalized["preprocess_options"]),
            "source_input_char_count": source_input_char_count,
            "cleaned_input_char_count": cleaned_input_char_count,
            "clean_reduced_char_count": max(0, source_input_char_count - cleaned_input_char_count),
            "clean_regex_rule_names": list(normalized["regex_rule_names"]),
            "summary": summary,
        }, declared_result_scope="full_dataset", runtime_result_scope="full_dataset"),
    }


@skill_handler("python-ai")
def run_dataset_prepare(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_prepare_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    source_row_count = len(rows)
    if normalized["max_rows"] > 0:
        rows = rows[: normalized["max_rows"]]
    output_path = Path(normalized["output_path"])
    output_format = _prepare_output_format(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = normalized["progress_path"]
    started_at = time.monotonic()
    processed_source_rows = 0
    _write_progress(
        progress_path,
        processed_rows=0,
        total_rows=len(rows),
        started_at=started_at,
        message="prepare queued",
    )

    client = rt._anthropic_prepare_client(normalized["model"], llm_mode=normalized["llm_mode"])
    kept_count = 0
    review_count = 0
    dropped_count = 0
    skipped_rows = 0
    prepared_batch: list[tuple[int, dict[str, Any], str, str, list[str]]] = []
    prepared_rows: list[dict[str, Any]] = []
    usage_records: list[dict[str, Any]] = []

    handle = output_path.open("w", encoding="utf-8") if output_format == "jsonl" else None
    try:
        for source_index, row in enumerate(rows):
            processed_source_rows = source_index + 1
            raw_text = _joined_text(row, normalized["text_columns"], normalized["text_joiner"])
            if not raw_text:
                skipped_rows += 1
                _write_progress(
                    progress_path,
                    processed_rows=processed_source_rows,
                    total_rows=len(rows),
                    started_at=started_at,
                    message="prepare scanning rows",
                )
                continue

            prepared_batch.append((source_index, row, raw_text, raw_text, []))
            if len(prepared_batch) >= normalized["prepare_batch_size"]:
                batch_results, batch_usage = rt._prepare_rows(
                    [item[3] for item in prepared_batch],
                    client=client,
                    model=normalized["model"],
                    batch_size=normalized["prepare_batch_size"],
                    prompt_version_override=normalized["prepare_prompt_version"],
                    prompt_template_override=normalized["prepare_prompt_template"],
                    batch_prompt_template_override=normalized["prepare_batch_prompt_template"],
                )
                usage_records.append(batch_usage)
                kept_count, review_count, dropped_count = _write_prepared_rows(
                    handle,
                    prepared_rows,
                    prepared_batch,
                    batch_results,
                    dataset_version_id=normalized["dataset_version_id"],
                    kept_count=kept_count,
                    review_count=review_count,
                    dropped_count=dropped_count,
                )
                prepared_batch = []
                _write_progress(
                    progress_path,
                    processed_rows=processed_source_rows,
                    total_rows=len(rows),
                    started_at=started_at,
                    message="prepare processing rows",
                )

        if prepared_batch:
            batch_results, batch_usage = rt._prepare_rows(
                [item[3] for item in prepared_batch],
                client=client,
                model=normalized["model"],
                batch_size=normalized["prepare_batch_size"],
                prompt_version_override=normalized["prepare_prompt_version"],
                prompt_template_override=normalized["prepare_prompt_template"],
                batch_prompt_template_override=normalized["prepare_batch_prompt_template"],
            )
            usage_records.append(batch_usage)
            kept_count, review_count, dropped_count = _write_prepared_rows(
                handle,
                prepared_rows,
                prepared_batch,
                batch_results,
                dataset_version_id=normalized["dataset_version_id"],
                kept_count=kept_count,
                review_count=review_count,
                dropped_count=dropped_count,
            )
            _write_progress(
                progress_path,
                processed_rows=len(rows),
                total_rows=len(rows),
                started_at=started_at,
                message="prepare finalizing",
            )
    except Exception:
        _write_progress(
            progress_path,
            processed_rows=processed_source_rows,
            total_rows=len(rows),
            started_at=started_at,
            message="prepare failed",
        )
        raise
    finally:
        if handle is not None:
            handle.close()

    if output_format == "parquet":
        rt._write_parquet_rows(output_path, prepared_rows, schema=_prepare_output_schema())
    _write_progress(
        progress_path,
        processed_rows=len(rows),
        total_rows=len(rows),
        started_at=started_at,
        message="prepare completed",
    )

    notes = [
        "dataset prepare artifact generated by python-ai worker",
        f"dataset source: {normalized['dataset_name']}",
        f"prepared output: {output_path}",
    ]
    usage = rt._merge_usage_records(usage_records)
    usage_provider = str(usage.get("provider") or "").strip()
    attempted_llm_model = client._config.model if client and client.is_enabled() else ""
    llm_fallback_count, llm_fallback_reason, llm_fallback_reasons = _prepare_llm_fallback_summary(prepared_rows)
    prepare_model = "fallback-normalizer-v1"
    if usage_provider == "anthropic" and client and client.is_enabled():
        prepare_model = client._config.model
    elif usage_provider == "mixed" and client and client.is_enabled():
        prepare_model = f"{client._config.model}+fallback-normalizer-v1"
    notes.append(f"prepare model: {prepare_model}")
    notes.append(f"prepare batch size: {normalized['prepare_batch_size']}")
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")
    if llm_fallback_count > 0:
        fallback_note = f"llm fallback used: count={llm_fallback_count}, model={attempted_llm_model}, reason={llm_fallback_reason}"
        notes.append(fallback_note)
        LOGGER.warning(
            "llm.fallback.triggered",
            skill_name="dataset_prepare",
            dataset_version_id=normalized["dataset_version_id"],
            model=attempted_llm_model,
            fallback_count=llm_fallback_count,
            reason=llm_fallback_reason,
        )

    prompt_version = "dataset-prepare-fallback-v1"
    prepare_strategy = "deterministic-fallback"
    if usage_provider in {"anthropic", "mixed"} and client and client.is_enabled():
        prompt_version = str(prepared_rows[0].get("prepare_prompt_version") or "").strip() if prepared_rows else ""
        if not prompt_version:
            prompt_version = normalized["prepare_prompt_version"] or (
                "dataset-prepare-anthropic-batch-v1" if normalized["prepare_batch_size"] > 1 else "dataset-prepare-anthropic-v1"
            )
        if usage_provider == "mixed":
            prepare_strategy = "mixed-anthropic-fallback"
        else:
            prepare_strategy = "anthropic-batch" if normalized["prepare_batch_size"] > 1 else "anthropic-row"

    runtime_result_scope = (
        "partial_build"
        if 0 < normalized["max_rows"] < source_row_count
        else "full_dataset"
    )
    return {
        "notes": notes,
        "artifact": rt._set_scope_fields({
            "skill_name": "dataset_prepare",
            "dataset_version_id": normalized["dataset_version_id"],
            "source_dataset_name": normalized["dataset_name"],
            "prepare_uri": str(output_path),
            "prepared_ref": str(output_path),
            "prepare_format": output_format,
            "prepare_model": prepare_model,
            "prepare_prompt_version": prompt_version,
            "prepare_strategy": prepare_strategy,
            "prepare_batch_size": normalized["prepare_batch_size"],
            "max_rows": normalized["max_rows"],
            "progress_ref": progress_path,
            "llm_provider": "anthropic" if attempted_llm_model else "",
            "llm_model": attempted_llm_model,
            "llm_fallback": llm_fallback_count > 0,
            "llm_fallback_count": llm_fallback_count,
            "llm_fallback_reason": llm_fallback_reason,
            "llm_fallback_reasons": llm_fallback_reasons,
            "text_column": normalized["text_column"],
            "text_columns": list(normalized["text_columns"]),
            "text_joiner": normalized["text_joiner"],
            "prepared_text_column": "normalized_text",
            "row_id_column": "row_id",
            "storage_contract_version": "unstructured-storage-v1",
            "summary": {
                "input_row_count": len(rows),
                "source_row_count": source_row_count,
                "max_rows": normalized["max_rows"],
                "output_row_count": kept_count + review_count,
                "kept_count": kept_count,
                "review_count": review_count,
                "dropped_count": dropped_count,
                "text_column": normalized["text_column"],
                "text_columns": list(normalized["text_columns"]),
                "text_joiner": normalized["text_joiner"],
                "prepare_batch_size": normalized["prepare_batch_size"],
            },
            "usage": usage,
        }, declared_result_scope="full_dataset", runtime_result_scope=runtime_result_scope),
    }


def _write_prepared_rows(
    handle: Any,
    prepared_rows: list[dict[str, Any]],
    batch_rows: list[tuple[int, dict[str, Any], str, str, list[str]]],
    batch_results: list[dict[str, Any]],
    *,
    dataset_version_id: str,
    kept_count: int,
    review_count: int,
    dropped_count: int,
) -> tuple[int, int, int]:
    for (source_index, row, raw_text, _regex_cleaned_text, applied_regex_rules), prepared in zip(batch_rows, batch_results):
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
        prepared_row["row_id"] = _row_id(prepared_row, source_index, dataset_version_id)
        prepared_row["raw_text"] = str(row.get("raw_text") or raw_text)
        prepared_row["normalized_text"] = prepared["normalized_text"]
        prepared_row["prepare_disposition"] = disposition
        prepared_row["prepare_reason"] = prepared["reason"]
        prepared_row["quality_flags"] = prepared["quality_flags"]
        prepared_row["prepare_prompt_version"] = prepared["prompt_version"]
        prepared_row["prepare_regex_applied_rules"] = list(applied_regex_rules)
        prepared_rows.append(prepared_row)
        if handle is not None:
            handle.write(json.dumps(prepared_row, ensure_ascii=False))
            handle.write("\n")
    return kept_count, review_count, dropped_count


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return dict(decoded)
    return {}


def _json_float_list(value: Any) -> list[float]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        if not isinstance(decoded, list):
            return []
        items = decoded
    else:
        return []
    values: list[float] = []
    for item in items:
        try:
            values.append(float(item))
        except (TypeError, ValueError):
            return []
    return values


def _cluster_chunk_lookup(chunk_ref: str) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rt._iter_rows(chunk_ref):
        chunk_id = str(row.get("chunk_id") or "").strip()
        if chunk_id:
            lookup[chunk_id] = row
    return lookup


def _cluster_records_from_index(index_rows: list[dict[str, Any]], chunk_lookup: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in index_rows:
        chunk_id = str(row.get("chunk_id") or "").strip()
        chunk_row = chunk_lookup.get(chunk_id, {})
        token_counts = Counter(_json_object(row.get("token_counts_json")))
        dense_vector = _json_float_list(row.get("embedding_json"))
        text = str(chunk_row.get("chunk_text") or "").strip()
        if not token_counts and not dense_vector:
            continue
        record: dict[str, Any] = {
            "source_index": int(row.get("source_index") or 0),
            "row_id": str(row.get("row_id") or chunk_row.get("row_id") or "").strip(),
            "chunk_id": chunk_id,
            "chunk_index": int(row.get("chunk_index") or chunk_row.get("chunk_index") or 0),
            "text": text,
            "token_counts": dict(token_counts),
            "norm": rt._vector_norm(token_counts),
        }
        if dense_vector:
            record["embedding"] = dense_vector
            record["embedding_dim"] = int(row.get("embedding_dim") or len(dense_vector))
            record["embedding_provider"] = str(row.get("embedding_provider") or "").strip()
        records.append(record)
    return records


@skill_handler("python-ai")
def run_dataset_cluster_build(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_cluster_build_payload(payload)
    summary_path = Path(normalized["output_path"])
    membership_path = _derive_cluster_membership_output_path(summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    membership_path.parent.mkdir(parents=True, exist_ok=True)

    index_rows = rt._iter_rows(normalized["embedding_index_source_ref"])
    chunk_lookup = _cluster_chunk_lookup(normalized["chunk_ref"])
    records = _cluster_records_from_index(index_rows, chunk_lookup)
    clusters = rt._cluster_embedding_records(
        records,
        normalized["cluster_similarity_threshold"],
        normalized["sample_n"],
        normalized["top_n"],
        include_members=True,
    )
    similarity_backends = {
        str(cluster.get("similarity_backend") or "").strip()
        for cluster in clusters
        if str(cluster.get("similarity_backend") or "").strip()
    }
    similarity_backend = "mixed" if len(similarity_backends) > 1 else next(iter(similarity_backends), "token-overlap")
    noise_count = len([cluster for cluster in clusters if int(cluster.get("document_count") or 0) == 1])
    membership_rows: list[dict[str, Any]] = []
    summary_clusters: list[dict[str, Any]] = []
    for cluster_rank, cluster in enumerate(clusters, start=1):
        cluster_id = str(cluster.get("cluster_id") or "").strip()
        members = list(cluster.get("members") or [])
        sample_ids = {
            str(item.get("chunk_id") or "").strip()
            for item in list(cluster.get("sample_documents") or [])
            if isinstance(item, dict)
        }
        for member in members:
            if not isinstance(member, dict):
                continue
            membership_rows.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_rank": cluster_rank,
                    "cluster_document_count": int(cluster.get("document_count") or 0),
                    "source_index": int(member.get("source_index") or 0),
                    "row_id": str(member.get("row_id") or "").strip(),
                    "chunk_id": str(member.get("chunk_id") or "").strip(),
                    "chunk_index": int(member.get("chunk_index") or 0),
                    "text": str(member.get("text") or "").strip(),
                    "is_sample": str(member.get("chunk_id") or "").strip() in sample_ids,
                }
            )
        summary_cluster = dict(cluster)
        summary_cluster.pop("members", None)
        summary_clusters.append(summary_cluster)

    cluster_artifact = rt._set_scope_fields({
        "skill_name": "embedding_cluster",
        "dataset_name": normalized["dataset_name"],
        "embedding_source_backend": "embedding-index-parquet",
        "embedding_index_ref": normalized["embedding_index_source_ref"],
        "cluster_execution_mode": "materialized_full_dataset",
        "cluster_materialization_scope": "full_dataset",
        "cluster_materialized_ref_used": True,
        "cluster_fallback_reason": "",
        "cluster_ref": str(summary_path),
        "cluster_format": "json",
        "cluster_summary_ref": str(summary_path),
        "cluster_summary_format": "json",
        "cluster_membership_ref": str(membership_path),
        "cluster_membership_format": "parquet",
        "chunk_ref": normalized["chunk_ref"],
        "chunk_format": "parquet" if normalized["chunk_ref"].endswith(".parquet") else "",
        "summary": {
            "cluster_count": len(summary_clusters),
            "clustered_document_count": len(records),
            "noise_count": noise_count,
            "similarity_backend": similarity_backend,
            "cluster_similarity_threshold": normalized["cluster_similarity_threshold"],
            "top_n": normalized["top_n"],
            "sample_n": normalized["sample_n"],
            "embedding_source_backend": "embedding-index-parquet",
            "cluster_membership_row_count": len(membership_rows),
        },
        "clusters": summary_clusters,
    }, declared_result_scope="cluster_subset", runtime_result_scope="full_dataset")
    summary_path.write_text(json.dumps(cluster_artifact, ensure_ascii=False), encoding="utf-8")
    rt._write_parquet_rows(membership_path, membership_rows, schema=_cluster_membership_output_schema())

    return {
        "notes": [
            f"dataset cluster artifact generated by python-ai worker",
            f"cluster source index: {normalized['embedding_index_source_ref']}",
            f"cluster output: {summary_path}",
            f"cluster membership output: {membership_path}",
            f"cluster_count: {len(summary_clusters)}",
            f"similarity_backend: {similarity_backend}",
        ],
        "artifact": rt._set_scope_fields({
            "skill_name": "dataset_cluster_build",
            "dataset_version_id": normalized["dataset_version_id"],
            "cluster_execution_mode": "materialized_build",
            "cluster_materialization_scope": "full_dataset",
            "cluster_ref": str(summary_path),
            "cluster_format": "json",
            "cluster_summary_ref": str(summary_path),
            "cluster_summary_format": "json",
            "cluster_membership_ref": str(membership_path),
            "cluster_membership_format": "parquet",
            "cluster_algorithm": "dense-hybrid-v1",
            "cluster_source_embedding_ref": normalized["embedding_index_source_ref"],
            "chunk_ref": normalized["chunk_ref"],
            "summary": cluster_artifact["summary"],
        }, declared_result_scope="full_dataset", runtime_result_scope="full_dataset"),
    }


@skill_handler("python-ai")
def run_embedding(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_embedding_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    embedding_path = Path(normalized["output_path"]) if normalized["output_path"] else None
    embedding_index_path = Path(normalized["index_output_path"])
    chunk_path = Path(normalized["chunk_output_path"]) if normalized["chunk_output_path"] else _derive_chunk_output_path(embedding_index_path)
    if embedding_path is not None:
        embedding_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    embedding_index_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_rows, source_row_count = _build_chunk_rows(
        rows,
        text_column=normalized["text_column"],
        dataset_version_id=normalized["dataset_version_id"],
        chunk_max_chars=normalized["chunk_max_chars"],
        chunk_overlap_chars=normalized["chunk_overlap_chars"],
    )
    rt._write_parquet_rows(chunk_path, chunk_rows, schema=_chunk_output_schema())
    chunk_count = 0
    embedding_model = normalized["embedding_model"]
    embedding_provider = "token-overlap"
    embedding_vector_dim = 0
    embedding_representation = "token-overlap"
    embedding_batch_size = 64
    dense_enabled = rt._looks_dense_embedding_model(normalized["embedding_model"])
    dense_available = False
    dense_usage_records: list[dict[str, Any]] = []
    dense_fallback_noted = False
    notes = [
        "embedding sidecar generated by python-ai worker",
        f"dataset source: {normalized['dataset_name']}",
        f"chunk output: {chunk_path}",
        f"embedding index source: {embedding_index_path}",
        f"chunk_max_chars: {normalized['chunk_max_chars']}",
        f"chunk_overlap_chars: {normalized['chunk_overlap_chars']}",
    ]
    embedding_index_rows: list[dict[str, Any]] = []
    handle = embedding_path.open("w", encoding="utf-8") if embedding_path is not None else None
    try:
        for start in range(0, len(chunk_rows), embedding_batch_size):
            chunk_batch = chunk_rows[start : start + embedding_batch_size]
            batch_texts = [str(chunk.get("chunk_text") or "").strip() for chunk in chunk_batch]
            batch_dense_vectors: list[list[float]] = []
            batch_dense_result = rt._generate_dense_embeddings(
                batch_texts,
                model=normalized["embedding_model"],
                dimensions=normalized["embedding_dimensions"],
            )
            if batch_dense_result is not None:
                dense_available = True
                batch_dense_vectors = [list(vector) for vector in batch_dense_result["embeddings"]]
                embedding_model = str(batch_dense_result["model"] or normalized["embedding_model"]).strip() or normalized["embedding_model"]
                embedding_provider = str(batch_dense_result["provider"] or "openai").strip() or "openai"
                embedding_vector_dim = int(batch_dense_result["dimensions"] or 0)
                embedding_representation = "dense+token-overlap"
                dense_usage_records.append(dict(batch_dense_result.get("usage") or {}))
            elif dense_enabled and not dense_fallback_noted:
                embedding_model = rt.TOKEN_OVERLAP_EMBEDDING_MODEL
                notes.append("dense embedding unavailable; fell back to token-overlap")
                dense_fallback_noted = True
            for batch_index, chunk in enumerate(chunk_batch):
                document = str(chunk.get("chunk_text") or "").strip()
                token_counts = Counter(rt._tokenize(document))
                record = {
                    "source_index": int(chunk.get("source_row_index") or 0),
                    "row_id": str(chunk.get("row_id") or "").strip(),
                    "chunk_id": str(chunk.get("chunk_id") or "").strip(),
                    "chunk_index": int(chunk.get("chunk_index") or 0),
                    "text": document,
                    "char_start": int(chunk.get("char_start") or 0),
                    "char_end": int(chunk.get("char_end") or 0),
                    "token_counts": dict(token_counts),
                    "norm": rt._vector_norm(token_counts),
                }
                if batch_dense_vectors:
                    record["embedding"] = list(batch_dense_vectors[batch_index])
                    record["embedding_dim"] = embedding_vector_dim
                    record["embedding_provider"] = embedding_provider
                if handle is not None:
                    handle.write(json.dumps(record, ensure_ascii=False))
                    handle.write("\n")
                embedding_index_rows.append(
                    {
                        "source_index": record["source_index"],
                        "row_id": record["row_id"],
                        "chunk_id": record["chunk_id"],
                        "chunk_index": record["chunk_index"],
                        "char_start": record["char_start"],
                        "char_end": record["char_end"],
                        "embedding_json": json.dumps(list(batch_dense_vectors[batch_index]), ensure_ascii=False) if batch_dense_vectors else "",
                        "embedding_dim": embedding_vector_dim if batch_dense_vectors else 0,
                        "embedding_provider": embedding_provider if batch_dense_vectors else "",
                        "token_counts_json": json.dumps(dict(token_counts), ensure_ascii=False),
                    }
                )
                chunk_count += 1
    finally:
        if handle is not None:
            handle.close()
    rt._write_parquet_rows(embedding_index_path, embedding_index_rows, schema=_embedding_index_output_schema())
    if dense_available:
        notes.append(f"embedding provider: {embedding_provider}")
        notes.append(f"embedding vector dim: {embedding_vector_dim}")
    if embedding_path is None:
        notes.append("embedding jsonl debug export disabled; primary source is embeddings.index.parquet")
    else:
        notes.append(f"embedding debug export: {embedding_path}")
    usage = (
        rt._merge_usage_records(dense_usage_records)
        if dense_available
        else rt._free_usage_metadata(
            provider="token-overlap",
            model=embedding_model,
            operation="embedding",
            request_count=1,
            input_text_count=chunk_count,
            vector_count=chunk_count,
            cost_status="free_fallback",
        )
    )

    return {
        "notes": notes,
        "artifact": rt._set_scope_fields({
            "skill_name": "embedding",
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": str(embedding_path) if embedding_path is not None else "",
            "embedding_ref": str(embedding_path) if embedding_path is not None else "",
            "embedding_format": "jsonl" if embedding_path is not None else "",
            "embedding_debug_export_enabled": embedding_path is not None,
            "embedding_index_source_ref": str(embedding_index_path),
            "embedding_index_source_format": "parquet",
            "chunk_ref": str(chunk_path),
            "chunk_format": "parquet",
            "embedding_model": embedding_model,
            "embedding_provider": embedding_provider,
            "embedding_vector_dim": embedding_vector_dim,
            "embedding_representation": embedding_representation,
            "document_count": chunk_count,
            "source_row_count": source_row_count,
            "chunk_count": chunk_count,
            "row_id_column": "row_id",
            "chunk_id_column": "chunk_id",
            "chunk_index_column": "chunk_index",
            "chunk_text_column": "chunk_text",
            "chunking_strategy": "text-window-v1",
            "storage_contract_version": "unstructured-storage-v1",
            "usage": usage,
        }, declared_result_scope="full_dataset", runtime_result_scope="full_dataset"),
    }


@skill_handler("python-ai")
def run_sentiment_label(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_sentiment_build_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    source_row_count = len(rows)
    if normalized["max_rows"] > 0:
        rows = rows[: normalized["max_rows"]]
    output_path = Path(normalized["output_path"])
    output_format = _artifact_output_format(output_path, "sentiment")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    client = rt._anthropic_sentiment_client(normalized["model"], llm_mode=normalized["llm_mode"])
    label_counts: Counter[str] = Counter()
    skipped_rows = 0
    labeled_count = 0
    labeled_rows: list[dict[str, Any]] = []
    usage_records: list[dict[str, Any]] = []

    handle = output_path.open("w", encoding="utf-8") if output_format == "jsonl" else None
    try:
        labeled_batch: list[tuple[int, dict[str, Any], str]] = []
        for index, row in enumerate(rows):
            text = _joined_text(row, normalized["text_columns"], normalized["text_joiner"])
            if not text:
                skipped_rows += 1
                continue
            labeled_batch.append((index, row, text))
            if len(labeled_batch) < normalized["sentiment_batch_size"]:
                continue
            labels, usage = rt._label_sentiments(
                [item[2] for item in labeled_batch],
                client=client,
                batch_size=normalized["sentiment_batch_size"],
                prompt_version_override=normalized["sentiment_prompt_version"],
                prompt_template_override=normalized["sentiment_prompt_template"],
                batch_prompt_template_override=normalized["sentiment_batch_prompt_template"],
            )
            usage_records.append(usage)
            labeled_count = _write_labeled_rows(
                handle,
                labeled_rows,
                labeled_batch,
                labels,
                dataset_version_id=normalized["dataset_version_id"],
                output_format=output_format,
                label_counts=label_counts,
                labeled_count=labeled_count,
            )
            labeled_batch = []
        if labeled_batch:
            labels, usage = rt._label_sentiments(
                [item[2] for item in labeled_batch],
                client=client,
                batch_size=normalized["sentiment_batch_size"],
                prompt_version_override=normalized["sentiment_prompt_version"],
                prompt_template_override=normalized["sentiment_prompt_template"],
                batch_prompt_template_override=normalized["sentiment_batch_prompt_template"],
            )
            usage_records.append(usage)
            labeled_count = _write_labeled_rows(
                handle,
                labeled_rows,
                labeled_batch,
                labels,
                dataset_version_id=normalized["dataset_version_id"],
                output_format=output_format,
                label_counts=label_counts,
                labeled_count=labeled_count,
            )
    finally:
        if handle is not None:
            handle.close()

    if output_format == "parquet":
        rt._write_parquet_rows(output_path, labeled_rows, schema=_sentiment_output_schema())

    usage = rt._merge_usage_records(usage_records)
    usage_provider = str(usage.get("provider") or "").strip()
    attempted_llm_model = client._config.model if client and client.is_enabled() else ""
    llm_fallback_count, llm_fallback_reason, llm_fallback_reasons = _sentiment_llm_fallback_summary(labeled_rows)
    sentiment_model = "sentiment-fallback-v1"
    sentiment_strategy = "deterministic-fallback"
    prompt_version = "sentiment-fallback-v1"
    if usage_provider == "anthropic" and client and client.is_enabled():
        sentiment_model = client._config.model
        sentiment_strategy = "anthropic-batch" if normalized["sentiment_batch_size"] > 1 else "anthropic-row"
        prompt_version = str(labeled_rows[0].get("sentiment_prompt_version") or "").strip() if labeled_rows else ""
        if not prompt_version:
            prompt_version = normalized["sentiment_prompt_version"] or (
                "sentiment-anthropic-batch-v1" if normalized["sentiment_batch_size"] > 1 else "sentiment-anthropic-v1"
            )
    elif usage_provider == "mixed" and client and client.is_enabled():
        sentiment_model = f"{client._config.model}+sentiment-fallback-v1"
        sentiment_strategy = "mixed-anthropic-fallback"
        prompt_version = str(labeled_rows[0].get("sentiment_prompt_version") or "").strip() if labeled_rows else ""
        if not prompt_version:
            prompt_version = normalized["sentiment_prompt_version"] or (
                "sentiment-anthropic-batch-v1" if normalized["sentiment_batch_size"] > 1 else "sentiment-anthropic-v1"
            )

    notes = [
        "sentiment label artifact generated by python-ai worker",
        f"dataset source: {normalized['dataset_name']}",
        f"sentiment output: {output_path}",
        f"sentiment model: {sentiment_model}",
        f"sentiment batch size: {normalized['sentiment_batch_size']}",
    ]
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")
    if llm_fallback_count > 0:
        fallback_note = f"llm fallback used: count={llm_fallback_count}, model={attempted_llm_model}, reason={llm_fallback_reason}"
        notes.append(fallback_note)
        LOGGER.warning(
            "llm.fallback.triggered",
            skill_name="sentiment_label",
            dataset_version_id=normalized["dataset_version_id"],
            model=attempted_llm_model,
            fallback_count=llm_fallback_count,
            reason=llm_fallback_reason,
        )

    runtime_result_scope = (
        "partial_build"
        if 0 < normalized["max_rows"] < source_row_count
        else "full_dataset"
    )
    return {
        "notes": notes,
        "artifact": rt._set_scope_fields({
            "skill_name": "sentiment_label",
            "dataset_version_id": normalized["dataset_version_id"],
            "source_dataset_name": normalized["dataset_name"],
            "sentiment_uri": str(output_path),
            "sentiment_ref": str(output_path),
            "sentiment_format": output_format,
            "sentiment_model": sentiment_model,
            "sentiment_prompt_version": prompt_version,
            "sentiment_strategy": sentiment_strategy,
            "sentiment_label_column": "sentiment_label",
            "sentiment_confidence_column": "sentiment_confidence",
            "sentiment_reason_column": "sentiment_reason",
            "llm_provider": "anthropic" if attempted_llm_model else "",
            "llm_model": attempted_llm_model,
            "llm_fallback": llm_fallback_count > 0,
            "llm_fallback_count": llm_fallback_count,
            "llm_fallback_reason": llm_fallback_reason,
            "llm_fallback_reasons": llm_fallback_reasons,
            "text_column": normalized["text_column"],
            "text_columns": list(normalized["text_columns"]),
            "text_joiner": normalized["text_joiner"],
            "row_id_column": "row_id",
            "storage_contract_version": "unstructured-storage-v1",
            "max_rows": normalized["max_rows"],
            "summary": {
                "input_row_count": len(rows),
                "source_row_count": source_row_count,
                "max_rows": normalized["max_rows"],
                "labeled_row_count": labeled_count,
                "text_column": normalized["text_column"],
                "text_columns": list(normalized["text_columns"]),
                "text_joiner": normalized["text_joiner"],
                "sentiment_batch_size": normalized["sentiment_batch_size"],
                "label_counts": dict(sorted(label_counts.items())),
            },
            "usage": usage,
        }, declared_result_scope="full_dataset", runtime_result_scope=runtime_result_scope),
    }


def _write_labeled_rows(
    handle: Any,
    labeled_rows: list[dict[str, Any]],
    batch_rows: list[tuple[int, dict[str, Any], str]],
    labels: list[dict[str, Any]],
    *,
    dataset_version_id: str,
    output_format: str,
    label_counts: Counter[str],
    labeled_count: int,
) -> int:
    for (index, row, _text), labeled in zip(batch_rows, labels):
        source_index = _stable_source_index(row, index)
        labeled_row = {
            "source_row_index": source_index,
            "row_id": _row_id(row, source_index, dataset_version_id),
            "sentiment_label": labeled["label"],
            "sentiment_confidence": labeled["confidence"],
            "sentiment_reason": labeled["reason"],
            "sentiment_prompt_version": labeled["prompt_version"],
        }
        if output_format == "jsonl":
            labeled_row = dict(row) | labeled_row
        labeled_rows.append(labeled_row)
        if handle is not None:
            handle.write(json.dumps(labeled_row, ensure_ascii=False))
            handle.write("\n")
        label_counts.update([str(labeled["label"])])
        labeled_count += 1
    return labeled_count
