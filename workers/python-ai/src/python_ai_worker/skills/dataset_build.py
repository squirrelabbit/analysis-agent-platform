from __future__ import annotations

"""Dataset preparation and enrichment skill handlers."""

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .. import runtime as rt


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


def _prepare_output_format(path: Path) -> str:
    return _artifact_output_format(path, "prepare")


def _artifact_output_format(path: Path, artifact_name: str) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".jsonl":
        return "jsonl"
    raise ValueError(f"{artifact_name} output_path must end with .parquet or .jsonl")


def _derive_chunk_output_path(embedding_path: Path) -> Path:
    name = embedding_path.name
    if name.endswith(".embeddings.jsonl"):
        return embedding_path.with_name(name[: -len(".embeddings.jsonl")] + ".chunks.parquet")
    if name.endswith(".jsonl"):
        return embedding_path.with_name(name[: -len(".jsonl")] + ".chunks.parquet")
    return embedding_path.with_name(name + ".chunks.parquet")


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


def run_dataset_prepare(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_prepare_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    output_path = Path(normalized["output_path"])
    output_format = _prepare_output_format(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    client = rt._anthropic_prepare_client(normalized["model"])
    kept_count = 0
    review_count = 0
    dropped_count = 0
    skipped_rows = 0
    prepared_batch: list[tuple[int, dict[str, Any], str]] = []
    prepared_rows: list[dict[str, Any]] = []

    handle = output_path.open("w", encoding="utf-8") if output_format == "jsonl" else None
    try:
        for source_index, row in enumerate(rows):
            raw_text = str(row.get(normalized["text_column"]) or "").strip()
            if not raw_text:
                skipped_rows += 1
                continue

            prepared_batch.append((source_index, row, raw_text))
            if len(prepared_batch) >= normalized["prepare_batch_size"]:
                batch_results = rt._prepare_rows(
                    [item[2] for item in prepared_batch],
                    client=client,
                    model=normalized["model"],
                    batch_size=normalized["prepare_batch_size"],
                )
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

        if prepared_batch:
            batch_results = rt._prepare_rows(
                [item[2] for item in prepared_batch],
                client=client,
                model=normalized["model"],
                batch_size=normalized["prepare_batch_size"],
            )
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
    finally:
        if handle is not None:
            handle.close()

    if output_format == "parquet":
        rt._write_parquet_rows(output_path, prepared_rows)

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
    notes.append(f"prepare batch size: {normalized['prepare_batch_size']}")
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")

    prompt_version = "dataset-prepare-fallback-v1"
    prepare_strategy = "deterministic-fallback"
    if client and client.is_enabled():
        prompt_version = "dataset-prepare-anthropic-batch-v1" if normalized["prepare_batch_size"] > 1 else "dataset-prepare-anthropic-v1"
        prepare_strategy = "anthropic-batch" if normalized["prepare_batch_size"] > 1 else "anthropic-row"

    return {
        "notes": notes,
        "artifact": {
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
            "prepared_text_column": "normalized_text",
            "row_id_column": "row_id",
            "storage_contract_version": "unstructured-storage-v1",
            "summary": {
                "input_row_count": len(rows),
                "output_row_count": kept_count + review_count,
                "kept_count": kept_count,
                "review_count": review_count,
                "dropped_count": dropped_count,
                "text_column": normalized["text_column"],
                "prepare_batch_size": normalized["prepare_batch_size"],
            },
        },
    }


def _write_prepared_rows(
    handle: Any,
    prepared_rows: list[dict[str, Any]],
    batch_rows: list[tuple[int, dict[str, Any], str]],
    batch_results: list[dict[str, Any]],
    *,
    dataset_version_id: str,
    kept_count: int,
    review_count: int,
    dropped_count: int,
) -> tuple[int, int, int]:
    for (source_index, row, raw_text), prepared in zip(batch_rows, batch_results):
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
        prepared_row["raw_text"] = raw_text
        prepared_row["normalized_text"] = prepared["normalized_text"]
        prepared_row["prepare_disposition"] = disposition
        prepared_row["prepare_reason"] = prepared["reason"]
        prepared_row["quality_flags"] = prepared["quality_flags"]
        prepared_row["prepare_prompt_version"] = prepared["prompt_version"]
        prepared_rows.append(prepared_row)
        if handle is not None:
            handle.write(json.dumps(prepared_row, ensure_ascii=False))
            handle.write("\n")
    return kept_count, review_count, dropped_count


def run_embedding(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_embedding_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    embedding_path = Path(normalized["output_path"])
    chunk_path = Path(normalized["chunk_output_path"]) if normalized["chunk_output_path"] else _derive_chunk_output_path(embedding_path)
    embedding_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_rows, source_row_count = _build_chunk_rows(
        rows,
        text_column=normalized["text_column"],
        dataset_version_id=normalized["dataset_version_id"],
        chunk_max_chars=normalized["chunk_max_chars"],
        chunk_overlap_chars=normalized["chunk_overlap_chars"],
    )
    rt._write_parquet_rows(chunk_path, chunk_rows)
    chunk_count = 0
    chunk_texts = [str(chunk.get("chunk_text") or "").strip() for chunk in chunk_rows]
    dense_result = rt._generate_dense_embeddings(
        chunk_texts,
        model=normalized["embedding_model"],
        dimensions=normalized["embedding_dimensions"],
    )
    embedding_model = normalized["embedding_model"]
    embedding_provider = "token-overlap"
    embedding_vector_dim = 0
    embedding_representation = "token-overlap"
    dense_vectors: list[list[float]] = []
    notes = [
        "embedding sidecar generated by python-ai worker",
        f"dataset source: {normalized['dataset_name']}",
        f"chunk output: {chunk_path}",
        f"chunk_max_chars: {normalized['chunk_max_chars']}",
        f"chunk_overlap_chars: {normalized['chunk_overlap_chars']}",
    ]
    if dense_result is not None:
        dense_vectors = [list(vector) for vector in dense_result["embeddings"]]
        embedding_model = str(dense_result["model"] or normalized["embedding_model"]).strip() or normalized["embedding_model"]
        embedding_provider = str(dense_result["provider"] or "openai").strip() or "openai"
        embedding_vector_dim = int(dense_result["dimensions"] or 0)
        embedding_representation = "dense+token-overlap"
        notes.append(f"embedding provider: {embedding_provider}")
        notes.append(f"embedding vector dim: {embedding_vector_dim}")
        prompt_tokens = int(dense_result.get("usage_prompt_tokens") or 0)
        if prompt_tokens > 0:
            notes.append(f"embedding prompt tokens: {prompt_tokens}")
    elif rt._looks_dense_embedding_model(normalized["embedding_model"]):
        embedding_model = rt.TOKEN_OVERLAP_EMBEDDING_MODEL
        notes.append("dense embedding unavailable; fell back to token-overlap")

    with embedding_path.open("w", encoding="utf-8") as handle:
        for chunk_index, chunk in enumerate(chunk_rows):
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
            if dense_vectors:
                record["embedding"] = list(dense_vectors[chunk_index])
                record["embedding_dim"] = embedding_vector_dim
                record["embedding_provider"] = embedding_provider
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")
            chunk_count += 1

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "embedding",
            "dataset_name": normalized["dataset_name"],
            "embedding_uri": str(embedding_path),
            "embedding_ref": str(embedding_path),
            "embedding_format": "jsonl",
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
        },
    }


def run_sentiment_label(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_sentiment_build_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    output_path = Path(normalized["output_path"])
    output_format = _artifact_output_format(output_path, "sentiment")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    client = rt._anthropic_prepare_client(normalized["model"])
    label_counts: Counter[str] = Counter()
    skipped_rows = 0
    labeled_count = 0
    labeled_rows: list[dict[str, Any]] = []

    handle = output_path.open("w", encoding="utf-8") if output_format == "jsonl" else None
    try:
        for index, row in enumerate(rows):
            text = str(row.get(normalized["text_column"]) or "").strip()
            if not text:
                skipped_rows += 1
                continue
            labeled = rt._label_sentiment(text, client=client)
            source_index = _stable_source_index(row, index)
            labeled_row = {
                "source_row_index": source_index,
                "row_id": _row_id(row, source_index, normalized["dataset_version_id"]),
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
            label_counts.update([labeled["label"]])
            labeled_count += 1
    finally:
        if handle is not None:
            handle.close()

    if output_format == "parquet":
        rt._write_parquet_rows(output_path, labeled_rows)

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
            "sentiment_ref": str(output_path),
            "sentiment_format": output_format,
            "sentiment_model": sentiment_model,
            "sentiment_prompt_version": prompt_version,
            "sentiment_label_column": "sentiment_label",
            "sentiment_confidence_column": "sentiment_confidence",
            "sentiment_reason_column": "sentiment_reason",
            "row_id_column": "row_id",
            "storage_contract_version": "unstructured-storage-v1",
            "summary": {
                "input_row_count": len(rows),
                "labeled_row_count": labeled_count,
                "text_column": normalized["text_column"],
                "label_counts": dict(sorted(label_counts.items())),
            },
        },
    }
