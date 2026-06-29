from __future__ import annotations

"""dataset_build entry point 모듈이 공유하는 helper.

여러 entry point에서 호출되는 row identifier, progress writer, output format
detector, text joiner를 모아둔다. 단일 entry point에만 종속된 helper는 각
entry point 모듈에 그대로 둔다.
"""

import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


# ── artifact provenance 표준 블록 (ADR-031 2단계) ───────────────────────────
# LLM build 결과는 "값"만이 아니라 "어떤 model/prompt/taxonomy/chunking/verify로
# 만들어졌나"가 결과의 일부다. 각 build summary에 흩어져 있던 provenance를 표준
# `provenance` 블록으로 통일해, "왜 지난번이랑 다르지?" 추적을 가능하게 한다.
ARTIFACT_SCHEMA_VERSION = "1.0"


def _stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_provenance(
    *,
    producer_task: str,
    dataset_version_id: str,
    model_id: str | None = None,
    prompt_version: str | None = None,
    taxonomy_id: str | None = None,
    judge_model_id: str | None = None,
    verify_mode: str | None = None,
    chunking_config: dict[str, Any] | None = None,
    input_artifact_refs: list[str] | None = None,
) -> dict[str, Any]:
    """artifact provenance 표준 블록을 만든다.

    ``deterministic_hash``는 LLM **출력**이 아니라 **입력+설정**(producer_task / model /
    judge_model / prompt / taxonomy / verify_mode / chunking_config / input_refs)
    식별자다. LLM은 같은 입력도 run마다 출력이 다를 수 있으므로(self-consistency
    측정 참조), "같은 설정으로 만든 결과인가"를 식별하는 용도다. ``created_at``은
    실제 생성 시각(hash에는 포함 안 함).
    """
    refs = list(input_artifact_refs or [])
    chunk_hash = _stable_hash(chunking_config) if chunking_config else None
    identity = {
        "producer_task": producer_task,
        "model_id": model_id,
        "judge_model_id": judge_model_id,
        "prompt_version": prompt_version,
        "taxonomy_id": taxonomy_id,
        "verify_mode": verify_mode,
        "chunking_config": chunking_config,
        "input_artifact_refs": sorted(refs),
    }
    return {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "producer_task": producer_task,
        "dataset_version_id": dataset_version_id,
        "model_id": model_id,
        "judge_model_id": judge_model_id,
        "prompt_version": prompt_version,
        "taxonomy_id": taxonomy_id,
        "verify_mode": verify_mode,
        "chunking_config_hash": chunk_hash,
        "input_artifact_refs": refs,
        "deterministic_hash": _stable_hash(identity),
        "created_at": datetime.now(UTC).isoformat(),
    }


# ── partial-failure 가드 (ADR-031 3단계 — cross-cutting 공통화) ──────────────
# LLOA build에서 일부 doc 실패는 per-doc 격리로 흡수하지만, 실패율이 임계를 넘으면
# (LLOA 서버 다운 등) degraded 결과를 "완료"로 덮지 않게 fail-loud 중단한다. 이 가드가
# doc_genuineness/clause_label에 같은 골격으로 중복돼 있어 공통화한다. verify 경로는
# per-doc judge 격리라 별도(이 가드 미적용).
class DatasetBuildFailureRateExceeded(RuntimeError):
    """LLOA build 실패율이 임계(DATASET_BUILD_MAX_FAILURE_RATE)를 넘어 fail-loud 중단."""


def check_failure_rate(
    *,
    task: str,
    failures: int,
    total: int,
    max_rate: float,
    detail: str,
    reason: str = "LLOA 실패율",
) -> None:
    """failures/total >= max_rate면 DatasetBuildFailureRateExceeded를 raise.

    DatasetBuildFailureRateExceeded는 RuntimeError 서브클래스라 기존 RuntimeError를
    잡는 호출부와 호환된다(동작 보존, 타입만 구체화).
    """
    if total > 0 and failures / total >= max_rate:
        raise DatasetBuildFailureRateExceeded(
            f"{task} aborted: {reason} {failures / total:.0%} "
            f"({detail}, total={total}) >= 임계 {max_rate:.0%}. LLOA 서버/응답 상태를 "
            "확인하고 재시도하세요 (DATASET_BUILD_MAX_FAILURE_RATE로 조정 가능)."
        )


def stable_source_index(row: dict[str, Any], fallback_index: int) -> int:
    try:
        return int(row.get("source_row_index") or fallback_index)
    except (TypeError, ValueError):
        return fallback_index


def row_id(row: dict[str, Any], fallback_index: int, dataset_version_id: str) -> str:
    existing = str(row.get("row_id") or "").strip()
    if existing:
        return existing
    source_index = stable_source_index(row, fallback_index)
    prefix = dataset_version_id or "dataset"
    return f"{prefix}:row:{source_index}"


def unique_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        result.append(normalized)
        seen.add(normalized)
    return result


def write_progress(
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


def artifact_output_format(path: Path, artifact_name: str) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".jsonl":
        return "jsonl"
    raise ValueError(f"{artifact_name} output_path must end with .parquet or .jsonl")


def joined_text(row: dict[str, Any], text_columns: list[str], text_joiner: str) -> str:
    parts: list[str] = []
    for column in text_columns:
        value = str(row.get(column) or "").strip()
        if value:
            parts.append(value)
    return text_joiner.join(parts).strip()


def _normalize_text_columns_payload(payload: dict[str, Any], default_column: str) -> tuple[str, list[str], str]:
    """payload의 text_columns/text_column/text_joiner를 한 번에 정규화.

    옛 ``runtime/payloads.py`` 헬퍼였는데 δ-4 (5/21)로 payloads.py 모듈이
    제거되면서 dataset_build 도메인 helper로 inline 이전. clean.py가 유일한
    호출처라 _common.py에 둔다.
    """
    raw_columns = payload.get("text_columns")
    columns: list[str] = []
    if isinstance(raw_columns, list):
        seen: set[str] = set()
        for item in raw_columns:
            column = str(item or "").strip()
            if not column or column in seen:
                continue
            seen.add(column)
            columns.append(column)

    requested_label = str(payload.get("text_column") or "").strip()
    if not columns:
        columns = [requested_label or default_column]

    if requested_label and len(columns) == 1:
        text_column = requested_label
    elif len(columns) == 1:
        text_column = columns[0]
    else:
        text_column = " + ".join(columns)

    text_joiner = payload.get("text_joiner")
    if text_joiner is None:
        text_joiner = "\n\n"
    else:
        text_joiner = str(text_joiner)
    return text_column, columns, text_joiner


def _normalize_dataset_clean_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """dataset_clean payload normalize. 옛 runtime/payloads.py 헬퍼에서 이전
    (δ-4 정리). 5/21 — preprocess_options 4 boolean 제거.
    silverone 2026-05-28 — date_column optional 추가 (clean 정식화).
    """
    from ..runtime.common import _normalize_prepare_regex_rule_names

    dataset_name = str(payload.get("dataset_name") or "").strip()
    if not dataset_name:
        raise ValueError("dataset_name is required")
    output_path = str(payload.get("output_path") or f"{dataset_name}.cleaned.parquet").strip()
    if not output_path:
        raise ValueError("output_path is required")
    text_column, text_columns, text_joiner = _normalize_text_columns_payload(payload, "text")
    date_column_raw = payload.get("date_column")
    date_column = str(date_column_raw).strip() if date_column_raw is not None else ""
    return {
        "dataset_version_id": str(payload.get("dataset_version_id") or "").strip(),
        "dataset_name": dataset_name,
        "text_column": text_column,
        "text_columns": text_columns,
        "text_joiner": text_joiner,
        "output_path": output_path,
        "progress_path": str(payload.get("progress_path") or "").strip(),
        "regex_rule_names": _normalize_prepare_regex_rule_names(payload.get("regex_rule_names")),
        "date_column": date_column or None,
    }
