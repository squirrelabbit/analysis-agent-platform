from __future__ import annotations

"""dataset_clean entry point — raw row의 placeholder noise strip + regex 정제."""

import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..obs import get, skill_handler
from ._common import (
    _normalize_dataset_clean_payload,
    artifact_output_format,
    joined_text,
    row_id,
    write_progress,
)
from .schema_inference import (
    AnalysisColumn,
    coerce_timestamp,
    coerce_value,
    infer_analysis_columns,
)

LOGGER = get(__name__)

# clean 단계는 placeholder 문자열만 제거(`존재하지 않는 이미지입니다.` 등).
# tier 4 fallback: config/noise_patterns/<rule>.json. payload에 inject되면
# 그것을 우선 사용 (1~3 tier resolver는 후속 plan에서).
_DEFAULT_NOISE_PATTERN_NAME = "festival-v1"


def _find_noise_patterns_config_root() -> Path | None:
    """host repo와 container(/app) 모두 자동 탐지. parents loop으로 첫 매치."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "config" / "noise_patterns"
        if candidate.is_dir():
            return candidate
    return None


def _load_noise_patterns(payload: dict[str, Any]) -> tuple[list[re.Pattern[str]], list[str]]:
    """payload에 inject된 inline patterns → 없으면 tier 4 config fallback.

    반환: (compiled regex 리스트, 원본 pattern 문자열 리스트). 운영자 audit용
    pattern 문자열은 summary에 그대로 노출."""
    raw_patterns: list[str] = []
    injected = payload.get("noise_patterns_content")
    if isinstance(injected, dict):
        injected_list = injected.get("patterns")
        if isinstance(injected_list, list):
            raw_patterns = [str(p) for p in injected_list if str(p).strip()]
    if not raw_patterns:
        rule_name = str(payload.get("noise_patterns_rule_name") or _DEFAULT_NOISE_PATTERN_NAME).strip()
        config_root = _find_noise_patterns_config_root()
        config_path = config_root / f"{rule_name}.json" if config_root else None
        if config_path and config_path.exists():
            try:
                data = json.loads(config_path.read_text(encoding="utf-8"))
                patterns = data.get("patterns") if isinstance(data, dict) else None
                if isinstance(patterns, list):
                    raw_patterns = [str(p) for p in patterns if str(p).strip()]
            except (OSError, json.JSONDecodeError) as exc:
                LOGGER.warning(
                    "noise_patterns.config_load_failed",
                    error_category=type(exc).__name__,
                    error_message=str(exc),
                    config_path=str(config_path),
                )
    compiled: list[re.Pattern[str]] = []
    for raw in raw_patterns:
        try:
            compiled.append(re.compile(raw))
        except re.error as exc:
            LOGGER.warning(
                "noise_patterns.regex_compile_failed",
                error_category=type(exc).__name__,
                error_message=str(exc),
                pattern=raw,
            )
    return compiled, raw_patterns


def _apply_noise_scrub(text: str, patterns: list[re.Pattern[str]]) -> tuple[str, dict[str, int]]:
    """text에 inline scrub 적용. 매치된 pattern은 공백으로 치환, 다중 공백은 단일로.
    반환: (scrubbed text, pattern별 hit count 딕셔너리)."""
    hits: dict[str, int] = {}
    if not patterns or not text:
        return text, hits
    scrubbed = text
    for pattern in patterns:
        matched = pattern.findall(scrubbed)
        if matched:
            hits[pattern.pattern] = hits.get(pattern.pattern, 0) + len(matched)
            scrubbed = pattern.sub(" ", scrubbed)
    if hits:
        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()
    return scrubbed, hits


def _clean_output_schema(analysis_columns: list[AnalysisColumn] | None = None) -> Any:
    """silverone 2026-05-28 (clean 정식화) — 표준 9 컬럼 + (2026-06-08) 분석 컬럼.

    - 분석 path가 의존하는 표준 컬럼을 top-level로 노출.
    - 원본 source 컬럼(한글/BOM/괄호 포함)은 source_json에 보존 — SQL identifier
      문제(SAFE_SQL_IDENTIFIER_RE)를 피하면서 운영자가 원본 row를 확인 가능.
    - 추가로 추론된 analysis_columns를 SQL-safe alias + typed 컬럼으로 materialize
      (integer→int64, float→float64, timestamp→string ISO, string→string).
      advertised type == parquet 적재 type. source_json은 그대로 유지.
    """
    arrow, _ = rt._require_pyarrow()
    fields = [
        ("row_id", arrow.string()),
        ("doc_id", arrow.string()),
        ("source_row_index", arrow.int64()),
        ("raw_text", arrow.string()),
        ("cleaned_text", arrow.string()),
        ("created_at", arrow.string()),
        ("clean_status", arrow.string()),
        ("clean_reason", arrow.string()),
        ("source_json", arrow.string()),
    ]
    for col in analysis_columns or []:
        if col.type == "integer":
            arrow_type = arrow.int64()
        elif col.type == "float":
            arrow_type = arrow.float64()
        else:
            # timestamp는 created_at과 동일하게 ISO string으로 저장. string도 string.
            arrow_type = arrow.string()
        fields.append((col.name, arrow_type))
    return arrow.schema(fields)


def _coerce_created_at(value: Any) -> str | None:
    """date_column 값을 ISO 8601 UTC string으로 변환. 실패 시 None.
    분석 컬럼 timestamp 추론과 동일 규칙(schema_inference.coerce_timestamp) 사용."""
    return coerce_timestamp(value)


@skill_handler("python-ai")
def run_dataset_clean(payload: dict[str, Any]) -> dict[str, Any]:
    # clean 단계는 noise 문자열 strip + regex rule 정제만 책임진다.
    normalized = _normalize_dataset_clean_payload(payload)
    rows = rt._iter_rows(normalized["dataset_name"])
    source_row_count = len(rows)
    output_path = Path(normalized["output_path"])
    output_format = artifact_output_format(output_path, "clean")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = normalized["progress_path"]
    started_at = time.monotonic()
    write_progress(
        progress_path,
        processed_rows=0,
        total_rows=source_row_count,
        started_at=started_at,
        message="clean queued",
    )

    # 5/11: inline noise scrub patterns 로드 (tier 4 config fallback)
    noise_compiled, noise_pattern_strs = _load_noise_patterns(payload)
    noise_pattern_hits: Counter[str] = Counter()
    noise_scrub_applied_row_count = 0

    kept_count = 0
    dropped_count = 0
    skipped_rows = 0
    regex_rule_hits: Counter[str] = Counter()
    source_input_char_count = 0
    cleaned_input_char_count = 0
    date_parse_miss_count = 0
    cleaned_rows: list[dict[str, Any]] = []

    # silverone 2026-06-08 (파일럿) — CSV 메타 컬럼을 queryable typed 분석 컬럼으로
    # 추론. text_columns(raw_text로 사용)와 date_column(created_at로 표준화)은 제외.
    analysis_exclude = list(normalized["text_columns"])
    if normalized["date_column"]:
        analysis_exclude.append(normalized["date_column"])
    analysis_columns: list[AnalysisColumn] = infer_analysis_columns(rows, analysis_exclude)

    handle = output_path.open("w", encoding="utf-8") if output_format == "jsonl" else None
    try:
        for source_index, row in enumerate(rows):
            raw_text = joined_text(row, normalized["text_columns"], normalized["text_joiner"])
            if not raw_text:
                skipped_rows += 1
                dropped_count += 1
                write_progress(
                    progress_path,
                    processed_rows=source_index + 1,
                    total_rows=source_row_count,
                    started_at=started_at,
                    message="clean scanning rows",
                )
                continue

            regex_cleaned_text, applied_regex_rules = rt._apply_prepare_regex_rules(raw_text, normalized["regex_rule_names"])
            regex_rule_hits.update(applied_regex_rules)
            # 5/11: inline noise scrub — placeholder 문자열 strip (row 차단 X)
            scrubbed_text, scrub_hits = _apply_noise_scrub(regex_cleaned_text, noise_compiled)
            if scrub_hits:
                noise_scrub_applied_row_count += 1
                noise_pattern_hits.update(scrub_hits)
            # 5/21: preprocess_options 4종(remove_english/numbers/special/monosyllables)
            # 제거됨. 한글 SNS 후기 분석에서 영문/숫자/공백/모노음절은 모두 의미
            # 신호라 거친 제거가 해롭다. 남은 책임은 known noise phrase strip +
            # whitespace 정규화. 도메인 필터링은 regex_rule_names로 명시.
            cleaned_text = rt._strip_known_noise_phrases(scrubbed_text)
            source_input_char_count += len(raw_text)
            cleaned_input_char_count += len(cleaned_text)
            if not cleaned_text:
                dropped_count += 1
                continue

            kept_count += 1
            # silverone 2026-05-28 (clean 정식화) — 표준 9 컬럼만 build.
            # 원본 row(한글/BOM/괄호 포함)는 source_json에 직렬화 보존.
            row_identifier = row_id(dict(row), source_index, normalized["dataset_version_id"])
            created_at_value: str | None = None
            if normalized["date_column"]:
                created_at_value = _coerce_created_at(row.get(normalized["date_column"]))
                if created_at_value is None:
                    date_parse_miss_count += 1
            cleaned_row = {
                "row_id": row_identifier,
                "doc_id": row_identifier,
                "source_row_index": source_index,
                "raw_text": raw_text,
                "cleaned_text": cleaned_text,
                "created_at": created_at_value,
                "clean_status": "keep",
                "clean_reason": "text kept after deterministic cleaning",
                "source_json": json.dumps(dict(row), ensure_ascii=False),
            }
            # 추론된 분석 컬럼을 typed 값으로 materialize (alias = SQL-safe 컬럼명).
            for col in analysis_columns:
                cleaned_row[col.name] = coerce_value(row.get(col.source_column), col.type)
            cleaned_rows.append(cleaned_row)
            if handle is not None:
                handle.write(json.dumps(cleaned_row, ensure_ascii=False))
                handle.write("\n")

            write_progress(
                progress_path,
                processed_rows=source_index + 1,
                total_rows=source_row_count,
                started_at=started_at,
                message="clean processing rows",
            )
    except Exception:
        write_progress(
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
        rt._write_parquet_rows(output_path, cleaned_rows, schema=_clean_output_schema(analysis_columns))
    write_progress(
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
        "date_column": normalized["date_column"],
        "date_parse_miss_count": date_parse_miss_count,
        # silverone 2026-06-08 (파일럿) — materialize된 분석 컬럼 메타. control-plane이
        # 이걸 docs_extra_columns로 planner에 전달. name=parquet alias(=advertise),
        # type=parquet/advertise type, label=원본명, source_column=원본명.
        "analysis_columns": [
            {"name": c.name, "type": c.type, "label": c.label, "source_column": c.label}
            for c in analysis_columns
        ],
        "source_input_char_count": source_input_char_count,
        "cleaned_input_char_count": cleaned_input_char_count,
        "clean_reduced_char_count": max(0, source_input_char_count - cleaned_input_char_count),
        "clean_regex_rule_names": list(normalized["regex_rule_names"]),
        "clean_regex_rule_hits": dict(regex_rule_hits),
        # 5/13 (silverone): garbage filter는 clean 단계에서 분리 — 5/8 통합 retract.
        # 5/11 inline noise scrub 결과 — placeholder 문자열 strip 통계
        "noise_pattern_count": len(noise_pattern_strs),
        "noise_pattern_strs": list(noise_pattern_strs),
        "noise_scrub_applied_row_count": noise_scrub_applied_row_count,
        "noise_pattern_hits": dict(noise_pattern_hits),
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
            "source_input_char_count": source_input_char_count,
            "cleaned_input_char_count": cleaned_input_char_count,
            "clean_reduced_char_count": max(0, source_input_char_count - cleaned_input_char_count),
            "clean_regex_rule_names": list(normalized["regex_rule_names"]),
            "summary": summary,
        }, declared_result_scope="full_dataset", runtime_result_scope="full_dataset"),
    }
