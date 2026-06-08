from __future__ import annotations

"""CSV 기반 분석 컬럼 자동 추론 (silverone 2026-06-08, 파일럿).

clean 단계가 원본 CSV의 메타 컬럼(수집채널/좋아요수/게시일 등)을 cleaned.parquet의
queryable typed column으로 materialize하기 위한 deterministic helper.

핵심 원칙:
- LLOA/프론트 UI 없이 CSV 샘플 기반 deterministic 추론.
- timestamp / integer / float / string 4종. confidence 낮으면 string fallback.
- advertised type == parquet에 실제 적재되는 type (integer→int64, float→float64,
  string→string, timestamp→ISO string; created_at과 동일하게 문자열 ISO로 저장하고
  날짜 연산은 lexical 비교/CAST로 동작).
- text_columns / date_column / 표준 clean 컬럼은 제외.
- alias는 SQL-safe identifier로 생성하고 표준 컬럼·기존 alias와 충돌 방지.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# clean output 표준 9 컬럼 — analysis alias가 이것과 겹치면 안 된다.
STANDARD_CLEAN_COLUMNS: frozenset[str] = frozenset(
    {
        "row_id",
        "doc_id",
        "source_row_index",
        "raw_text",
        "cleaned_text",
        "created_at",
        "clean_status",
        "clean_reason",
        "source_json",
    }
)

# 추론 샘플 크기 + 임계. int/float은 100%(엄격), timestamp는 0.9(invalid date 일부 허용).
INFER_SAMPLE_SIZE = 200
_TIMESTAMP_MIN_RATIO = 0.9

_TS_FORMATS = (
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
)


@dataclass(frozen=True)
class AnalysisColumn:
    """추론된 분석 컬럼.

    - source_column: 원본 CSV의 실제 키(BOM 포함 가능) — row 조회용.
    - name: SQL-safe alias = parquet 컬럼명 = planner advertise 이름.
    - type: timestamp | integer | float | string.
    - label: 화면/planner 표시용 원본 컬럼명(BOM 제거).
    """

    source_column: str
    name: str
    type: str
    label: str


def _strip_key(value: Any) -> str:
    return str(value).strip().lstrip("﻿")


def coerce_timestamp(value: Any) -> str | None:
    """날짜 문자열 → ISO 8601 UTC string. 실패/빈값/Invalid → None.
    created_at 표준화와 동일 규칙 (clean._coerce_created_at도 이 함수를 쓴다)."""
    if value is None:
        return None
    raw = _strip_key(value)
    if not raw or raw.lower().startswith("invalid"):
        return None
    for fmt in _TS_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return None


def _clean_number(raw: str) -> str:
    return raw.replace(",", "").strip()


def _is_int(raw: str) -> bool:
    s = _clean_number(raw)
    if not s:
        return False
    try:
        int(s)
        return True
    except ValueError:
        return False


def _is_float(raw: str) -> bool:
    s = _clean_number(raw)
    if not s:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def infer_column_type(values: list[Any]) -> str:
    """비어 있지 않은 샘플 값으로 type 추론. 애매하면 string."""
    nonempty = [str(v).strip() for v in values if v is not None and str(v).strip() != ""]
    if not nonempty:
        return "string"
    n = len(nonempty)
    ts_hits = sum(1 for v in nonempty if coerce_timestamp(v) is not None)
    if ts_hits / n >= _TIMESTAMP_MIN_RATIO:
        return "timestamp"
    if all(_is_int(v) for v in nonempty):
        return "integer"
    if all(_is_float(v) for v in nonempty):
        return "float"
    return "string"


def _safe_alias(source_column: str, position: int, used: set[str]) -> str:
    """원본 컬럼명 → SQL-safe identifier. 한글/BOM/괄호 등은 col_<position>으로 fallback.
    표준 컬럼·기존 alias와 충돌 시 suffix를 붙인다."""
    base = _strip_key(source_column)
    slug = re.sub(r"[^a-z0-9_]+", "_", base.lower()).strip("_")
    if not slug or not slug[0].isalpha():
        slug = f"col_{position}"
    alias = slug
    i = 1
    while alias in used:
        alias = f"{slug}_{i}"
        i += 1
    used.add(alias)
    return alias


def coerce_value(value: Any, type_: str) -> Any:
    """parquet 적재용 값으로 변환. 빈값/파싱 실패 → None.
    integer/float은 실제 숫자, timestamp는 ISO string, string은 원본 보존."""
    if value is None:
        return None
    raw = str(value).strip()
    if raw == "":
        return None
    if type_ == "timestamp":
        return coerce_timestamp(raw)
    if type_ == "integer":
        try:
            return int(_clean_number(raw))
        except ValueError:
            return None
    if type_ == "float":
        try:
            return float(_clean_number(raw))
        except ValueError:
            return None
    return str(value)


def infer_analysis_columns(
    rows: list[dict[str, Any]],
    exclude_source_columns: list[str] | None = None,
    sample_size: int = INFER_SAMPLE_SIZE,
) -> list[AnalysisColumn]:
    """CSV row(dict) 목록에서 분석 컬럼을 추론한다.

    - 컬럼 순서/이름은 첫 행의 키(CSV 헤더) 기준.
    - text_columns / date_column(= created_at) / 표준 clean 컬럼은 제외.
    - 각 컬럼은 앞 ``sample_size`` 행 샘플로 type 추론, SQL-safe alias 부여.
    """
    if not rows:
        return []
    exclude = {_strip_key(c) for c in (exclude_source_columns or [])}
    used: set[str] = set(STANDARD_CLEAN_COLUMNS)
    sample = rows[:sample_size]
    header = list(rows[0].keys())
    result: list[AnalysisColumn] = []
    for position, src in enumerate(header, start=1):
        norm = _strip_key(src)
        if not norm or norm in exclude or norm in STANDARD_CLEAN_COLUMNS:
            continue
        ctype = infer_column_type([r.get(src) for r in sample])
        alias = _safe_alias(norm, position, used)
        result.append(AnalysisColumn(source_column=src, name=alias, type=ctype, label=norm))
    return result


__all__ = [
    "AnalysisColumn",
    "STANDARD_CLEAN_COLUMNS",
    "INFER_SAMPLE_SIZE",
    "coerce_timestamp",
    "coerce_value",
    "infer_column_type",
    "infer_analysis_columns",
]
