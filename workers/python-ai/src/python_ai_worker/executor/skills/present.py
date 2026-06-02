from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


# silverone 2026-05-26 (SQL-4, audit M7) — present 응답 row 한도 정책.
# default는 1000, user가 명시한 limit이 있으면 그 값을 사용.
# 어떤 경우에도 PRESENT_HARD_CAP_ROWS(10000)를 초과하지 않는다 (validator에서
# > 10000은 reject되므로 여기까지 오면 안 되지만, 방어로 한 번 더 clamp).
DEFAULT_PRESENT_MAX_ROWS = 1000
PRESENT_HARD_CAP_ROWS = 10000


def resolve_max_rows(params: dict[str, Any]) -> int:
    """present.params.limit에서 적용할 max_rows를 결정."""

    raw = params.get("limit")
    if raw is None:
        return DEFAULT_PRESENT_MAX_ROWS
    if isinstance(raw, bool) or not isinstance(raw, int):
        return DEFAULT_PRESENT_MAX_ROWS
    if raw <= 0:
        return DEFAULT_PRESENT_MAX_ROWS
    return min(raw, PRESENT_HARD_CAP_ROWS)


def _projection(params: dict[str, Any]) -> str:
    """present.columns가 있으면 그 컬럼만 SELECT (projection), 없으면 ``*``.

    silverone 2026-06-02 — present.columns hard constraint. 컬럼이 input에 없으면
    DuckDB Binder Error로 execution이 실패한다 (validator가 추론 가능한 input은
    미리 repair 대상으로 잡고, 추론 불가 input은 여기서 런타임 검증된다)."""
    columns = params.get("columns")
    if not isinstance(columns, list) or not columns:
        return "*"
    names: list[str] = []
    for col in columns:
        if not isinstance(col, str) or not col.strip():
            return "*"  # 비정상 columns는 무시하고 전체 — validator가 별도로 잡는다.
        names.append(safe_identifier(col.strip()))
    return ", ".join(names)


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    input_ref = safe_identifier(params["input"])
    sql = f"SELECT {_projection(params)} FROM {input_ref}"
    extra = {
        "format": params.get("format"),
        "title": params.get("title"),
        "max_rows": resolve_max_rows(params),
    }
    return sql, extra


__all__ = ["DEFAULT_PRESENT_MAX_ROWS", "PRESENT_HARD_CAP_ROWS", "build_sql", "resolve_max_rows"]
