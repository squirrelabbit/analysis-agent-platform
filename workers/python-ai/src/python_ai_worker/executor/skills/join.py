from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import quote_identifier, safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    left_ref = safe_identifier(params["left"])
    right_ref = safe_identifier(params["right"])
    on_keys: list[str] = list(params["on"])
    how = str(params["how"]).strip().upper()
    join_clause = "INNER JOIN" if how == "INNER" else f"{how} JOIN"

    on_clause = " AND ".join(
        f"l.{quote_identifier(k)} = r.{quote_identifier(k)}" for k in on_keys
    )

    left_cols = context.get_column_names(params["left"])
    right_cols = context.get_column_names(params["right"])
    on_set = set(on_keys)

    select_parts: list[str] = []
    for key in on_keys:
        ident = quote_identifier(key)
        select_parts.append(f"COALESCE(l.{ident}, r.{ident}) AS {ident}")
    for col in left_cols:
        if col in on_set:
            continue
        ident = quote_identifier(col)
        select_parts.append(f"l.{ident} AS {ident}")
    for col in right_cols:
        if col in on_set:
            continue
        # left에 같은 이름 컬럼이 있으면 right_ prefix로 충돌 회피
        alias = f"right_{col}" if col in left_cols else col
        select_parts.append(f"r.{quote_identifier(col)} AS {quote_identifier(alias)}")

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {left_ref} AS l {join_clause} {right_ref} AS r ON {on_clause}"
    )
    return sql, {}


__all__ = ["build_sql"]
