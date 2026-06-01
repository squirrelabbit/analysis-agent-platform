from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    left_table = params["left"]
    right_table = params["right"]
    join_keys = list(params["join_key"])
    left_label_text = str(params["left_label"]).strip()
    right_label_text = str(params["right_label"]).strip()
    join_set = set(join_keys)

    left_cols = context.get_column_names(left_table)
    right_cols = context.get_column_names(right_table)

    select_parts: list[str] = []
    for key in join_keys:
        ident = safe_identifier(key)
        select_parts.append(f"COALESCE(l.{ident}, r.{ident}) AS {ident}")
    for col in left_cols:
        if col in join_set:
            continue
        alias = f"{left_label_text}_{col}"
        select_parts.append(f"l.{safe_identifier(col)} AS {safe_identifier(alias)}")
    for col in right_cols:
        if col in join_set:
            continue
        alias = f"{right_label_text}_{col}"
        select_parts.append(f"r.{safe_identifier(col)} AS {safe_identifier(alias)}")

    on_clause = " AND ".join(
        f"l.{safe_identifier(k)} = r.{safe_identifier(k)}" for k in join_keys
    )
    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {safe_identifier(left_table)} AS l "
        f"FULL OUTER JOIN {safe_identifier(right_table)} AS r ON {on_clause}"
    )
    return sql, {}


__all__ = ["build_sql"]
