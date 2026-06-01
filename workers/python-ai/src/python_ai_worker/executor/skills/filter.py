from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import ExecutorError, safe_identifier, sql_literal, sql_literal_list

if TYPE_CHECKING:
    from ..context import ExecutorContext


# silverone 2026-05-26 (SQL-3.1, audit C4) — timestamp/date column + string
# value 비교는 명시 CAST로 보낸다. DuckDB가 silent하게 lexicographic 비교를
# 하거나 cast 실패로 silent miss할 가능성을 차단.
def _is_timestamp_type(column_type: str) -> bool:
    return column_type.upper().startswith(("TIMESTAMP", "DATE"))


def _wrap_value_for_column(value: Any, column_type: str) -> str:
    """value의 SQL 표현. timestamp/date column + string value면 명시 CAST."""

    literal = sql_literal(value)
    if isinstance(value, str) and _is_timestamp_type(column_type):
        # CAST AS TIMESTAMP가 DATE column에도 OK (DuckDB가 DATE comparison으로 좁힘).
        return f"CAST({literal} AS TIMESTAMP)"
    return literal


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    input_ref = safe_identifier(params["input"])
    column = safe_identifier(params["column"])
    operator = str(params["operator"]).strip()
    value = params.get("value")

    # 입력 테이블의 column type을 보고 cast 정책을 결정. 조회 비용은
    # DESCRIBE 1회 — DuckDB가 캐시. 실패하면 빈 dict로 fallback (기존 동작).
    try:
        column_types = context.get_column_types(params["input"])
    except Exception:  # noqa: BLE001
        column_types = {}
    column_type = column_types.get(params["column"], "")

    def value_sql(v: Any) -> str:
        return _wrap_value_for_column(v, column_type)

    if operator == "eq":
        predicate = f"{column} = {value_sql(value)}"
    elif operator == "neq":
        predicate = f"{column} <> {value_sql(value)}"
    elif operator == "gt":
        predicate = f"{column} > {value_sql(value)}"
    elif operator == "gte":
        predicate = f"{column} >= {value_sql(value)}"
    elif operator == "lt":
        predicate = f"{column} < {value_sql(value)}"
    elif operator == "lte":
        predicate = f"{column} <= {value_sql(value)}"
    elif operator == "in":
        if _is_timestamp_type(column_type) and isinstance(value, list):
            casted = ", ".join(value_sql(v) for v in value)
            predicate = f"{column} IN ({casted})"
        else:
            predicate = f"{column} IN ({sql_literal_list(value)})"
    elif operator == "not_in":
        if _is_timestamp_type(column_type) and isinstance(value, list):
            casted = ", ".join(value_sql(v) for v in value)
            predicate = f"{column} NOT IN ({casted})"
        else:
            predicate = f"{column} NOT IN ({sql_literal_list(value)})"
    elif operator == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise ExecutorError("filter.between requires a 2-element list")
        predicate = f"{column} BETWEEN {value_sql(value[0])} AND {value_sql(value[1])}"
    elif operator == "contains":
        predicate = f"{column} LIKE {sql_literal('%' + str(value) + '%')}"
    elif operator == "is_null":
        predicate = f"{column} IS NULL"
    elif operator == "not_null":
        predicate = f"{column} IS NOT NULL"
    else:
        raise ExecutorError(f"filter.operator unsupported: {operator}")

    return f"SELECT * FROM {input_ref} WHERE {predicate}", {}


__all__ = ["build_sql"]
