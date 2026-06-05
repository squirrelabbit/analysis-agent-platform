from __future__ import annotations

"""executor skill 공통 helper.

skill SQL builder는 (params, context) → (sql, extra) 함수.
identifier와 literal escape는 SQL injection 방지 + DuckDB grammar 충돌 방지.
"""

import datetime as _dt
from typing import Any, Iterable

from ...sql_identifiers import SAFE_SQL_IDENTIFIER_RE


class ExecutorError(RuntimeError):
    """skill SQL 생성/실행 중 발생한 오류. 메시지에 step 컨텍스트 포함을 권장."""


def safe_identifier(value: Any) -> str:
    """SQL identifier (table/column/alias) escape. validator R2 (2026-05-27)로
    plan_v2 step id와 같은 ``SAFE_SQL_IDENTIFIER_RE``를 공유한다 — validator를
    통과한 step id는 그대로 사용 가능."""

    text = str(value or "").strip()
    if not SAFE_SQL_IDENTIFIER_RE.match(text):
        raise ExecutorError(f"unsafe SQL identifier: {value!r}")
    return text


def quote_identifier(value: Any) -> str:
    """SQL **column** identifier를 double-quote로 감싼다 (silverone 2026-06-05).

    비-ASCII(한글 등) 컬럼명도 안전하게 지원하기 위함. 내부 `"`는 `""`로 escape해
    SQL injection / grammar 충돌을 막는다. table/step id는 ASCII 정책(safe_identifier)을
    그대로 유지하고, **컬럼 참조에만** 이 함수를 쓴다."""

    text = str(value or "").strip()
    if not text:
        raise ExecutorError(f"empty SQL identifier: {value!r}")
    return '"' + text.replace('"', '""') + '"'


def sql_literal(value: Any) -> str:
    """plan param 값을 SQL literal로 변환. None → NULL, bool/숫자 그대로,
    문자열은 single-quote escape, datetime/date는 ISO 문자열로."""

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (_dt.datetime, _dt.date)):
        return f"'{value.isoformat()}'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def sql_literal_list(values: Any) -> str:
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
        raise ExecutorError("filter.value must be a list for in/not_in")
    literals = [sql_literal(item) for item in values]
    if not literals:
        raise ExecutorError("filter.value list must not be empty")
    return ", ".join(literals)


__all__ = [
    "ExecutorError",
    "safe_identifier",
    "quote_identifier",
    "sql_literal",
    "sql_literal_list",
]
