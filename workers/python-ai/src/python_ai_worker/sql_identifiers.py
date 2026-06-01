from __future__ import annotations

"""SQL identifier 단일 source (validator R2, 2026-05-27).

validator(plan step id, compare label)와 executor(safe_identifier, view 이름)가
같은 기준을 공유하기 위한 모듈. validator contract refactor audit (2026-05-27)
§3-C / §4-C에서 식별된 4중 정의(`_STEP_ID_PATTERN`, `_IDENT_SAFE_PATTERN`,
`_IDENT_PATTERN`, `_SAFE_IDENT`)를 여기로 통합한다.

plan_v2의 step id / compare label / 모든 column·alias는 DuckDB SQL identifier
로 직접 사용된다. letter/underscore로 시작하고 [a-zA-Z0-9_]만 허용해 quoting
없이 안전하게 쓸 수 있는 형태로 강제한다.

본 PR(R2)에서는 *위치만 통합*한다. 의미(허용 범위)는 그대로다 — 한국어·공백·
하이픈·숫자 시작은 계속 reject.
"""

import re


SAFE_SQL_IDENTIFIER_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def is_safe_sql_identifier(name: object) -> bool:
    """``name``이 SAFE_SQL_IDENTIFIER_RE에 매치되면 True. 비-문자열은 False."""

    if not isinstance(name, str):
        return False
    return bool(SAFE_SQL_IDENTIFIER_RE.match(name))


__all__ = ["SAFE_SQL_IDENTIFIER_RE", "is_safe_sql_identifier"]
