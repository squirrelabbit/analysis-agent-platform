"""executor skill SQL builders.

각 skill 모듈은 ``build_sql(params, context) -> (sql, extra)`` 함수를 제공한다.
``runner.py``는 ``SKILL_BUILDERS`` dispatch table만 호출하고 SQL 본문은 보지 않는다.
"""

from typing import Any, Callable, TYPE_CHECKING

from . import aggregate as _aggregate
from . import calculate as _calculate
from . import compare as _compare
from . import filter as _filter
from . import join as _join
from . import present as _present
from . import sort as _sort
from .base import ExecutorError

if TYPE_CHECKING:
    from ..context import ExecutorContext


SkillBuilder = Callable[[dict[str, Any], "ExecutorContext"], "tuple[str, dict[str, Any]]"]


SKILL_BUILDERS: dict[str, SkillBuilder] = {
    "join": _join.build_sql,
    "filter": _filter.build_sql,
    "aggregate": _aggregate.build_sql,
    "compare": _compare.build_sql,
    "calculate": _calculate.build_sql,
    "sort": _sort.build_sql,
    "present": _present.build_sql,
}


__all__ = ["ExecutorError", "SKILL_BUILDERS", "SkillBuilder"]
