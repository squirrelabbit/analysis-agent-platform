from __future__ import annotations

"""SkillContract Protocol — skill별 validate/infer_output_columns 단일 owner.

validator R5 pilot (2026-05-27). 각 skill의 ``validate``와
``infer_output_columns``를 한 클래스에 묶어 정합성을 보장한다.

ctx 타입은 ``planner.validator._StepContext`` — cycle 회피를 위해 TYPE_CHECKING
forward reference로만 노출. 구현체는 ``ctx.issue(code, message)``만 호출하면
된다 (인터페이스는 validator helper와 동일).
"""

from typing import TYPE_CHECKING, Any, Callable, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..validator import _StepContext


@runtime_checkable
class SkillContract(Protocol):
    """skill별 validate/infer 책임을 한 곳에 두는 contract.

    ``name``: ``SKILL_CATALOG``의 키와 동일해야 한다. ``validate``는 validator
    가 단계별로 호출하고, ``infer_output_columns``는 후행 step의 column 존재
    검증에 사용된다.
    """

    name: str

    def validate(self, params: dict[str, Any], ctx: "_StepContext") -> None: ...

    def infer_output_columns(
        self,
        params: dict[str, Any],
        upstream: Callable[[str], "set[str] | None"],
    ) -> "set[str] | None": ...


__all__ = ["SkillContract"]
