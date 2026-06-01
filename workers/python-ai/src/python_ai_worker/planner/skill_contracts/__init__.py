"""SkillContract 모음 (validator R5 pilot, 2026-05-27).

audit §5 (validator_contract_refactor_audit_2026-05-27.md)에서 제안한
SkillContract Protocol의 *pilot* 도입. 본 단계에서는 ``present`` skill 하나
만 contract 구조로 전환한다 — 구조가 작동하는지, 다른 skill로 확장 가능
한지 검증하는 게 목적.

R5 pilot 정책:
- validator behavior 변경 없음
- 기존 issue code 유지
- executor builder 흡수 없음 (R6 후속)
- 다른 skill (filter/join/aggregate/compare/calculate/sort/summarize)은
  ``planner/validator.py``의 ``_validate_X`` 함수 그대로 사용

전체 전환은 ADR 승격 후 별도 PR로 진행.
"""

from .base import SkillContract
from .present import PresentSkillContract
from .registry import CONTRACTS
from .sort import SortSkillContract

__all__ = ["CONTRACTS", "PresentSkillContract", "SkillContract", "SortSkillContract"]
