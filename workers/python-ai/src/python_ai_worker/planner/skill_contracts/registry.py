from __future__ import annotations

"""SkillContract registry — pilot으로 등록된 skill만 포함한다 (validator R5).

R5 (present pilot 2026-05-27) → R5-sort (sort 추가 2026-05-27).
다른 5 skill(filter/join/aggregate/compare/calculate/summarize)은 옛
``planner.validator._validate_X`` 그대로 사용한다 — validator dispatch가
contract registry lookup miss 시 옛 분기로 떨어진다.
"""

from typing import Mapping

from .base import SkillContract
from .present import PresentSkillContract
from .sort import SortSkillContract


CONTRACTS: Mapping[str, SkillContract] = {
    "present": PresentSkillContract(),
    "sort": SortSkillContract(),
}


__all__ = ["CONTRACTS"]
