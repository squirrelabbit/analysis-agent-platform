"""validator R5 pilot (2026-05-27) — SkillContract 구조 잠금.

R5 (present pilot) → R5-sort (sort 추가). 현재 등록된 skill: present, sort.
구체 validation 회귀는 ``test_planner_validator.py``의 SqlContractTests +
R10 / R9 등에 의존하고, 여기서는 contract 구조 자체가 작동하는지만 잠근다.
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner.skill_contracts import (
    CONTRACTS,
    PresentSkillContract,
    SkillContract,
    SortSkillContract,
)


class PresentSkillContractPilotTests(unittest.TestCase):
    def test_present_contract_registered(self) -> None:
        self.assertIn("present", CONTRACTS)
        self.assertIsInstance(CONTRACTS["present"], PresentSkillContract)

    def test_present_contract_name_matches_skill_catalog_key(self) -> None:
        self.assertEqual(CONTRACTS["present"].name, "present")

    def test_present_contract_implements_protocol(self) -> None:
        # runtime_checkable Protocol — duck-type 검증.
        self.assertIsInstance(CONTRACTS["present"], SkillContract)

    def test_present_contract_infer_returns_none(self) -> None:
        # present는 chain 끝 step이라 후행 검증 없음 — None 반환이 의도.
        result = CONTRACTS["present"].infer_output_columns(
            {"input": "any_step", "format": "table"},
            upstream=lambda _ref: None,
        )
        self.assertIsNone(result)


class SortSkillContractPilotTests(unittest.TestCase):
    def test_sort_contract_registered(self) -> None:
        self.assertIn("sort", CONTRACTS)
        self.assertIsInstance(CONTRACTS["sort"], SortSkillContract)

    def test_sort_contract_name_matches_skill_catalog_key(self) -> None:
        self.assertEqual(CONTRACTS["sort"].name, "sort")

    def test_sort_contract_implements_protocol(self) -> None:
        self.assertIsInstance(CONTRACTS["sort"], SkillContract)

    def test_sort_contract_infer_passes_through_upstream(self) -> None:
        # sort는 row 순서만 바꾸므로 output columns == input columns.
        result = CONTRACTS["sort"].infer_output_columns(
            {"input": "ref", "by": ["aspect"], "order": "desc"},
            upstream=lambda ref: {"aspect", "count"},
        )
        self.assertEqual(result, {"aspect", "count"})

    def test_sort_contract_infer_returns_none_when_upstream_unknown(self) -> None:
        result = CONTRACTS["sort"].infer_output_columns(
            {"input": "ref", "by": ["aspect"]},
            upstream=lambda ref: None,
        )
        self.assertIsNone(result)

    def test_sort_contract_infer_returns_none_for_empty_input(self) -> None:
        # input_ref 누락 시 None — _check_input_ref가 별도로 issue 발생시킨다.
        result = CONTRACTS["sort"].infer_output_columns(
            {"by": ["aspect"]},
            upstream=lambda ref: {"aspect"},
        )
        self.assertIsNone(result)


class SkillContractRegistryScopeTests(unittest.TestCase):
    def test_registry_scope(self) -> None:
        # R5-sort 시점 등록 skill. 다른 skill 추가는 의도적 PR이 필요.
        self.assertEqual(set(CONTRACTS.keys()), {"present", "sort"})


if __name__ == "__main__":
    unittest.main()
