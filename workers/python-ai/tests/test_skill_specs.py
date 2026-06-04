"""Skill Contract v2 Step 0 — spec이 현 SKILL_CATALOG / validator 상수를 충실히
재현함을 잠근다 (silverone 2026-06-04).

행동 변화 0. 이 테스트는 두 가지를 증명한다:
1. render_params_schema(spec) == 현 SKILL_CATALOG[...].params_schema
   → Step 2에서 SKILL_CATALOG를 생성기로 교체해도 planner prompt 출력 불변.
2. spec.operations(구조적 사실)가 validator의 기존 상수와 일치
   → Step 1에서 validator를 spec-driven으로 옮길 안전 토대.
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner.schema import CALCULATE_OPERATIONS, SKILL_CATALOG
from python_ai_worker.planner.skill_specs import (
    CALCULATE_SPEC,
    PRESENT_SPEC,
    SPECS,
    render_params_schema,
)
from python_ai_worker.planner.validator import _CALCULATE_OP_REQUIRED_KEYS


class ParamsSchemaEquivalenceTests(unittest.TestCase):
    """생성된 params_schema가 현 SKILL_CATALOG와 byte 동일 (prompt 출력 불변 보장)."""

    def test_calculate_params_schema_matches_catalog(self) -> None:
        self.assertEqual(
            render_params_schema(CALCULATE_SPEC),
            SKILL_CATALOG["calculate"].params_schema,
        )

    def test_present_params_schema_matches_catalog(self) -> None:
        self.assertEqual(
            render_params_schema(PRESENT_SPEC),
            SKILL_CATALOG["present"].params_schema,
        )

    def test_spec_skill_metadata_matches_catalog(self) -> None:
        # description / input_type / output_type 도 SKILL_CATALOG와 일치해야
        # spec이 단일 source로 SKILL_CATALOG 항목을 대체할 수 있다.
        for name, spec in SPECS.items():
            catalog = SKILL_CATALOG[name]
            with self.subTest(skill=name):
                self.assertEqual(spec.name, catalog.name)
                self.assertEqual(spec.description, catalog.description)
                self.assertEqual(spec.input_type, catalog.input_type)
                self.assertEqual(spec.output_type, catalog.output_type)

    def test_present_param_names_match_catalog_keys(self) -> None:
        spec_keys = [p.name for p in PRESENT_SPEC.params]
        self.assertEqual(spec_keys, list(SKILL_CATALOG["present"].params_schema.keys()))


class CalculateOperationFidelityTests(unittest.TestCase):
    """spec.operations가 validator 기존 상수와 일치 (Step 1 토대)."""

    def test_operation_names_match_enum(self) -> None:
        spec_ops = {op.name for op in CALCULATE_SPEC.operations}
        self.assertEqual(spec_ops, set(CALCULATE_OPERATIONS))

    def test_required_keys_match_validator_constant(self) -> None:
        # _CALCULATE_OP_REQUIRED_KEYS에 있는 operation은 spec.required와 동일.
        by_name = {op.name: op for op in CALCULATE_SPEC.operations}
        for op_name, required in _CALCULATE_OP_REQUIRED_KEYS.items():
            with self.subTest(operation=op_name):
                self.assertIn(op_name, by_name)
                self.assertEqual(by_name[op_name].required, tuple(required))

    def test_ratio_uses_alt_required_not_in_constant(self) -> None:
        # ratio는 validator에서 특수 처리(둘 중 하나) → 상수에 없고 alt_required로 표현.
        self.assertNotIn("ratio", _CALCULATE_OP_REQUIRED_KEYS)
        ratio = next(op for op in CALCULATE_SPEC.operations if op.name == "ratio")
        self.assertEqual(
            ratio.alt_required,
            (("numerator", "denominator"), ("left", "right")),
        )
        self.assertEqual(ratio.required, ())

    def test_share_of_total_structured_facts(self) -> None:
        op = next(op for op in CALCULATE_SPEC.operations if op.name == "share_of_total")
        self.assertEqual(op.required, ("value",))
        self.assertEqual(op.column_refs, ("partition_by",))
        self.assertEqual(op.unit, "fraction_0_1")


if __name__ == "__main__":
    unittest.main()
