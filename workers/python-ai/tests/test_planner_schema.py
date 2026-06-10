"""plan_v2 schema lock test — schema/skill catalog 변경 시 의도적인 PR 필요.

silverone 2026-05-21 결정 (multi-table input, 8 skill, reserved name)을 잠근다.
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner import (
    PLAN_VERSION,
    RESERVED_INPUT_NAMES,
    SKILL_CATALOG,
    TABLE_SCHEMAS,
)
from python_ai_worker.planner.schema import (
    AGGREGATE_FUNCTIONS,
    CALCULATE_OPERATIONS,
    FILTER_OPERATORS,
    JOIN_HOWS,
    NUMERIC_COLUMN_TYPES,
    PRESENT_FORMATS,
    RESERVED_COLUMN_TYPES,
    RESERVED_STRING_COLUMNS,
    SORT_ORDERS,
    TEXT_COLUMN_TYPES,
    TIMESTAMP_COLUMN_TYPES,
)


class PlanV2VersionAndReservedNamesTests(unittest.TestCase):
    def test_plan_version_is_v2(self) -> None:
        self.assertEqual(PLAN_VERSION, "v2")

    def test_reserved_input_names_locked(self) -> None:
        # silverone 2026-06-10 — clause_keywords(optional artifact) 예약 추가.
        self.assertEqual(
            RESERVED_INPUT_NAMES,
            frozenset({"docs", "clauses", "genuineness", "clause_keywords"}),
        )


class PlanV2TableSchemaTests(unittest.TestCase):
    def test_table_schemas_locked(self) -> None:
        # silverone 2026-06-10 — clause_keywords(optional) 추가. core 3 + optional 1.
        self.assertEqual(
            set(TABLE_SCHEMAS.keys()),
            {"docs", "clauses", "genuineness", "clause_keywords"},
        )

    def test_table_names_match_reserved_names(self) -> None:
        self.assertEqual(set(TABLE_SCHEMAS.keys()), set(RESERVED_INPUT_NAMES))

    def test_docs_invariant_columns(self) -> None:
        docs = TABLE_SCHEMAS["docs"]
        column_names = [c.name for c in docs.columns]
        self.assertEqual(
            column_names,
            ["doc_id", "row_id", "raw_text", "cleaned_text", "created_at"],
        )
        self.assertTrue(docs.dynamic_columns, "docs는 dataset별 원본 컬럼이 추가됨")

    def test_clauses_columns_include_clause_id(self) -> None:
        # 5/21 (silverone): clause_label artifact lock에는 clause_id가 없지만
        # plan_v2에서는 executor가 적재 시 row 식별자를 생성하는 전제로 추가.
        # clause 단위 count / evidence trace / UI drill-down에 필요.
        clauses = TABLE_SCHEMAS["clauses"]
        column_names = [c.name for c in clauses.columns]
        self.assertEqual(
            column_names,
            ["doc_id", "clause_id", "clause", "sentiment", "aspect", "prompt_version", "source"],
        )
        self.assertFalse(clauses.dynamic_columns)

    def test_genuineness_locked_to_doc_genuineness_v1(self) -> None:
        genuineness = TABLE_SCHEMAS["genuineness"]
        column_names = [c.name for c in genuineness.columns]
        self.assertEqual(
            column_names,
            ["doc_id", "genuineness", "reason", "prompt_version", "source"],
        )
        self.assertFalse(genuineness.dynamic_columns)

    def test_doc_id_invariant_across_three_tables(self) -> None:
        for table_name in ("docs", "clauses", "genuineness"):
            with self.subTest(table=table_name):
                column_names = {c.name for c in TABLE_SCHEMAS[table_name].columns}
                self.assertIn("doc_id", column_names, f"{table_name}에 doc_id 컬럼 필요 — join key")


class PlanV2SkillCatalogTests(unittest.TestCase):
    def test_skill_catalog_exactly_eight(self) -> None:
        self.assertEqual(
            set(SKILL_CATALOG.keys()),
            {"join", "filter", "aggregate", "compare", "calculate", "sort", "present", "summarize"},
        )

    def test_skill_names_match_keys(self) -> None:
        for key, spec in SKILL_CATALOG.items():
            with self.subTest(skill=key):
                self.assertEqual(spec.name, key)

    def test_join_params(self) -> None:
        params = SKILL_CATALOG["join"].params_schema
        self.assertEqual(set(params.keys()), {"left", "right", "on", "how"})
        self.assertEqual(SKILL_CATALOG["join"].input_type, "table_pair")
        self.assertEqual(SKILL_CATALOG["join"].output_type, "table")

    def test_filter_params(self) -> None:
        params = SKILL_CATALOG["filter"].params_schema
        self.assertEqual(set(params.keys()), {"input", "column", "operator", "value"})
        self.assertEqual(SKILL_CATALOG["filter"].input_type, "table")

    def test_aggregate_params(self) -> None:
        params = SKILL_CATALOG["aggregate"].params_schema
        self.assertEqual(set(params.keys()), {"input", "group_by", "metrics"})

    def test_compare_params(self) -> None:
        params = SKILL_CATALOG["compare"].params_schema
        self.assertEqual(
            set(params.keys()), {"left", "right", "join_key", "left_label", "right_label"}
        )
        self.assertEqual(SKILL_CATALOG["compare"].input_type, "table_pair")

    def test_calculate_params(self) -> None:
        params = SKILL_CATALOG["calculate"].params_schema
        self.assertEqual(set(params.keys()), {"input", "expressions"})

    def test_sort_params(self) -> None:
        params = SKILL_CATALOG["sort"].params_schema
        self.assertEqual(set(params.keys()), {"input", "by", "order", "limit"})

    def test_present_params_and_output(self) -> None:
        params = SKILL_CATALOG["present"].params_schema
        # silverone 2026-06-02 — columns 추가 (present.columns hard constraint).
        self.assertEqual(set(params.keys()), {"input", "format", "title", "columns", "limit"})
        self.assertEqual(SKILL_CATALOG["present"].output_type, "presentation")

    def test_summarize_params_and_output(self) -> None:
        params = SKILL_CATALOG["summarize"].params_schema
        self.assertEqual(set(params.keys()), {"input", "focus", "prompt_version"})
        self.assertEqual(SKILL_CATALOG["summarize"].output_type, "text")


class PlanV2SkillParamEnumTests(unittest.TestCase):
    """validator R4-A (2026-05-27) — skill param enum 단일 source 잠금.
    enum frozenset이 schema.py에 있고, SKILL_CATALOG.params_schema string에
    그 enum 값이 모두 등장하는지 검증. params_schema string의 ordering은
    prompt cache hit에 영향을 주므로 hand-written 그대로 보존."""

    def test_enum_values_locked_at_audit_time(self) -> None:
        # 2026-05-27 R4-A 도입 시점 값 — 변경 시 의도적인 PR 필요.
        self.assertEqual(
            FILTER_OPERATORS,
            frozenset({
                "eq", "neq", "in", "not_in", "gt", "gte", "lt", "lte",
                "between", "contains", "is_null", "not_null",
            }),
        )
        self.assertEqual(JOIN_HOWS, frozenset({"inner", "left", "right", "outer"}))
        self.assertEqual(AGGREGATE_FUNCTIONS, frozenset({"count", "sum", "avg", "min", "max"}))
        self.assertEqual(
            CALCULATE_OPERATIONS,
            # silverone 2026-06-02 — share_of_total 추가 (그룹별 전체 대비 비중).
            frozenset(
                {"add", "subtract", "multiply", "divide", "percent_change", "ratio", "share_of_total"}
            ),
        )
        self.assertEqual(SORT_ORDERS, frozenset({"asc", "desc"}))
        self.assertEqual(PRESENT_FORMATS, frozenset({"table", "chart", "json"}))

    def test_filter_operators_all_appear_in_params_schema(self) -> None:
        spec_text = SKILL_CATALOG["filter"].params_schema["operator"]
        for op in FILTER_OPERATORS:
            self.assertIn(op, spec_text, f"filter.operator enum '{op}' missing from params_schema")

    def test_join_hows_all_appear_in_params_schema(self) -> None:
        spec_text = SKILL_CATALOG["join"].params_schema["how"]
        for how in JOIN_HOWS:
            self.assertIn(how, spec_text)

    def test_aggregate_functions_all_appear_in_params_schema(self) -> None:
        # function enum은 metrics description 안에 들어 있다.
        spec_text = SKILL_CATALOG["aggregate"].params_schema["metrics"]
        for fn in AGGREGATE_FUNCTIONS:
            self.assertIn(fn, spec_text)

    def test_calculate_operations_all_appear_in_params_schema(self) -> None:
        spec_text = SKILL_CATALOG["calculate"].params_schema["expressions"]
        for op in CALCULATE_OPERATIONS:
            self.assertIn(op, spec_text)

    def test_sort_orders_all_appear_in_params_schema(self) -> None:
        spec_text = SKILL_CATALOG["sort"].params_schema["order"]
        for order in SORT_ORDERS:
            self.assertIn(order, spec_text)

    def test_present_formats_all_appear_in_params_schema(self) -> None:
        spec_text = SKILL_CATALOG["present"].params_schema["format"]
        for fmt in PRESENT_FORMATS:
            self.assertIn(fmt, spec_text)


class PlanV2ClausesAspectTaxonomyDerivedTests(unittest.TestCase):
    """taxonomy-driven config Phase 3-A (silverone 2026-05-27) —
    ``clauses.aspect`` ColumnSpec.description이 config/taxonomies/festival-v2.json
    에서 derive되는지 잠금. Phase 2-A에서 clause_label의 _ALLOWED_ASPECT를
    옮긴 것에 이어, planner 측 schema description도 single source.
    """

    def setUp(self) -> None:
        from python_ai_worker.taxonomies import load_taxonomy

        self.taxonomy = load_taxonomy("festival-v2")
        self.aspect_column = next(
            c for c in TABLE_SCHEMAS["clauses"].columns if c.name == "aspect"
        )

    def test_description_matches_pipe_joined_taxonomy_keys(self) -> None:
        expected = " | ".join(self.taxonomy.aspect_keys)
        self.assertEqual(self.aspect_column.description, expected)

    def test_description_contains_all_9_keys(self) -> None:
        for key in self.taxonomy.aspect_keys:
            self.assertIn(key, self.aspect_column.description)

    def test_description_excludes_deprecated_aspect_keys(self) -> None:
        # 옛 7-aspect 중 retire된 키들이 planner schema에 남지 않아야 함.
        for deprecated in ("atmosphere", "contents", "convenience", "value", "overall"):
            self.assertNotIn(
                deprecated,
                self.aspect_column.description,
                f"deprecated aspect '{deprecated}' present in planner schema description",
            )

    def test_description_includes_new_aspect_keys(self) -> None:
        # taxonomy-driven 도입의 핵심 — 새 aspect가 planner LLM에 노출됨.
        for key in ("show_program", "ambiance_scenery", "facility_crowd"):
            self.assertIn(key, self.aspect_column.description)


class PlanV2EnVariantAspectTaxonomySyncTests(unittest.TestCase):
    """en variant prompt(planner-v2-anthropic-en-v1.md)의 aspect enum line이
    festival-v2 taxonomy와 정합한지 잠금. en variant는 experimental이라 runtime
    render 안 함 — Phase 3-A에서는 *hand-coded 일치만* 잠금.
    Phase 4 또는 별도 PR에서 en variant도 placeholder + render path로 전환 가능.
    """

    def setUp(self) -> None:
        from pathlib import Path

        from python_ai_worker.taxonomies import load_taxonomy

        self.taxonomy = load_taxonomy("festival-v2")
        # repo root: workers/python-ai/tests/test_planner_schema.py
        #         → workers/python-ai/tests → workers/python-ai → workers → <repo root>
        repo_root = Path(__file__).resolve().parents[3]
        self.en_md = (
            repo_root / "config" / "prompts" / "planner-v2-anthropic-en-v1.md"
        ).read_text(encoding="utf-8")

    def test_en_variant_contains_all_9_aspect_keys(self) -> None:
        for key in self.taxonomy.aspect_keys:
            self.assertIn(key, self.en_md, f"en variant missing aspect key '{key}'")

    def test_en_variant_excludes_deprecated_aspect_keys(self) -> None:
        # markdown table cell delimiter로 검색 — 옛 키가 자유 본문에 우연히 들어
        # 있는 경우는 false positive 방지 (현재 prompt 본문에 없음).
        for deprecated in ("atmosphere", "convenience", "value", "overall"):
            self.assertNotIn(
                f" {deprecated} ",
                self.en_md,
                f"deprecated aspect '{deprecated}' present in en variant",
            )


class PlanV2ColumnTypeClassificationTests(unittest.TestCase):
    """validator R3 (2026-05-27) — column type 분류 상수가 TABLE_SCHEMAS에서
    derive되고 의미가 보존되는지 잠금. 옛 위치는 validator.py module-level
    상수였고 schema.py로 이전. 의미(어떤 type이 numeric/timestamp/text 계열
    인지)는 변경 없음."""

    def test_reserved_column_types_derived_from_table_schemas(self) -> None:
        # 매뉴얼 중복 map이 아니라 TABLE_SCHEMAS를 그대로 펼친 형태여야 한다.
        for table_name, table in TABLE_SCHEMAS.items():
            self.assertIn(table_name, RESERVED_COLUMN_TYPES)
            for col in table.columns:
                self.assertEqual(
                    RESERVED_COLUMN_TYPES[table_name][col.name],
                    col.type,
                    f"{table_name}.{col.name} type mismatch with TABLE_SCHEMAS",
                )

    def test_reserved_column_tables_cover_all_schemas(self) -> None:
        # silverone 2026-06-10 — clause_keywords(optional) 포함.
        self.assertEqual(
            set(RESERVED_COLUMN_TYPES.keys()),
            {"docs", "clauses", "genuineness", "clause_keywords"},
        )

    def test_docs_created_at_classified_as_timestamp(self) -> None:
        col_type = RESERVED_COLUMN_TYPES["docs"]["created_at"]
        self.assertIn(col_type, TIMESTAMP_COLUMN_TYPES)
        self.assertNotIn(col_type, NUMERIC_COLUMN_TYPES)
        self.assertNotIn(col_type, TEXT_COLUMN_TYPES)

    def test_clauses_sentiment_classified_as_text(self) -> None:
        # SQL-3.2 (R7) — aggregate sum/avg가 sentiment 같은 string column을
        # reject하려면 type이 TEXT 계열로 분류돼야 한다.
        col_type = RESERVED_COLUMN_TYPES["clauses"]["sentiment"]
        self.assertIn(col_type, TEXT_COLUMN_TYPES)
        self.assertNotIn(col_type, NUMERIC_COLUMN_TYPES)
        self.assertNotIn(col_type, TIMESTAMP_COLUMN_TYPES)

    def test_reserved_string_columns_for_clauses_include_label_columns(self) -> None:
        # SQL-2.3 — calculate expression이 RESERVED string column을 참조하면
        # reject. clauses의 sentiment/aspect/clause는 string columns에 포함돼야 한다.
        cols = RESERVED_STRING_COLUMNS["clauses"]
        self.assertIn("sentiment", cols)
        self.assertIn("aspect", cols)
        self.assertIn("clause", cols)
        # created_at은 timestamp라 string set에 들어가면 안 된다.
        self.assertNotIn("created_at", RESERVED_STRING_COLUMNS.get("docs", frozenset()))

    def test_type_classifications_are_disjoint(self) -> None:
        # numeric/timestamp/text 분류는 raw type 이름 수준에서 겹치지 않아야 한다.
        self.assertFalse(NUMERIC_COLUMN_TYPES & TIMESTAMP_COLUMN_TYPES)
        self.assertFalse(NUMERIC_COLUMN_TYPES & TEXT_COLUMN_TYPES)
        self.assertFalse(TIMESTAMP_COLUMN_TYPES & TEXT_COLUMN_TYPES)


if __name__ == "__main__":
    unittest.main()
