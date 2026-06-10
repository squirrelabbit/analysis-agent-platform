"""validator schema-lineage (silverone 2026-06-09).

step input의 기여 reserved table을 추적해, join 안 된 다른 table 전유 컬럼을
참조하면 plan 단계에서 params.column_not_in_input으로 잡는다(executor DuckDB
Binder Error → planner self-correct 가능한 issue로 승격). docs dynamic 컬럼/
공유 컬럼(doc_id 등)은 false positive 방지로 건드리지 않는다.
"""

from __future__ import annotations

import unittest

from python_ai_worker.planner.validator import collect_plan_issues


def _plan(*steps):
    return {"plan_version": "v2", "steps": list(steps)}


def _join(step_id, left, right):
    return {"id": step_id, "skill": "join",
            "params": {"left": left, "right": right, "on": ["doc_id"], "how": "inner"}}


def _codes(plan):
    return [i.code for i in collect_plan_issues(plan)]


class CrossTableLineageTests(unittest.TestCase):
    def test_filter_genuineness_on_docs_clauses_join_flagged(self):
        # 실제 회귀: join(docs,clauses) 후 filter(genuineness ...) — genuineness 미join.
        plan = _plan(
            _join("joined", "clauses", "docs"),
            {"id": "f", "skill": "filter",
             "params": {"input": "joined", "column": "genuineness", "operator": "neq", "value": "non_review"}},
            {"id": "p", "skill": "present", "params": {"input": "f", "columns": ["clause"]}},
        )
        self.assertIn("params.column_not_in_input", _codes(plan))

    def test_genuineness_joined_in_passes(self):
        # genuineness까지 join하면 통과(positive).
        plan = _plan(
            _join("dc", "clauses", "docs"),
            _join("dcg", "dc", "genuineness"),
            {"id": "f", "skill": "filter",
             "params": {"input": "dcg", "column": "genuineness", "operator": "neq", "value": "non_review"}},
            {"id": "p", "skill": "present", "params": {"input": "f", "columns": ["clause"]}},
        )
        self.assertNotIn("params.column_not_in_input", _codes(plan))

    def test_aggregate_sentiment_on_docs_only_flagged(self):
        # docs만 거친 step에 clause-level group_by → 잡힘.
        plan = _plan(
            {"id": "fd", "skill": "filter",
             "params": {"input": "docs", "column": "created_at", "operator": "between", "value": ["2025-08-01", "2025-08-31"]}},
            {"id": "agg", "skill": "aggregate",
             "params": {"input": "fd", "group_by": ["sentiment"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}]}},
            {"id": "p", "skill": "present", "params": {"input": "agg", "columns": ["sentiment", "count"]}},
        )
        self.assertIn("params.column_not_in_input", _codes(plan))

    def test_docs_dynamic_column_not_false_positive(self):
        # channel 같은 docs 추가(dynamic) 컬럼은 _COLUMN_OWNER에 없어 건드리지 않는다.
        plan = _plan(
            {"id": "fd", "skill": "filter",
             "params": {"input": "docs", "column": "created_at", "operator": "between", "value": ["2025-08-01", "2025-08-31"]}},
            {"id": "agg", "skill": "aggregate",
             "params": {"input": "fd", "group_by": ["channel"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}]}},
            {"id": "p", "skill": "present", "params": {"input": "agg", "columns": ["channel", "count"]}},
        )
        self.assertNotIn("params.column_not_in_input", _codes(plan))

    def test_shared_doc_id_not_false_positive(self):
        # doc_id는 세 table 공유 컬럼이라 ownership 없음 → 어떤 lineage에서도 통과.
        plan = _plan(
            {"id": "fd", "skill": "filter",
             "params": {"input": "docs", "column": "doc_id", "operator": "neq", "value": "x"}},
            {"id": "p", "skill": "present", "params": {"input": "fd", "columns": ["doc_id"]}},
        )
        self.assertNotIn("params.column_not_in_input", _codes(plan))

    def test_reserved_table_direct_input_untouched(self):
        # reserved table 직접 input은 기존 _check_columns_on_table가 담당(여기선 무이슈).
        plan = _plan(
            {"id": "agg", "skill": "aggregate",
             "params": {"input": "clauses", "group_by": ["sentiment"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}]}},
            {"id": "p", "skill": "present", "params": {"input": "agg", "columns": ["sentiment", "count"]}},
        )
        self.assertNotIn("params.column_not_in_input", _codes(plan))

    def test_clause_keywords_provides_sentiment_aspect(self):
        # silverone 2026-06-10 — clause_keywords는 sentiment/aspect/clause를 비정규화해
        # 보유한다(clauses 파생). filter(input=clause_keywords, column=sentiment/aspect)는
        # 정당하므로 lineage가 오탐(column_not_in_input)으로 잡으면 안 된다.
        plan = _plan(
            {"id": "fk", "skill": "filter",
             "params": {"input": "clause_keywords", "column": "aspect", "operator": "eq", "value": "food"}},
            {"id": "fk2", "skill": "filter",
             "params": {"input": "fk", "column": "sentiment", "operator": "eq", "value": "negative"}},
            {"id": "agg", "skill": "aggregate",
             "params": {"input": "fk2", "group_by": ["keyword"],
                        "metrics": [{"name": "count", "function": "count", "column": "*"}]}},
            {"id": "p", "skill": "present", "params": {"input": "agg", "columns": ["keyword", "count"]}},
        )
        self.assertNotIn("params.column_not_in_input", _codes(plan))


if __name__ == "__main__":
    unittest.main()
