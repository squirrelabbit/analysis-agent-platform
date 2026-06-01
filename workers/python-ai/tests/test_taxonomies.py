"""Taxonomy config loader 잠금 test (Phase 1, 2026-05-27).

config/taxonomies/festival-v2.json + loader 검증. clause_label / planner /
artifact 연동(Phase 2~4)은 아직 안 함 — 본 test는 config 형식만 잠근다.
"""

from __future__ import annotations

import json
import unittest

from python_ai_worker.taxonomies import (
    AspectSpec,
    Taxonomy,
    TaxonomyError,
    TaxonomyMismatchError,
    check_taxonomy_compatibility,
    load_taxonomy,
    parse_taxonomy,
    render_aspect_taxonomy_block,
    render_sentiment_taxonomy_block,
)


_EXPECTED_FESTIVAL_V2_KEYS = (
    "show_program",
    "experience_booth",
    "ambiance_scenery",
    "food",
    "price_cost",
    "facility_crowd",
    "access_traffic",
    "operation_service",
    "etc",
)


def _minimal_valid_dict() -> dict:
    """검증 가능한 최소 taxonomy dict. 각 항목별 reject test의 baseline."""

    return {
        "taxonomy_id": "test-x",
        "domain": "test",
        "aspects": [
            {"key": "a", "label": "A", "description": "A desc"},
            {"key": "etc", "label": "기타", "description": "기타"},
        ],
        "sentiments": ["positive", "negative", "neutral"],
        "fallback_aspect": "etc",
    }


class FestivalV2LoadTests(unittest.TestCase):
    """config/taxonomies/festival-v2.json 실제 file load 검증."""

    def setUp(self) -> None:
        self.taxonomy = load_taxonomy("festival-v2")

    def test_load_success_returns_taxonomy(self) -> None:
        self.assertIsInstance(self.taxonomy, Taxonomy)
        self.assertEqual(self.taxonomy.taxonomy_id, "festival-v2")
        self.assertEqual(self.taxonomy.domain, "festival")

    def test_aspect_keys_match_9_taxonomy(self) -> None:
        self.assertEqual(self.taxonomy.aspect_keys, _EXPECTED_FESTIVAL_V2_KEYS)

    def test_aspect_keys_set_helper(self) -> None:
        self.assertEqual(
            self.taxonomy.aspect_keys_set,
            frozenset(_EXPECTED_FESTIVAL_V2_KEYS),
        )

    def test_fallback_aspect_is_etc(self) -> None:
        self.assertEqual(self.taxonomy.fallback_aspect, "etc")
        self.assertIn(self.taxonomy.fallback_aspect, self.taxonomy.aspect_keys_set)

    def test_sentiments_three(self) -> None:
        self.assertEqual(
            self.taxonomy.sentiments,
            ("positive", "negative", "neutral"),
        )

    def test_aspect_label_and_description_non_empty(self) -> None:
        for aspect in self.taxonomy.aspects:
            self.assertIsInstance(aspect, AspectSpec)
            self.assertTrue(aspect.key)
            self.assertTrue(aspect.label, f"empty label for aspect '{aspect.key}'")
            self.assertTrue(
                aspect.description, f"empty description for aspect '{aspect.key}'"
            )

    def test_taxonomy_hash_stable(self) -> None:
        # 같은 file을 두 번 load해도 hash 동일.
        a = load_taxonomy("festival-v2").taxonomy_hash
        b = load_taxonomy("festival-v2").taxonomy_hash
        self.assertEqual(a, b)
        # sha256 hex digest 길이.
        self.assertEqual(len(self.taxonomy.taxonomy_hash), 64)


class ParseTaxonomyValidationTests(unittest.TestCase):
    """parse_taxonomy reject 분기 잠금."""

    def _parse(self, data: dict, *, expected_id: str | None = None) -> Taxonomy:
        return parse_taxonomy(json.dumps(data), expected_id=expected_id)

    def test_minimal_valid_dict_parses(self) -> None:
        tax = self._parse(_minimal_valid_dict())
        self.assertEqual(tax.aspect_keys, ("a", "etc"))

    def test_duplicate_aspect_key_rejected(self) -> None:
        data = _minimal_valid_dict()
        data["aspects"].append({"key": "a", "label": "dup", "description": "dup"})
        with self.assertRaises(TaxonomyError) as cm:
            self._parse(data)
        self.assertIn("duplicated", str(cm.exception))

    def test_unknown_fallback_aspect_rejected(self) -> None:
        data = _minimal_valid_dict()
        data["fallback_aspect"] = "unknown"
        with self.assertRaises(TaxonomyError) as cm:
            self._parse(data)
        self.assertIn("fallback_aspect", str(cm.exception))

    def test_missing_required_field_rejected(self) -> None:
        for missing_key in (
            "taxonomy_id",
            "domain",
            "aspects",
            "sentiments",
            "fallback_aspect",
        ):
            data = _minimal_valid_dict()
            del data[missing_key]
            with self.assertRaises(TaxonomyError) as cm:
                self._parse(data)
            self.assertIn(
                missing_key,
                str(cm.exception),
                f"expected '{missing_key}' in error message",
            )

    def test_empty_aspects_rejected(self) -> None:
        data = _minimal_valid_dict()
        data["aspects"] = []
        with self.assertRaises(TaxonomyError):
            self._parse(data)

    def test_duplicate_sentiment_rejected(self) -> None:
        data = _minimal_valid_dict()
        data["sentiments"] = ["positive", "positive", "neutral"]
        with self.assertRaises(TaxonomyError) as cm:
            self._parse(data)
        self.assertIn("duplicated", str(cm.exception))

    def test_taxonomy_id_mismatch_rejected(self) -> None:
        data = _minimal_valid_dict()
        with self.assertRaises(TaxonomyError) as cm:
            self._parse(data, expected_id="other-id")
        self.assertIn("mismatch", str(cm.exception))

    def test_non_object_rejected(self) -> None:
        with self.assertRaises(TaxonomyError):
            parse_taxonomy("[]")

    def test_invalid_json_rejected(self) -> None:
        with self.assertRaises(TaxonomyError) as cm:
            parse_taxonomy("{ not json }")
        self.assertIn("parse error", str(cm.exception))

    def test_aspect_missing_label_rejected(self) -> None:
        data = _minimal_valid_dict()
        del data["aspects"][0]["label"]
        with self.assertRaises(TaxonomyError) as cm:
            self._parse(data)
        self.assertIn("label", str(cm.exception))


class RenderTaxonomyBlockTests(unittest.TestCase):
    """Phase 2-B (2026-05-27) — taxonomy를 prompt placeholder로 inject할 수
    있는 markdown block render 잠금."""

    def setUp(self) -> None:
        self.taxonomy = load_taxonomy("festival-v2")

    def test_aspect_block_contains_all_9_keys(self) -> None:
        block = render_aspect_taxonomy_block(self.taxonomy)
        for key in (
            "show_program",
            "experience_booth",
            "ambiance_scenery",
            "food",
            "price_cost",
            "facility_crowd",
            "access_traffic",
            "operation_service",
            "etc",
        ):
            self.assertIn(key, block, f"aspect key '{key}' missing from block")

    def test_aspect_block_includes_korean_label_and_description(self) -> None:
        block = render_aspect_taxonomy_block(self.taxonomy)
        # 한국어 label과 description 포함 — prompt가 LLM에 한국어 가이드를 전달.
        self.assertIn("분위기/경관", block)
        self.assertIn("드론쇼", block)
        self.assertIn("화장실", block)

    def test_aspect_block_excludes_deprecated_aspect_keys(self) -> None:
        block = render_aspect_taxonomy_block(self.taxonomy)
        # 옛 7-aspect 중 retire된 키들이 새 taxonomy에 없어야 한다.
        for deprecated in ("atmosphere", "contents", "convenience", "value", "overall"):
            self.assertNotIn(
                deprecated, block, f"deprecated key '{deprecated}' present in block"
            )

    def test_aspect_block_starts_with_markdown_table_header(self) -> None:
        block = render_aspect_taxonomy_block(self.taxonomy)
        first_line, second_line, *_ = block.splitlines()
        self.assertEqual(first_line, "| Aspect | 설명 |")
        self.assertEqual(second_line, "|---|---|")

    def test_aspect_block_deterministic(self) -> None:
        a = render_aspect_taxonomy_block(self.taxonomy)
        b = render_aspect_taxonomy_block(self.taxonomy)
        self.assertEqual(a, b)

    def test_aspect_block_row_order_matches_taxonomy(self) -> None:
        # taxonomy.aspects 순서를 그대로 따라가는지 — taxonomy config JSON 순서.
        block = render_aspect_taxonomy_block(self.taxonomy)
        # 본문에서 각 aspect key 위치를 찾아 순서가 일치하는지 확인.
        positions = [block.find(a.key) for a in self.taxonomy.aspects]
        self.assertEqual(positions, sorted(positions))

    def test_sentiment_block_lists_all(self) -> None:
        block = render_sentiment_taxonomy_block(self.taxonomy)
        for s in self.taxonomy.sentiments:
            self.assertIn(f"- {s}", block)


class CheckTaxonomyCompatibilityTests(unittest.TestCase):
    """Phase 3-B (silverone 2026-05-27) — analyze 시 artifact taxonomy ↔
    planner taxonomy 정합성 체크 4 분기."""

    def setUp(self) -> None:
        self.planner = load_taxonomy("festival-v2")

    def test_ok_when_id_and_hash_match(self) -> None:
        result = check_taxonomy_compatibility(
            planner_taxonomy=self.planner,
            artifact_taxonomy_id=self.planner.taxonomy_id,
            artifact_taxonomy_hash=self.planner.taxonomy_hash,
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["artifact_taxonomy_id"], self.planner.taxonomy_id)
        self.assertEqual(result["planner_taxonomy_id"], self.planner.taxonomy_id)

    def test_legacy_missing_when_artifact_id_none(self) -> None:
        result = check_taxonomy_compatibility(
            planner_taxonomy=self.planner,
            artifact_taxonomy_id=None,
            artifact_taxonomy_hash=None,
        )
        self.assertEqual(result["status"], "legacy_missing")
        self.assertIsNone(result["artifact_taxonomy_id"])
        self.assertIsNone(result["artifact_taxonomy_hash"])
        # planner 정보는 그대로 노출 — 운영자가 어떤 taxonomy 기준이었는지 추적.
        self.assertEqual(result["planner_taxonomy_id"], "festival-v2")
        self.assertEqual(result["planner_taxonomy_hash"], self.planner.taxonomy_hash)

    def test_legacy_missing_when_artifact_id_empty_string(self) -> None:
        # blank/whitespace 도 legacy로 처리.
        result = check_taxonomy_compatibility(
            planner_taxonomy=self.planner,
            artifact_taxonomy_id="   ",
            artifact_taxonomy_hash="",
        )
        self.assertEqual(result["status"], "legacy_missing")

    def test_id_mismatch_raises_fail_loud(self) -> None:
        with self.assertRaises(TaxonomyMismatchError) as cm:
            check_taxonomy_compatibility(
                planner_taxonomy=self.planner,
                artifact_taxonomy_id="festival-v1",
                artifact_taxonomy_hash="abc",
            )
        # error message에 양쪽 id가 포함되어야 함.
        self.assertIn("festival-v1", str(cm.exception))
        self.assertIn("festival-v2", str(cm.exception))
        self.assertEqual(cm.exception.artifact_taxonomy_id, "festival-v1")
        self.assertEqual(cm.exception.planner_taxonomy_id, "festival-v2")

    def test_id_mismatch_is_value_error_subclass(self) -> None:
        # task_router의 except (ValueError, ...)에서 잡혀야 한다.
        with self.assertRaises(ValueError):
            check_taxonomy_compatibility(
                planner_taxonomy=self.planner,
                artifact_taxonomy_id="other-id",
                artifact_taxonomy_hash=None,
            )

    def test_hash_mismatch_warning_with_same_id(self) -> None:
        # 같은 id + 다른 hash → warning, 실행 허용.
        result = check_taxonomy_compatibility(
            planner_taxonomy=self.planner,
            artifact_taxonomy_id=self.planner.taxonomy_id,
            artifact_taxonomy_hash="deadbeef" * 8,
        )
        self.assertEqual(result["status"], "hash_mismatch")
        self.assertEqual(result["artifact_taxonomy_id"], self.planner.taxonomy_id)
        self.assertEqual(result["artifact_taxonomy_hash"], "deadbeef" * 8)
        self.assertEqual(result["planner_taxonomy_hash"], self.planner.taxonomy_hash)

    def test_ok_when_artifact_hash_none_but_id_matches(self) -> None:
        # hash가 None이면 hash mismatch 분기 skip → ok.
        result = check_taxonomy_compatibility(
            planner_taxonomy=self.planner,
            artifact_taxonomy_id=self.planner.taxonomy_id,
            artifact_taxonomy_hash=None,
        )
        self.assertEqual(result["status"], "ok")

    def test_result_dict_keys_locked(self) -> None:
        result = check_taxonomy_compatibility(
            planner_taxonomy=self.planner,
            artifact_taxonomy_id=self.planner.taxonomy_id,
            artifact_taxonomy_hash=self.planner.taxonomy_hash,
        )
        self.assertEqual(
            set(result.keys()),
            {
                "planner_taxonomy_id",
                "artifact_taxonomy_id",
                "planner_taxonomy_hash",
                "artifact_taxonomy_hash",
                "status",
            },
        )


class PlanV2SmokeFixtureSidecarTests(unittest.TestCase):
    """Phase 4 fixture builder (silverone 2026-05-27) — plan_v2_smoke fixture의
    ``clause_label_summary.json`` sidecar가 festival-v2 taxonomy와 정합한지
    잠금. smoke_analyze_endpoint.sh가 이 sidecar를 읽어 worker payload에
    inject하므로 ``taxonomy_check.status``가 ``ok`` 분기를 보장한다."""

    def setUp(self) -> None:
        import json
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[3]
        self.fixture_dir = (
            repo_root / "workers" / "python-ai" / "tests" / "fixtures" / "plan_v2_smoke"
        )
        sidecar_path = self.fixture_dir / "clause_label_summary.json"
        self.sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        self.taxonomy = load_taxonomy("festival-v2")

    def test_sidecar_taxonomy_id_matches_config(self) -> None:
        self.assertEqual(self.sidecar["taxonomy_id"], "festival-v2")

    def test_sidecar_taxonomy_hash_matches_loaded_taxonomy(self) -> None:
        self.assertEqual(self.sidecar["taxonomy_hash"], self.taxonomy.taxonomy_hash)

    def test_sidecar_aspect_counts_within_taxonomy_keys(self) -> None:
        # fixture의 aspect 값이 festival-v2 enum 안에 있어야 함.
        for aspect_key in self.sidecar.get("aspect_counts", {}):
            self.assertIn(aspect_key, self.taxonomy.aspect_keys_set)

    def test_sidecar_excludes_deprecated_aspect_keys(self) -> None:
        deprecated = {"atmosphere", "contents", "convenience", "value", "overall"}
        for key in self.sidecar.get("aspect_counts", {}):
            self.assertNotIn(key, deprecated)

    def test_sidecar_prompt_version_locked(self) -> None:
        self.assertEqual(self.sidecar["prompt_version"], "dataset-clause-label-v3")

    def test_fixture_clauses_use_taxonomy_aspects(self) -> None:
        # clause_label.jsonl의 모든 aspect 값이 taxonomy.aspect_keys_set에 있어야
        # 함. ``taxonomy_check.status=ok``의 전제.
        import json

        clauses_path = self.fixture_dir / "clause_label.jsonl"
        with clauses_path.open("r", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                self.assertIn(
                    record["aspect"],
                    self.taxonomy.aspect_keys_set,
                    f"unknown aspect '{record['aspect']}' in fixture",
                )


class LoadTaxonomyMissingFileTest(unittest.TestCase):
    def test_load_unknown_taxonomy_id_raises(self) -> None:
        with self.assertRaises(TaxonomyError) as cm:
            load_taxonomy("does-not-exist-xyz")
        self.assertIn("not found", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
