"""ADR-018 (β2) / silverone 5/20 prompt 갱신 후 clause_label artifact shape 잠금.

5/20 prompt 결정 변경:
- LLOA response는 *최외곽 JSON array* (clauses_json wrapper 없음)
- clause 객체 필드: clause / sentiment / aspect 3개만 (clause_index / sentiment_reason /
  aspect_reason 제거)
- aspect taxonomy 7종 prompt hardcoded (worker payload에 inject 안 함)
- worker side 추가 필드: doc_id, prompt_version, source (총 5+1=6 키)

후속 plan skill 회귀 대응이 이 shape에 의존. silent regression 방지가 lock 목적.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


_LOCKED_CLAUSE_KEYS = {
    "doc_id",
    "clause",
    "sentiment",
    "aspect",
    "prompt_version",
    "source",
}

_REMOVED_LEGACY_KEYS = {
    # ADR-017 / ADR-018 / 5/20 결정으로 명시적으로 제거된 옛 field. 이 set 어느
    # key라도 새 artifact에 등장하면 silent schema 회귀.
    "is_relevant",
    "source_sentence_id",
    "scope",
    "clause_text",
    "clause_order",
    "clause_index",
    "sentiment_reason",
    "aspect_reason",
    "source_row_id",
    "source_timestamp",
    "source_meta",
    "quality_tier",
}

_LOCKED_SENTIMENT_LABELS = {"positive", "negative", "neutral"}
_LOCKED_ASPECT_LABELS = {
    "show_program",
    "experience_booth",
    "ambiance_scenery",
    "food",
    "price_cost",
    "facility_crowd",
    "access_traffic",
    "operation_service",
    "etc",
}


def _llm_completion(content: str, *, finish_reason: str = "stop") -> dict:
    return {
        "choices": [{
            "message": {"content": content, "reasoning_content": ""},
            "finish_reason": finish_reason,
        }],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
    }


def _fake_urlopen_with_clauses(clauses_by_doc: dict[str, list[dict]]):
    """doc_id별 LLOA 응답 (5/20 prompt — 최외곽 array). doc_id는 user message의
    ``제목: ... 본문: ...`` 첫 줄 제목으로 매핑."""
    class _Resp:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
        body = json.loads(req.data.decode("utf-8"))
        user_text = body["messages"][1]["content"]
        # user content는 "제목: <title>\n본문: <text>" 형식. clauses_by_doc dict
        # key를 doc id 대신 *body 첫 30자* prefix로 매칭 (test fixture만 의도).
        # 더 단순화: 첫 doc만 처리하는 test가 대부분.
        doc_key = next(iter(clauses_by_doc))
        clauses = clauses_by_doc.get(doc_key, [])
        # 5/20 prompt — 최외곽 array. 단 worker는 dict wrapper도 hint로 받음.
        completion = json.dumps(clauses, ensure_ascii=False)
        return _Resp(_llm_completion(completion))

    return _fake


class ClauseArtifactShapeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "clause_label.jsonl"
        rows = [
            {"row_id": "row:1", "doc_title": "강릉야행 후기",
             "cleaned_text": "여좌천 로망스다리 쪽은 사람이 정말 많았는데 야경은 환상적이었어요."},
        ]
        with self.clean_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _run(self, clauses_by_doc: dict[str, list[dict]]) -> list[dict]:
        from python_ai_worker.dataset_build import clause_label
        from python_ai_worker.config import WorkerConfig

        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=8192,
            lloa_timeout_sec=60,
            lloa_reasoning_effort="low",
            lloa_prepend_no_think=True,
        )
        original_init = clause_label.LloaClient.__init__
        fake_urlopen = _fake_urlopen_with_clauses(clauses_by_doc)

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=fake_urlopen)

        with patch.object(clause_label, "load_config", return_value=fake_config), \
             patch.object(clause_label.LloaClient, "__init__", _init_with_fake):
            clause_label.run_dataset_clause_label({
                "dataset_version_id": "dvid:test",
                "clean_artifact_ref": str(self.clean_path),
                "output_path": str(self.output_path),
                "concurrency": 1,  # 단일 doc test — 순서 보존 + 결정론
                # 5/20 결정으로 default가 genuine_review+mixed 필터 ON.
                # 이 test는 fixture에 doc_genuineness artifact 없으므로
                # explicit opt-out (모든 doc 처리)으로 진행.
                "include_genuineness": [],
            })

        records: list[dict] = []
        with self.output_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def test_locked_keys_present(self) -> None:
        records = self._run({
            "row:1": [
                {"clause": "사람이 정말 많았는데", "sentiment": "neutral", "aspect": "ambiance_scenery"},
                {"clause": "야경은 환상적이었어요", "sentiment": "positive", "aspect": "ambiance_scenery"},
            ],
        })
        self.assertEqual(len(records), 2)
        for clause in records:
            for key in _LOCKED_CLAUSE_KEYS:
                self.assertIn(key, clause, f"clause record missing locked key: {key}")

    def test_removed_legacy_keys_absent(self) -> None:
        records = self._run({
            "row:1": [
                {"clause": "야경은 환상적이었어요", "sentiment": "positive", "aspect": "ambiance_scenery"},
            ],
        })
        for clause in records:
            for legacy_key in _REMOVED_LEGACY_KEYS:
                self.assertNotIn(legacy_key, clause, f"legacy key leaked: {legacy_key}")

    def test_sentiment_within_three_label_set(self) -> None:
        records = self._run({
            "row:1": [
                {"clause": "야경 환상적", "sentiment": "positive", "aspect": "ambiance_scenery"},
            ],
        })
        for clause in records:
            self.assertIn(clause["sentiment"], _LOCKED_SENTIMENT_LABELS)

    def test_aspect_within_seven_label_set(self) -> None:
        records = self._run({
            "row:1": [
                {"clause": "야경 환상적", "sentiment": "positive", "aspect": "ambiance_scenery"},
                {"clause": "음식 맛있음", "sentiment": "positive", "aspect": "food"},
            ],
        })
        for clause in records:
            self.assertIn(clause["aspect"], _LOCKED_ASPECT_LABELS)

    def test_invalid_sentiment_falls_back_to_neutral(self) -> None:
        # LLOA가 schema 위반("great" 등)을 보내면 worker가 neutral로 정규화.
        records = self._run({
            "row:1": [
                {"clause": "야경 환상적", "sentiment": "great", "aspect": "ambiance_scenery"},
            ],
        })
        self.assertEqual(records[0]["sentiment"], "neutral")

    def test_invalid_aspect_falls_back_to_etc(self) -> None:
        # LLOA가 aspect taxonomy 밖 값을 보내면 worker가 etc로 정규화.
        records = self._run({
            "row:1": [
                {"clause": "야경 환상적", "sentiment": "positive", "aspect": "scenery"},
            ],
        })
        self.assertEqual(records[0]["aspect"], "etc")

    def test_prompt_version_locked_to_v3(self) -> None:
        records = self._run({
            "row:1": [
                {"clause": "야경 환상적", "sentiment": "positive", "aspect": "ambiance_scenery"},
            ],
        })
        self.assertEqual(records[0]["prompt_version"], "dataset-clause-label-v3")

    def test_source_marked_lloa_on_success(self) -> None:
        records = self._run({
            "row:1": [
                {"clause": "야경 환상적", "sentiment": "positive", "aspect": "ambiance_scenery"},
            ],
        })
        self.assertEqual(records[0]["source"], "lloa")


class ClauseLabelTaxonomySourceTests(unittest.TestCase):
    """taxonomy-driven config Phase 2-A (silverone 2026-05-27).

    ``clause_label._ALLOWED_ASPECT``가 config/taxonomies/festival-v2.json에서
    derive되는지 잠금. 기존 _LOCKED_ASPECT_LABELS는 *hand-coded literal*로
    값을 잠그고, 본 class는 *taxonomy source*를 잠근다 — 둘 다 통과해야
    taxonomy config가 single source 역할을 하면서 옛 값과 호환된다.
    """

    def test_default_taxonomy_id_is_festival_v2(self) -> None:
        from python_ai_worker.dataset_build.clause_label import (
            DEFAULT_CLAUSE_LABEL_TAXONOMY_ID,
        )

        self.assertEqual(DEFAULT_CLAUSE_LABEL_TAXONOMY_ID, "festival-v2")

    def test_allowed_aspect_matches_festival_v2_keys(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _ALLOWED_ASPECT
        from python_ai_worker.taxonomies import load_taxonomy

        festival = load_taxonomy("festival-v2")
        self.assertEqual(_ALLOWED_ASPECT, festival.aspect_keys_set)

    def test_allowed_aspect_matches_hand_coded_lock(self) -> None:
        # Phase 2-A 도입 후에도 옛 hand-coded lock과 같은 9개 값이어야 함.
        from python_ai_worker.dataset_build.clause_label import _ALLOWED_ASPECT

        self.assertEqual(_ALLOWED_ASPECT, _LOCKED_ASPECT_LABELS)

    def test_fallback_aspect_from_taxonomy(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _FALLBACK_ASPECT

        self.assertEqual(_FALLBACK_ASPECT, "etc")

    def test_fallback_aspect_is_member_of_allowed(self) -> None:
        from python_ai_worker.dataset_build.clause_label import (
            _ALLOWED_ASPECT,
            _FALLBACK_ASPECT,
        )

        self.assertIn(_FALLBACK_ASPECT, _ALLOWED_ASPECT)


class ClauseLabelPromptInjectionTests(unittest.TestCase):
    """Phase 2-B (silverone 2026-05-27) — clause_label prompt template의
    ``{{ASPECT_TAXONOMY}}`` placeholder가 taxonomy config에서 렌더된 markdown
    table로 치환되는지 잠금."""

    def test_loaded_prompt_contains_9_aspect_keys(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _load_prompt_template

        body, version = _load_prompt_template({})
        self.assertEqual(version, "dataset-clause-label-v3")
        for key in _LOCKED_ASPECT_LABELS:
            self.assertIn(key, body, f"aspect key '{key}' missing from rendered prompt")

    def test_loaded_prompt_has_no_placeholder_after_render(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _load_prompt_template

        body, _ = _load_prompt_template({})
        self.assertNotIn("{{ASPECT_TAXONOMY}}", body)

    def test_loaded_prompt_excludes_deprecated_aspect(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _load_prompt_template

        body, _ = _load_prompt_template({})
        # 옛 7-aspect 키들이 새 prompt body에 없어야 한다.
        for deprecated in ("atmosphere", "convenience", "value", "overall"):
            self.assertNotIn(
                f"| {deprecated} |",
                body,
                f"deprecated aspect '{deprecated}' present in rendered prompt",
            )

    def test_inline_prompt_with_placeholder_is_rendered(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _load_prompt_template

        body, version = _load_prompt_template(
            {
                "clause_label_prompt_content": "header\n{{ASPECT_TAXONOMY}}\nfooter",
                "clause_label_prompt_version": "inline-test",
            }
        )
        self.assertEqual(version, "inline-test")
        self.assertNotIn("{{ASPECT_TAXONOMY}}", body)
        self.assertIn("show_program", body)
        self.assertIn("ambiance_scenery", body)
        self.assertTrue(body.startswith("header\n"))
        self.assertTrue(body.endswith("\nfooter"))

    def test_inline_prompt_without_placeholder_unchanged(self) -> None:
        # placeholder가 없으면 inline prompt는 그대로 통과 (옛 호환).
        from python_ai_worker.dataset_build.clause_label import _load_prompt_template

        inline = "fully custom inline prompt\nwith no placeholder"
        body, version = _load_prompt_template(
            {"clause_label_prompt_content": inline}
        )
        self.assertEqual(body, inline)
        self.assertEqual(version, "request_inline")


class ClauseLabelTaxonomyMetadataTests(unittest.TestCase):
    """Phase 2-B (silverone 2026-05-27) — clause_label artifact summary에
    taxonomy_id / taxonomy_hash가 기록되는지 잠금. analyze 시 정합성 체크
    (Phase 3)의 기반."""

    def test_summary_metadata_includes_taxonomy_id_and_hash(self) -> None:
        from python_ai_worker.dataset_build import clause_label as cl
        from python_ai_worker.taxonomies import load_taxonomy

        # _TAXONOMY와 festival-v2 file이 일치하는지 + 정의된 id/hash가 summary에
        # 넣을 값과 같은지. summary 자체는 build를 돌려야 만들어지므로 본 test는
        # *source* 잠금.
        expected = load_taxonomy("festival-v2")
        self.assertEqual(cl._TAXONOMY.taxonomy_id, expected.taxonomy_id)
        self.assertEqual(cl._TAXONOMY.taxonomy_hash, expected.taxonomy_hash)


if __name__ == "__main__":
    unittest.main()
