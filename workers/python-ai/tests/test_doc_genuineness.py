"""dataset_doc_genuineness skill — 3-tier 분류 + LLOA mock 단위 테스트.

ADR-017 / 5/19 결정. LLOA client는 urlopen 주입 패턴(test_lloa_client.py와
동일)으로 mock한다. fixture: cleaned doc 4건 (genuine_review / mixed /
non_review / empty).
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _fake_urlopen_factory(responses_by_doc: dict[str, dict]):
    """doc_id별 LLOA 응답을 정해 두고 순서대로 반환한다."""
    call_log: list[dict] = []

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
        user_obj = json.loads(user_text)
        doc_id = user_obj["doc_id"]
        call_log.append({"doc_id": doc_id, "doc_text": user_obj["doc_text"]})
        if doc_id not in responses_by_doc:
            raise AssertionError(f"unexpected doc_id in test: {doc_id}")
        return _Resp(responses_by_doc[doc_id])

    return _fake, call_log


def _llm_completion(content: str, *, finish_reason: str = "stop") -> dict:
    return {
        "choices": [{
            "message": {"content": content, "reasoning_content": ""},
            "finish_reason": finish_reason,
        }],
        "usage": {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
    }


class DocGenuinenessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "doc_genuineness.jsonl"
        rows = [
            {"row_id": "row:1", "cleaned_text": "오늘 강릉야행 다녀왔어요. 야경이 환상적이었습니다."},
            {"row_id": "row:2", "cleaned_text": "행사 안내: 일시 11/9 ~ 11/10 / 장소 강릉. 자세한 정보 확인."},
            {"row_id": "row:3", "cleaned_text": "원래는 동해 다녀왔는데 다음엔 강릉야행 가볼까 합니다. 사진 봤어요."},
            {"row_id": "row:4", "cleaned_text": ""},
        ]
        with self.clean_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _payload(self, **overrides) -> dict:
        # silverone 2026-05-22 (PR-α2) — doc_genuineness config 필수 (subject_name).
        # 기존 fixture는 festival 도메인이므로 그 값으로 inject.
        payload = {
            "dataset_version_id": "dvid:test",
            "clean_artifact_ref": str(self.clean_path),
            "output_path": str(self.output_path),
            "doc_genuineness": {
                "subject_type": "festival",
                "subject_name": "강릉 국가유산야행",
                "subject_aliases": ["문화유산야행", "문화재야행", "강릉야행"],
                "recruitment_keywords": ["서포터즈", "푸드트럭"],
            },
        }
        payload.update(overrides)
        return payload

    def _patch_config_and_run(self, urlopen_fn):
        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig

        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=2048,
            lloa_timeout_sec=30,
            lloa_reasoning_effort=None,
            lloa_prepend_no_think=True,
        )

        original_init = doc_genuineness.LloaClient.__init__

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=urlopen_fn)

        with patch.object(doc_genuineness, "load_config", return_value=fake_config), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            return doc_genuineness.run_dataset_doc_genuineness(self._payload())

    def test_three_tier_classification_success(self) -> None:
        # silverone 2026-05-22 — T/F/A prompt 채택 후 mixed 대신 uncertain 분기
        # 잠금. mixed enum은 backward compat용으로 enum에는 남아 있다.
        responses = {
            "row:1": _llm_completion(
                '{"doc_id":"row:1","genuineness":"genuine_review","reason":"1인칭 야경 후기 중심."}'
            ),
            "row:2": _llm_completion(
                '{"doc_id":"row:2","genuineness":"non_review","reason":"행사 안내문 패턴."}'
            ),
            "row:3": _llm_completion(
                '{"doc_id":"row:3","genuineness":"uncertain","reason":"정보 부족."}'
            ),
        }
        fake_urlopen, log = _fake_urlopen_factory(responses)
        result = self._patch_config_and_run(fake_urlopen)

        # row:4는 empty라 LLOA 호출 안 되어야 함
        self.assertEqual(len(log), 3)
        self.assertEqual({c["doc_id"] for c in log}, {"row:1", "row:2", "row:3"})

        artifact = result["artifact"]
        summary = artifact["summary"]
        self.assertEqual(artifact["skill_name"], "dataset_doc_genuineness")
        self.assertEqual(summary["tier_counts"]["genuine_review"], 1)
        self.assertEqual(summary["tier_counts"]["mixed"], 0)
        self.assertEqual(summary["tier_counts"]["non_review"], 2)  # row:2 LLOA + row:4 empty shortcut
        self.assertEqual(summary["tier_counts"]["uncertain"], 1)
        self.assertEqual(summary["parse_failures"], 0)
        self.assertEqual(summary["input_row_count"], 4)
        self.assertEqual(summary["processed_row_count"], 4)

        # output jsonl 검증
        lines = self.output_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 4)
        records = [json.loads(line) for line in lines]
        by_id = {r["doc_id"]: r for r in records}
        self.assertEqual(by_id["row:1"]["genuineness"], "genuine_review")
        self.assertEqual(by_id["row:1"]["source"], "lloa")
        self.assertEqual(by_id["row:4"]["genuineness"], "non_review")
        self.assertEqual(by_id["row:4"]["source"], "empty_text_shortcut")

    def test_parse_failure_falls_back_to_non_review(self) -> None:
        responses = {
            "row:1": _llm_completion("not json at all"),
            "row:2": _llm_completion(
                '{"doc_id":"row:2","genuineness":"non_review","reason":"행사 안내문."}'
            ),
            "row:3": _llm_completion(
                '{"doc_id":"row:3","genuineness":"invalid_tier","reason":"잘못된 라벨"}'
            ),
        }
        fake_urlopen, _ = _fake_urlopen_factory(responses)
        result = self._patch_config_and_run(fake_urlopen)

        summary = result["artifact"]["summary"]
        # row:1 parse fail → non_review fallback
        # row:3 invalid tier → non_review fallback
        # row:2 success non_review + row:4 empty
        self.assertEqual(summary["parse_failures"], 2)
        self.assertEqual(summary["tier_counts"]["non_review"], 4)
        self.assertEqual(summary["tier_counts"]["uncertain"], 0)

        records = [
            json.loads(line)
            for line in self.output_path.read_text(encoding="utf-8").strip().splitlines()
        ]
        by_id = {r["doc_id"]: r for r in records}
        self.assertEqual(by_id["row:1"]["source"], "lloa_parse_failure")
        self.assertEqual(by_id["row:3"]["source"], "lloa_parse_failure")

    def test_missing_lloa_api_key_raises(self) -> None:
        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig

        no_key_config = WorkerConfig(lloa_api_key=None)
        with patch.object(doc_genuineness, "load_config", return_value=no_key_config), \
             self.assertRaisesRegex(ValueError, "LLOA API key"):
            doc_genuineness.run_dataset_doc_genuineness(self._payload())

    def test_missing_required_payload_raises(self) -> None:
        from python_ai_worker.dataset_build import doc_genuineness

        with self.assertRaisesRegex(ValueError, "dataset_doc_genuineness requires"):
            doc_genuineness.run_dataset_doc_genuineness({"dataset_version_id": "x"})

    def test_applied_snapshot_in_summary(self) -> None:
        # silverone 2026-05-22 (PR-α2) — 실행 당시 적용된 subject variables가
        # summary.applied에 snapshot으로 저장된다.
        responses = {
            "row:1": _llm_completion('{"doc_id":"row:1","genuineness":"genuine_review","reason":"본인 방문."}'),
            "row:2": _llm_completion('{"doc_id":"row:2","genuineness":"non_review","reason":"공식 안내."}'),
            "row:3": _llm_completion('{"doc_id":"row:3","genuineness":"uncertain","reason":"정보 부족."}'),
        }
        fake_urlopen, _ = _fake_urlopen_factory(responses)
        result = self._patch_config_and_run(fake_urlopen)
        applied = result["artifact"]["summary"]["applied"]
        self.assertEqual(applied["subject_name"], "강릉 국가유산야행")
        self.assertEqual(applied["subject_aliases"], ["문화유산야행", "문화재야행", "강릉야행"])
        self.assertEqual(applied["recruitment_keywords"], ["서포터즈", "푸드트럭"])
        self.assertEqual(applied["subject_type"], "festival")
        self.assertEqual(applied["prompt_version"], "dataset-doc-genuineness-v1")


class DocGenuinenessRenderTests(unittest.TestCase):
    """prompt placeholder 치환 + 조건부 블록 잠금. silverone 2026-05-22 (PR-α2).

    LLOA 호출 없이 ``_render_prompt`` / ``_extract_doc_genuineness_config`` 만
    단위로 검증한다."""

    def test_placeholders_substituted(self) -> None:
        from python_ai_worker.dataset_build.doc_genuineness import (
            _extract_doc_genuineness_config,
            _render_prompt,
            _strip_front_matter,
        )
        from python_ai_worker.prompt_options import resolve_prompt_path

        prompt_path = resolve_prompt_path("doc_genuineness")
        self.assertIsNotNone(prompt_path)
        template = _strip_front_matter(prompt_path.read_text(encoding="utf-8"))

        config = _extract_doc_genuineness_config({
            "doc_genuineness": {
                "subject_type": "festival",
                "subject_name": "강릉 국가유산야행",
                "subject_aliases": ["문화유산야행", "문화재야행", "강릉야행"],
                "recruitment_keywords": ["서포터즈", "푸드트럭"],
            }
        })
        rendered = _render_prompt(template, config)

        # subject_name이 본문에 자연스럽게 inject되고, placeholder는 남지 않음.
        self.assertIn("'강릉 국가유산야행'", rendered)
        self.assertNotIn("{{subject_name}}", rendered)
        self.assertNotIn("{{#if", rendered)
        self.assertNotIn("{{/if}}", rendered)

        # aliases는 quoted list로 inject + alias 안내 문장이 살아 있어야 함.
        self.assertIn("'문화유산야행', '문화재야행', '강릉야행'", rendered)
        self.assertIn("also referred to as", rendered)

        # recruitment_keywords도 quoted list로. 2026-05-28 분석팀 검증 prompt
        # 회귀로 본문 표현이 "Recruitment or applications: <list> 모집 등"으로
        # 정렬됐다 (silverone 2026-05-26 C2+M1 한국어 통일은 이번 회귀로 덮어씀).
        self.assertIn("'서포터즈', '푸드트럭'", rendered)
        self.assertIn("Recruitment or applications", rendered)

        # 분석팀 검증 prompt — schema label은 인라인 헤더 형태로 노출. T/F/A는
        # prompt 본문에 등장하지 않는다 (production label 직접 출력).
        self.assertIn("**`genuine_review`**", rendered)
        self.assertIn("**`non_review`**", rendered)
        self.assertIn("**`uncertain`**", rendered)
        self.assertNotIn("\"is_festival_doc\"", rendered)
        self.assertNotIn("→ Y\n", rendered)
        self.assertNotIn("→ F\n", rendered)
        self.assertNotIn("→ A\n", rendered)

    def test_empty_lists_drop_conditional_sections(self) -> None:
        from python_ai_worker.dataset_build.doc_genuineness import (
            _extract_doc_genuineness_config,
            _render_prompt,
            _strip_front_matter,
        )
        from python_ai_worker.prompt_options import resolve_prompt_path

        template = _strip_front_matter(resolve_prompt_path("doc_genuineness").read_text(encoding="utf-8"))

        config = _extract_doc_genuineness_config({
            "doc_genuineness": {
                "subject_name": "전주 한옥마을 음식 후기",
                # aliases / keywords 미지정 → 빈 list default
            }
        })
        rendered = _render_prompt(template, config)

        self.assertIn("'전주 한옥마을 음식 후기'", rendered)
        # 빈 list는 sentinel 없이 섹션 자체가 사라져야 한다.
        self.assertNotIn("also referred to as", rendered)
        # 2026-05-28 분석팀 검증 prompt 회귀 — recruitment 라인이 영어로 회귀.
        # 빈 list면 통째 사라지므로 본문에 등장하지 않아야 한다.
        self.assertNotIn("Recruitment or applications", rendered)
        # quoted list 흔적도 없어야 함.
        self.assertNotIn("''", rendered)
        self.assertNotIn(", '", rendered.split("Examples")[0])  # examples는 그대로 둠

    def test_subject_name_missing_raises(self) -> None:
        from python_ai_worker.dataset_build.doc_genuineness import _extract_doc_genuineness_config

        with self.assertRaisesRegex(ValueError, "subject_name"):
            _extract_doc_genuineness_config({"doc_genuineness": {}})

        with self.assertRaisesRegex(ValueError, "subject_name"):
            _extract_doc_genuineness_config({"doc_genuineness": {"subject_name": "   "}})

    def test_payload_doc_genuineness_missing_raises(self) -> None:
        from python_ai_worker.dataset_build.doc_genuineness import _extract_doc_genuineness_config

        with self.assertRaisesRegex(ValueError, "subject_name"):
            _extract_doc_genuineness_config({})  # 키 자체 누락

    def test_defaults_for_optional_fields(self) -> None:
        from python_ai_worker.dataset_build.doc_genuineness import _extract_doc_genuineness_config

        config = _extract_doc_genuineness_config({
            "doc_genuineness": {"subject_name": "abc"}
        })
        self.assertEqual(config["subject_type"], "generic")
        self.assertEqual(config["subject_aliases"], [])
        self.assertEqual(config["recruitment_keywords"], [])


if __name__ == "__main__":
    unittest.main()
