"""dataset_doc_genuineness skill — 3-tier 분류 + LLOA mock 단위 테스트.

ADR-017 / 5/19 결정. LLOA client는 urlopen 주입 패턴(test_lloa_client.py와
동일)으로 mock한다. fixture: cleaned doc 4건 (genuine_review / non_review /
uncertain / empty).
"""
from __future__ import annotations

import io
import json
import tempfile
import unittest
import urllib.error
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

    def _patch_config_and_run(self, urlopen_fn, payload=None):
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
            return doc_genuineness.run_dataset_doc_genuineness(payload or self._payload())

    def test_three_tier_classification_success(self) -> None:
        # silverone 2026-05-22 — T/F/A prompt 채택 후 mixed 대신 uncertain 분기.
        # silverone 2026-06-16 — legacy mixed tier 완전 제거 (enum/count에서 빠짐).
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
        self.assertNotIn("mixed", summary["tier_counts"])
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
        # 파싱 실패율이 임계(50%) 미만이면(여기 1/4=25%) build는 계속되고 해당 doc만
        # non_review fallback. (50% 이상이면 별도 fail-loud 테스트 참조)
        responses = {
            "row:1": _llm_completion("not json at all"),
            "row:2": _llm_completion(
                '{"doc_id":"row:2","genuineness":"non_review","reason":"행사 안내문."}'
            ),
            "row:3": _llm_completion(
                '{"doc_id":"row:3","genuineness":"genuine_review","reason":"본인 방문 후기."}'
            ),
        }
        fake_urlopen, _ = _fake_urlopen_factory(responses)
        result = self._patch_config_and_run(fake_urlopen)

        summary = result["artifact"]["summary"]
        # row:1 parse fail → non_review fallback. row:2 non_review + row:4 empty.
        self.assertEqual(summary["parse_failures"], 1)
        self.assertEqual(summary["tier_counts"]["non_review"], 3)
        self.assertEqual(summary["tier_counts"]["genuine_review"], 1)
        self.assertEqual(summary["tier_counts"]["uncertain"], 0)

        records = [
            json.loads(line)
            for line in self.output_path.read_text(encoding="utf-8").strip().splitlines()
        ]
        by_id = {r["doc_id"]: r for r in records}
        self.assertEqual(by_id["row:1"]["source"], "lloa_parse_failure")

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
        self.assertEqual(applied["prompt_version"], "v1")


def _fake_urlopen_with_failures(responses_by_doc: dict[str, dict], fail_doc_ids: set[str]):
    """fail_doc_ids에 든 doc은 LLOA HTTP 400을 raise, 나머지는 정상 응답.

    silverone 2026-06-04 — per-doc 격리 잠금용. urllib HTTPError(400)을 던져
    초장문 LLOA 거부(request too large / context_length) 상황을 재현한다.
    """
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
        user_obj = json.loads(body["messages"][1]["content"])
        doc_id = user_obj["doc_id"]
        call_log.append({"doc_id": doc_id, "doc_text": user_obj["doc_text"]})
        if doc_id in fail_doc_ids:
            raise urllib.error.HTTPError(
                req.full_url,
                400,
                "Bad Request",
                {},
                io.BytesIO(b'{"error":"request too large"}'),
            )
        return _Resp(responses_by_doc[doc_id])

    return _fake, call_log


class DocGenuinenessTruncateIsolateTests(DocGenuinenessTests):
    """silverone 2026-06-04 — LLOA 입력 안정화 (truncate + per-doc 격리).

    DocGenuinenessTests의 fixture(row:1~4) + helper 재사용. chunking 아님:
    clean row 1 = 결과 1 유지, 호출 직전 input_text만 truncate.
    """

    def test_long_doc_truncated_before_lloa(self) -> None:
        # max_input_chars를 작게 줘서 비어 있지 않은 3 doc 모두 truncate되게 한다.
        responses = {
            "row:1": _llm_completion('{"doc_id":"row:1","genuineness":"genuine_review","reason":"후기."}'),
            "row:2": _llm_completion('{"doc_id":"row:2","genuineness":"non_review","reason":"안내."}'),
            "row:3": _llm_completion('{"doc_id":"row:3","genuineness":"uncertain","reason":"불명."}'),
        }
        fake_urlopen, log = _fake_urlopen_with_failures(responses, set())
        result = self._patch_config_and_run(
            fake_urlopen, payload=self._payload(max_input_chars=10)
        )

        # LLOA에 실제 보낸 doc_text는 10자로 잘려야 한다.
        for entry in log:
            self.assertLessEqual(len(entry["doc_text"]), 10)

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["max_input_chars"], 10)
        self.assertEqual(summary["truncated_docs"], 3)
        self.assertEqual(summary["request_failures"], 0)

        records = {
            json.loads(line)["doc_id"]: json.loads(line)
            for line in self.output_path.read_text(encoding="utf-8").strip().splitlines()
        }
        self.assertTrue(records["row:1"]["truncated"])
        self.assertEqual(records["row:1"]["used_length"], 10)
        self.assertGreater(records["row:1"]["original_length"], 10)
        # empty shortcut(row:4)은 truncate 대상 아님.
        self.assertFalse(records["row:4"]["truncated"])
        self.assertEqual(records["row:4"]["original_length"], 0)

    def test_no_truncation_under_default_limit(self) -> None:
        responses = {
            "row:1": _llm_completion('{"doc_id":"row:1","genuineness":"genuine_review","reason":"후기."}'),
            "row:2": _llm_completion('{"doc_id":"row:2","genuineness":"non_review","reason":"안내."}'),
            "row:3": _llm_completion('{"doc_id":"row:3","genuineness":"uncertain","reason":"불명."}'),
        }
        fake_urlopen, _ = _fake_urlopen_with_failures(responses, set())
        result = self._patch_config_and_run(fake_urlopen)  # default payload

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["max_input_chars"], 20_000)
        self.assertEqual(summary["truncated_docs"], 0)

        record = next(
            json.loads(line)
            for line in self.output_path.read_text(encoding="utf-8").strip().splitlines()
            if json.loads(line)["doc_id"] == "row:1"
        )
        self.assertFalse(record["truncated"])
        self.assertEqual(record["original_length"], record["used_length"])
        self.assertGreater(record["original_length"], 0)

    def test_request_failure_isolated_build_continues(self) -> None:
        # row:1이 LLOA 400으로 실패해도 build는 죽지 않고, 해당 doc만 uncertain 격리.
        responses = {
            "row:2": _llm_completion('{"doc_id":"row:2","genuineness":"non_review","reason":"안내."}'),
            "row:3": _llm_completion('{"doc_id":"row:3","genuineness":"uncertain","reason":"불명."}'),
        }
        fake_urlopen, _ = _fake_urlopen_with_failures(responses, {"row:1"})
        result = self._patch_config_and_run(fake_urlopen)  # build가 raise하지 않아야 함

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["request_failures"], 1)
        self.assertEqual(summary["parse_failures"], 0)
        # row:1(격리 uncertain) + row:3(uncertain) = 2
        self.assertEqual(summary["tier_counts"]["uncertain"], 2)
        # row:2(non_review) + row:4(empty) = 2
        self.assertEqual(summary["tier_counts"]["non_review"], 2)
        self.assertEqual(summary["processed_row_count"], 4)

        records = {
            json.loads(line)["doc_id"]: json.loads(line)
            for line in self.output_path.read_text(encoding="utf-8").strip().splitlines()
        }
        self.assertEqual(records["row:1"]["genuineness"], "uncertain")
        self.assertEqual(records["row:1"]["source"], "lloa_request_failure")
        self.assertGreater(records["row:1"]["original_length"], 0)

    def test_high_request_failure_rate_aborts_build(self) -> None:
        # silverone 2026-06-08 — LLOA 다운 등으로 실패율이 임계(0.5) 이상이면 build를
        # fail-loud로 중단한다(전부 uncertain "완료"로 덮지 않음). row:1~3 LLOA 실패
        # = 3/4(75%) >= 50% → RuntimeError. output 파일도 쓰지 않는다.
        responses = {}
        fake_urlopen, _ = _fake_urlopen_with_failures(responses, {"row:1", "row:2", "row:3"})
        with self.assertRaisesRegex(RuntimeError, "LLOA 실패율"):
            self._patch_config_and_run(fake_urlopen)
        self.assertFalse(self.output_path.exists() and self.output_path.read_text(encoding="utf-8").strip())

    def test_resolve_max_input_chars_priority(self) -> None:
        import os
        from python_ai_worker.dataset_build.doc_genuineness import (
            _resolve_max_input_chars,
            _DEFAULT_MAX_INPUT_CHARS,
            _DOC_GENUINENESS_MAX_INPUT_CHARS_ENV,
        )

        # payload > env > default
        with patch.dict(os.environ, {_DOC_GENUINENESS_MAX_INPUT_CHARS_ENV: "5000"}):
            self.assertEqual(_resolve_max_input_chars({"max_input_chars": 123}), 123)
            self.assertEqual(_resolve_max_input_chars({}), 5000)
            # invalid env → default
        with patch.dict(os.environ, {_DOC_GENUINENESS_MAX_INPUT_CHARS_ENV: "0"}):
            self.assertEqual(_resolve_max_input_chars({}), _DEFAULT_MAX_INPUT_CHARS)
        # invalid payload → default (env 없음)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_DOC_GENUINENESS_MAX_INPUT_CHARS_ENV, None)
            self.assertEqual(
                _resolve_max_input_chars({"max_input_chars": -5}), _DEFAULT_MAX_INPUT_CHARS
            )


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
