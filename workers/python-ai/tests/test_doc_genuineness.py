"""dataset_doc_genuineness skill — 3-tier 분류 + LLOA mock 단위 테스트.

ADR-017 / 5/19 결정. LLOA client는 urlopen 주입 패턴(test_lloa_client.py와
동일)으로 mock한다. fixture: cleaned doc 4건 (genuine_review / non_review /
uncertain / empty).
"""
from __future__ import annotations

import io
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from python_ai_worker.dataset_build.doc_genuineness import (
    _DEFAULT_CONCURRENCY,
    _DOC_GENUINENESS_CONCURRENCY_ENV,
    _MAX_CONCURRENCY,
    _resolve_concurrency,
)


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




# ===== concurrency 잠금 (구 test_doc_genuineness_concurrency.py 병합 2026-06-16) =====
# _llm_completion 은 위 정의 재사용 (토큰값은 어느 테스트도 assert 안 함).

def _fake_urlopen_factory_with_delay(responses_by_doc, delay_sec: float = 0.05):
    """LLOA 호출마다 delay를 주고 호출된 thread name을 기록 — 병렬성 검증용."""
    call_log: list[dict] = []
    log_lock = threading.Lock()

    class _Resp:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    def _fake(req, timeout=None):
        body = json.loads(req.data.decode("utf-8"))
        user_text = body["messages"][1]["content"]
        user_obj = json.loads(user_text)
        doc_id = user_obj["doc_id"]
        with log_lock:
            call_log.append({
                "doc_id": doc_id,
                "thread": threading.current_thread().name,
                "started_at": time.monotonic(),
            })
        time.sleep(delay_sec)
        if doc_id not in responses_by_doc:
            raise AssertionError(f"unexpected doc_id in test: {doc_id}")
        return _Resp(responses_by_doc[doc_id])

    return _fake, call_log


class DocGenuinenessConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "doc_genuineness.jsonl"
        # 8 docs (concurrency=8과 동일 — full saturation 검증)
        self.rows = [
            {"row_id": f"row:{i}", "cleaned_text": f"본문 {i}번째 doc의 실제 후기 텍스트 내용입니다."}
            for i in range(8)
        ]
        with self.clean_path.open("w", encoding="utf-8") as f:
            for row in self.rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _payload(self, **overrides) -> dict:
        payload = {
            "dataset_version_id": "dvid:test",
            "clean_artifact_ref": str(self.clean_path),
            "output_path": str(self.output_path),
            "doc_genuineness": {
                "subject_type": "festival",
                "subject_name": "강릉 국가유산야행",
                "subject_aliases": ["문화유산야행"],
                "recruitment_keywords": [],
            },
        }
        payload.update(overrides)
        return payload

    def _run_with_concurrency(self, urlopen_fn, payload_overrides=None):
        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig

        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=2048,
            lloa_timeout_sec=30,
            lloa_reasoning_effort="low",
            lloa_prepend_no_think=True,
        )

        original_init = doc_genuineness.LloaClient.__init__

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=urlopen_fn)

        payload = self._payload(**(payload_overrides or {}))
        with patch.object(doc_genuineness, "load_config", return_value=fake_config), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            return doc_genuineness.run_dataset_doc_genuineness(payload)

    def test_default_concurrency_exposed_in_summary(self):
        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"genuine_review","reason":"r{i}"}}'
            )
            for i in range(8)
        }
        urlopen_fn, _ = _fake_urlopen_factory_with_delay(responses, delay_sec=0)
        result = self._run_with_concurrency(urlopen_fn)
        summary = result["artifact"]["summary"]
        self.assertEqual(summary["concurrency"], 8, "default concurrency must be 8")
        self.assertEqual(summary["reasoning_effort"], "low")
        self.assertEqual(summary["processed_row_count"], 8)
        self.assertEqual(summary["parse_failures"], 0)

    def test_payload_concurrency_override(self):
        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"non_review","reason":"r"}}'
            )
            for i in range(8)
        }
        urlopen_fn, _ = _fake_urlopen_factory_with_delay(responses, delay_sec=0)
        result = self._run_with_concurrency(urlopen_fn, payload_overrides={"concurrency": 2})
        self.assertEqual(result["artifact"]["summary"]["concurrency"], 2)

    def test_output_preserves_original_row_order_under_concurrency(self):
        # 의도적으로 LLOA 호출에 delay를 주고 ordering 확인.
        # ThreadPoolExecutor는 as_completed로 처리하지만 jsonl write는 원본
        # row 순서로 한다.
        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"genuine_review","reason":"r{i}"}}'
            )
            for i in range(8)
        }
        urlopen_fn, log = _fake_urlopen_factory_with_delay(responses, delay_sec=0.02)
        self._run_with_concurrency(urlopen_fn)

        # 출력 파일 line 순서 == 원본 row 순서.
        lines = self.output_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 8)
        for idx, line in enumerate(lines):
            record = json.loads(line)
            self.assertEqual(record["doc_id"], f"row:{idx}",
                             f"line {idx} doc_id mismatch — concurrency가 order를 깨면 안 됨")

        # 병렬 호출이 실제로 일어났는지 — 동일 thread만 쓰면 sequential과 동일.
        unique_threads = {entry["thread"] for entry in log}
        self.assertGreater(len(unique_threads), 1,
                           f"concurrency 적용 안 됨 — 단일 thread만 사용됨: {unique_threads}")

    def test_empty_docs_skipped_from_lloa_calls(self):
        # rows[7]을 empty로 만들면 LLOA 호출은 7건만 일어나야.
        empty_row_path = Path(self.tmpdir.name) / "clean_with_empty.jsonl"
        rows_with_empty = self.rows[:7] + [{"row_id": "row:7", "cleaned_text": ""}]
        with empty_row_path.open("w", encoding="utf-8") as f:
            for row in rows_with_empty:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"genuine_review","reason":"r"}}'
            )
            for i in range(7)
        }
        urlopen_fn, log = _fake_urlopen_factory_with_delay(responses, delay_sec=0)

        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig
        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=2048,
            lloa_timeout_sec=30,
            lloa_reasoning_effort="low",
            lloa_prepend_no_think=True,
        )
        original_init = doc_genuineness.LloaClient.__init__

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=urlopen_fn)

        payload = self._payload(clean_artifact_ref=str(empty_row_path))
        with patch.object(doc_genuineness, "load_config", return_value=fake_config), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            result = doc_genuineness.run_dataset_doc_genuineness(payload)

        # 7건만 LLOA에 도달, 1건은 empty shortcut.
        self.assertEqual(len(log), 7)
        self.assertNotIn("row:7", {e["doc_id"] for e in log})

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["processed_row_count"], 8)
        # row:7 empty shortcut + row:0~6 genuine_review
        self.assertEqual(summary["tier_counts"]["genuine_review"], 7)
        self.assertEqual(summary["tier_counts"]["non_review"], 1)



# ===== concurrency env fallback 잠금 (구 test_doc_genuineness_concurrency_env.py) =====

class ResolveConcurrencyEnvTests(unittest.TestCase):
    def setUp(self) -> None:
        # 다른 test에서 set한 env가 새지 않도록 항상 깨끗하게 시작.
        self._saved_env = os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)

    def tearDown(self) -> None:
        os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)
        if self._saved_env is not None:
            os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = self._saved_env

    def test_env_absent_uses_default(self):
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        self.assertEqual(_DEFAULT_CONCURRENCY, 8, "default constant 변경 잠금")

    def test_env_overrides_default(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        self.assertEqual(_resolve_concurrency({}), 4)

    def test_payload_overrides_env(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        self.assertEqual(_resolve_concurrency({"concurrency": 2}), 2)

    def test_env_invalid_falls_back_to_default(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "abc"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = ""
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "3.5"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)

    def test_env_zero_or_negative_falls_back_to_default(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "0"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "-3"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)

    def test_env_too_large_caps_at_max(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "999"
        self.assertEqual(_resolve_concurrency({}), _MAX_CONCURRENCY)
        self.assertEqual(_MAX_CONCURRENCY, 32, "max cap 변경 잠금")

    def test_payload_invalid_then_env_fallback(self):
        # payload가 invalid면 env로 fallback (payload value 없는 것으로 취급).
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "5"
        self.assertEqual(_resolve_concurrency({"concurrency": -1}), 5)
        self.assertEqual(_resolve_concurrency({"concurrency": "abc"}), 5)
        self.assertEqual(_resolve_concurrency({"concurrency": 0}), 5)

    def test_payload_too_large_caps_at_max(self):
        self.assertEqual(_resolve_concurrency({"concurrency": 999}), _MAX_CONCURRENCY)

    def test_payload_bool_rejected(self):
        # bool은 int subclass라 silent로 1/0이 될 위험 — reject 확인.
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "5"
        self.assertEqual(_resolve_concurrency({"concurrency": True}), 5)
        self.assertEqual(_resolve_concurrency({"concurrency": False}), 5)


class SummaryReflectsResolvedConcurrencyTests(unittest.TestCase):
    """run_dataset_doc_genuineness가 _resolve_concurrency 결과를
    summary.concurrency에 그대로 기록하는지 fixture로 확인.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "doc_genuineness.jsonl"
        # 1 doc — concurrency 값 잠금만 확인하면 충분.
        with self.clean_path.open("w", encoding="utf-8") as f:
            f.write(json.dumps(
                {"row_id": "row:0", "cleaned_text": "본문 텍스트."},
                ensure_ascii=False,
            ) + "\n")
        self._saved_env = os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)

    def tearDown(self) -> None:
        os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)
        if self._saved_env is not None:
            os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = self._saved_env

    def _payload(self, **overrides) -> dict:
        payload = {
            "dataset_version_id": "dvid:test",
            "clean_artifact_ref": str(self.clean_path),
            "output_path": str(self.output_path),
            "doc_genuineness": {
                "subject_type": "festival",
                "subject_name": "강릉 국가유산야행",
                "subject_aliases": ["문화유산야행"],
                "recruitment_keywords": [],
            },
        }
        payload.update(overrides)
        return payload

    def _run(self, urlopen_fn, payload_overrides=None):
        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig

        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=2048,
            lloa_timeout_sec=30,
            lloa_reasoning_effort="low",
            lloa_prepend_no_think=True,
        )
        original_init = doc_genuineness.LloaClient.__init__

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=urlopen_fn)

        payload = self._payload(**(payload_overrides or {}))
        with patch.object(doc_genuineness, "load_config", return_value=fake_config), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            return doc_genuineness.run_dataset_doc_genuineness(payload)

    def _stub_urlopen(self):
        class _Resp:
            def __init__(self, payload: dict) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return json.dumps(self._payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return None

        def _fake(req, timeout=None):
            return _Resp({
                "choices": [{
                    "message": {"content": '{"doc_id":"row:0","genuineness":"genuine_review","reason":"r"}', "reasoning_content": ""},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            })

        return _fake

    def test_summary_records_env_concurrency(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        result = self._run(self._stub_urlopen())
        self.assertEqual(result["artifact"]["summary"]["concurrency"], 4)

    def test_summary_records_capped_value(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "999"
        result = self._run(self._stub_urlopen())
        self.assertEqual(result["artifact"]["summary"]["concurrency"], _MAX_CONCURRENCY)

    def test_summary_records_payload_over_env(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        result = self._run(self._stub_urlopen(), payload_overrides={"concurrency": 2})
        self.assertEqual(result["artifact"]["summary"]["concurrency"], 2)



# ===== verify mode 잠금 / ADR-026 (구 test_doc_genuineness_verify.py) =====

def _completion(content: str) -> dict:
    return {
        "choices": [{"message": {"content": content, "reasoning_content": ""}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


def _fake_config():
    from python_ai_worker.config import WorkerConfig

    return WorkerConfig(
        lloa_api_key="k",
        lloa_api_url="http://lloa.example/v1/chat/completions",
        lloa_model="model-a",
        lloa_max_tokens=2048,
        lloa_timeout_sec=30,
        lloa_reasoning_effort=None,
        lloa_prepend_no_think=True,
    )


class DocGenuinenessVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        rows = [
            {"row_id": "d1", "cleaned_text": "강릉야행 다녀왔어요. 야경 환상적."},  # 합의(genuine)
            {"row_id": "d2", "cleaned_text": "행사 안내문입니다. 일정 확인."},      # 불일치 → judge
            {"row_id": "d3", "cleaned_text": ""},                                   # empty shortcut
        ]
        with self.clean.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    def _run(self, *, judge_chosen: str, judge_label: str | None, judge_conf: float):
        from python_ai_worker.dataset_build import doc_genuineness_verify as v

        # model별 응답: a/b classify는 doc_id로, judge는 candidate 포함 여부로 구분.
        def make_urlopen(model: str):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                body = json.loads(req.data.decode("utf-8"))
                user = body["messages"][1]["content"]
                obj = json.loads(user)
                if "candidate_1" in obj:  # judge 호출
                    return _Resp(_completion(json.dumps({
                        "chosen": judge_chosen,
                        "final_label": judge_label,
                        "confidence": judge_conf,
                        "reason": "judge reason",
                    }, ensure_ascii=False)))
                doc_id = obj["doc_id"]
                # d1은 둘 다 genuine, d2는 a=genuine b=non_review로 불일치.
                if doc_id == "d1":
                    label = "genuine_review"
                else:  # d2
                    label = "genuine_review" if model == "model-a" else "non_review"
                return _Resp(_completion(json.dumps({"genuineness": label, "reason": "r"}, ensure_ascii=False)))

            return _fake

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=make_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1",
            "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out),
            "verify": True,
            "classify_models": ["model-a", "model-b"],
            "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init):
            result = v.run_dataset_doc_genuineness_verify(payload)
        records = {}
        with self.out.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line)
                    records[rec["doc_id"]] = rec
        return result, records

    def test_agreement_and_judge_accept(self) -> None:
        # judge가 candidate 중 non_review 쪽(=model-b) 채택하도록.
        # d2: a=genuine(candidate_? ), b=non. judge_label은 매핑 후 검증하므로
        # chosen=candidate_1/2 어느 쪽이든 코드가 winner.genuineness로 final 설정.
        result, recs = self._run(judge_chosen="candidate_1", judge_label="genuine_review", judge_conf=0.9)
        # d1 합의.
        self.assertEqual(recs["d1"]["resolution"], "model_agreement")
        self.assertEqual(recs["d1"]["final_label"], "genuine_review")
        self.assertFalse(recs["d1"]["needs_review"])
        self.assertFalse(recs["d1"]["is_disagreement"])
        # d3 empty shortcut.
        self.assertEqual(recs["d3"]["resolution"], "empty_text_shortcut")
        self.assertEqual(recs["d3"]["final_label"], "non_review")
        # d2 불일치 → judge.
        self.assertTrue(recs["d2"]["is_disagreement"])
        self.assertEqual(recs["d2"]["resolution"], "judge_on_disagreement")
        self.assertIn(recs["d2"]["judge_result"]["decision"], {"accept_a", "accept_b"})
        self.assertIn(recs["d2"]["final_label"], {"genuine_review", "non_review"})
        self.assertFalse(recs["d2"]["needs_review"])  # conf 0.9 >= 0.85
        # summary.
        s = result["artifact"]["summary"]
        self.assertEqual(s["mode"], "verify")
        self.assertEqual(s["agreement_count"], 1)
        self.assertEqual(s["disagreement_count"], 1)
        self.assertEqual(s["judge_count"], 1)
        self.assertEqual(s["models"], {"a": "model-a", "b": "model-b", "judge": "model-judge"})

    def test_low_confidence_needs_review(self) -> None:
        _result, recs = self._run(judge_chosen="candidate_2", judge_label="non_review", judge_conf=0.5)
        self.assertTrue(recs["d2"]["needs_review"])  # conf 0.5 < 0.85

    def test_judge_review_decision_nulls_final(self) -> None:
        _result, recs = self._run(judge_chosen="review", judge_label=None, judge_conf=0.4)
        self.assertEqual(recs["d2"]["judge_result"]["decision"], "review")
        self.assertIsNone(recs["d2"]["final_label"])
        self.assertTrue(recs["d2"]["needs_review"])

    def test_revise_third_label(self) -> None:
        _result, recs = self._run(judge_chosen="other", judge_label="uncertain", judge_conf=0.9)
        self.assertEqual(recs["d2"]["judge_result"]["decision"], "revise")
        self.assertEqual(recs["d2"]["final_label"], "uncertain")

    def _run_with_urlopen(self, make_urlopen):
        from python_ai_worker.dataset_build import doc_genuineness_verify as v

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=make_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init):
            v.run_dataset_doc_genuineness_verify(payload)
        recs = {}
        with self.out.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    recs[r["doc_id"]] = r
        return recs

    def test_partial_classify_isolation(self) -> None:
        # model-b는 항상 빈 content(실패), model-a는 정상 → partial_classify.
        def make_urlopen(model):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                obj = json.loads(json.loads(req.data.decode("utf-8"))["messages"][1]["content"])
                if "candidate_1" in obj:
                    return _Resp(_completion(json.dumps({"chosen": "candidate_1", "final_label": "genuine_review", "confidence": 0.9, "reason": "r"})))
                if model == "model-b":
                    return _Resp(_completion(""))  # 빈 content → LloaResponseParseError
                return _Resp(_completion(json.dumps({"genuineness": "genuine_review", "reason": "a ok"})))
            return _fake

        recs = self._run_with_urlopen(make_urlopen)
        for doc in ("d1", "d2"):
            self.assertEqual(recs[doc]["resolution"], "partial_classify", doc)
            self.assertEqual(recs[doc]["final_label"], "genuine_review", doc)  # a 라벨 채택
            self.assertTrue(recs[doc]["needs_review"], doc)
            self.assertIsNone(recs[doc]["model_b_result"], doc)  # b 실패

    def test_both_classify_fail_uncertain(self) -> None:
        # 두 모델 모두 빈 content → classify_error + uncertain(빈칸 아님).
        def make_urlopen(_model):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                return _Resp(_completion(""))
            return _fake

        recs = self._run_with_urlopen(make_urlopen)
        self.assertEqual(recs["d1"]["resolution"], "classify_error")
        self.assertEqual(recs["d1"]["final_label"], "uncertain")
        self.assertTrue(recs["d1"]["needs_review"])

    def test_classify_models_validation(self) -> None:
        from python_ai_worker.dataset_build import doc_genuineness_verify as v

        with patch.object(v, "load_config", return_value=_fake_config()):
            with self.assertRaises(ValueError):
                v.run_dataset_doc_genuineness_verify({
                    "dataset_version_id": "x", "clean_artifact_ref": str(self.clean),
                    "output_path": str(self.out), "verify": True,
                    "classify_models": ["only-one"],
                    "doc_genuineness": {"subject_name": "x"},
                })



class DocGenuinenessChunkAggregateTests(unittest.TestCase):
    """긴 문서 chunk aggregate 잠금 (ADR-029). opt-in(chunking=true) + cleaned_text >
    max_input_chars일 때만 chunk 경로. split_anchor_sentences를 patch해 결정론적."""

    SENTENCES = ["문장하나.", "문장둘.", "진짜방문후기셋.", "문장넷.", "문장다섯.", "문장여섯."]

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean = Path(self.tmpdir.name) / "clean.jsonl"
        self.out = Path(self.tmpdir.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "x" * 500}, ensure_ascii=False) + "\n")

    def _run(self, *, tier_fn, max_input_chars=10, chunking=True, max_chunk_sentences=2):
        from python_ai_worker.dataset_build import doc_genuineness as dg
        from python_ai_worker.config import WorkerConfig

        fake_config = WorkerConfig(
            lloa_api_key="k", lloa_api_url="http://x/v1/chat/completions", lloa_model="m",
            lloa_max_tokens=2048, lloa_timeout_sec=30, lloa_reasoning_effort=None, lloa_prepend_no_think=True,
        )

        class _Resp:
            def __init__(self, payload):
                self._p = payload

            def read(self):
                return json.dumps(self._p).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return None

        def fake_urlopen(req, timeout=None):
            user = json.loads(json.loads(req.data.decode("utf-8"))["messages"][1]["content"])
            tier = tier_fn(user["doc_text"])
            if tier is None:  # 실패 시뮬 — 빈 content → parse 실패
                return _Resp(_llm_completion(""))
            return _Resp(_llm_completion(json.dumps({"genuineness": tier, "reason": "r"}, ensure_ascii=False)))

        orig = dg.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            orig(self, config, urlopen=fake_urlopen)

        payload = {
            "dataset_version_id": "v", "clean_artifact_ref": str(self.clean), "output_path": str(self.out),
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉"},
            "chunking": chunking, "max_input_chars": max_input_chars, "max_chunk_sentences": max_chunk_sentences,
        }
        with patch.object(dg, "load_config", return_value=fake_config), \
             patch.object(dg.LloaClient, "__init__", _init), \
             patch.object(dg, "split_anchor_sentences", return_value=self.SENTENCES):
            result = dg.run_dataset_doc_genuineness(payload)
        recs = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        return result, recs

    def test_genuine_hit_wins_over_non_review(self) -> None:
        # chunk1(진짜방문후기 포함)만 genuine, 나머지 non_review → final genuine_review.
        result, recs = self._run(tier_fn=lambda t: "genuine_review" if "진짜방문후기" in t else "non_review")
        self.assertEqual(len(recs), 1)
        rec = recs[0]
        self.assertEqual(rec["genuineness"], "genuine_review")
        self.assertTrue(rec["chunked"])
        self.assertEqual(rec["chunk_count"], 3)
        self.assertEqual(rec["genuine_spans"], [{"chunk_index": 1, "sentence_start": 3, "sentence_end": 4}])
        ch = result["artifact"]["summary"]["chunking"]
        self.assertTrue(ch["enabled"])
        self.assertEqual(ch["chunked_doc_count"], 1)
        self.assertEqual(ch["chunk_count"], 3)
        self.assertEqual(ch["genuine_span_count"], 1)

    def test_all_non_review(self) -> None:
        _result, recs = self._run(tier_fn=lambda t: "non_review")
        self.assertEqual(recs[0]["genuineness"], "non_review")
        self.assertEqual(recs[0]["genuine_spans"], [])

    def test_uncertain_when_no_genuine_but_uncertain_present(self) -> None:
        _result, recs = self._run(tier_fn=lambda t: "uncertain" if "진짜방문후기" in t else "non_review")
        self.assertEqual(recs[0]["genuineness"], "uncertain")

    def test_chunk_failure_isolated(self) -> None:
        # chunk1 classify 실패 → uncertain 취급, build 계속, chunk_failure_count↑, needs_review.
        result, recs = self._run(tier_fn=lambda t: None if "진짜방문후기" in t else "non_review")
        self.assertEqual(recs[0]["genuineness"], "uncertain")  # 실패 chunk=uncertain, 나머지 non_review
        self.assertTrue(recs[0]["needs_review"])
        self.assertEqual(recs[0]["chunk_failure_count"], 1)
        self.assertEqual(result["artifact"]["summary"]["chunking"]["chunk_failure_count"], 1)

    def test_short_doc_uses_truncate_not_chunk(self) -> None:
        # max_input_chars 크게 → 500자 doc은 threshold 이하라 단일 호출(truncate 경로).
        result, recs = self._run(tier_fn=lambda t: "non_review", max_input_chars=10000)
        self.assertNotIn("chunked", recs[0])
        self.assertEqual(recs[0]["source"], "lloa")
        self.assertEqual(result["artifact"]["summary"]["chunking"]["chunked_doc_count"], 0)

    def test_chunking_off_by_default(self) -> None:
        # chunking 미지정(기본 OFF) + 긴 doc → truncate 경로(기존 계약 보존).
        _result, recs = self._run(tier_fn=lambda t: "non_review", chunking=False, max_input_chars=10)
        self.assertNotIn("chunked", recs[0])


class DocGenuinenessVerifyChunkingTests(unittest.TestCase):
    """verify 경로 chunk aggregate (ADR-029 step5). 모델별 chunk aggregate → 교차검증 +
    합의 genuine 시 genuine_spans union. chunking opt-in."""

    SENTENCES = ["문장하나.", "문장둘.", "진짜방문후기셋.", "문장넷.", "문장다섯.", "문장여섯."]

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "x" * 500}, ensure_ascii=False) + "\n")

    def test_verify_chunk_aggregate_genuine_agreement(self) -> None:
        from python_ai_worker.dataset_build import doc_genuineness as dg
        from python_ai_worker.dataset_build import doc_genuineness_verify as dgv
        from python_ai_worker.config import WorkerConfig

        cfg = WorkerConfig(
            lloa_api_key="k", lloa_api_url="http://x/v1/chat/completions", lloa_model="m",
            lloa_max_tokens=2048, lloa_timeout_sec=30, lloa_reasoning_effort=None, lloa_prepend_no_think=True,
        )

        class _Resp:
            def __init__(self, p):
                self._p = p

            def read(self):
                return json.dumps(self._p).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return None

        def fake_urlopen(req, timeout=None):
            user = json.loads(json.loads(req.data.decode("utf-8"))["messages"][1]["content"])
            tier = "genuine_review" if "진짜방문후기" in user.get("doc_text", "") else "non_review"
            return _Resp(_llm_completion(json.dumps({"genuineness": tier, "reason": "r"}, ensure_ascii=False)))

        orig = dgv.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            orig(self, config, urlopen=fake_urlopen)

        payload = {
            "dataset_version_id": "v", "clean_artifact_ref": str(self.clean), "output_path": str(self.out),
            "verify": True, "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉"},
            "chunking": True, "max_input_chars": 10, "max_chunk_sentences": 2,
        }
        with patch.object(dgv, "load_config", return_value=cfg), \
             patch.object(dgv.LloaClient, "__init__", _init), \
             patch.object(dg, "split_anchor_sentences", return_value=self.SENTENCES):
            result = dg.run_dataset_doc_genuineness(payload)  # verify=true → 위임
        rec = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()][0]
        self.assertEqual(rec["final_label"], "genuine_review")
        self.assertEqual(rec["resolution"], "model_agreement")
        self.assertTrue(rec["chunked"])
        # 두 모델 합의 genuine → union spans (chunk1 = 문장 3~4).
        self.assertEqual(rec["genuine_spans"], [{"chunk_index": 1, "sentence_start": 3, "sentence_end": 4}])
        ch = result["artifact"]["summary"]["chunking"]
        self.assertTrue(ch["enabled"])
        self.assertEqual(ch["chunked_doc_count"], 1)
        self.assertEqual(ch["genuine_span_doc_count"], 1)


if __name__ == "__main__":
    unittest.main()
