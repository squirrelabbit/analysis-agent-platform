"""LloaClient unit tests — urlopen mock으로 HTTP/JSON 파싱 검증."""
from __future__ import annotations

import json
import unittest

from python_ai_worker.clients.lloa import (
    LloaClient,
    LloaConfig,
    LloaResponseParseError,
    _parse_json_text,
    _strip_markdown_fence,
)


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _config(api_key: str = "test-key", reasoning_effort: str | None = None,
            prepend_no_think: bool = True) -> LloaConfig:
    return LloaConfig(
        api_key=api_key,
        api_url="http://lloa.example/v1/chat/completions",
        model="wisenut/wise-lloa-max-v1.2.1",
        max_tokens=1024,
        timeout_sec=30,
        reasoning_effort=reasoning_effort,
        prepend_no_think=prepend_no_think,
    )


def _completion(content: str, *, finish_reason: str = "stop",
                reasoning: str = "", usage: dict | None = None) -> dict:
    return {
        "choices": [{
            "message": {"content": content, "reasoning_content": reasoning},
            "finish_reason": finish_reason,
        }],
        "usage": usage or {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


class LloaClientTests(unittest.TestCase):
    def test_create_json_response_object(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["url"] = req.full_url
            captured["headers"] = dict(req.headers)
            captured["body"] = json.loads(req.data.decode("utf-8"))
            captured["timeout"] = timeout
            return _FakeResponse(_completion('{"ok": true}'))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        resp = client.create_json_response(system="sys", user="hi")

        self.assertEqual(resp.body, {"ok": True})
        self.assertEqual(resp.finish_reason, "stop")
        self.assertEqual(captured["url"], "http://lloa.example/v1/chat/completions")
        self.assertEqual(captured["headers"]["Authorization"], "Bearer test-key")
        self.assertEqual(captured["body"]["model"], "wisenut/wise-lloa-max-v1.2.1")
        self.assertEqual(captured["body"]["temperature"], 0)
        self.assertEqual(captured["body"]["max_tokens"], 1024)
        # prepend_no_think=True (default) → system 첫 줄에 directive 자동 prepend
        self.assertTrue(captured["body"]["messages"][0]["content"].startswith("/no_think"))
        self.assertEqual(captured["body"]["messages"][1]["content"], "hi")
        # reasoning_effort=None → payload에 키 자체 없음
        self.assertNotIn("reasoning_effort", captured["body"])

    def test_create_json_response_array(self) -> None:
        """LLOA가 JSON array를 반환할 수 있어야 한다 (clause_label 패턴)."""
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(_completion('[{"clause":"a","sentiment":"positive"}]'))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        resp = client.create_json_response(system="sys", user="doc")

        self.assertIsInstance(resp.body, list)
        self.assertEqual(resp.body[0]["clause"], "a")

    def test_reasoning_effort_passed_through(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse(_completion('{}'))

        client = LloaClient(_config(reasoning_effort="low"), urlopen=fake_urlopen)
        client.create_json_response(system="sys", user="hi")

        self.assertEqual(captured["body"]["reasoning_effort"], "low")

    def test_prepend_no_think_skip_when_already_present(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse(_completion('{}'))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        client.create_json_response(system="/no_think\nDo the thing", user="hi")

        # prefix가 이미 있으면 중복 prepend 안 함 — `/no_think\n` 한 번만
        sys_content = captured["body"]["messages"][0]["content"]
        self.assertEqual(sys_content.count("/no_think"), 1)

    def test_prepend_no_think_disabled(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse(_completion('{}'))

        client = LloaClient(_config(prepend_no_think=False), urlopen=fake_urlopen)
        client.create_json_response(system="Plain system", user="hi")

        sys_content = captured["body"]["messages"][0]["content"]
        self.assertFalse(sys_content.startswith("/no_think"))

    def test_disabled_when_no_api_key(self) -> None:
        client = LloaClient(_config(api_key=""))
        self.assertFalse(client.is_enabled())
        with self.assertRaises(ValueError):
            client.create_json_response(system="x", user="y")

    def test_markdown_fence_strip(self) -> None:
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(_completion('```json\n{"ok": true}\n```'))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        resp = client.create_json_response(system="sys", user="hi")
        self.assertEqual(resp.body, {"ok": True})

    def test_parse_error_includes_raw_and_finish(self) -> None:
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(_completion("not json at all", finish_reason="length"))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        with self.assertRaises(LloaResponseParseError) as ctx:
            client.create_json_response(system="sys", user="hi")
        self.assertEqual(ctx.exception.finish_reason, "length")
        self.assertEqual(ctx.exception.raw_text, "not json at all")

    def test_empty_content_raises(self) -> None:
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(_completion("", finish_reason="length"))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        with self.assertRaises(LloaResponseParseError) as ctx:
            client.create_json_response(system="sys", user="hi")
        self.assertEqual(ctx.exception.finish_reason, "length")

    def test_usage_and_reasoning_preserved(self) -> None:
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(_completion(
                '{}',
                reasoning="model internal thoughts",
                usage={"prompt_tokens": 200, "completion_tokens": 50, "total_tokens": 250},
            ))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        resp = client.create_json_response(system="sys", user="hi")
        self.assertEqual(resp.usage["prompt_tokens"], 200)
        self.assertEqual(resp.reasoning, "model internal thoughts")

    def test_max_tokens_override(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse(_completion('{}'))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        client.create_json_response(system="sys", user="hi", max_tokens=512)
        self.assertEqual(captured["body"]["max_tokens"], 512)

    def test_per_call_latency_logged(self) -> None:
        # silverone 2026-06-30 — 성능 baseline의 per-call p95 측정용 호출당 로그.
        from unittest.mock import patch
        from python_ai_worker.clients import lloa

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(_completion(
                '{"ok": true}',
                usage={"prompt_tokens": 200, "completion_tokens": 50, "total_tokens": 250},
            ))

        client = LloaClient(_config(), urlopen=fake_urlopen)
        with patch.object(lloa.LOGGER, "info") as info:
            client.create_json_response(system="sys", user="hi")

        events = [c for c in info.call_args_list if c.args and c.args[0] == "lloa.call.completed"]
        self.assertEqual(len(events), 1)
        kwargs = events[0].kwargs
        self.assertIn("duration_ms", kwargs)
        self.assertIsInstance(kwargs["duration_ms"], float)
        self.assertEqual(kwargs["prompt_tokens"], 200)
        self.assertEqual(kwargs["completion_tokens"], 50)
        self.assertEqual(kwargs["total_tokens"], 250)
        self.assertEqual(kwargs["finish_reason"], "stop")


class HelperTests(unittest.TestCase):
    def test_strip_markdown_fence_plain(self) -> None:
        self.assertEqual(_strip_markdown_fence('{"a":1}'), '{"a":1}')

    def test_strip_markdown_fence_with_json_tag(self) -> None:
        self.assertEqual(_strip_markdown_fence('```json\n{"a":1}\n```'), '{"a":1}')

    def test_strip_markdown_fence_bare(self) -> None:
        self.assertEqual(_strip_markdown_fence('```\n[1,2]\n```'), '[1,2]')

    def test_parse_json_text_object(self) -> None:
        self.assertEqual(_parse_json_text('{"a":1}'), {"a": 1})

    def test_parse_json_text_array(self) -> None:
        self.assertEqual(_parse_json_text('[1, 2]'), [1, 2])

    def test_parse_json_text_with_prefix_garbage(self) -> None:
        """LLOA가 가끔 앞에 explanation 추가 → substring 추출 fallback."""
        self.assertEqual(_parse_json_text('explanation { "a": 1 }'), {"a": 1})

    def test_parse_json_text_with_array_prefix(self) -> None:
        self.assertEqual(_parse_json_text('here is array [1,2,3]'), [1, 2, 3])

    def test_parse_json_text_invalid(self) -> None:
        with self.assertRaises(json.JSONDecodeError):
            _parse_json_text("no json here")


class LloaRetryTests(unittest.TestCase):
    """LLOA HTTP retry — transient 오류는 backoff 재시도, parse 실패는 즉시 실패."""

    def _retry_config(self) -> LloaConfig:
        # base_delay=0 → sleep 없이 즉시 재시도(테스트 빠름).
        return LloaConfig(
            api_key="k", api_url="http://lloa.example/v1/chat/completions",
            model="m", max_tokens=10, timeout_sec=5,
            retry_max_attempts=3, retry_base_delay_sec=0.0, retry_max_delay_sec=0.0,
        )

    def test_retries_transient_http_error_then_succeeds(self) -> None:
        from urllib.error import HTTPError

        calls = {"n": 0}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            if calls["n"] == 1:
                raise HTTPError(req.full_url, 503, "Service Unavailable", {}, None)
            return _FakeResponse(_completion('{"genuineness": "genuine_review"}'))

        client = LloaClient(self._retry_config(), urlopen=fake_urlopen)
        resp = client.create_json_response(system="s", user="u")
        self.assertEqual(calls["n"], 2)  # 1 fail(503) + 1 success
        self.assertEqual(resp.body, {"genuineness": "genuine_review"})

    def test_parse_error_is_not_retried(self) -> None:
        calls = {"n": 0}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            return _FakeResponse(_completion("definitely not json"))

        client = LloaClient(self._retry_config(), urlopen=fake_urlopen)
        with self.assertRaises(LloaResponseParseError):
            client.create_json_response(system="s", user="u")
        self.assertEqual(calls["n"], 1)  # parse 실패는 non-retryable → 1회만


if __name__ == "__main__":
    unittest.main()
