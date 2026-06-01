from __future__ import annotations

import json
import unittest

from python_ai_worker.clients.anthropic import (
    AnthropicClient,
    AnthropicConfig,
    AnthropicResponseParseError,
    _strict_object_schema,
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


class AnthropicClientTests(unittest.TestCase):
    def test_create_json_parses_text_block(self) -> None:
        captured_request = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured_request["url"] = req.full_url
            captured_request["headers"] = dict(req.headers)
            captured_request["body"] = json.loads(req.data.decode("utf-8"))
            captured_request["timeout"] = timeout
            return _FakeResponse({"content": [{"type": "text", "text": '{"ok": true}'}]})

        client = AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

        result = client.create_json(prompt="hello", schema={"type": "object"})

        self.assertEqual(result, {"ok": True})
        self.assertEqual(captured_request["url"], "https://api.anthropic.com/v1/messages")
        self.assertEqual(captured_request["body"]["model"], "claude-sonnet-4-6")
        self.assertEqual(captured_request["headers"]["X-api-key"], "test-key")
        self.assertEqual(captured_request["headers"]["Anthropic-version"], "2023-06-01")

    def test_create_json_response_preserves_usage(self) -> None:
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(
                {
                    "content": [{"type": "text", "text": '{"ok": true}'}],
                    "usage": {
                        "input_tokens": 120,
                        "output_tokens": 40,
                    },
                }
            )

        client = AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

        result = client.create_json_response(prompt="hello", schema={"type": "object"})

        self.assertEqual(result.body, {"ok": True})
        self.assertEqual(result.usage["input_tokens"], 120)
        self.assertEqual(result.usage["output_tokens"], 40)

    def test_system_block_is_attached_with_cache_control_when_requested(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse({"content": [{"type": "text", "text": '{"ok": true}'}]})

        client = AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

        client.create_json(
            prompt="user input",
            schema={"type": "object"},
            system="cacheable system rules",
            cache_system=True,
        )

        self.assertEqual(captured["body"]["system"], [
            {
                "type": "text",
                "text": "cacheable system rules",
                "cache_control": {"type": "ephemeral"},
            }
        ])

    def test_system_block_omits_cache_control_when_caching_disabled(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse({"content": [{"type": "text", "text": '{"ok": true}'}]})

        client = AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

        client.create_json(
            prompt="user input",
            schema={"type": "object"},
            system="rules",
            cache_system=False,
        )

        self.assertEqual(captured["body"]["system"], [
            {"type": "text", "text": "rules"}
        ])

    def test_no_system_block_when_system_is_blank(self) -> None:
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse({"content": [{"type": "text", "text": '{"ok": true}'}]})

        client = AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

        client.create_json(prompt="user input", schema={"type": "object"})

        self.assertNotIn("system", captured["body"])


class AnthropicResponseParseErrorTests(unittest.TestCase):
    """Lock test for 5/6 진단 가시화. issue_evidence_summary가 매번 다른
    모양 (21초 JSONDecodeError vs 90초 timeout)으로 실패할 때 raw text +
    stop_reason을 caller에 보존해 obs warning에 dump 가능하게 한다.
    """

    def _client_with(self, payload: dict) -> AnthropicClient:
        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _FakeResponse(payload)

        return AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

    def test_invalid_json_raises_with_raw_text_and_stop_reason(self) -> None:
        # Anthropic이 max_tokens hit 등으로 잘린 JSON을 반환할 때 raw text와
        # stop_reason이 caller에 노출되어야 함. festival v4 trace에서
        # JSONDecodeError가 raw 정보 없이 raise되어 fallback로만 빠지던 부채.
        client = self._client_with({
            "content": [{"type": "text", "text": '{"summary": "잘린 응답'}],
            "stop_reason": "max_tokens",
        })
        with self.assertRaises(AnthropicResponseParseError) as ctx:
            client.create_json_response(prompt="x", schema={"type": "object"})
        self.assertIn("not parseable", str(ctx.exception))
        self.assertEqual(ctx.exception.stop_reason, "max_tokens")
        self.assertIn("잘린 응답", ctx.exception.raw_text)

    def test_empty_text_block_also_carries_stop_reason(self) -> None:
        # tool_use만 있고 text block이 없는 응답 — stop_reason도 caller가
        # 보고 분기 가능해야 함.
        client = self._client_with({
            "content": [],
            "stop_reason": "tool_use",
        })
        with self.assertRaises(AnthropicResponseParseError) as ctx:
            client.create_json_response(prompt="x", schema={"type": "object"})
        self.assertEqual(ctx.exception.stop_reason, "tool_use")
        self.assertIn("did not contain text blocks", str(ctx.exception))

    def test_response_carries_stop_reason_on_success(self) -> None:
        # 정상 응답일 때도 stop_reason을 노출 — 운영자가 "정상이지만
        # max_tokens에 닿았는지" 판단 가능.
        client = self._client_with({
            "content": [{"type": "text", "text": '{"ok": true}'}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 50, "output_tokens": 30},
        })
        result = client.create_json_response(prompt="x", schema={"type": "object"})
        self.assertEqual(result.body, {"ok": True})
        self.assertEqual(result.stop_reason, "end_turn")


class StrictObjectSchemaTests(unittest.TestCase):
    """Lock test for the Anthropic structured-output strict-mode invariant.

    Anthropic's ``output_config.format.schema`` returns HTTP 400 when any
    nested object schema lacks ``additionalProperties: false``. The
    ``_strict_object_schema`` helper normalizes schemas before send-off so
    callers don't have to repeat the field on every nested object. These
    tests are the regression net for the 2026-04-30 silent regression where
    every LLM-backed skill (planner, evidence, final answer) failed with
    HTTPError because planner_schema's ``inputs`` field had
    ``additionalProperties: True``.
    """

    def test_top_level_object_gets_additional_properties_false(self) -> None:
        normalized = _strict_object_schema({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
        })
        self.assertEqual(normalized["additionalProperties"], False)

    def test_nested_object_in_properties_gets_normalized(self) -> None:
        normalized = _strict_object_schema({
            "type": "object",
            "properties": {
                "outer": {
                    "type": "object",
                    "properties": {"x": {"type": "string"}},
                },
            },
        })
        self.assertEqual(normalized["additionalProperties"], False)
        self.assertEqual(
            normalized["properties"]["outer"]["additionalProperties"], False
        )

    def test_array_items_object_gets_normalized(self) -> None:
        normalized = _strict_object_schema({
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"x": {"type": "string"}},
            },
        })
        self.assertEqual(normalized["items"]["additionalProperties"], False)

    def test_explicit_additional_properties_true_is_overridden_to_false(self) -> None:
        # This is the exact pattern that caused the 2026-04-30 regression:
        # planner_schema had ``inputs: {additionalProperties: True}`` to allow
        # dynamic skill input fields, but Anthropic strict mode forbids true.
        normalized = _strict_object_schema({
            "type": "object",
            "properties": {
                "inputs": {
                    "type": "object",
                    "properties": {"top_n": {"type": "integer"}},
                    "additionalProperties": True,
                },
            },
        })
        self.assertEqual(
            normalized["properties"]["inputs"]["additionalProperties"], False
        )

    def test_oneof_anyof_allof_branches_normalized(self) -> None:
        normalized = _strict_object_schema({
            "anyOf": [
                {"type": "object", "properties": {"a": {"type": "string"}}},
                {"type": "string"},
            ],
        })
        self.assertEqual(normalized["anyOf"][0]["additionalProperties"], False)
        # non-object branch passes through unchanged
        self.assertEqual(normalized["anyOf"][1], {"type": "string"})

    def test_non_object_schema_passes_through(self) -> None:
        self.assertEqual(
            _strict_object_schema({"type": "string"}),
            {"type": "string"},
        )

    def test_idempotent(self) -> None:
        once = _strict_object_schema({
            "type": "object",
            "properties": {
                "outer": {
                    "type": "object",
                    "properties": {"x": {"type": "string"}},
                },
            },
        })
        twice = _strict_object_schema(once)
        self.assertEqual(once, twice)

    def test_does_not_mutate_input(self) -> None:
        original = {
            "type": "object",
            "properties": {"x": {"type": "string"}},
            "additionalProperties": True,
        }
        snapshot = json.dumps(original, sort_keys=True)
        _strict_object_schema(original)
        self.assertEqual(json.dumps(original, sort_keys=True), snapshot)


class CreateJsonResponseSchemaInjectionTests(unittest.TestCase):
    """Verify the strict-mode helper is actually wired into the request path."""

    def test_payload_schema_has_additional_properties_false_on_nested(self) -> None:
        captured = {}

        def fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeResponse({"content": [{"type": "text", "text": '{"ok": true}'}]})

        client = AnthropicClient(
            AnthropicConfig(
                api_key="test-key",
                model="claude-sonnet-4-6",
                api_url="https://api.anthropic.com/v1/messages",
                version="2023-06-01",
                max_tokens=512,
                timeout_sec=12,
            ),
            urlopen=fake_urlopen,
        )

        # Mirror planner_schema's pre-fix shape: inputs has additionalProperties: True.
        client.create_json(
            prompt="planner",
            schema={
                "type": "object",
                "properties": {
                    "inputs": {
                        "type": "object",
                        "properties": {"top_n": {"type": "integer"}},
                        "additionalProperties": True,
                    },
                },
            },
        )

        sent_schema = captured["body"]["output_config"]["format"]["schema"]
        self.assertEqual(sent_schema["additionalProperties"], False)
        self.assertEqual(
            sent_schema["properties"]["inputs"]["additionalProperties"], False
        )


if __name__ == "__main__":
    unittest.main()
