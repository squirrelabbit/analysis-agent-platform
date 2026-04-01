from __future__ import annotations

import json
import unittest

from python_ai_worker.anthropic_client import AnthropicClient, AnthropicConfig


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


if __name__ == "__main__":
    unittest.main()
