from __future__ import annotations

import socket
import unittest
from unittest.mock import MagicMock
from urllib.error import HTTPError, URLError

from python_ai_worker.runtime.llm_guards import (
    LLMRetryExhausted,
    RetryPolicy,
    is_retryable_exception,
    with_retry,
)


def _make_http_error(status: int) -> HTTPError:
    return HTTPError(url="https://example", code=status, msg="boom", hdrs=None, fp=None)


class RetryPolicyTests(unittest.TestCase):
    def test_normalized_clamps_negative_attempts(self) -> None:
        policy = RetryPolicy(max_attempts=-2, base_delay_sec=-1, max_delay_sec=-5).normalized()
        self.assertEqual(policy.max_attempts, 1)
        self.assertEqual(policy.base_delay_sec, 0.0)
        self.assertEqual(policy.max_delay_sec, 0.0)

    def test_delay_for_uses_exponential_backoff_with_cap(self) -> None:
        policy = RetryPolicy(max_attempts=5, base_delay_sec=0.5, max_delay_sec=2.0)
        self.assertEqual(policy.delay_for(1), 0.5)
        self.assertEqual(policy.delay_for(2), 1.0)
        self.assertEqual(policy.delay_for(3), 2.0)
        self.assertEqual(policy.delay_for(4), 2.0)


class IsRetryableTests(unittest.TestCase):
    def test_429_and_5xx_are_retryable(self) -> None:
        for status in (408, 429, 500, 502, 503, 504):
            self.assertTrue(is_retryable_exception(_make_http_error(status)), msg=str(status))

    def test_other_4xx_are_not_retryable(self) -> None:
        for status in (400, 401, 403, 404, 422):
            self.assertFalse(is_retryable_exception(_make_http_error(status)), msg=str(status))

    def test_url_and_socket_errors_are_retryable(self) -> None:
        self.assertTrue(is_retryable_exception(URLError("timed out")))
        self.assertTrue(is_retryable_exception(socket.timeout()))
        self.assertTrue(is_retryable_exception(TimeoutError("nope")))
        self.assertTrue(is_retryable_exception(ConnectionError("nope")))

    def test_value_error_is_not_retryable(self) -> None:
        self.assertFalse(is_retryable_exception(ValueError("bad payload")))


class WithRetryTests(unittest.TestCase):
    def test_succeeds_on_first_try_without_sleeping(self) -> None:
        sleep = MagicMock()
        result = with_retry(
            "op",
            lambda: "ok",
            policy=RetryPolicy(max_attempts=3, base_delay_sec=0.5, max_delay_sec=2.0),
            sleep=sleep,
        )
        self.assertEqual(result, "ok")
        sleep.assert_not_called()

    def test_retries_429_until_success(self) -> None:
        attempts: list[int] = []
        sleep = MagicMock()

        def fake_call() -> str:
            attempts.append(1)
            if len(attempts) < 3:
                raise _make_http_error(429)
            return "ok"

        result = with_retry(
            "op",
            fake_call,
            policy=RetryPolicy(max_attempts=4, base_delay_sec=0.1, max_delay_sec=1.0),
            sleep=sleep,
        )
        self.assertEqual(result, "ok")
        self.assertEqual(len(attempts), 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(sleep.call_args_list[0].args[0], 0.1)
        self.assertEqual(sleep.call_args_list[1].args[0], 0.2)

    def test_non_retryable_4xx_raises_immediately(self) -> None:
        sleep = MagicMock()

        def fake_call() -> None:
            raise _make_http_error(400)

        with self.assertRaises(HTTPError):
            with_retry(
                "op",
                fake_call,
                policy=RetryPolicy(max_attempts=3, base_delay_sec=0.1, max_delay_sec=1.0),
                sleep=sleep,
            )
        sleep.assert_not_called()

    def test_exhausted_retries_raise_llm_retry_exhausted(self) -> None:
        sleep = MagicMock()

        def fake_call() -> None:
            raise _make_http_error(503)

        with self.assertRaises(LLMRetryExhausted):
            with_retry(
                "op",
                fake_call,
                policy=RetryPolicy(max_attempts=2, base_delay_sec=0.1, max_delay_sec=1.0),
                sleep=sleep,
            )
        self.assertEqual(sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()
