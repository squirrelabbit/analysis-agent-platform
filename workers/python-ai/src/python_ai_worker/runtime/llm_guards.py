from __future__ import annotations

"""LLM call guards: retry policy for transient Anthropic failures.

Wraps the raw Anthropic client invocations so that 429, 5xx, and connection
errors are retried with exponential backoff up to a configurable attempt
limit. The wrapper sits at the ``_create_json_*_logged`` layer in
``runtime/llm.py`` so the underlying ``AnthropicClient`` stays a thin HTTP
client.

Token-budget enforcement was previously attempted at this layer via a
contextvar ledger, but that scope was per-HTTP-request, not per-execution.
Codex adversarial review surfaced that an execution can issue many skill
HTTP calls and the per-request ledger reset every time, undermining the
budget guard. Execution-wide ceiling enforcement was therefore moved to the
Go control plane (``PythonAIClient.Run``) where the multi-step orchestration
already lives and per-step usage is collected from the response payload.
"""

import socket
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.error import HTTPError, URLError

from ..obs import get

_LOG = get("llm.guards")

_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class LLMRetryExhausted(RuntimeError):
    """Raised when retries are exhausted without a successful call."""


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base_delay_sec: float
    max_delay_sec: float

    def normalized(self) -> "RetryPolicy":
        base = max(0.0, float(self.base_delay_sec))
        return RetryPolicy(
            max_attempts=max(1, int(self.max_attempts)),
            base_delay_sec=base,
            max_delay_sec=max(base, max(0.0, float(self.max_delay_sec))),
        )

    def delay_for(self, attempt: int) -> float:
        if attempt <= 0 or self.base_delay_sec <= 0:
            return 0.0
        delay = self.base_delay_sec * (2 ** (attempt - 1))
        return min(delay, self.max_delay_sec)


def is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, HTTPError):
        return int(exc.code or 0) in _RETRYABLE_HTTP_STATUSES
    if isinstance(exc, URLError):
        return True
    if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError)):
        return True
    return False


def with_retry(
    operation: str,
    fn: Callable[[], Any],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """Call ``fn`` with retry on transient errors.

    Non-retryable exceptions are re-raised immediately. After
    ``max_attempts`` retryable failures, ``LLMRetryExhausted`` wraps the last
    error so callers can distinguish "ran out of retries" from "instant fail".
    """
    normalized = policy.normalized()
    last_error: BaseException | None = None
    for attempt in range(1, normalized.max_attempts + 1):
        try:
            return fn()
        except BaseException as exc:  # noqa: BLE001 — we re-raise after policy decision
            last_error = exc
            if not is_retryable_exception(exc):
                raise
            if attempt >= normalized.max_attempts:
                _LOG.warning(
                    "llm.retry.exhausted",
                    operation=operation,
                    attempts=attempt,
                    error_category=type(exc).__name__,
                )
                raise LLMRetryExhausted(
                    f"{operation} retries exhausted after {attempt} attempts: {exc}"
                ) from exc
            delay = normalized.delay_for(attempt)
            _LOG.warning(
                "llm.retry.scheduled",
                operation=operation,
                attempt=attempt,
                next_delay_sec=round(delay, 3),
                error_category=type(exc).__name__,
            )
            if delay > 0:
                sleep(delay)
    # Defensive: should never reach here unless max_attempts<=0
    raise LLMRetryExhausted(f"{operation} retries exhausted (no attempt executed)") from last_error


__all__ = [
    "LLMRetryExhausted",
    "RetryPolicy",
    "is_retryable_exception",
    "with_retry",
]
