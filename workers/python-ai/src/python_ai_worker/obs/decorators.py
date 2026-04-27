"""Skill execution logging decorators.

Usage:
    from ..obs import skill_handler

    @skill_handler("python-ai")
    def run_my_skill(payload: dict) -> dict:
        ...
"""
from __future__ import annotations

import functools
import time
from typing import Any, Callable

from .logger import get

_LOG = get("skills")


def skill_handler(runtime_layer: str) -> Callable[[Callable[..., dict]], Callable[..., dict]]:
    """Decorator that emits skill.executed.* structured events around a skill function.

    Args:
        runtime_layer: Identifies the execution environment (e.g. "python-ai").

    The decorated function must have the signature ``fn(payload: dict) -> dict``.
    structlog contextvars (request_id, etc.) are automatically included via
    merge_contextvars — no additional bind() call needed inside the decorator.
    """

    def decorator(fn: Callable[..., dict]) -> Callable[..., dict]:
        @functools.wraps(fn)
        def wrapper(payload: dict, *args: Any, **kwargs: Any) -> dict:
            skill_name = fn.__name__
            input_shape = _summarize_input(payload)
            _LOG.info(
                "skill.executed.started",
                msg="skill started",
                skill_name=skill_name,
                runtime_layer=runtime_layer,
                input_shape=input_shape,
            )
            t0 = time.monotonic()
            try:
                result = fn(payload, *args, **kwargs)
                duration_ms = int((time.monotonic() - t0) * 1000)
                output_shape = _summarize_output((result or {}).get("artifact") or {})
                _LOG.info(
                    "skill.executed.completed",
                    msg="skill completed",
                    skill_name=skill_name,
                    runtime_layer=runtime_layer,
                    duration_ms=duration_ms,
                    output_shape=output_shape,
                )
                return result
            except Exception as exc:
                duration_ms = int((time.monotonic() - t0) * 1000)
                _LOG.error(
                    "skill.executed.failed",
                    msg="skill failed",
                    skill_name=skill_name,
                    runtime_layer=runtime_layer,
                    duration_ms=duration_ms,
                    error_category=type(exc).__name__,
                )
                raise

        return wrapper

    return decorator


def _summarize_input(payload: dict) -> str:
    """Return a safe, PII-free summary of a skill input payload."""
    if not isinstance(payload, dict):
        return "empty"
    keys = sorted(k for k in payload if k not in ("prior_artifacts",))
    return f"keys=[{', '.join(keys[:8])}]"


def _summarize_output(artifact: Any) -> str:
    """Return a safe, PII-free summary of a skill output artifact."""
    if not isinstance(artifact, dict):
        return "empty"
    row_count: int | None = None
    for key in ("rows", "matches", "items", "documents", "results", "clusters", "keywords", "groups"):
        val = artifact.get(key)
        if isinstance(val, list):
            row_count = len(val)
            break
    cols = [
        k for k in artifact
        if not k.startswith("_") and k not in ("skill_name", "step_id", "step_name", "usage")
    ]
    parts: list[str] = []
    if row_count is not None:
        parts.append(f"row_count={row_count}")
    if cols:
        parts.append(f"columns=[{', '.join(cols[:6])}]")
    return ", ".join(parts) if parts else "artifact_ok"
