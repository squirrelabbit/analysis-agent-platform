"""Unit tests for python_ai_worker.obs.decorators.skill_handler."""
from __future__ import annotations

import structlog.testing

from python_ai_worker.obs.decorators import _summarize_input, _summarize_output, skill_handler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_skill(fn_name: str = "run_test_skill", *, return_value: dict | None = None, raise_exc: Exception | None = None):
    """Build a decorated skill function with a configurable body."""

    def _inner(payload: dict) -> dict:
        if raise_exc is not None:
            raise raise_exc
        return return_value or {"artifact": {"rows": [{"id": 1}, {"id": 2}], "status": "ok"}, "notes": []}

    _inner.__name__ = fn_name
    return skill_handler("python-ai")(_inner)


# ---------------------------------------------------------------------------
# Test: successful execution (completed event)
# ---------------------------------------------------------------------------

def test_skill_handler_emits_started_and_completed():
    skill = _make_skill(return_value={"artifact": {"rows": [1, 2, 3]}, "notes": []})
    with structlog.testing.capture_logs() as cap:
        result = skill({"dataset_name": "t.parquet", "text_column": "text"})

    assert result["artifact"]["rows"] == [1, 2, 3]

    events = [e["event"] for e in cap]
    assert "skill.executed.started" in events, f"events: {events}"
    assert "skill.executed.completed" in events, f"events: {events}"
    assert "skill.executed.failed" not in events

    started = next(e for e in cap if e["event"] == "skill.executed.started")
    assert started["skill_name"] == "run_test_skill"
    assert started["runtime_layer"] == "python-ai"
    assert "input_shape" in started

    completed = next(e for e in cap if e["event"] == "skill.executed.completed")
    assert completed["skill_name"] == "run_test_skill"
    assert isinstance(completed["duration_ms"], int)
    assert completed["duration_ms"] >= 0
    assert "output_shape" in completed


# ---------------------------------------------------------------------------
# Test: failure execution (failed event + exception re-raised)
# ---------------------------------------------------------------------------

def test_skill_handler_emits_failed_on_exception():
    skill = _make_skill(raise_exc=ValueError("bad input"))
    with structlog.testing.capture_logs() as cap:
        try:
            skill({"dataset_name": "t.parquet"})
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError to propagate")

    events = [e["event"] for e in cap]
    assert "skill.executed.started" in events
    assert "skill.executed.failed" in events
    assert "skill.executed.completed" not in events

    failed = next(e for e in cap if e["event"] == "skill.executed.failed")
    assert failed["error_category"] == "ValueError"
    assert isinstance(failed["duration_ms"], int)
    assert failed["duration_ms"] >= 0


# ---------------------------------------------------------------------------
# Test: return value is preserved unchanged
# ---------------------------------------------------------------------------

def test_skill_handler_preserves_return_value():
    expected = {"artifact": {"result": "ok", "score": 42}, "notes": ["n1"]}
    skill = _make_skill(return_value=expected)
    with structlog.testing.capture_logs():
        result = skill({})
    assert result == expected


# ---------------------------------------------------------------------------
# Test: functools.wraps preserves original function name
# ---------------------------------------------------------------------------

def test_skill_handler_preserves_function_name():
    def run_named_skill(payload: dict) -> dict:
        return {}

    wrapped = skill_handler("python-ai")(run_named_skill)
    assert wrapped.__name__ == "run_named_skill"


# ---------------------------------------------------------------------------
# Test: _summarize_input
# ---------------------------------------------------------------------------

def test_summarize_input_filters_prior_artifacts():
    payload = {"dataset_name": "f.parquet", "text_column": "text", "prior_artifacts": {"x": {}}}
    result = _summarize_input(payload)
    assert "prior_artifacts" not in result
    assert "dataset_name" in result
    assert "text_column" in result


def test_summarize_input_empty():
    assert _summarize_input({}) == "keys=[]"


def test_summarize_input_non_dict():
    assert _summarize_input(None) == "empty"  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Test: _summarize_output
# ---------------------------------------------------------------------------

def test_summarize_output_row_count_from_rows_key():
    artifact = {"rows": [1, 2, 3], "status": "ok"}
    result = _summarize_output(artifact)
    assert "row_count=3" in result


def test_summarize_output_no_list_key():
    artifact = {"summary": "ok", "count": 5}
    result = _summarize_output(artifact)
    assert "columns=" in result


def test_summarize_output_empty_dict():
    assert _summarize_output({}) == "artifact_ok"


def test_summarize_output_non_dict():
    assert _summarize_output(None) == "empty"  # type: ignore[arg-type]
