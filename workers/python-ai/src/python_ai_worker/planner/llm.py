from __future__ import annotations

"""plan_v2 LLM caller — user_question → plan_v2 JSON 생성.

silverone 2026-05-21 6단계 결정:
- provider/model: 기존 Anthropic 기본 (env 재사용 — hardcode X)
- JSON parse 실패 시 1회 repair retry — 원 응답 + parse error 전달
- validator 실패 시 1회 self-correct retry — validator issue list 전달
- dataset-specific docs 컬럼은 호출자가 ``docs_extra_columns``로 주입
- usage는 응답 metadata에만 누적. control plane 통합은 후속.
"""

import copy
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from ..obs import get
from ..runtime.llm import _create_json_response_logged
from .prompt import DatasetSpecificColumn, render_planner_prompt
from .validator import ValidationIssue, collect_plan_issues


_LOG = get("planner.llm")


class PlannerCallError(RuntimeError):
    """planner LLM 호출 흐름의 base error. ``attempts``에 시도별 진단 포함."""

    def __init__(self, message: str, attempts: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.attempts = list(attempts or [])


class PlannerParseError(PlannerCallError):
    """LLM 응답이 plan JSON으로 parse되지 않음 (repair retry까지 실패)."""


class PlannerValidationError(PlannerCallError):
    """parse는 됐지만 validator 통과 못 함 (self-correct retry까지 실패).

    ``issues``에 마지막 validator issue 목록을 둔다.
    """

    def __init__(self, message: str, attempts: list[dict[str, Any]], issues: list[ValidationIssue]) -> None:
        super().__init__(message, attempts)
        self.issues = list(issues)


@dataclass(frozen=True)
class PlannerResult:
    plan: dict[str, Any]
    prompt_version: str
    attempts: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)


# LLM은 plan 본문을 ``plan_json`` 문자열 한 필드로만 반환한다. plan_v2 schema가
# nested + skill별 free dict인 ``params``를 가져 Anthropic structured-output
# grammar 한도를 넘기 때문 (기존 planner의 ``inputs: string`` 패턴과 동일).
# JSON parse는 Python이 책임진다.
_PLAN_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"plan_json": {"type": "string"}},
    "required": ["plan_json"],
    "additionalProperties": False,
}


_MAX_TOKENS = 4000


def generate_plan(
    *,
    user_question: str,
    anthropic_client: Any,
    docs_extra_columns: list[DatasetSpecificColumn] | None = None,
    conversation_context: list[dict[str, Any]] | None = None,
    prompt_version: str = "",
    today: str = "",
    timezone: str = "",
) -> PlannerResult:
    """user_question에서 plan_v2를 만든다.

    Args:
        user_question: 사용자 질문 원문.
        anthropic_client: ``runtime.llm._anthropic_client()``로 만든 client.
            ``is_enabled() == False``면 ``PlannerCallError``.
        docs_extra_columns: docs table에 dataset별 추가된 컬럼. None이면 추가
            컬럼 없음.
        prompt_version: 명시적 prompt 버전. 비어 있으면 default.

    Raises:
        PlannerCallError: client 비활성화 등 호출 자체 실패.
        PlannerParseError: parse retry 후에도 JSON parse 실패.
        PlannerValidationError: validator self-correct retry 후에도 실패.

    Returns:
        ``PlannerResult`` — ``plan`` 필드가 validator 통과한 plan_v2 dict.
    """
    question = (user_question or "").strip()
    if not question:
        raise PlannerCallError("user_question must not be empty")
    if anthropic_client is None or not anthropic_client.is_enabled():
        raise PlannerCallError("planner requires an enabled anthropic client")

    # silverone 2026-05-26 — light/heavy 분리 실측 결과 retry rate 80%로 net
    # 비용 증가. rollback 후 heavy-only cached planner로 복귀. cache wiring +
    # prefix contract는 유지.
    resolved_version, system_prompt, user_prompt = render_planner_prompt(
        user_question=question,
        dataset_specific_columns=docs_extra_columns or [],
        conversation_context=conversation_context or [],
        version=prompt_version,
        today=today,
        timezone=timezone,
    )

    attempts: list[dict[str, Any]] = []
    # silverone 2026-05-26 (cost-opt) — cache 토큰 누적도 함께 추적한다.
    # ``cache_creation_input_tokens``는 첫 호출에서만 발생하고, 같은 system
    # prompt를 다시 보낼 때는 ``cache_read_input_tokens``로만 잡힌다.
    usage_accum: dict[str, int] = {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
    }

    # ===== attempt 1 =====
    response_a = _call_planner(
        anthropic_client,
        system_prompt=system_prompt,
        prompt=user_prompt,
        operation_tag="planner.initial",
    )
    _accumulate_usage(usage_accum, response_a.get("__usage__") or {})
    plan_a, parse_error_a = _try_parse_plan(response_a.get("plan_json"))
    attempts.append({
        "phase": "initial",
        "parsed": plan_a is not None,
        "parse_error": parse_error_a,
        "raw": response_a.get("plan_json"),
        "usage": response_a.get("__usage__") or {},
        "prompt_version": resolved_version,
    })

    # ===== parse retry (1회) =====
    if plan_a is None:
        repair_user_prompt = _build_parse_repair_prompt(
            base_user_prompt=user_prompt,
            raw_response=str(response_a.get("plan_json") or ""),
            parse_error=parse_error_a or "",
        )
        response_b = _call_planner(
            anthropic_client,
            system_prompt=system_prompt,
            prompt=repair_user_prompt,
            operation_tag="planner.parse_repair",
        )
        _accumulate_usage(usage_accum, response_b.get("__usage__") or {})
        plan_a, parse_error_b = _try_parse_plan(response_b.get("plan_json"))
        attempts.append({
            "phase": "parse_repair",
            "parsed": plan_a is not None,
            "parse_error": parse_error_b,
            "raw": response_b.get("plan_json"),
            "usage": response_b.get("__usage__") or {},
            "prompt_version": resolved_version,
        })
        if plan_a is None:
            raise PlannerParseError(
                f"planner parse retry failed: {parse_error_b or parse_error_a or 'unknown'}",
                attempts=attempts,
            )

    # ===== validator =====
    issues = collect_plan_issues(plan_a)
    attempts[-1]["validation_issues"] = [_issue_to_dict(i) for i in issues]

    # ===== self-correct retry (1회) =====
    if issues:
        self_correct_user_prompt = _build_validator_self_correct_prompt(
            base_user_prompt=user_prompt,
            previous_plan=plan_a,
            issues=issues,
        )
        response_c = _call_planner(
            anthropic_client,
            system_prompt=system_prompt,
            prompt=self_correct_user_prompt,
            operation_tag="planner.validator_repair",
        )
        _accumulate_usage(usage_accum, response_c.get("__usage__") or {})
        plan_b, parse_error_c = _try_parse_plan(response_c.get("plan_json"))
        if plan_b is None:
            attempts.append({
                "phase": "validator_repair",
                "parsed": False,
                "parse_error": parse_error_c,
                "raw": response_c.get("plan_json"),
                "usage": response_c.get("__usage__") or {},
                "prompt_version": resolved_version,
            })
            raise PlannerValidationError(
                f"planner self-correct retry failed: parse error '{parse_error_c}' "
                f"after validator issues {[i.code for i in issues]}",
                attempts=attempts,
                issues=issues,
            )
        issues_b = collect_plan_issues(plan_b)
        attempts.append({
            "phase": "validator_repair",
            "parsed": True,
            "validation_issues": [_issue_to_dict(i) for i in issues_b],
            "raw": response_c.get("plan_json"),
            "usage": response_c.get("__usage__") or {},
            "prompt_version": resolved_version,
        })
        if issues_b:
            # silverone 2026-06-02 — self-correct 후에도 남은 issue가 present.columns
            # 관련뿐이면 결정론적으로 columns를 떼고 재검증한다. present는 columns
            # 누락 시 SELECT * (전체 컬럼) fallback이므로, 비율 질문에 건수라도
            # 보여주는 게 전체 분석을 500으로 떨구는 것보다 낫다. columns 외 issue가
            # 하나라도 있으면 기존대로 raise (자동 복구 불가).
            repaired_plan = _repair_present_columns(plan_b, issues_b)
            if repaired_plan is not None:
                attempts.append({
                    "phase": "columns_repair",
                    "parsed": True,
                    "repaired_codes": sorted({i.code for i in issues_b}),
                    "prompt_version": resolved_version,
                })
                get(__name__).warning(
                    "planner.columns_repair",
                    extra={
                        "event": "planner.columns_repair",
                        "repaired_codes": sorted({i.code for i in issues_b}),
                    },
                )
                plan_a = repaired_plan
            else:
                raise PlannerValidationError(
                    f"planner self-correct retry still produced validator issues: "
                    f"{[i.code for i in issues_b]}",
                    attempts=attempts,
                    issues=issues_b,
                )
        else:
            plan_a = plan_b

    return PlannerResult(
        plan=plan_a,
        prompt_version=resolved_version,
        attempts=attempts,
        usage=usage_accum,
    )


# ===== internals =====


# silverone 2026-06-02 — 결정론적으로 자동 복구 가능한 present.columns issue code.
# 이 집합에 속한 issue만 남았을 때 present.columns를 떼고 재검증한다.
# - columns_unknown/not_list/invalid: present 전용 contract (prior-step input 경로)
# - column_unknown(단수): 공유 helper(_check_columns_on_table)가 RESERVED 테이블
#   input present.columns에 쓰는 코드. filter/aggregate 등도 같은 코드를 쓰므로,
#   columns drop 후 재검증이 완전히 깨끗할 때만 복구가 commit된다(아래 함수). 즉
#   present 외 출처의 column_unknown은 재검증에서 다시 걸려 raise된다.
_COLUMNS_REPAIRABLE_CODES = frozenset(
    {
        "params.columns_unknown",
        "params.columns_not_list",
        "params.columns_invalid",
        "params.column_unknown",
    }
)


def _repair_present_columns(
    plan: dict[str, Any], issues: list[ValidationIssue]
) -> "dict[str, Any] | None":
    """남은 issue가 present.columns 관련뿐이면 present step에서 columns를 떼고
    재검증한 plan을 반환. 그 외 issue가 하나라도 있거나 재검증이 여전히 실패하면
    None (자동 복구 불가 → caller가 raise)."""

    if not issues:
        return None
    if any(i.code not in _COLUMNS_REPAIRABLE_CODES for i in issues):
        return None

    repaired = copy.deepcopy(plan)
    steps = repaired.get("steps")
    if not isinstance(steps, list):
        return None
    dropped = False
    for step in steps:
        if not isinstance(step, dict):
            continue
        if str(step.get("skill") or "").strip() != "present":
            continue
        params = step.get("params")
        if isinstance(params, dict) and "columns" in params:
            params.pop("columns", None)
            dropped = True
    if not dropped:
        return None
    if collect_plan_issues(repaired):
        return None
    return repaired


def _call_planner(
    anthropic_client: Any,
    *,
    system_prompt: str,
    prompt: str,
    operation_tag: str,
) -> dict[str, Any]:
    """planner LLM 호출. silverone 2026-05-26 (cost-opt) — system_prompt는
    Anthropic prompt cache의 ephemeral 블록(``cache_system=True``)으로 전송해
    재호출 시 cache hit를 노린다. prompt(user)는 매번 변동."""

    started_at = time.monotonic()
    decision = _create_json_response_logged(
        anthropic_client,
        operation=operation_tag,
        prompt=prompt,
        schema=_PLAN_RESPONSE_SCHEMA,
        max_tokens=_MAX_TOKENS,
        system=system_prompt,
        cache_system=True,
    )
    if decision is None or not hasattr(decision, "body"):
        raise PlannerCallError(f"planner LLM returned non-decision: {type(decision).__name__}")
    body = decision.body if isinstance(decision.body, dict) else {}
    raw_usage = dict(getattr(decision, "usage", {}) or {})
    input_tokens = int(raw_usage.get("input_tokens") or 0)
    output_tokens = int(raw_usage.get("output_tokens") or 0)
    cache_creation = int(raw_usage.get("cache_creation_input_tokens") or 0)
    cache_read = int(raw_usage.get("cache_read_input_tokens") or 0)
    usage = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "cache_creation_input_tokens": cache_creation,
        "cache_read_input_tokens": cache_read,
        "cache_hit": cache_read > 0,
    }
    duration_ms = int((time.monotonic() - started_at) * 1000)
    _LOG.info(
        "planner.call.completed",
        operation=operation_tag,
        duration_ms=duration_ms,
        has_plan_json=bool(body.get("plan_json")),
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        cache_creation_input_tokens=cache_creation,
        cache_read_input_tokens=cache_read,
        cache_hit=usage["cache_hit"],
    )
    return {"plan_json": body.get("plan_json"), "__usage__": usage}


def _try_parse_plan(raw: Any) -> tuple[dict[str, Any] | None, str]:
    text = str(raw or "").strip()
    if not text:
        return None, "empty plan_json"
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(parsed, dict):
        return None, f"plan_json must decode to an object, got {type(parsed).__name__}"
    return parsed, ""


def _accumulate_usage(accum: dict[str, int], usage: dict[str, Any]) -> None:
    """planner attempts에서 발생한 token usage를 누적한다.

    silverone 2026-05-26 (cost-opt) — cache_creation / cache_read 토큰도
    함께 누적. ``cache_hit``은 boolean이라 누적하지 않고, accum의 ``cache_read_input_tokens``
    값이 양수면 retry 포함 어디서든 한 번이라도 cache hit이 있었다는 뜻.
    """
    if not isinstance(usage, dict):
        return
    for key in (
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ):
        value = usage.get(key)
        if isinstance(value, int):
            accum[key] = accum.get(key, 0) + value


def _issue_to_dict(issue: ValidationIssue) -> dict[str, Any]:
    return {
        "code": issue.code,
        "message": issue.message,
        "step_id": issue.step_id,
        "step_index": issue.step_index,
    }


def _build_parse_repair_prompt(*, base_user_prompt: str, raw_response: str, parse_error: str) -> str:
    """parse 실패 retry용 user prompt 빌더. silverone 2026-05-26 (cost-opt) —
    system_prompt는 그대로 두고 user_prompt에만 이전 응답 + 지시를 덧붙여
    cache 적중률을 유지한다."""
    return (
        f"{base_user_prompt}\n\n"
        "## 이전 응답 (JSON parse 실패)\n\n"
        f"```\n{raw_response}\n```\n\n"
        f"## 발생한 parse error\n\n{parse_error}\n\n"
        "## 추가 지시\n\n"
        "- 위 응답을 수정해서 valid한 plan_v2 JSON 한 개만 출력한다.\n"
        "- 다른 텍스트, 주석, 코드 펜스 없이 raw JSON만 출력한다.\n"
        "- 반드시 plan_json 필드 안에 plan_v2 객체 전체를 문자열로 담아 반환한다."
    )


def _build_validator_self_correct_prompt(
    *,
    base_user_prompt: str,
    previous_plan: dict[str, Any],
    issues: list[ValidationIssue],
) -> str:
    """validator 실패 retry용 user prompt 빌더. system은 그대로, user에만
    이전 plan + validator issues + 추가 지시를 덧붙인다."""
    issues_block = "\n".join(
        f"- [{issue.code}] (step_id={issue.step_id or '-'}) {issue.message}"
        for issue in issues
    )
    return (
        f"{base_user_prompt}\n\n"
        "## 이전 응답 (plan_v2 validator 위반)\n\n"
        "```json\n"
        f"{json.dumps(previous_plan, ensure_ascii=False, indent=2)}\n"
        "```\n\n"
        "## validator가 보고한 문제\n\n"
        f"{issues_block}\n\n"
        "## 추가 지시\n\n"
        "- 위 plan을 수정해서 모든 validator issue를 해결한다.\n"
        "- skill 이름, table 이름, 컬럼 이름 모두 위 카탈로그/스키마 기준으로만 사용한다.\n"
        "- 새 step을 추가/삭제해도 좋다. 기존 step id는 그대로 보존하지 않아도 된다.\n"
        "- plan_json 필드 안에 수정된 plan_v2 객체 전체를 문자열로 담아 반환한다."
    )


__all__ = [
    "PlannerCallError",
    "PlannerParseError",
    "PlannerResult",
    "PlannerValidationError",
    "generate_plan",
]
