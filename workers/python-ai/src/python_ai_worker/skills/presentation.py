from __future__ import annotations

"""Presentation-layer handlers for user-facing grounded answers."""

from typing import Any

from .. import runtime as rt
from ..config import load_config


def run_execution_final_answer(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_execution_final_answer_payload(payload)
    enriched = _enrich_execution_final_answer_payload(normalized)
    client = rt._anthropic_client()

    if client is not None and client.is_enabled():
        try:
            result = rt._run_execution_final_answer_with_llm(client, enriched)
            result["artifact"] = _artifact_view(result["answer"])
            return result
        except Exception as exc:
            fallback = rt._run_execution_final_answer_fallback(enriched)
            fallback["notes"].append(f"execution_final_answer fallback reason: {exc}")
            fallback["artifact"] = _artifact_view(fallback["answer"])
            return fallback

    result = rt._run_execution_final_answer_fallback(enriched)
    result["artifact"] = _artifact_view(result["answer"])
    return result


def _artifact_view(answer: dict[str, Any]) -> dict[str, Any]:
    artifact = dict(answer)
    artifact["skill_name"] = "execution_final_answer"
    return artifact


def _enrich_execution_final_answer_payload(normalized: dict[str, Any]) -> dict[str, Any]:
    result_v1 = dict(normalized["result_v1"])
    answer = result_v1.get("answer") or {}
    if not isinstance(answer, dict):
        answer = {}
    evidence_candidates = _build_evidence_candidates(answer.get("evidence") or [])
    warnings = rt._coerce_string_list(result_v1.get("warnings"))
    key_findings = rt._coerce_string_list(answer.get("key_findings"))
    follow_up_questions = rt._coerce_string_list(answer.get("follow_up_questions"))
    summary = str(answer.get("summary") or "").strip()
    if not summary:
        summary = "실행 결과가 생성되었지만 대표 요약은 비어 있습니다."
    headline = str(result_v1.get("primary_skill_name") or "").strip()
    if headline:
        headline = f"{headline} 결과 요약"
    else:
        headline = "분석 결과 요약"
    if warnings:
        fallback_caveats = list(warnings[:3])
    else:
        fallback_caveats = ["확인 필요: 최종 답변은 실행 결과와 근거 snippet 범위 안에서만 해석해야 합니다."]
    result_context = _build_result_context(result_v1, summary, key_findings, warnings)

    enriched = dict(normalized)
    enriched["result_context"] = result_context
    enriched["evidence_candidates"] = evidence_candidates
    enriched["fallback_headline"] = headline
    enriched["fallback_answer_text"] = summary
    enriched["fallback_key_points"] = key_findings[:5]
    enriched["fallback_caveats"] = fallback_caveats
    enriched["fallback_evidence"] = [dict(item) for item in evidence_candidates[:3]]
    enriched["fallback_follow_up_questions"] = follow_up_questions[:5]
    if not enriched.get("prompt_version"):
        config = load_config()
        enriched["prompt_version"] = str(config.anthropic_execution_final_answer_prompt_version or "").strip()
    return enriched


def _build_result_context(
    result_v1: dict[str, Any],
    summary: str,
    key_findings: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    step_results = []
    for item in list(result_v1.get("step_results") or [])[:8]:
        if not isinstance(item, dict):
            continue
        step_results.append(
            {
                "step_id": str(item.get("step_id") or "").strip(),
                "skill_name": str(item.get("skill_name") or "").strip(),
                "status": str(item.get("status") or "").strip(),
                "summary": str(item.get("summary") or "").strip(),
            }
        )
    waiting = result_v1.get("waiting") or {}
    if not isinstance(waiting, dict):
        waiting = {}
    return {
        "status": str(result_v1.get("status") or "").strip(),
        "primary_skill_name": str(result_v1.get("primary_skill_name") or "").strip(),
        "summary": summary,
        "key_findings": key_findings[:6],
        "warnings": warnings[:6],
        "waiting": waiting,
        "step_results": step_results,
    }


def _build_evidence_candidates(items: list[Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for index, raw_item in enumerate(items, start=1):
        if not isinstance(raw_item, dict):
            continue
        snippet = str(raw_item.get("snippet") or raw_item.get("text") or "").strip()
        candidate = {
            "evidence_id": f"evidence-{index}",
            "rank": index,
            "snippet": " ".join(snippet.split())[:240],
            "rationale": str(raw_item.get("rationale") or "").strip(),
        }
        for key in ("artifact_key", "row_id", "chunk_id", "chunk_ref", "chunk_format"):
            value = str(raw_item.get(key) or "").strip()
            if value:
                candidate[key] = value
        for key in ("source_index", "chunk_index", "char_start", "char_end"):
            value = raw_item.get(key)
            if value is None or value == "":
                continue
            try:
                candidate[key] = int(value)
            except (TypeError, ValueError):
                continue
        candidates.append(candidate)
    return candidates[:6]


__all__ = [
    "run_execution_final_answer",
]
