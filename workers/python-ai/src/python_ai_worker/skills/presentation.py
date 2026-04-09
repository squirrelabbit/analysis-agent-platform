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
    summary = " ".join(str(answer.get("summary") or "").split()).strip()
    if not summary:
        summary = "실행 결과가 생성되었지만 대표 요약은 비어 있습니다."
    if not key_findings:
        key_findings = _derive_key_findings_from_steps(list(result_v1.get("step_results") or []))
    headline = str(result_v1.get("primary_skill_name") or "").strip()
    if headline:
        headline = f"{headline} 결과 요약"
    else:
        headline = "분석 결과 요약"
    fallback_caveats = _derive_fallback_caveats(warnings, evidence_candidates)
    if not follow_up_questions:
        follow_up_questions = _derive_follow_up_questions(result_v1, evidence_candidates)
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
        "warning_count": len(warnings),
        "evidence_candidate_count": len(_build_evidence_candidates(answer_evidence(result_v1))),
        "selection_source": str(answer_dict(result_v1).get("selection_source") or "").strip(),
        "citation_mode": str(answer_dict(result_v1).get("citation_mode") or "").strip(),
        "waiting": waiting,
        "step_results": step_results,
    }


def answer_dict(result_v1: dict[str, Any]) -> dict[str, Any]:
    answer = result_v1.get("answer") or {}
    if not isinstance(answer, dict):
        return {}
    return answer


def answer_evidence(result_v1: dict[str, Any]) -> list[Any]:
    answer = answer_dict(result_v1)
    raw = answer.get("evidence") or []
    if isinstance(raw, list):
        return raw
    return []


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


def _derive_key_findings_from_steps(items: list[Any]) -> list[str]:
    findings: list[str] = []
    for raw_item in items[:8]:
        if not isinstance(raw_item, dict):
            continue
        status = str(raw_item.get("status") or "").strip()
        if status != "completed":
            continue
        summary = " ".join(str(raw_item.get("summary") or "").split()).strip()
        if summary:
            findings.append(summary)
    return rt._coerce_string_list(findings)[:5]


def _derive_fallback_caveats(warnings: list[str], evidence_candidates: list[dict[str, Any]]) -> list[str]:
    caveats = list(warnings[:3]) if warnings else []
    if len(evidence_candidates) == 0:
        caveats.append("확인 필요: 현재 final_answer에 연결된 근거 후보가 없습니다.")
    elif len(evidence_candidates) == 1:
        caveats.append("확인 필요: 현재 final_answer 근거 후보가 1건뿐이라 해석 범위가 제한적입니다.")
    if not caveats:
        caveats.append("확인 필요: 최종 답변은 실행 결과와 근거 snippet 범위 안에서만 해석해야 합니다.")
    return rt._coerce_string_list(caveats)[:4]


def _derive_follow_up_questions(result_v1: dict[str, Any], evidence_candidates: list[dict[str, Any]]) -> list[str]:
    primary_skill_name = str(result_v1.get("primary_skill_name") or "").strip()
    suggestions: list[str] = []
    if evidence_candidates:
        suggestions.append("근거 snippet을 더 자세히 볼까요?")
    if primary_skill_name == "issue_cluster_summary":
        suggestions.append("주요 군집별 대표 원문을 더 볼까요?")
    elif primary_skill_name == "issue_sentiment_summary":
        suggestions.append("감성 분포와 대표 근거를 더 볼까요?")
    else:
        suggestions.append("step별 중간 결과를 더 볼까요?")
    return rt._coerce_string_list(suggestions)[:5]


__all__ = [
    "run_execution_final_answer",
]
