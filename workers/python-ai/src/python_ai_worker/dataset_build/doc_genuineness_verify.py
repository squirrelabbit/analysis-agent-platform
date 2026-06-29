"""doc_genuineness verify mode — 교차모델 검증 + 불일치 judge (ADR-026).

흐름: doc → model_a classify + model_b classify → 일치면 final=합의,
불일치면 judge(원문 + 익명 후보 + 라벨 기준) → decision으로 final_label 확정.

핵심(self-confirmation 방지): judge에게 두 후보를 candidate_1/candidate_2로만
주고(어느 모델인지·judge 자신 답인지 숨김, doc_id 해시로 순서 셔플) 라벨 기준으로
독립 판정하게 한다. 결과는 다시 accept_a/accept_b로 복원한다.

기존 단일 모델 경로(run_dataset_doc_genuineness)는 그대로 두고, payload['verify']가
참일 때만 이 경로로 위임한다(= doc_genuineness 옵션 플래그).
"""
from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..clients.lloa import LloaClient, LloaConfig, LloaResponseParseError
from ..config import load_config
from ..prompt_options import load_prompt_body
from ._common import write_progress
from .doc_genuineness import (
    _ALLOWED_TIERS,
    _CHUNK_MAX_CHARS,
    _CHUNK_MAX_SENTENCES,
    _CHUNK_OVERLAP_SENTENCES,
    _chunk_aggregate_classify,
    _classify_doc,
    _extract_doc_genuineness_config,
    _load_prompt_template,
    _render_prompt,
    _resolve_concurrency,
    _resolve_max_input_chars,
    _truncate_text,
)


def _union_spans(*span_lists) -> list[dict[str, int]]:
    """여러 모델의 genuine_spans를 (sentence_start, sentence_end) 기준 dedup union."""
    seen: set[tuple[int, int]] = set()
    out: list[dict[str, int]] = []
    for spans in span_lists:
        for sp in spans or []:
            if not isinstance(sp, dict):
                continue
            try:
                key = (int(sp["sentence_start"]), int(sp["sentence_end"]))
            except (KeyError, TypeError, ValueError):
                continue
            if key not in seen:
                seen.add(key)
                out.append({"chunk_index": sp.get("chunk_index", -1), "sentence_start": key[0], "sentence_end": key[1]})
    return out

_JUDGE_PROMPT_TASK = "doc_genuineness_judge"
_JUDGE_DECISIONS = {"candidate_1", "candidate_2", "other", "review"}
# 불일치 + judge confidence가 이 값 미만이면 needs_review=true (ADR-026 보수 기본값).
_REVIEW_CONFIDENCE_FLOOR = 0.85


def is_verify_mode(payload: dict[str, Any]) -> bool:
    return bool(payload.get("verify"))


def _client_for_model(config, model: str, *, reasoning_effort, prepend_no_think: bool) -> LloaClient:
    return LloaClient(
        LloaConfig(
            api_key=config.lloa_api_key,
            api_url=config.lloa_api_url,
            model=model,
            max_tokens=config.lloa_max_tokens,
            timeout_sec=config.lloa_timeout_sec,
            reasoning_effort=reasoning_effort,
            prepend_no_think=prepend_no_think,
            retry_max_attempts=config.lloa_retry_max_attempts,
            retry_base_delay_sec=config.lloa_retry_base_delay_sec,
            retry_max_delay_sec=config.lloa_retry_max_delay_sec,
        )
    )


def _candidate_order_ab(doc_id: str) -> bool:
    """doc_id 해시로 후보 순서를 결정론적으로 셔플. True면 (a→candidate_1,
    b→candidate_2), False면 반대. process hash salt 영향 없게 md5 사용."""
    h = int(hashlib.md5(doc_id.encode("utf-8")).hexdigest(), 16)
    return h % 2 == 0


def _judge_doc(
    client: LloaClient,
    *,
    system_prompt: str,
    doc_id: str,
    doc_text: str,
    candidate_1: dict[str, Any],
    candidate_2: dict[str, Any],
    max_tokens: int,
) -> dict[str, Any]:
    """불일치 doc judge 호출. 후보는 익명(candidate_1/2)으로만 전달."""
    user_payload = json.dumps(
        {
            "doc_id": doc_id,
            "doc_text": doc_text,
            "candidate_1": {"genuineness": candidate_1["genuineness"], "reason": candidate_1["reason"]},
            "candidate_2": {"genuineness": candidate_2["genuineness"], "reason": candidate_2["reason"]},
        },
        ensure_ascii=False,
    )
    response = client.create_json_response(system=system_prompt, user=user_payload, max_tokens=max_tokens)
    body = response.body
    if not isinstance(body, dict):
        raise LloaResponseParseError(
            f"doc_genuineness_judge expected JSON object, got {type(body).__name__}",
            raw_text=str(body),
            finish_reason=response.finish_reason,
        )
    chosen = str(body.get("chosen") or "").strip()
    if chosen not in _JUDGE_DECISIONS:
        raise LloaResponseParseError(
            f"doc_genuineness_judge invalid chosen: {chosen!r} (expected {sorted(_JUDGE_DECISIONS)})",
            raw_text=json.dumps(body, ensure_ascii=False),
            finish_reason=response.finish_reason,
        )
    raw_label = body.get("final_label")
    final_label = str(raw_label).strip() if raw_label not in (None, "") else ""
    try:
        confidence = float(body.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return {
        "chosen": chosen,
        "final_label": final_label,
        "confidence": confidence,
        "reason": str(body.get("reason") or "").strip(),
        "usage": response.usage,
    }


def _resolve_judge(
    judge: dict[str, Any],
    *,
    a_result: dict[str, Any],
    b_result: dict[str, Any],
    order_ab: bool,
) -> tuple[str, str | None]:
    """judge의 익명 선택을 (decision, final_label)로 복원.
    decision ∈ accept_a | accept_b | revise | review. order_ab로 candidate→model 매핑.
    """
    chosen = judge["chosen"]
    if chosen == "review":
        return "review", None
    if chosen == "other":
        label = judge["final_label"]
        if label not in _ALLOWED_TIERS:
            return "review", None  # 잘못된 라벨 → 보수적으로 사람 검토
        return "revise", label
    # candidate_1/candidate_2 → model a/b 복원.
    if chosen == "candidate_1":
        winner_is_a = order_ab
    else:  # candidate_2
        winner_is_a = not order_ab
    if winner_is_a:
        return "accept_a", a_result["genuineness"]
    return "accept_b", b_result["genuineness"]


def run_dataset_doc_genuineness_verify(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    clean_artifact_ref = str(payload.get("clean_artifact_ref") or "").strip()
    output_path_raw = str(payload.get("output_path") or "").strip()
    progress_path = str(payload.get("progress_path") or "").strip()
    if not dataset_version_id or not clean_artifact_ref or not output_path_raw:
        raise ValueError(
            "dataset_doc_genuineness verify requires dataset_version_id, clean_artifact_ref, output_path"
        )

    classify_models = [str(m).strip() for m in (payload.get("classify_models") or []) if str(m).strip()]
    if len(classify_models) != 2 or classify_models[0] == classify_models[1]:
        raise ValueError(
            "dataset_doc_genuineness verify requires classify_models = 2 distinct model ids"
        )
    model_a, model_b = classify_models[0], classify_models[1]
    judge_model = str(payload.get("judge_model") or "").strip() or model_b

    output_path = Path(output_path_raw)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()

    config = load_config()
    if not (config.lloa_api_key or "").strip():
        raise ValueError(
            "dataset_doc_genuineness verify requires LLOA API key — set LLOA_API_KEY"
        )

    # classify는 기존 단일 모델과 동일(low + /no_think로 빠르게), judge는 불일치
    # 소수에만 도므로 thinking ON으로 품질 우선.
    client_a = _client_for_model(config, model_a, reasoning_effort="low", prepend_no_think=config.lloa_prepend_no_think)
    client_b = _client_for_model(config, model_b, reasoning_effort="low", prepend_no_think=config.lloa_prepend_no_think)
    judge_effort = str(payload.get("judge_reasoning_effort") or "medium").strip() or "medium"
    judge_client = _client_for_model(config, judge_model, reasoning_effort=judge_effort, prepend_no_think=False)

    template, prompt_version = _load_prompt_template(payload)
    doc_config = _extract_doc_genuineness_config(payload)
    classify_system_prompt = _render_prompt(template, doc_config)
    judge_body, judge_prompt_version = load_prompt_body(
        _JUDGE_PROMPT_TASK, str(payload.get("judge_prompt_version") or "").strip() or None
    )
    judge_system_prompt = _render_prompt(judge_body, doc_config)

    # max-v1.2.1이 /no_think을 무시하고 reasoning을 길게 토하면 1024로는 reasoning만
    # 차고 content가 0이 된다(finish_reason=length). 여유를 둬 reasoning+content가
    # 같이 들어가게 한다. judge(no_think off, reasoning on)는 더 필요.
    max_tokens = int(payload.get("max_tokens") or 4096)
    judge_max_tokens = int(payload.get("judge_max_tokens") or 4096)
    concurrency = _resolve_concurrency(payload)
    max_input_chars = _resolve_max_input_chars(payload)
    # ADR-029 — verify도 긴 문서 chunk aggregate. **기본 ON**(단일 모드와 동일):
    # cleaned_text > max_input_chars면 모델별로 chunk aggregate한 라벨을 교차검증한다.
    # judge 입력은 truncate(불일치 소수에만 도므로 v1 단순). chunking=false로 비활성화.
    chunking_enabled = payload.get("chunking", True) is not False
    chunk_max_sentences = max(1, int(payload.get("max_chunk_sentences") or _CHUNK_MAX_SENTENCES))
    chunk_max_chars = max(1, int(payload.get("max_chunk_chars") or _CHUNK_MAX_CHARS))
    chunk_overlap = max(0, int(payload.get("overlap_sentences") if payload.get("overlap_sentences") is not None else _CHUNK_OVERLAP_SENTENCES))
    chunked_doc_count = 0
    genuine_span_doc_count = 0

    rows = rt._iter_rows(clean_artifact_ref)
    total_rows = len(rows)
    if progress_path:
        write_progress(
            progress_path, processed_rows=0, total_rows=total_rows,
            started_at=started_at, message="doc_genuineness verify queued",
        )

    records_by_doc: dict[str, dict[str, Any]] = {}
    targets: list[tuple[int, str, str]] = []
    for index, row in enumerate(rows):
        doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
        cleaned_text = str(row.get("cleaned_text") or "").strip()
        if not cleaned_text:
            records_by_doc[doc_id] = {
                "doc_id": doc_id,
                "model_a": model_a, "model_a_result": {"genuineness": "non_review", "reason": "본문 비어 있음."},
                "model_b": model_b, "model_b_result": {"genuineness": "non_review", "reason": "본문 비어 있음."},
                "is_disagreement": False, "judge_required": False, "judge_result": None,
                "final_label": "non_review", "resolution": "empty_text_shortcut",
                "needs_review": False, "prompt_version": prompt_version, "source": "verify",
            }
            continue
        targets.append((index, doc_id, cleaned_text))

    counters = {
        "agreement": 0, "disagreement": 0, "judge": 0, "review": 0,
        "revised": 0, "classify_error": 0, "partial": 0,
    }

    def _process(item: tuple[int, str, str]) -> dict[str, Any]:
        _, doc_id, doc_text = item
        used_text, _ol, _ul, _tr = _truncate_text(doc_text, max_input_chars)
        use_chunking = chunking_enabled and len(doc_text) > max_input_chars
        rec: dict[str, Any] = {
            "doc_id": doc_id, "model_a": model_a, "model_b": model_b,
            "prompt_version": prompt_version, "source": "verify",
            "judge_required": False, "judge_result": None,
            "chunked": use_chunking,
        }

        # per-model 격리 — 한 모델이 실패해도 다른 모델 결과를 쓴다. 긴 문서(use_chunking)면
        # 모델별로 chunk aggregate한 라벨을 쓴다(ADR-029). genuine_spans도 모델별로 받는다.
        def _classify_safe(client: LloaClient):
            try:
                if use_chunking:
                    agg = _chunk_aggregate_classify(
                        client, system_prompt=classify_system_prompt, doc_id=doc_id, doc_text=doc_text,
                        max_tokens=max_tokens, max_sentences=chunk_max_sentences,
                        max_chars=chunk_max_chars, overlap=chunk_overlap,
                    )
                    return {"genuineness": agg["genuineness"], "reason": agg["reason"], "genuine_spans": agg["genuine_spans"]}, None
                return _classify_doc(
                    client, system_prompt=classify_system_prompt,
                    doc_id=doc_id, doc_text=used_text, max_tokens=max_tokens,
                ), None
            except (LloaResponseParseError, OSError, ValueError) as exc:
                return None, str(exc)

        a, a_err = _classify_safe(client_a)
        b, b_err = _classify_safe(client_b)
        rec["model_a_result"] = {"genuineness": a["genuineness"], "reason": a["reason"]} if a else None
        rec["model_b_result"] = {"genuineness": b["genuineness"], "reason": b["reason"]} if b else None

        if not a and not b:
            # 둘 다 실패 → uncertain으로 격리(빈칸 아님), 검토 필요.
            rec.update({
                "is_disagreement": False, "final_label": "uncertain",
                "resolution": "classify_error", "needs_review": True,
                "error": a_err or b_err,
            })
            return rec
        if not a or not b:
            # 한 모델만 성공 → 그 라벨 채택, 교차검증 미완이라 검토 필요.
            ok = a or b
            rec.update({
                "is_disagreement": False, "final_label": ok["genuineness"],
                "resolution": "partial_classify", "needs_review": True,
                "error": a_err or b_err,
            })
            if ok["genuineness"] == "genuine_review" and ok.get("genuine_spans"):
                rec["genuine_spans"] = _union_spans(ok.get("genuine_spans"))
            return rec

        if a["genuineness"] == b["genuineness"]:
            rec.update({
                "is_disagreement": False, "final_label": a["genuineness"],
                "resolution": "model_agreement", "needs_review": False,
            })
            # 합의 + genuine → 두 모델 spans union(clause_label이 소비, ADR-029).
            if a["genuineness"] == "genuine_review":
                spans = _union_spans(a.get("genuine_spans"), b.get("genuine_spans"))
                if spans:
                    rec["genuine_spans"] = spans
            return rec
        # 불일치 → judge (익명 후보).
        rec["is_disagreement"] = True
        rec["judge_required"] = True
        order_ab = _candidate_order_ab(doc_id)
        cand1, cand2 = (a, b) if order_ab else (b, a)
        try:
            judged = _judge_doc(
                judge_client, system_prompt=judge_system_prompt, doc_id=doc_id, doc_text=used_text,
                candidate_1=cand1, candidate_2=cand2, max_tokens=judge_max_tokens,
            )
        except (LloaResponseParseError, OSError, ValueError) as exc:
            rec.update({
                "final_label": None, "resolution": "judge_error",
                "needs_review": True, "error": str(exc),
            })
            return rec
        decision, final_label = _resolve_judge(judged, a_result=a, b_result=b, order_ab=order_ab)
        needs_review = decision == "review" or judged["confidence"] < _REVIEW_CONFIDENCE_FLOOR
        rec["judge_result"] = {
            "decision": decision, "final_label": final_label,
            "confidence": judged["confidence"], "reason": judged["reason"],
            "judge_model": judge_model,
        }
        rec.update({
            "final_label": final_label, "resolution": "judge_on_disagreement",
            "needs_review": needs_review,
        })
        return rec

    processed = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(_process, t): t for t in targets}
        for future in as_completed(futures):
            rec = future.result()
            records_by_doc[rec["doc_id"]] = rec
            if rec["resolution"] == "classify_error" or rec["resolution"] == "judge_error":
                counters["classify_error"] += 1
            elif rec["resolution"] == "partial_classify":
                counters["partial"] += 1
            elif rec["resolution"] == "model_agreement":
                counters["agreement"] += 1
            elif rec["resolution"] == "judge_on_disagreement":
                counters["disagreement"] += 1
                counters["judge"] += 1
                jr = rec.get("judge_result") or {}
                if jr.get("decision") == "revise":
                    counters["revised"] += 1
            if rec.get("needs_review"):
                counters["review"] += 1
            if rec.get("chunked"):
                chunked_doc_count += 1
            if rec.get("genuine_spans"):
                genuine_span_doc_count += 1
            processed += 1
            if progress_path and (processed % 10 == 0 or processed == len(targets)):
                write_progress(
                    progress_path, processed_rows=processed + (total_rows - len(targets)),
                    total_rows=total_rows, started_at=started_at, message="doc_genuineness verify processing",
                )

    # 원본 row 순서로 write + final_label tier 집계.
    final_tier_counts: dict[str, int] = {tier: 0 for tier in _ALLOWED_TIERS}
    null_final = 0
    with output_path.open("w", encoding="utf-8") as dst:
        for index, row in enumerate(rows):
            doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
            rec = records_by_doc.get(doc_id)
            if rec is None:
                continue
            fl = rec.get("final_label")
            if fl in final_tier_counts:
                final_tier_counts[fl] += 1
            else:
                null_final += 1
            dst.write(json.dumps(rec, ensure_ascii=False))
            dst.write("\n")

    if progress_path:
        write_progress(
            progress_path, processed_rows=total_rows, total_rows=total_rows,
            started_at=started_at, message="doc_genuineness verify completed",
        )

    summary = {
        "mode": "verify",
        "input_row_count": total_rows,
        "processed_row_count": len(records_by_doc),
        "agreement_count": counters["agreement"],
        "disagreement_count": counters["disagreement"],
        "judge_count": counters["judge"],
        "revised_count": counters["revised"],
        "review_count": counters["review"],
        "classify_error_count": counters["classify_error"],
        "partial_classify_count": counters["partial"],
        "final_tier_counts": final_tier_counts,
        "final_null_count": null_final,
        "chunking": {
            "enabled": chunking_enabled,
            "strategy": "sentence_window",
            "threshold_chars": max_input_chars,
            "max_chunk_sentences": chunk_max_sentences,
            "max_chunk_chars": chunk_max_chars,
            "overlap_sentences": chunk_overlap,
            "chunked_doc_count": chunked_doc_count,
            "genuine_span_doc_count": genuine_span_doc_count,
        },
        "models": {"a": model_a, "b": model_b, "judge": judge_model},
        "prompt_version": prompt_version,
        "judge_prompt_version": judge_prompt_version,
        "applied": {
            "prompt_version": prompt_version,
            "judge_prompt_version": judge_prompt_version,
            "classify_models": [model_a, model_b],
            "judge_model": judge_model,
            "subject_name": doc_config["subject_name"],
            "subject_type": doc_config["subject_type"],
        },
    }
    return {
        "notes": [
            f"dataset_doc_genuineness verify — {len(records_by_doc)} docs "
            f"(agreement={counters['agreement']}, disagreement={counters['disagreement']}, "
            f"judge={counters['judge']}, review={counters['review']}, "
            f"classify_error={counters['classify_error']})",
            f"models: a={model_a}, b={model_b}, judge={judge_model}",
        ],
        "artifact": rt._set_scope_fields(
            {
                "skill_name": "dataset_doc_genuineness",
                "dataset_version_id": dataset_version_id,
                "doc_genuineness_uri": str(output_path),
                "doc_genuineness_ref": str(output_path),
                "summary": summary,
            },
            declared_result_scope="full_dataset",
            runtime_result_scope="full_dataset",
        ),
    }
