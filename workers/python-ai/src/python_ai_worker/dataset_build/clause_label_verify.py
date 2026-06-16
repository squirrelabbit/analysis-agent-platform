"""dataset_clause_label verify mode — 문장 앵커 교차모델 검증 + 불일치 judge (ADR-028).

흐름: doc → kiwipiepy 문장 분리(+구두점조각 drop) → model_a/model_b가 *같은 문장
리스트*에 {relevant, sentiment, aspects[]} 라벨 → 문장별 reconcile →
  ① 두 라벨 같으면 final
  ② aspect superset/subset이면 union (judge 안 보냄)
  ③ relevance 불일치 / sentiment polarity flip(긍↔부정) / aspect disjoint만 judge
sentiment neutral↔긍/부정은 non-neutral 채택(sentiment_auto). 최종은 aspects[] explode로
기존 clause_label {doc_id, clause, sentiment, aspect} 호환 행으로 emit.

기존 단일 모델 경로(run_dataset_clause_label)는 그대로 두고, payload['verify']가 참일
때만 이 경로로 위임한다(= clause_label 옵션 플래그).

가드 (silverone 2026-06-16):
- judge는 doc당 batch + (분쟁 문장 수 / 입력 char) 초과 시 chunking.
- judge 결과 누락 문장 → needs_review. judge invalid aspect → fallback 없이 needs_review.
- judge 호출 실패 → 전체 build 실패가 아니라 그 chunk 분쟁 문장만 needs_review로 격리.
- per-row resolution: agree | union | sentiment_auto | judge | needs_review | partial_classify.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..clients.lloa import LloaClient, LloaConfig, LloaResponseParseError
from ..config import load_config
from ..prompt_options import load_prompt_body
from ._common import write_progress
from .clause_label import (
    _ALLOWED_ASPECT,
    _ALLOWED_SENTIMENT,
    _DEFAULT_CONCURRENCY,
    _FALLBACK_ASPECT,
    _extract_subject_config,
    _inject_taxonomy,
    _render_subject_prompt,
)

_CLASSIFY_PROMPT_TASK = "clause_label_verify"
_JUDGE_PROMPT_TASK = "clause_label_verify_judge"

# judge batch 가드
_MAX_DISPUTED_PER_JUDGE_CALL = 20
_MAX_JUDGE_INPUT_CHARS = 8000

_NON_ALNUM_KO = re.compile(r"[^가-힣A-Za-z0-9]")

_kiwi_singleton: Any = None


def is_verify_mode(payload: dict[str, Any]) -> bool:
    return bool(payload.get("verify"))


def _get_kiwi():
    global _kiwi_singleton
    if _kiwi_singleton is None:
        try:
            from kiwipiepy import Kiwi

            _kiwi_singleton = Kiwi()
        except Exception:  # noqa: BLE001 — 미설치/로드 실패 시 regex fallback
            _kiwi_singleton = False
    return _kiwi_singleton or None


def _split_anchor_sentences(text: str) -> list[str]:
    """kiwipiepy 문장 분리 + 구두점-only 조각 drop(clean ". ." 잔재). kiwipiepy 미설치
    시 runtime regex fallback(품질 낮음 — production은 kiwipiepy 의존성 보장)."""
    kiwi = _get_kiwi()
    sents: list[str]
    if kiwi is not None:
        try:
            sents = [s.text.strip() for s in kiwi.split_into_sents(text)]
        except Exception:  # noqa: BLE001
            sents = rt._split_sentences(text, language="ko")[0]
    else:
        sents = rt._split_sentences(text, language="ko")[0]
    return [s for s in sents if s and _NON_ALNUM_KO.sub("", s)]


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
        )
    )


def _parse_label_item(item: dict[str, Any]) -> tuple[int, dict[str, Any]] | None:
    try:
        idx = int(item.get("index"))
    except (TypeError, ValueError):
        return None
    relevant = bool(item.get("relevant"))
    aspects = {a for a in (str(x).strip() for x in item.get("aspects") or []) if a in _ALLOWED_ASPECT}
    if relevant and not aspects:
        aspects = {_FALLBACK_ASPECT}
    sentiment = str(item.get("sentiment") or "neutral").strip()
    if sentiment not in _ALLOWED_SENTIMENT:
        sentiment = "neutral"
    return idx, {"relevant": relevant, "sentiment": sentiment, "aspects": sorted(aspects)}


def _coerce_array(body: Any) -> list | None:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("result", "sentences", "data", "items"):
            if isinstance(body.get(key), list):
                return body[key]
    return None


def _label_sentences(client: LloaClient, system_prompt: str, sentences: list[str], max_tokens: int) -> dict[int, dict[str, Any]]:
    numbered = "\n".join(f"{i}. {s}" for i, s in enumerate(sentences, start=1))
    response = client.create_json_response(system=system_prompt, user=numbered, max_tokens=max_tokens)
    arr = _coerce_array(response.body)
    if arr is None:
        raise LloaResponseParseError(
            "clause_label_verify classify expected JSON array",
            raw_text=str(response.body),
            finish_reason=response.finish_reason,
        )
    out: dict[int, dict[str, Any]] = {}
    for item in arr:
        if isinstance(item, dict):
            parsed = _parse_label_item(item)
            if parsed is not None:
                out[parsed[0]] = parsed[1]
    return out


def _recon_sentiment(sa: str, sb: str) -> tuple[str, str | None, bool]:
    """returns (resolution, final, needs_judge). neutral↔비neutral→비neutral, 긍↔부정→judge."""
    if sa == sb:
        return "agree", sa, False
    pair = {sa, sb}
    if "neutral" in pair:
        final = (pair - {"neutral"}).pop()
        return "sentiment_auto", final, False
    # positive vs negative
    return "judge", None, True


def _recon_aspects(sa: set[str], sb: set[str]) -> tuple[str, set[str] | None, bool]:
    if sa == sb:
        return "agree", sa, False
    if sa < sb or sb < sa:
        return "union", sa | sb, False
    return "judge", None, True


def _reconcile_sentence(a: dict | None, b: dict | None) -> dict[str, Any]:
    """문장 1개 reconcile. status ∈ final | judge | drop | review."""
    if a is None or b is None:
        ok = a if a is not None else b
        if ok is None:
            return {"status": "review", "relevant": False, "sentiment": "neutral", "aspects": [], "resolution": "classify_missing"}
        return {"status": "review", "relevant": ok["relevant"], "sentiment": ok["sentiment"], "aspects": ok["aspects"], "resolution": "partial_classify"}
    if not a["relevant"] and not b["relevant"]:
        return {"status": "drop", "resolution": "both_irrelevant"}
    if a["relevant"] != b["relevant"]:
        return {"status": "judge", "trigger": "relevance"}
    s_res, s_final, s_judge = _recon_sentiment(a["sentiment"], b["sentiment"])
    asp_res, asp_final, asp_judge = _recon_aspects(set(a["aspects"]), set(b["aspects"]))
    if s_judge or asp_judge:
        return {"status": "judge", "trigger": "sentiment" if s_judge else "aspect"}
    resolution = "union" if asp_res == "union" else ("sentiment_auto" if s_res == "sentiment_auto" else "agree")
    return {"status": "final", "relevant": True, "sentiment": s_final, "aspects": sorted(asp_final), "resolution": resolution}


def _candidate_order_ab(key: str) -> bool:
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % 2 == 0


def _judge_batch(
    client: LloaClient,
    *,
    system_prompt: str,
    doc_id: str,
    disputed: list[dict[str, Any]],
    max_tokens: int,
) -> dict[int, dict[str, Any]]:
    """분쟁 문장 batch judge. disputed item: {idx, sentence, a, b}. 반환 idx→{relevant,
    sentiment, aspects, chosen, reason} (un-anonymized). chunk 가드 적용."""
    results: dict[int, dict[str, Any]] = {}
    chunk: list[dict[str, Any]] = []
    chunk_chars = 0

    def flush(items: list[dict[str, Any]]):
        if not items:
            return
        payload_items = []
        order_by_idx: dict[int, bool] = {}
        for it in items:
            idx = it["idx"]
            order_ab = _candidate_order_ab(f"{doc_id}:{idx}")
            order_by_idx[idx] = order_ab
            c1, c2 = (it["a"], it["b"]) if order_ab else (it["b"], it["a"])
            payload_items.append({
                "sentence_index": idx,
                "sentence": it["sentence"],
                "candidate_1": _cand(c1),
                "candidate_2": _cand(c2),
            })
        user = json.dumps(payload_items, ensure_ascii=False)
        response = client.create_json_response(system=system_prompt, user=user, max_tokens=max_tokens)
        arr = _coerce_array(response.body)
        if arr is None:
            raise LloaResponseParseError(
                "clause_label_verify judge expected JSON array",
                raw_text=str(response.body),
                finish_reason=response.finish_reason,
            )
        for item in arr:
            if not isinstance(item, dict):
                continue
            try:
                idx = int(item.get("sentence_index"))
            except (TypeError, ValueError):
                continue
            results[idx] = _parse_judge_item(item)

    for it in disputed:
        approx = len(it["sentence"]) + 80
        if chunk and (len(chunk) >= _MAX_DISPUTED_PER_JUDGE_CALL or chunk_chars + approx > _MAX_JUDGE_INPUT_CHARS):
            flush(chunk)
            chunk, chunk_chars = [], 0
        chunk.append(it)
        chunk_chars += approx
    flush(chunk)
    return results


def _cand(label: dict[str, Any]) -> dict[str, Any]:
    return {"relevant": label["relevant"], "sentiment": label["sentiment"], "aspects": label["aspects"]}


def _parse_judge_item(item: dict[str, Any]) -> dict[str, Any]:
    """judge 출력 1문장 파싱. invalid aspect는 fallback 없이 표시(needs_review 트리거)."""
    chosen = str(item.get("chosen") or "").strip()
    relevant = bool(item.get("relevant"))
    raw_aspects = [str(x).strip() for x in item.get("aspects") or []]
    invalid = [a for a in raw_aspects if a not in _ALLOWED_ASPECT]
    aspects = [a for a in raw_aspects if a in _ALLOWED_ASPECT]
    sentiment = str(item.get("sentiment") or "neutral").strip()
    sentiment_invalid = sentiment not in _ALLOWED_SENTIMENT
    if sentiment_invalid:
        sentiment = "neutral"
    return {
        "relevant": relevant,
        "sentiment": sentiment,
        "aspects": aspects,
        "chosen": chosen,
        "reason": str(item.get("reason") or "").strip(),
        "invalid": bool(invalid) or sentiment_invalid or chosen == "review",
    }


def _explode(doc_id: str, sentence: str, sentiment: str, aspects: list[str], resolution: str, needs_review: bool) -> list[dict[str, Any]]:
    """문장 → aspect별 clause 행. 기존 clause_label {doc_id, clause, sentiment, aspect} 호환."""
    if not aspects:
        aspects = [_FALLBACK_ASPECT]
    rows = []
    for aspect in aspects:
        rows.append({
            "doc_id": doc_id,
            "clause": sentence,
            "sentiment": sentiment,
            "aspect": aspect,
            "resolution": resolution,
            "needs_review": needs_review,
            "source": "verify",
        })
    return rows


def run_dataset_clause_label_verify(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    clean_artifact_ref = str(payload.get("clean_artifact_ref") or "").strip()
    output_path_raw = str(payload.get("output_path") or "").strip()
    progress_path = str(payload.get("progress_path") or "").strip()
    if not dataset_version_id or not clean_artifact_ref or not output_path_raw:
        raise ValueError(
            "dataset_clause_label verify requires dataset_version_id, clean_artifact_ref, output_path"
        )

    classify_models = [str(m).strip() for m in (payload.get("classify_models") or []) if str(m).strip()]
    if len(classify_models) != 2 or classify_models[0] == classify_models[1]:
        raise ValueError("dataset_clause_label verify requires classify_models = 2 distinct model ids")
    model_a, model_b = classify_models[0], classify_models[1]
    judge_model = str(payload.get("judge_model") or "").strip() or model_b

    output_path = Path(output_path_raw)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()

    config = load_config()
    if not (config.lloa_api_key or "").strip():
        raise ValueError("dataset_clause_label verify requires LLOA API key — set LLOA_API_KEY")

    client_a = _client_for_model(config, model_a, reasoning_effort="low", prepend_no_think=config.lloa_prepend_no_think)
    client_b = _client_for_model(config, model_b, reasoning_effort="low", prepend_no_think=config.lloa_prepend_no_think)
    judge_effort = str(payload.get("judge_reasoning_effort") or "medium").strip() or "medium"
    judge_client = _client_for_model(config, judge_model, reasoning_effort=judge_effort, prepend_no_think=False)

    subject_config = _extract_subject_config(payload)
    classify_body, classify_version = load_prompt_body(
        _CLASSIFY_PROMPT_TASK, str(payload.get("clause_label_verify_prompt_version") or "").strip() or None
    )
    classify_system_prompt = _render_subject_prompt(_inject_taxonomy(classify_body), subject_config)
    judge_body, judge_version = load_prompt_body(
        _JUDGE_PROMPT_TASK, str(payload.get("judge_prompt_version") or "").strip() or None
    )
    judge_system_prompt = _render_subject_prompt(_inject_taxonomy(judge_body), subject_config)

    max_tokens = int(payload.get("max_tokens") or 8192)
    judge_max_tokens = int(payload.get("judge_max_tokens") or 4096)
    concurrency = max(1, int(payload.get("concurrency") or _DEFAULT_CONCURRENCY))

    rows = rt._iter_rows(clean_artifact_ref)
    total_rows = len(rows)
    if progress_path:
        write_progress(progress_path, processed_rows=0, total_rows=total_rows, started_at=started_at, message="clause_label verify queued")

    targets: list[tuple[int, str, str]] = []
    for index, row in enumerate(rows):
        doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
        cleaned_text = str(row.get("cleaned_text") or "").strip()
        if cleaned_text:
            targets.append((index, doc_id, cleaned_text))

    def _process_doc(item: tuple[int, str, str]) -> tuple[str, list[dict[str, Any]], dict[str, int]]:
        _, doc_id, doc_text = item
        sentences = _split_anchor_sentences(doc_text)
        stats = {k: 0 for k in ("agree", "union", "sentiment_auto", "judge", "needs_review", "dropped", "partial")}
        rows_out: list[dict[str, Any]] = []
        if not sentences:
            return doc_id, rows_out, stats

        def _label_safe(client: LloaClient):
            try:
                return _label_sentences(client, classify_system_prompt, sentences, max_tokens)
            except (LloaResponseParseError, OSError, ValueError):
                return None

        la = _label_safe(client_a)
        lb = _label_safe(client_b)

        disputed: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []  # 최종 행 자리 보존(judge 후 채움)
        for i, sentence in enumerate(sentences, start=1):
            a = la.get(i) if la else None
            b = lb.get(i) if lb else None
            rec = _reconcile_sentence(a, b)
            status = rec["status"]
            if status == "drop":
                stats["dropped"] += 1
                continue
            if status == "final":
                stats[rec["resolution"]] += 1
                rows_out.extend(_explode(doc_id, sentence, rec["sentiment"], rec["aspects"], rec["resolution"], False))
            elif status == "review":
                stats["needs_review"] += 1
                if rec["resolution"] == "partial_classify":
                    stats["partial"] += 1
                if rec["relevant"]:
                    rows_out.extend(_explode(doc_id, sentence, rec["sentiment"], rec["aspects"], rec["resolution"], True))
            elif status == "judge":
                disputed.append({"idx": i, "sentence": sentence, "a": a, "b": b})

        if disputed:
            try:
                judged = _judge_batch(
                    judge_client, system_prompt=judge_system_prompt, doc_id=doc_id,
                    disputed=disputed, max_tokens=judge_max_tokens,
                )
            except (LloaResponseParseError, OSError, ValueError):
                judged = {}  # judge 호출 실패 → 분쟁 문장 전부 needs_review로 격리
            for d in disputed:
                idx, sentence = d["idx"], d["sentence"]
                jr = judged.get(idx)
                if jr is None:
                    # judge 결과 누락 → needs_review (union aspects + neutral, 보수)
                    union_aspects = sorted(set(d["a"]["aspects"] if d["a"] else []) | set(d["b"]["aspects"] if d["b"] else []))
                    stats["judge"] += 1
                    stats["needs_review"] += 1
                    rows_out.extend(_explode(doc_id, sentence, "neutral", union_aspects, "needs_review", True))
                    continue
                stats["judge"] += 1
                if not jr["relevant"]:
                    stats["dropped"] += 1
                    continue
                if jr["invalid"] or not jr["aspects"]:
                    # invalid aspect / 빈 aspect → fallback 없이 needs_review
                    stats["needs_review"] += 1
                    rows_out.extend(_explode(doc_id, sentence, jr["sentiment"], jr["aspects"], "needs_review", True))
                else:
                    rows_out.extend(_explode(doc_id, sentence, jr["sentiment"], jr["aspects"], "judge", False))
        return doc_id, rows_out, stats

    rows_by_doc: dict[str, list[dict[str, Any]]] = {}
    agg = {k: 0 for k in ("agree", "union", "sentiment_auto", "judge", "needs_review", "dropped", "partial")}
    processed = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(_process_doc, t): t for t in targets}
        for future in as_completed(futures):
            doc_id, recs, stats = future.result()
            rows_by_doc[doc_id] = recs
            for k, v in stats.items():
                agg[k] += v
            processed += 1
            if progress_path and (processed % 10 == 0 or processed == len(targets)):
                write_progress(progress_path, processed_rows=processed, total_rows=total_rows, started_at=started_at, message="clause_label verify processing")

    clause_count = 0
    sentiment_counts: dict[str, int] = {s: 0 for s in _ALLOWED_SENTIMENT}
    aspect_counts: dict[str, int] = {a: 0 for a in _ALLOWED_ASPECT}
    with output_path.open("w", encoding="utf-8") as dst:
        for index, row in enumerate(rows):
            doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
            for rec in rows_by_doc.get(doc_id, []):
                rec["prompt_version"] = classify_version
                dst.write(json.dumps(rec, ensure_ascii=False))
                dst.write("\n")
                clause_count += 1
                sentiment_counts[rec["sentiment"]] = sentiment_counts.get(rec["sentiment"], 0) + 1
                aspect_counts[rec["aspect"]] = aspect_counts.get(rec["aspect"], 0) + 1

    if progress_path:
        write_progress(progress_path, processed_rows=total_rows, total_rows=total_rows, started_at=started_at, message="clause_label verify completed")

    summary = {
        "mode": "verify",
        "input_row_count": total_rows,
        "processed_row_count": len(rows_by_doc),
        "clause_count": clause_count,
        "resolution_counts": {
            "agree": agg["agree"], "union": agg["union"], "sentiment_auto": agg["sentiment_auto"],
            "judge": agg["judge"], "needs_review": agg["needs_review"], "partial_classify": agg["partial"],
        },
        "dropped_irrelevant_count": agg["dropped"],
        "sentiment_counts": sentiment_counts,
        "aspect_counts": aspect_counts,
        "concurrency": concurrency,
        "models": {"a": model_a, "b": model_b, "judge": judge_model},
        "prompt_version": classify_version,
        "judge_prompt_version": judge_version,
        "applied": {
            "classify_models": [model_a, model_b],
            "judge_model": judge_model,
            "subject_name": subject_config["subject_name"],
            "subject_type": subject_config["subject_type"],
        },
    }
    return {
        "notes": [
            f"dataset_clause_label verify — {len(rows_by_doc)} docs, {clause_count} clause rows "
            f"(agree={agg['agree']}, union={agg['union']}, sentiment_auto={agg['sentiment_auto']}, "
            f"judge={agg['judge']}, needs_review={agg['needs_review']})",
            f"models: a={model_a}, b={model_b}, judge={judge_model}",
        ],
        "artifact": rt._set_scope_fields(
            {
                "skill_name": "dataset_clause_label",
                "dataset_version_id": dataset_version_id,
                "clause_label_uri": str(output_path),
                "clause_label_ref": str(output_path),
                "summary": summary,
            },
            declared_result_scope="full_dataset",
            runtime_result_scope="full_dataset",
        ),
    }
