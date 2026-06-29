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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..clients.lloa import LloaClient, LloaConfig, LloaResponseParseError
from ..config import load_config
from ..prompt_options import load_prompt_body
from . import _cancel
from ._chunking import build_sentence_chunks, split_anchor_sentences as _split_anchor_sentences
from ._common import write_progress
from .clause_label import (
    _ALLOWED_ASPECT,
    _ALLOWED_SENTIMENT,
    _DEFAULT_CONCURRENCY,
    _FALLBACK_ASPECT,
    _extract_subject_config,
    _inject_primary_area,
    _inject_taxonomy,
    _load_genuineness_filter,
    _render_subject_prompt,
    PRIMARY_AREA_KEYS,
    resolve_clause_label_taxonomy,
)

# 2026-06-17 — 단일·교차검증 통일: classify 프롬프트를 단일 모드와 동일한
# clause_label(문장 형식 v3/v4)로 일원화. 옛 clause_label_verify 프롬프트는 제거.
_CLASSIFY_PROMPT_TASK = "clause_label"
_JUDGE_PROMPT_TASK = "clause_label_verify_judge"

# ADR-030 Phase 1 — primary_area "대상" 축 병렬 관측 필드. 각 모델이 aspects와 별도로
# 문장의 *대상*을 1개로 라벨. 최종 라벨로는 안 쓰고(reconcile 안 함) model A/B
# 스냅샷 + summary 통계(일치율/혼동쌍/분포)로만 적재해 ADR-030을 데이터로 검증한다.
# allowed key set은 config/primary_area(PRIMARY_AREA_KEYS)에서 — 프롬프트 inject와 동일 source.


def _coerce_primary_area(v: Any) -> str | None:
    pa = str(v or "").strip()
    if not pa:
        return None
    return pa if pa in PRIMARY_AREA_KEYS else "etc"

# judge batch 가드
_MAX_DISPUTED_PER_JUDGE_CALL = 20
_MAX_JUDGE_INPUT_CHARS = 8000

# classify chunking (silverone 2026-06-16) — 긴 doc은 문장이 많아 classify 한 콜의
# 입력·출력이 커져 truncation/parse 실패를 일으킨다. 문장 앵커는 이미 고정 단위라
# doc을 쪼개지 않고 *LLM 호출 단위*만 chunk로 나눈다(doc_id·sentence_index 보존).
# doc_genuineness의 truncate와 성격이 다르다(그쪽은 row 1개 유지 + 입력 truncate).
_MAX_CHUNK_SENTENCES = 40
_MAX_CHUNK_CHARS = 12000
_DEFAULT_OVERLAP_SENTENCES = 0

# 문장 splitter·chunk helper는 _chunking 공통 모듈로 이전(ADR-029) — doc_genuineness와
# 동일 splitter를 써야 genuine_spans의 sentence_index가 정합한다.


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


def _parse_label_item(
    item: dict[str, Any],
    allowed_aspect: frozenset[str] = _ALLOWED_ASPECT,
    fallback_aspect: str = _FALLBACK_ASPECT,
) -> tuple[int, dict[str, Any]] | None:
    try:
        idx = int(item.get("index"))
    except (TypeError, ValueError):
        return None
    relevant = bool(item.get("relevant"))
    aspects = {a for a in (str(x).strip() for x in item.get("aspects") or []) if a in allowed_aspect}
    if relevant and not aspects:
        aspects = {fallback_aspect}
    sentiment = str(item.get("sentiment") or "neutral").strip()
    if sentiment not in _ALLOWED_SENTIMENT:
        sentiment = "neutral"
    # ADR-030 Phase 1 — primary_area 병렬 관측(있으면). aspects/sentiment 판정엔 안 쓴다.
    return idx, {
        "relevant": relevant,
        "sentiment": sentiment,
        "aspects": sorted(aspects),
        "primary_area": _coerce_primary_area(item.get("primary_area")),
    }


def _coerce_array(body: Any) -> list | None:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("result", "sentences", "data", "items"):
            if isinstance(body.get(key), list):
                return body[key]
    return None


def _label_sentences(
    client: LloaClient,
    system_prompt: str,
    sentences: list[str],
    max_tokens: int,
    allowed_aspect: frozenset[str] = _ALLOWED_ASPECT,
    fallback_aspect: str = _FALLBACK_ASPECT,
) -> dict[int, dict[str, Any]]:
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
            parsed = _parse_label_item(item, allowed_aspect, fallback_aspect)
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
        # silverone 2026-06-18 — 한 모델만 라벨(다른 모델이 문장 드롭) → judge로 보내
        # 권위 라벨을 받는다. 옛 partial_classify(단일 라벨 무비판 채택 + 검토 큐 격리)
        # 대신 judge가 원문 기준으로 독립 판정한다(후보 1개 + null 후보). judge가
        # 못 가린 것만 needs_review로 남아 검토 큐가 진짜 어려운 절만 갖게 된다.
        return {"status": "judge", "trigger": "partial_classify"}
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
    allowed_aspect: frozenset[str] = _ALLOWED_ASPECT,
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
            results[idx] = _parse_judge_item(item, allowed_aspect)

    for it in disputed:
        approx = len(it["sentence"]) + 80
        if chunk and (len(chunk) >= _MAX_DISPUTED_PER_JUDGE_CALL or chunk_chars + approx > _MAX_JUDGE_INPUT_CHARS):
            flush(chunk)
            chunk, chunk_chars = [], 0
        chunk.append(it)
        chunk_chars += approx
    flush(chunk)
    return results


def _cand(label: dict[str, Any] | None) -> dict[str, Any] | None:
    # partial(한 모델 미분류) → None을 judge에 null 후보로 전달. judge는 후보를 무시하고
    # 원문 기준으로 독립 판정하므로 null 후보가 있어도 권위 라벨을 낸다.
    if label is None:
        return None
    return {"relevant": label["relevant"], "sentiment": label["sentiment"], "aspects": label["aspects"]}


def _parse_judge_item(
    item: dict[str, Any], allowed_aspect: frozenset[str] = _ALLOWED_ASPECT
) -> dict[str, Any]:
    """judge 출력 1문장 파싱. invalid aspect는 fallback 없이 표시(needs_review 트리거)."""
    chosen = str(item.get("chosen") or "").strip()
    relevant = bool(item.get("relevant"))
    raw_aspects = [str(x).strip() for x in item.get("aspects") or []]
    invalid = [a for a in raw_aspects if a not in allowed_aspect]
    aspects = [a for a in raw_aspects if a in allowed_aspect]
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


def _model_result_obj(label: dict[str, Any] | None) -> dict[str, Any] | None:
    """classify 모델 1개 결과 snapshot. 검토 큐의 model A/B 비교용. None이면 미분류."""
    if not label:
        return None
    return {
        "relevant": bool(label.get("relevant")),
        "sentiment": str(label.get("sentiment") or "neutral"),
        "aspects": [str(a) for a in (label.get("aspects") or [])],
        # ADR-030 Phase 1 — 대상 축 병렬 관측(있으면). None이면 모델이 미산출.
        "primary_area": label.get("primary_area"),
    }


def _judge_result_obj(jr: dict[str, Any] | None) -> dict[str, Any] | None:
    """judge 결과 snapshot(불일치 해소 사유 포함). None이면 judge 미개입(합의/자동)."""
    if not jr:
        return None
    return {
        "relevant": bool(jr.get("relevant")),
        "sentiment": str(jr.get("sentiment") or "neutral"),
        "aspects": [str(a) for a in (jr.get("aspects") or [])],
        "reason": str(jr.get("reason") or ""),
    }


def _explode(
    doc_id: str, sentence: str, sentiment: str, aspects: list[str],
    resolution: str, needs_review: bool, *, sentence_index: int, chunk_index: int,
    model_a: dict[str, Any] | None = None, model_b: dict[str, Any] | None = None,
    judge: dict[str, Any] | None = None, fallback_aspect: str = _FALLBACK_ASPECT,
) -> list[dict[str, Any]]:
    """문장 → aspect별 clause 행. 기존 clause_label {doc_id, clause, sentiment, aspect}
    호환 + verify 추가 필드(resolution/needs_review/sentence_index/chunk_index) +
    검토 큐용 model A/B/judge snapshot(ADR-028 풍부 검토 큐)."""
    if not aspects:
        aspects = [fallback_aspect]
    model_a_result = _model_result_obj(model_a)
    model_b_result = _model_result_obj(model_b)
    judge_result = _judge_result_obj(judge)
    rows = []
    for aspect in aspects:
        rows.append({
            "doc_id": doc_id,
            "clause": sentence,
            "sentiment": sentiment,
            "aspect": aspect,
            "resolution": resolution,
            "needs_review": needs_review,
            "sentence_index": sentence_index,
            "chunk_index": chunk_index,
            "model_a_result": model_a_result,
            "model_b_result": model_b_result,
            "judge_result": judge_result,
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
    # taxonomy per-request (Phase 3) — payload['taxonomy_id']로 선택. aspect 주입·
    # validation·fallback 전부 이 taxonomy 기준. 미지정 시 DEFAULT.
    taxonomy = resolve_clause_label_taxonomy(payload)
    allowed_aspect = taxonomy.aspect_keys_set
    fallback_aspect = taxonomy.fallback_aspect
    classify_body, classify_version = load_prompt_body(
        _CLASSIFY_PROMPT_TASK, str(payload.get("clause_label_prompt_version") or "").strip() or None
    )
    classify_system_prompt = _render_subject_prompt(
        _inject_primary_area(_inject_taxonomy(classify_body, taxonomy)), subject_config
    )
    judge_body, judge_version = load_prompt_body(
        _JUDGE_PROMPT_TASK, str(payload.get("judge_prompt_version") or "").strip() or None
    )
    judge_system_prompt = _render_subject_prompt(_inject_taxonomy(judge_body, taxonomy), subject_config)

    max_tokens = int(payload.get("max_tokens") or 8192)
    judge_max_tokens = int(payload.get("judge_max_tokens") or 4096)
    concurrency = max(1, int(payload.get("concurrency") or _DEFAULT_CONCURRENCY))
    max_chunk_sentences = max(1, int(payload.get("max_chunk_sentences") or _MAX_CHUNK_SENTENCES))
    max_chunk_chars = max(1, int(payload.get("max_chunk_chars") or _MAX_CHUNK_CHARS))
    overlap_sentences = max(0, int(payload.get("overlap_sentences") if payload.get("overlap_sentences") is not None else _DEFAULT_OVERLAP_SENTENCES))

    # tier 필터(non_review skip) + genuine_spans (ADR-029) — 단일 모드 clause_label과
    # 동일 정책. spans가 있으면 _process_doc이 그 문장 구간만 처리한다.
    include_tiers, tier_by_doc, spans_by_doc = _load_genuineness_filter(payload)

    rows = rt._iter_rows(clean_artifact_ref)
    total_rows = len(rows)  # 전체 input row 수 (summary.input_row_count 용)

    targets: list[tuple[int, str, str]] = []
    skipped_by_filter = 0
    for index, row in enumerate(rows):
        doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
        cleaned_text = str(row.get("cleaned_text") or "").strip()
        if not cleaned_text:
            continue
        if include_tiers is not None and tier_by_doc.get(doc_id) not in include_tiers:
            skipped_by_filter += 1
            continue
        targets.append((index, doc_id, cleaned_text))

    # 진행률/ETA 분모는 실제 LLM 처리 대상(targets) 기준. genuineness 필터로 skip된
    # doc은 LLM 호출이 없어 즉시 끝나므로 분모에서 제외해야 화면 총계/ETA가 정직하다.
    # (전체 input은 summary.input_row_count로 따로 노출.)
    target_count = len(targets)
    if progress_path:
        write_progress(progress_path, processed_rows=0, total_rows=target_count, started_at=started_at, message="clause_label verify queued")

    def _process_doc(item: tuple[int, str, str]) -> tuple[str, list[dict[str, Any]], dict[str, int], int]:
        _, doc_id, doc_text = item
        stats = {k: 0 for k in ("agree", "union", "sentiment_auto", "judge", "needs_review", "dropped", "partial", "chunk_failures")}
        # 취소 후 시작된 doc은 작업 없이 즉시 반환 → 큐 잔여가 순식간에 비워짐.
        if cancel_event.is_set():
            return doc_id, [], stats, 0
        sentences = _split_anchor_sentences(doc_text)
        rows_out: list[dict[str, Any]] = []
        if not sentences:
            return doc_id, rows_out, stats, 0

        # genuine_spans 제한 (ADR-029) — doc_genuineness chunk aggregate가 진성 구간을
        # 주면 그 문장만 처리(non_review 구간 재처리 안 함). 없으면 전체 doc. 두 skill이
        # 공통 splitter라 sentence_index 정합 — 출력은 전역 1-based index를 그대로 보존.
        spans = spans_by_doc.get(doc_id)
        if spans:
            allowed: set[int] = set()
            for sp in spans:
                if not isinstance(sp, dict):
                    continue
                try:
                    s0 = int(sp.get("sentence_start"))
                    s1 = int(sp.get("sentence_end"))
                except (TypeError, ValueError):
                    continue
                for gi in range(max(1, s0), min(len(sentences), s1) + 1):
                    allowed.add(gi)
            pairs = [(gi, sentences[gi - 1]) for gi in sorted(allowed)]
        else:
            pairs = list(enumerate(sentences, start=1))
        if not pairs:
            return doc_id, rows_out, stats, 0

        # 문장 앵커 chunking — classify LLM 호출만 chunk로 나눈다(처리 대상 문장 기준).
        allowed_sentences = [s for _, s in pairs]
        chunks = build_sentence_chunks(
            allowed_sentences, max_sentences=max_chunk_sentences, max_chars=max_chunk_chars, overlap=overlap_sentences,
        )

        def _label_doc_chunked(client: LloaClient) -> tuple[dict[int, dict[str, Any]], dict[int, int], int]:
            """chunk별 classify → 전역 sentence_index 기준 merge. chunk-local index를 pairs로
            전역 index 복원(genuine_spans면 비연속). chunk 실패는 그 문장만 label 없음
            (merge에서 partial/needs_review). overlap이면 먼저 본 chunk 우선(결정론적)."""
            labels: dict[int, dict[str, Any]] = {}
            chunk_of: dict[int, int] = {}
            fails = 0
            for ci, (start0, sub) in enumerate(chunks):
                # 중단(silverone 2026-06-29) — 긴 문서가 in-flight여도 chunk 사이에서 멈춤.
                if cancel_event.is_set():
                    break
                try:
                    local = _label_sentences(client, classify_system_prompt, sub, max_tokens, allowed_aspect, fallback_aspect)
                except (LloaResponseParseError, OSError, ValueError):
                    fails += 1
                    continue
                for li, label in local.items():
                    pos = start0 + (li - 1)  # 0-based into allowed pairs
                    if 0 <= pos < len(pairs):
                        gi = pairs[pos][0]  # 전역 1-based sentence_index
                        if gi not in labels:
                            labels[gi] = label
                            chunk_of[gi] = ci
            return labels, chunk_of, fails

        la, chunk_a, fa = _label_doc_chunked(client_a)
        lb, chunk_b, fb = _label_doc_chunked(client_b)
        stats["chunk_failures"] = fa + fb

        def _chunk_idx(i: int) -> int:
            return chunk_a.get(i, chunk_b.get(i, 0))

        disputed: list[dict[str, Any]] = []
        for gi, sentence in pairs:
            a = la.get(gi)
            b = lb.get(gi)
            ci = _chunk_idx(gi)
            rec = _reconcile_sentence(a, b)
            status = rec["status"]
            if status == "drop":
                stats["dropped"] += 1
            elif status == "final":
                stats[rec["resolution"]] += 1
                rows_out.extend(_explode(doc_id, sentence, rec["sentiment"], rec["aspects"], rec["resolution"], False, sentence_index=gi, chunk_index=ci, model_a=a, model_b=b, fallback_aspect=fallback_aspect))
            elif status == "review":
                stats["needs_review"] += 1
                if rec["resolution"] == "partial_classify":
                    stats["partial"] += 1
                if rec["relevant"]:
                    rows_out.extend(_explode(doc_id, sentence, rec["sentiment"], rec["aspects"], rec["resolution"], True, sentence_index=gi, chunk_index=ci, model_a=a, model_b=b, fallback_aspect=fallback_aspect))
            elif status == "judge":
                disputed.append({"idx": gi, "sentence": sentence, "a": a, "b": b, "chunk_index": ci})

        if disputed:
            try:
                judged = _judge_batch(
                    judge_client, system_prompt=judge_system_prompt, doc_id=doc_id,
                    disputed=disputed, max_tokens=judge_max_tokens, allowed_aspect=allowed_aspect,
                )
            except (LloaResponseParseError, OSError, ValueError):
                judged = {}  # judge 호출 실패 → 분쟁 문장 전부 needs_review로 격리
            for d in disputed:
                idx, sentence, ci = d["idx"], d["sentence"], d["chunk_index"]
                jr = judged.get(idx)
                if jr is None:
                    # judge 결과 누락 → needs_review (union aspects + neutral, 보수)
                    union_aspects = sorted(set(d["a"]["aspects"] if d["a"] else []) | set(d["b"]["aspects"] if d["b"] else []))
                    stats["judge"] += 1
                    stats["needs_review"] += 1
                    rows_out.extend(_explode(doc_id, sentence, "neutral", union_aspects, "needs_review", True, sentence_index=idx, chunk_index=ci, model_a=d["a"], model_b=d["b"], fallback_aspect=fallback_aspect))
                    continue
                stats["judge"] += 1
                if not jr["relevant"]:
                    stats["dropped"] += 1
                    continue
                if jr["invalid"] or not jr["aspects"]:
                    # invalid aspect / 빈 aspect → fallback 없이 needs_review
                    stats["needs_review"] += 1
                    rows_out.extend(_explode(doc_id, sentence, jr["sentiment"], jr["aspects"], "needs_review", True, sentence_index=idx, chunk_index=ci, model_a=d["a"], model_b=d["b"], judge=jr, fallback_aspect=fallback_aspect))
                else:
                    rows_out.extend(_explode(doc_id, sentence, jr["sentiment"], jr["aspects"], "judge", False, sentence_index=idx, chunk_index=ci, model_a=d["a"], model_b=d["b"], judge=jr, fallback_aspect=fallback_aspect))
        return doc_id, rows_out, stats, len(chunks)

    rows_by_doc: dict[str, list[dict[str, Any]]] = {}
    agg = {k: 0 for k in ("agree", "union", "sentiment_auto", "judge", "needs_review", "dropped", "partial", "chunk_failures")}
    total_chunks = 0
    chunked_doc_count = 0
    processed = 0
    # 빌드 중단(silverone 2026-06-29) — /tasks/cancel로 event set 시 남은 doc 멈추고 보존.
    cancelled = False
    cancel_event = _cancel.begin(dataset_version_id)
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(_process_doc, t): t for t in targets}
        for future in as_completed(futures):
            # 취소 감지 시 즉시 멈춘다(남은 future 취소 + 탈출). 부분 결과 저장 안 함.
            # break해야 진행률도 그 자리에 멈춘다(드레인하면 완료수가 치솟음).
            if cancel_event.is_set():
                for pending in futures:
                    pending.cancel()
                cancelled = True
                break
            doc_id, recs, stats, chunk_count = future.result()
            rows_by_doc[doc_id] = recs
            for k, v in stats.items():
                agg[k] += v
            total_chunks += chunk_count
            if chunk_count > 1:
                chunked_doc_count += 1
            processed += 1
            if progress_path and (processed % 10 == 0 or processed == target_count):
                write_progress(progress_path, processed_rows=processed, total_rows=target_count, started_at=started_at, message="clause_label verify processing")
    _cancel.end(dataset_version_id)

    clause_count = 0
    sentiment_counts: dict[str, int] = {s: 0 for s in _ALLOWED_SENTIMENT}
    aspect_counts: dict[str, int] = {a: 0 for a in taxonomy.aspect_keys_set}
    # ADR-030 Phase 1 — primary_area 병렬 관측 통계(문장 단위 dedup). aspects 판정과 독립.
    pa_seen: set = set()
    pa_both = pa_agree = pa_a_overall = 0
    pa_conf: dict[str, int] = {}
    pa_dist_a: dict[str, int] = {}
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
                pk = (doc_id, rec.get("sentence_index"))
                if pk not in pa_seen:
                    pa_seen.add(pk)
                    a_pa = (rec.get("model_a_result") or {}).get("primary_area")
                    b_pa = (rec.get("model_b_result") or {}).get("primary_area")
                    if a_pa and b_pa:
                        pa_both += 1
                        pa_dist_a[a_pa] = pa_dist_a.get(a_pa, 0) + 1
                        if a_pa == "overall":
                            pa_a_overall += 1
                        if a_pa == b_pa:
                            pa_agree += 1
                        else:
                            ck = "↔".join(sorted([a_pa, b_pa]))
                            pa_conf[ck] = pa_conf.get(ck, 0) + 1

    if progress_path:
        write_progress(
            progress_path,
            processed_rows=processed if cancelled else target_count,
            total_rows=target_count,
            started_at=started_at,
            message="clause_label verify cancelled" if cancelled else "clause_label verify completed",
        )

    summary = {
        "mode": "verify",
        "input_row_count": total_rows,
        "processed_row_count": len(rows_by_doc),
        "cancelled": cancelled,
        "clause_count": clause_count,
        "resolution_counts": {
            "agree": agg["agree"], "union": agg["union"], "sentiment_auto": agg["sentiment_auto"],
            "judge": agg["judge"], "needs_review": agg["needs_review"], "partial_classify": agg["partial"],
        },
        "dropped_irrelevant_count": agg["dropped"],
        "sentiment_counts": sentiment_counts,
        "aspect_counts": aspect_counts,
        # ADR-030 Phase 1 — 대상 축 병렬 관측. 최종 라벨 아님(reconcile 안 함). 두 모델
        # 일치율/혼동쌍/분포/overall비중으로 2축 구조를 데이터 검증.
        "primary_area": {
            "both_count": pa_both,
            "agree_count": pa_agree,
            "agreement_rate": round(pa_agree / pa_both, 4) if pa_both else None,
            "overall_ratio_a": round(pa_a_overall / pa_both, 4) if pa_both else None,
            "distribution_model_a": pa_dist_a,
            "confusion_top": dict(sorted(pa_conf.items(), key=lambda kv: -kv[1])[:10]),
        },
        "concurrency": concurrency,
        "chunking": {
            "enabled": True,
            "strategy": "sentence_window",
            "max_chunk_sentences": max_chunk_sentences,
            "max_chunk_chars": max_chunk_chars,
            "overlap_sentences": overlap_sentences,
            "chunk_count": total_chunks,
            "chunked_doc_count": chunked_doc_count,
            "chunk_failure_count": agg["chunk_failures"],
        },
        "models": {"a": model_a, "b": model_b, "judge": judge_model},
        # 어떤 aspect taxonomy로 빌드됐는지 — analyze 시 planner taxonomy_id와 정합성
        # 체크에 사용 (single 모드와 동일 계약). per-request taxonomy(Phase 3).
        "taxonomy_id": taxonomy.taxonomy_id,
        "taxonomy_hash": taxonomy.taxonomy_hash,
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
