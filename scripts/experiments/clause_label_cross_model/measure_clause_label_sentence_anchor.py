#!/usr/bin/env python3.11
"""문장 앵커 교차모델 일치율 측정 (1회성 진단).

가설: clause_label의 발산은 *분절(segmentation)*에 집중돼 있다(절 겹침 ~25%,
정렬된 절 라벨은 84% 일치 — 2026-06-16 측정). 분절을 결정론적 문장 단위(kss)로
고정하면, 두 모델이 *같은 문장 리스트*에 라벨하므로 1:1 비교가 살아난다. 이때
문장별 aspect/sentiment 일치율이 충분히 높으면 "문장 앵커 + 교차검증" 설계가
유효하다(doc_genuineness verify 패턴을 문장 단위로 복원).

흐름: doc → kss 문장 분리 → 각 모델이 문장별 {relevant, aspect, sentiment} →
인덱스로 1:1 정렬 → 일치율 측정.

측정 항목:
  - relevant 일치율  = 두 모델이 "이 문장이 후기성 절이냐"에 합의하는 비율
                       (← 분절 swing의 문장단위 대체 지표)
  - aspect / sentiment 일치율 = 둘 다 relevant로 본 문장에서 라벨 일치
  → clause-mode(분절 자유)의 84% 대비 얼마나 안정적인가 비교

실행 (LLOA env 필요):
  LLOA_API_KEY=... LLOA_API_URL=... LLOA_REASONING_EFFORT=low \
  PYTHONPATH=workers/python-ai/src python3.11 \
    scripts/measure_clause_label_sentence_anchor.py \
      --models wisenut/wise-lloa-max-v1.2.1,wisenut/wise-lloa-ultra-v1.1.0 \
      --limit 50 --out-dir /tmp/clause_sentence_anchor
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from python_ai_worker.clients.lloa import LloaClient, LloaConfig
from python_ai_worker.config import load_config
from python_ai_worker.dataset_build.clause_label import (
    _ALLOWED_ASPECT,
    _ALLOWED_SENTIMENT,
    _FALLBACK_ASPECT,
)
from python_ai_worker.runtime.common import _split_sentences

SUBJECT_NAME = "강릉 국가유산야행"
SUBJECT_TYPE = "festival"


def build_system_prompt() -> str:
    aspects = ", ".join(sorted(_ALLOWED_ASPECT))
    return (
        f"당신은 '{SUBJECT_NAME}'({SUBJECT_TYPE}) 관련 SNS 후기 문장을 분류한다.\n"
        "입력은 번호가 매겨진 문장 목록이다. 각 입력 문장마다 정확히 하나의 객체를 반환한다.\n"
        "필드:\n"
        "- index: 입력 문장 번호(1부터, 입력과 동일)\n"
        f"- relevant: 문장이 '{SUBJECT_NAME}' 관련 의견/경험/평가면 true, "
        "단순 사실나열·인사·홍보·노이즈·무관 내용이면 false\n"
        f"- aspect: relevant=true면 다음 중 하나 [{aspects}], 애매하면 \"{_FALLBACK_ASPECT}\". "
        f"false면 \"{_FALLBACK_ASPECT}\"\n"
        "- sentiment: relevant=true면 positive|negative|neutral. false면 \"neutral\"\n"
        "출력은 입력 문장 수와 정확히 같은 길이의 JSON 배열만. 설명·코드펜스 금지. /no_think"
    )


def label_doc(client: LloaClient, system_prompt: str, sentences: list[str], max_tokens: int) -> dict[int, dict]:
    """문장 리스트 → {index(1-based): {relevant, aspect, sentiment}}."""
    numbered = "\n".join(f"{i}. {s}" for i, s in enumerate(sentences, start=1))
    user = f"대상: {SUBJECT_NAME}\n문장 목록:\n{numbered}"
    resp = client.create_json_response(system=system_prompt, user=user, max_tokens=max_tokens)
    body = resp.body
    if isinstance(body, dict):
        for key in ("result", "sentences", "data", "items"):
            if isinstance(body.get(key), list):
                body = body[key]
                break
    out: dict[int, dict] = {}
    if not isinstance(body, list):
        return out
    for item in body:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except (TypeError, ValueError):
            continue
        relevant = bool(item.get("relevant"))
        aspect = str(item.get("aspect") or _FALLBACK_ASPECT).strip()
        if aspect not in _ALLOWED_ASPECT:
            aspect = _FALLBACK_ASPECT
        sentiment = str(item.get("sentiment") or "neutral").strip()
        if sentiment not in _ALLOWED_SENTIMENT:
            sentiment = "neutral"
        out[idx] = {"relevant": relevant, "aspect": aspect, "sentiment": sentiment}
    return out


def build_docs(csv_path: Path, limit: int | None) -> list[tuple[str, list[str]]]:
    docs: list[tuple[str, list[str]]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if limit is not None and len(docs) >= limit:
                break
            body = (row.get("본문") or "").strip()
            if not body:
                continue
            row_id = (row.get("수집ID(고유)") or f"row:{i}").strip()
            sentences, _ = _split_sentences(body, language="ko")
            sentences = [s for s in sentences if s.strip()]
            if sentences:
                docs.append((row_id, sentences))
    return docs


def client_for(model: str, cfg) -> LloaClient:
    return LloaClient(
        LloaConfig(
            api_key=cfg.lloa_api_key,
            api_url=cfg.lloa_api_url,
            model=model,
            max_tokens=cfg.lloa_max_tokens,
            timeout_sec=cfg.lloa_timeout_sec,
            reasoning_effort=cfg.lloa_reasoning_effort,
            prepend_no_think=cfg.lloa_prepend_no_think,
        )
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="docs/eval/quality_v1/datasets/festival_sample_50.csv")
    ap.add_argument("--models", required=True)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--max-tokens", type=int, default=8192)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out-dir", default="/tmp/clause_sentence_anchor")
    args = ap.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    if len(models) != 2:
        raise SystemExit("--models 는 콤마구분 2개")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    if not (cfg.lloa_api_key or "").strip():
        raise SystemExit("LLOA_API_KEY 필요")
    system_prompt = build_system_prompt()
    docs = build_docs(Path(args.csv), args.limit)
    total_sentences = sum(len(s) for _, s in docs)
    print(f"[docs] {len(docs)} docs, {total_sentences} sentences (kss)")

    clients = {m: client_for(m, cfg) for m in models}
    # (doc_idx, model) 작업을 병렬로
    tasks = [(di, m) for di in range(len(docs)) for m in models]
    results: dict[tuple[int, str], dict[int, dict]] = {}

    def run(task):
        di, m = task
        _, sents = docs[di]
        try:
            return task, label_doc(clients[m], system_prompt, sents, args.max_tokens)
        except Exception as e:  # noqa: BLE001
            return task, {"__error__": str(e)}

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for task, labels in ex.map(run, tasks):
            results[task] = labels
    wall = time.monotonic() - t0

    # 집계
    ma, mb = models
    rel_agree = rel_total = 0
    both_rel = 0
    aspect_agree = sent_agree = both_agree = 0
    a_rel_count = b_rel_count = 0
    parse_skipped = 0

    for di, (row_id, sents) in enumerate(docs):
        la = results.get((di, ma), {})
        lb = results.get((di, mb), {})
        if "__error__" in la or "__error__" in lb:
            parse_skipped += 1
            continue
        for idx in range(1, len(sents) + 1):
            a = la.get(idx)
            b = lb.get(idx)
            if a is None or b is None:
                continue  # 한 모델이 그 문장 라벨 누락 → 비교 제외(별도 카운트 가능)
            rel_total += 1
            if a["relevant"]:
                a_rel_count += 1
            if b["relevant"]:
                b_rel_count += 1
            if a["relevant"] == b["relevant"]:
                rel_agree += 1
            if a["relevant"] and b["relevant"]:
                both_rel += 1
                if a["aspect"] == b["aspect"]:
                    aspect_agree += 1
                if a["sentiment"] == b["sentiment"]:
                    sent_agree += 1
                if a["aspect"] == b["aspect"] and a["sentiment"] == b["sentiment"]:
                    both_agree += 1

    def pct(n, d):
        return f"{100*n/d:.1f}%" if d else "n/a"

    report = [
        "# clause_label 문장 앵커 교차모델 일치율",
        f"- 모델 A = {ma} / B = {mb}",
        f"- doc {len(docs)} / 문장 {total_sentences} (kss) / 비교가능 문장쌍 {rel_total}",
        f"- parse 실패 doc(한 모델이라도 에러) = {parse_skipped}",
        f"- wall = {wall:.1f}s (doc×model {len(tasks)} 호출, workers={args.workers})",
        "",
        "## relevant(절 여부) 일치 — 분절 swing의 문장단위 대체 지표",
        f"- relevant 일치율: {pct(rel_agree, rel_total)}  (= 두 모델이 후기성 문장 여부에 합의)",
        f"- A relevant 비율: {pct(a_rel_count, rel_total)} / B relevant 비율: {pct(b_rel_count, rel_total)}",
        f"- 둘 다 relevant 문장: {both_rel}",
        "",
        "## 라벨 일치 (둘 다 relevant 문장 기준)",
        f"- aspect 일치: {pct(aspect_agree, both_rel)}",
        f"- sentiment 일치: {pct(sent_agree, both_rel)}",
        f"- 둘 다 일치: {pct(both_agree, both_rel)}",
        "",
        "## 비교 기준 (2026-06-16 clause-mode: 분절 자유)",
        "- clause-mode: 절 겹침 24.7% / 정렬절 aspect 83.8% sentiment 84.8%",
        "- 문장앵커 relevant 일치율이 높으면 → 분절 swing이 문장 고정으로 해소됨 → 설계 GO",
        "- 라벨 일치율이 clause-mode와 비슷하거나 높으면 → judge 부하 가벼움",
    ]
    text = "\n".join(report)
    (out_dir / "sentence_anchor_report.md").write_text(text + "\n", encoding="utf-8")
    # raw 저장
    raw = {f"{di}:{m}": results.get((di, m), {}) for di in range(len(docs)) for m in models}
    (out_dir / "raw_labels.json").write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    print("\n" + text)
    print(f"\n[report] {out_dir/'sentence_anchor_report.md'}")


if __name__ == "__main__":
    main()
