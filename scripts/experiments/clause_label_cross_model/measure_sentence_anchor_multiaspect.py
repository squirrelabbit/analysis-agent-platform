#!/usr/bin/env python3.11
"""문장 앵커 + multi-label aspect 교차모델 측정 (1회성 진단).

측정 2(단일 aspect, 2026-06-16)에서 relevant kappa 0.731 / sentiment 94.3%로
"문장 고정 → 교차검증 가능" 가설은 통과했으나 aspect 일치율 69.8%로 떨어졌다.
가설: aspect는 sub-sentence라 "1문장=1aspect" 강제가 인위적 불일치를 만든다.
→ 문장별 aspect를 *multi-label*로 바꿔 재측정해 이 구조적 원인을 격리한다.

추가 진단:
  - aspect cardinality 분포 (≥2 aspect 문장 비율) — 단일강제가 진범인지 직접 검증
  - aspect-set Jaccard / exact-set match
  - subset 관계 (Ultra aspects ⊆ Max?) — Jaccard 상승이 진짜 합의인지 구분

실행:
  LLOA_API_KEY=... LLOA_API_URL=... LLOA_REASONING_EFFORT=low \
  PYTHONPATH=workers/python-ai/src python3.11 \
    scripts/measure_sentence_anchor_multiaspect.py \
      --models wisenut/wise-lloa-max-v1.2.1,wisenut/wise-lloa-ultra-v1.1.0 \
      --limit 50 --out-dir /tmp/clause_sentence_multiaspect
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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
_REGEX_SENTENCE_SPLIT = re.compile(r"(?<=[.!?。！？])\s+|\n+")


def build_system_prompt() -> str:
    aspects = ", ".join(sorted(_ALLOWED_ASPECT))
    return (
        f"당신은 '{SUBJECT_NAME}'({SUBJECT_TYPE}) 관련 SNS 후기 문장을 분류한다.\n"
        "입력은 번호가 매겨진 문장 목록이다. 각 입력 문장마다 정확히 하나의 객체를 반환한다.\n"
        "필드:\n"
        "- index: 입력 문장 번호(1부터, 입력과 동일)\n"
        f"- relevant: 문장이 '{SUBJECT_NAME}' 관련 의견/경험/평가면 true, "
        "단순 사실나열·인사·홍보·노이즈·무관 내용이면 false\n"
        f"- aspects: relevant=true면 이 문장이 *다루는 모든* aspect를 [{aspects}]에서 "
        "골라 배열로(보통 1~2개, 여러 측면 언급 시 모두 포함). 애매하면 "
        f"[\"{_FALLBACK_ASPECT}\"]. relevant=false면 []\n"
        "- sentiment: relevant=true면 positive|negative|neutral. false면 \"neutral\"\n"
        "출력은 입력 문장 수와 정확히 같은 길이의 JSON 배열만. 설명·코드펜스 금지. /no_think"
    )


def load_system_prompt(path: str) -> str:
    if not path:
        return build_system_prompt()
    return Path(path).read_text(encoding="utf-8").strip()


def label_doc(client, system_prompt, sentences, max_tokens):
    numbered = "\n".join(f"{i}. {s}" for i, s in enumerate(sentences, start=1))
    user = f"대상: {SUBJECT_NAME}\n문장 목록:\n{numbered}"
    resp = client.create_json_response(system=system_prompt, user=user, max_tokens=max_tokens)
    body = resp.body
    if isinstance(body, dict):
        for key in ("result", "sentences", "data", "items"):
            if isinstance(body.get(key), list):
                body = body[key]
                break
    out = {}
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
        raw_aspects = item.get("aspects")
        aspects = set()
        if isinstance(raw_aspects, list):
            for a in raw_aspects:
                a = str(a).strip()
                if a in _ALLOWED_ASPECT:
                    aspects.add(a)
        if relevant and not aspects:
            aspects = {_FALLBACK_ASPECT}
        sentiment = str(item.get("sentiment") or "neutral").strip()
        if sentiment not in _ALLOWED_SENTIMENT:
            sentiment = "neutral"
        out[idx] = {"relevant": relevant, "aspects": sorted(aspects), "sentiment": sentiment}
    return out


def split_sentences(text: str, *, splitter: str, kiwi=None) -> tuple[list[str], str]:
    body = str(text or "").strip()
    if not body:
        return [], "empty"
    if splitter == "kiwipiepy":
        if kiwi is None:
            raise RuntimeError("kiwipiepy splitter requested but Kiwi instance is missing")
        return [s.text.strip() for s in kiwi.split_into_sents(body) if s.text.strip()], "kiwipiepy"
    if splitter == "regex":
        return [s.strip() for s in _REGEX_SENTENCE_SPLIT.split(body) if s.strip()], "regex"
    return [s for s in _split_sentences(body, language="ko")[0] if s.strip()], "runtime"


def build_docs(csv_path, limit, *, splitter: str):
    kiwi = None
    if splitter == "kiwipiepy":
        try:
            from kiwipiepy import Kiwi
        except ImportError as exc:
            raise RuntimeError(
                "kiwipiepy splitter requested but kiwipiepy is not installed. "
                "Use python3.11 with workers/python-ai dependencies."
            ) from exc
        kiwi = Kiwi()
    docs = []
    backend_counts = {}
    with csv_path.open("r", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if limit is not None and len(docs) >= limit:
                break
            body = (row.get("본문") or "").strip()
            if not body:
                continue
            row_id = (row.get("수집ID(고유)") or f"row:{i}").strip()
            sents, backend = split_sentences(body, splitter=splitter, kiwi=kiwi)
            backend_counts[backend] = backend_counts.get(backend, 0) + 1
            if sents:
                docs.append((row_id, sents))
    return docs, backend_counts


def client_for(model, cfg):
    return LloaClient(LloaConfig(
        api_key=cfg.lloa_api_key, api_url=cfg.lloa_api_url, model=model,
        max_tokens=cfg.lloa_max_tokens, timeout_sec=cfg.lloa_timeout_sec,
        reasoning_effort=cfg.lloa_reasoning_effort, prepend_no_think=cfg.lloa_prepend_no_think,
    ))


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="docs/eval/quality_v1/datasets/festival_sample_50.csv")
    ap.add_argument("--models", required=True)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--max-tokens", type=int, default=8192)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out-dir", default="/tmp/clause_sentence_multiaspect")
    ap.add_argument(
        "--system-prompt-file",
        default="",
        help="튜닝 프롬프트 파일. 생략하면 내장 측정 프롬프트 사용.",
    )
    ap.add_argument(
        "--splitter",
        choices=("kiwipiepy", "regex", "runtime"),
        default="kiwipiepy",
        help="문장 앵커 splitter. Gate 1은 kiwipiepy를 명시 사용한다.",
    )
    args = ap.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    if len(models) != 2:
        raise SystemExit("--models 2개")
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    if not (cfg.lloa_api_key or "").strip():
        raise SystemExit("LLOA_API_KEY 필요")
    system_prompt = load_system_prompt(args.system_prompt_file)
    docs, backend_counts = build_docs(Path(args.csv), args.limit, splitter=args.splitter)
    total_sentences = sum(len(s) for _, s in docs)
    print(f"[docs] {len(docs)} docs, {total_sentences} sentences, splitter={args.splitter} {backend_counts}")

    clients = {m: client_for(m, cfg) for m in models}
    tasks = [(di, m) for di in range(len(docs)) for m in models]
    results = {}

    def run(task):
        di, m = task
        try:
            return task, label_doc(clients[m], system_prompt, docs[di][1], args.max_tokens)
        except Exception as e:  # noqa: BLE001
            return task, {"__error__": str(e)}

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for task, labels in ex.map(run, tasks):
            results[task] = labels
    wall = time.monotonic() - t0

    ma, mb = models
    rel_agree = rel_total = both_rel = a_rel = b_rel = a_only = b_only = 0
    sent_agree = 0
    jac_sum = exact_match = subset_ba = 0  # subset: B(Ultra) aspects ⊆ A(Max)
    a_card = []  # aspects per relevant sentence (A)
    b_card = []
    a_multi = b_multi = 0  # ≥2 aspect 문장 수
    error_counts = {m: 0 for m in models}
    error_examples: list[str] = []
    skipped_docs = 0

    for di, (row_id, sents) in enumerate(docs):
        la = results.get((di, ma), {}); lb = results.get((di, mb), {})
        had_error = False
        for m, labels in ((ma, la), (mb, lb)):
            if "__error__" in labels:
                error_counts[m] += 1
                had_error = True
                if len(error_examples) < 5:
                    error_examples.append(f"{row_id} {m}: {labels['__error__']}")
        if had_error:
            skipped_docs += 1
            continue
        for idx in range(1, len(sents) + 1):
            a = la.get(idx); b = lb.get(idx)
            if a is None or b is None:
                continue
            rel_total += 1
            ar, br = a["relevant"], b["relevant"]
            if ar: a_rel += 1
            if br: b_rel += 1
            if ar == br: rel_agree += 1
            if ar and not br: a_only += 1
            if br and not ar: b_only += 1
            if ar and br:
                both_rel += 1
                if a["sentiment"] == b["sentiment"]: sent_agree += 1
                sa, sb = set(a["aspects"]), set(b["aspects"])
                jac_sum += jaccard(sa, sb)
                if sa == sb: exact_match += 1
                if sb and sb <= sa: subset_ba += 1
                a_card.append(len(sa)); b_card.append(len(sb))
                if len(sa) >= 2: a_multi += 1
                if len(sb) >= 2: b_multi += 1

    # relevant kappa
    tot = rel_total
    if tot == 0:
        raise RuntimeError(
            "no comparable sentence labels produced; "
            f"skipped_docs={skipped_docs}, error_counts={error_counts}, "
            f"error_examples={error_examples[:3]}"
        )
    po = rel_agree / tot if tot else 0
    pa, pb = a_rel / tot, b_rel / tot
    pe = pa * pb + (1 - pa) * (1 - pb)
    kappa = (po - pe) / (1 - pe) if (1 - pe) else 0

    def pct(n, d): return f"{100*n/d:.1f}%" if d else "n/a"

    report = [
        "# 문장 앵커 + multi-label aspect 교차모델 측정",
        f"- 모델 A={ma} / B={mb}",
        f"- splitter={args.splitter} backend_counts={backend_counts}",
        f"- system_prompt_file={args.system_prompt_file or '(built-in)'}",
        f"- doc {len(docs)} / 문장 {total_sentences} / 비교쌍 {rel_total} / wall {wall:.1f}s",
        f"- skipped_docs={skipped_docs} / error_counts={error_counts}",
        "",
        "## relevant (분절 대체 지표)",
        f"- Cohen's kappa: {kappa:.3f}   (raw 일치 {pct(rel_agree,tot)})",
        f"- A relevant {a_rel} / B relevant {b_rel} / both {both_rel} / A-only {a_only} / B-only {b_only}",
        "",
        "## sentiment (both-relevant 기준)",
        f"- 일치: {pct(sent_agree, both_rel)}",
        "",
        "## aspect (multi-label, both-relevant 기준)",
        f"- aspect-set Jaccard 평균: {jac_sum/both_rel:.3f}" if both_rel else "- n/a",
        f"- exact-set 일치: {pct(exact_match, both_rel)}",
        f"- subset(B⊆A, Ultra⊆Max): {pct(subset_ba, both_rel)}",
        "",
        "## aspect cardinality (가설 ② 직접 검증)",
        f"- A aspects/문장: mean {statistics.mean(a_card):.2f}" if a_card else "- n/a",
        f"- B aspects/문장: mean {statistics.mean(b_card):.2f}" if b_card else "- n/a",
        f"- ≥2 aspect 문장: A {pct(a_multi, both_rel)} / B {pct(b_multi, both_rel)}",
        "  → ≥2 비율 높으면 단일-aspect 강제가 측정2 aspect 70%의 진범",
        "  → 낮은데 Jaccard도 낮으면 taxonomy/prompt 문제(→샘플 확대)",
        "",
        "## 비교 기준",
        "- 측정2(단일aspect): relevant kappa 0.731 / sentiment 94.3% / aspect 69.8%",
        "- GO: kappa≥0.70, sentiment≥90%, aspect-set Jaccard≥0.75",
    ]
    text = "\n".join(report)
    (out_dir / "multiaspect_report.md").write_text(text + "\n", encoding="utf-8")
    raw = {f"{di}:{m}": results.get((di, m), {}) for di in range(len(docs)) for m in models}
    (out_dir / "raw_labels.json").write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    print("\n" + text)
    print(f"\n[report] {out_dir/'multiaspect_report.md'}")


if __name__ == "__main__":
    main()
