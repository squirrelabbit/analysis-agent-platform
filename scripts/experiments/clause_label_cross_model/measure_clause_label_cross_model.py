#!/usr/bin/env python3.11
"""clause_label 교차모델 발산 측정 (1회성 진단).

목적: doc_genuineness verify(ADR-026)를 clause_label에도 적용할지 ROI 판단.
같은 cleaned docs에 두 LLOA 모델로 clause_label을 돌려, 절 분리/커버리지/
라벨 일치가 실제로 얼마나 발산하는지 수치화한다. production 코드 변경 없음
(clause_label의 기존 model_id override + include_genuineness:[] 사용).

측정 항목:
  1) 절 개수 발산   — 모델별 doc당 절 수, 차이 분포
  2) 커버리지 갭    — 절 fuzzy 정렬 후 matched / A-only / B-only, 단일모델 누락률
                      (= 2모델 union 대비 각 모델이 놓친 비율 → 28% 누락 문제 정량화)
  3) 라벨 일치      — matched 절의 sentiment / aspect 일치율
  4) doc-level      — 모델 간 aspect 집합 jaccard 분포
  5) 비용/시간      — 모델별 wall time

실행 (LLOA env 필요):
  LLOA_API_KEY=... LLOA_API_URL=... \
  PYTHONPATH=workers/python-ai/src python3.11 \
    scripts/measure_clause_label_cross_model.py \
      --csv docs/eval/quality_v1/datasets/festival_sample_50.csv \
      --models wisenut/wise-lloa-max-v1.2.1,wisenut/wise-lloa-ultra-v1.1.0 \
      --limit 50 --out-dir /tmp/clause_xmodel

결과 요약은 vault `검토-raw/`에 기록할 것 (CLAUDE.md 문서 위치 규칙).
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import time
from pathlib import Path
from typing import Any


def build_clean_jsonl(csv_path: Path, out_path: Path, limit: int | None) -> int:
    """festival CSV(제목/본문/수집ID) → clause_label 입력 jsonl(row_id/doc_title/cleaned_text)."""
    written = 0
    with csv_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        for i, row in enumerate(reader):
            if limit is not None and written >= limit:
                break
            body = (row.get("본문") or "").strip()
            if not body:
                continue
            row_id = (row.get("수집ID(고유)") or f"row:{i}").strip()
            rec = {
                "row_id": row_id,
                "doc_title": (row.get("제목") or "").strip(),
                "cleaned_text": body,
            }
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1
    return written


def run_model(clean_ref: Path, model_id: str, out_path: Path, concurrency: int) -> dict[str, Any]:
    """clause_label을 한 모델로 실행하고 (summary, wall_sec) 반환."""
    from python_ai_worker.dataset_build.clause_label import run_dataset_clause_label

    payload = {
        "dataset_version_id": f"measure:{model_id}",
        "clean_artifact_ref": str(clean_ref),
        "output_path": str(out_path),
        "model_id": model_id,
        "include_genuineness": [],  # filter off — 전 doc 처리
        "concurrency": concurrency,
    }
    t0 = time.monotonic()
    result = run_dataset_clause_label(payload)
    wall = time.monotonic() - t0
    return {"summary": result.get("artifact", {}).get("summary", {}), "wall_sec": wall}


def load_clauses_by_doc(jsonl_path: Path) -> dict[str, list[dict[str, str]]]:
    by_doc: dict[str, list[dict[str, str]]] = {}
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            by_doc.setdefault(rec["doc_id"], []).append(
                {"clause": rec.get("clause", ""), "sentiment": rec.get("sentiment", ""), "aspect": rec.get("aspect", "")}
            )
    return by_doc


def _norm(text: str) -> str:
    return re.sub(r"[\s\W_]+", "", text.lower())


def _trigrams(text: str) -> set[str]:
    n = _norm(text)
    if len(n) < 3:
        return {n} if n else set()
    return {n[i : i + 3] for i in range(len(n) - 2)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def align(a_clauses: list[dict], b_clauses: list[dict], threshold: float = 0.6) -> dict[str, Any]:
    """A/B 절을 fuzzy 정렬 (exact-norm 우선, 그다음 trigram jaccard greedy)."""
    a_norm = [_norm(c["clause"]) for c in a_clauses]
    b_norm = [_norm(c["clause"]) for c in b_clauses]
    b_used = [False] * len(b_clauses)
    matched: list[tuple[int, int]] = []

    # 1) exact-normalized
    for ai, an in enumerate(a_norm):
        for bi, bn in enumerate(b_norm):
            if not b_used[bi] and an and an == bn:
                matched.append((ai, bi))
                b_used[bi] = True
                break
    a_matched = {ai for ai, _ in matched}

    # 2) fuzzy trigram
    a_tri = [_trigrams(c["clause"]) for c in a_clauses]
    b_tri = [_trigrams(c["clause"]) for c in b_clauses]
    for ai in range(len(a_clauses)):
        if ai in a_matched:
            continue
        best_bi, best_score = -1, threshold
        for bi in range(len(b_clauses)):
            if b_used[bi]:
                continue
            s = _jaccard(a_tri[ai], b_tri[bi])
            if s >= best_score:
                best_bi, best_score = bi, s
        if best_bi >= 0:
            matched.append((ai, best_bi))
            b_used[best_bi] = True
            a_matched.add(ai)

    a_only = len(a_clauses) - len(matched)
    b_only = len(b_clauses) - len(matched)
    sent_agree = sum(1 for ai, bi in matched if a_clauses[ai]["sentiment"] == b_clauses[bi]["sentiment"])
    aspect_agree = sum(1 for ai, bi in matched if a_clauses[ai]["aspect"] == b_clauses[bi]["aspect"])
    both_agree = sum(
        1
        for ai, bi in matched
        if a_clauses[ai]["sentiment"] == b_clauses[bi]["sentiment"] and a_clauses[ai]["aspect"] == b_clauses[bi]["aspect"]
    )
    return {
        "matched": len(matched),
        "a_only": a_only,
        "b_only": b_only,
        "sent_agree": sent_agree,
        "aspect_agree": aspect_agree,
        "both_agree": both_agree,
    }


def analyze(a_by_doc: dict, b_by_doc: dict, label_a: str, label_b: str) -> str:
    doc_ids = sorted(set(a_by_doc) | set(b_by_doc))
    a_counts, b_counts, count_deltas = [], [], []
    tot_matched = tot_a_only = tot_b_only = 0
    tot_sent = tot_aspect = tot_both = 0
    aspect_jaccards = []
    per_model_omission_a, per_model_omission_b = [], []

    for did in doc_ids:
        a = a_by_doc.get(did, [])
        b = b_by_doc.get(did, [])
        a_counts.append(len(a))
        b_counts.append(len(b))
        count_deltas.append(abs(len(a) - len(b)))
        al = align(a, b)
        tot_matched += al["matched"]
        tot_a_only += al["a_only"]
        tot_b_only += al["b_only"]
        tot_sent += al["sent_agree"]
        tot_aspect += al["aspect_agree"]
        tot_both += al["both_agree"]
        union = al["matched"] + al["a_only"] + al["b_only"]
        if union:
            per_model_omission_a.append((union - len(a)) / union)
            per_model_omission_b.append((union - len(b)) / union)
        aspect_jaccards.append(_jaccard({c["aspect"] for c in a}, {c["aspect"] for c in b}))

    def pct(n, d):
        return f"{100*n/d:.1f}%" if d else "n/a"

    def stat(xs):
        return f"mean={statistics.mean(xs):.2f} median={statistics.median(xs):.1f} max={max(xs)}" if xs else "n/a"

    lines = [
        f"# clause_label 교차모델 발산 측정",
        f"- 모델 A = {label_a}",
        f"- 모델 B = {label_b}",
        f"- 공통 doc 수 = {len(doc_ids)}",
        "",
        "## 1) 절 개수 발산",
        f"- A 절수/doc: {stat(a_counts)}  (총 {sum(a_counts)})",
        f"- B 절수/doc: {stat(b_counts)}  (총 {sum(b_counts)})",
        f"- |A-B| 절수차/doc: {stat(count_deltas)}",
        "",
        "## 2) 커버리지 갭 (fuzzy 정렬)",
        f"- matched={tot_matched}  A-only={tot_a_only}  B-only={tot_b_only}",
        f"- union 총절수 = {tot_matched + tot_a_only + tot_b_only}",
        f"- 단일모델 누락률 (union 대비): A {statistics.mean(per_model_omission_a)*100:.1f}% / "
        f"B {statistics.mean(per_model_omission_b)*100:.1f}%" if per_model_omission_a else "- 누락률 n/a",
        f"  → 2모델 union이 단일모델보다 이만큼 더 잡음 (28% 누락 가설 검증)",
        "",
        "## 3) 라벨 일치 (matched 절 기준)",
        f"- sentiment 일치: {pct(tot_sent, tot_matched)}",
        f"- aspect 일치: {pct(tot_aspect, tot_matched)}",
        f"- 둘 다 일치: {pct(tot_both, tot_matched)}",
        "",
        "## 4) doc-level aspect 집합 jaccard",
        f"- {stat(aspect_jaccards)}",
        "",
        "## 해석 가이드",
        "- 절수차/누락률 큼 → 커버리지(B) 문제 큼 → union/judge 설계 가치↑",
        "- matched 라벨 일치 낮음 → 라벨(A) 신뢰성 문제 → 디커플링 verify 가치↑",
        "- 둘 다 높게 일치 → 교차검증 ROI 낮음 (단일 모델로 충분)",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="docs/eval/quality_v1/datasets/festival_sample_50.csv")
    ap.add_argument("--clean-ref", default="", help="기존 clean artifact jsonl 사용 시 (없으면 CSV에서 생성)")
    ap.add_argument("--models", required=True, help="콤마구분 2개 model_id")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--out-dir", default="/tmp/clause_xmodel")
    args = ap.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    if len(models) != 2:
        raise SystemExit("--models 는 콤마구분 2개 model_id 여야 함")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.clean_ref:
        clean_ref = Path(args.clean_ref)
        print(f"[clean] 기존 artifact 사용: {clean_ref}")
    else:
        clean_ref = out_dir / "clean_input.jsonl"
        n = build_clean_jsonl(Path(args.csv), clean_ref, args.limit)
        print(f"[clean] CSV → {clean_ref} ({n} docs)")

    outputs = {}
    for idx, model_id in enumerate(models):
        out_path = out_dir / f"clauses_{idx}_{model_id.replace('/', '_')}.jsonl"
        print(f"[run] model={model_id} → {out_path}")
        meta = run_model(clean_ref, model_id, out_path, args.concurrency)
        s = meta["summary"]
        print(
            f"  done: clauses={s.get('clause_count')} docs={s.get('processed_row_count')} "
            f"parse_fail={s.get('parse_failures')} wall={meta['wall_sec']:.1f}s"
        )
        outputs[idx] = out_path

    a_by_doc = load_clauses_by_doc(outputs[0])
    b_by_doc = load_clauses_by_doc(outputs[1])
    report = analyze(a_by_doc, b_by_doc, models[0], models[1])
    report_path = out_dir / "divergence_report.md"
    report_path.write_text(report + "\n", encoding="utf-8")
    print("\n" + report)
    print(f"\n[report] {report_path}")


if __name__ == "__main__":
    main()
