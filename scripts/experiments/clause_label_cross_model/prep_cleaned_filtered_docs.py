#!/usr/bin/env python3.11
"""정제+필터 docs 생성 (splitter audit / 문장앵커 측정의 production 조건 재현).

production clause_label 입력 = raw → clean → doc_genuineness → non_review 제외.
지금까지 측정은 raw 본문이라 노이즈(URL/지도위젯/홍보문)로 불리했다. 이 스크립트가
clean(rule-based) + doc_genuineness(LLOA) + non_review 필터를 적용해 festival 호환
CSV(본문=cleaned_text)를 만든다 → 기존 audit/측정 스크립트를 --csv만 바꿔 재사용.

실행 (LLOA 필요 — doc_genuineness):
  LLOA_API_KEY=... LLOA_API_URL=... \
  PYTHONPATH=workers/python-ai/src python3.11 \
    scripts/prep_cleaned_filtered_docs.py --csv data/festival.csv --limit 150 \
      --out-dir /tmp/prep_cleaned
출력: <out-dir>/cleaned_filtered.csv  (수집ID(고유)/제목/본문=cleaned_text)
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from python_ai_worker.dataset_build.clean import run_dataset_clean
from python_ai_worker.dataset_build.doc_genuineness import run_dataset_doc_genuineness

SUBJECT = {
    "subject_type": "festival",
    "subject_name": "강릉 국가유산야행",
    "subject_aliases": ["문화유산야행", "문화재야행", "강릉야행", "국가유산야행"],
    "recruitment_keywords": ["서포터즈", "푸드트럭", "모집", "체험단"],
}


def slice_csv(src: Path, dst: Path, limit: int) -> int:
    with src.open("r", encoding="utf-8-sig") as fin, dst.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        n = 0
        for row in reader:
            if n >= limit:
                break
            writer.writerow(row)
            n += 1
    return n


def read_jsonl(path: Path) -> list[dict]:
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/festival.csv")
    ap.add_argument("--limit", type=int, default=150)
    ap.add_argument("--out-dir", default="/tmp/prep_cleaned")
    ap.add_argument("--text-columns", default="제목,본문")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    sliced = out / "sliced.csv"
    n = slice_csv(Path(args.csv), sliced, args.limit)
    print(f"[slice] {n} rows → {sliced}")

    # 1) clean (rule-based)
    cleaned = out / "cleaned.jsonl"
    run_dataset_clean({
        "dataset_version_id": "audit",
        "dataset_name": str(sliced),
        "output_path": str(cleaned),
        "text_columns": [c.strip() for c in args.text_columns.split(",") if c.strip()],
    })
    clean_rows = read_jsonl(cleaned)
    nonempty = [r for r in clean_rows if str(r.get("cleaned_text") or "").strip()]
    print(f"[clean] {len(clean_rows)} rows, cleaned_text 있음 {len(nonempty)}")

    # 2) doc_genuineness (LLOA 1콜/doc)
    gen = out / "genuineness.jsonl"
    run_dataset_doc_genuineness({
        "dataset_version_id": "audit",
        "clean_artifact_ref": str(cleaned),
        "output_path": str(gen),
        "doc_genuineness": SUBJECT,
    })
    gen_rows = read_jsonl(gen)
    tier_by_doc = {r["doc_id"]: r.get("genuineness") for r in gen_rows}
    from collections import Counter
    print(f"[genuineness] tier 분포: {dict(Counter(tier_by_doc.values()))}")

    # 3) non_review 제외 → genuine_review/uncertain만
    keep = {d for d, t in tier_by_doc.items() if t in ("genuine_review", "uncertain")}
    out_csv = out / "cleaned_filtered.csv"
    kept = 0
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["수집ID(고유)", "제목", "본문"])
        w.writeheader()
        for r in clean_rows:
            did = r.get("row_id")
            text = str(r.get("cleaned_text") or "").strip()
            if did in keep and text:
                w.writerow({"수집ID(고유)": did, "제목": "", "본문": text})
                kept += 1
    print(f"[filter] non_review 제외 후 {kept} docs → {out_csv}")
    print(f"\n다음: 이 CSV로 audit/측정 재실행")
    print(f"  --csv {out_csv}")


if __name__ == "__main__":
    main()
