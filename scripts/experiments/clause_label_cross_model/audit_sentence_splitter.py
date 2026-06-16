#!/usr/bin/env python3.11
"""문장 분리기 audit (1회성 진단) — 문장 앵커 설계의 전제조건 게이트.

문장 앵커는 "정답 절 분리"가 아니라 *비교용 앵커*다. 완벽할 필요는 없지만
안정적·검증가능해야 한다. 모델이 아니라 splitter 품질을 본다.

주의(2026-06-16): 코드의 `_split_sentences`는 `import kss`를 시도하나 의존성은
kss가 아니라 kiwipiepy다 → 현 fallback은 regex(마침표마다 과분할/SNS 무종결문
병합). 진짜 후보는 regex vs **kiwipiepy**. 이 audit이 둘을 비교한다.

자동 통계 + bad-split 휴리스틱 + 최악 예시 덤프(사람 눈 리뷰용).

판정(사용자 기준):
  bad split <=5%  → splitter 단독 사용 가능
  5~15%           → splitter + 후처리 규칙
  >15%            → 문장앵커 대신 문단/슬라이딩 anchor 고려

실행:
  PYTHONPATH=workers/python-ai/src python3.11 \
    scripts/audit_sentence_splitter.py --limit 150 --out-dir /tmp/splitter_audit
"""
from __future__ import annotations

import argparse
import csv
import re
import statistics
from pathlib import Path

# 종결형 없이 이어지는 연결어미(과분할/무종결 의심): ~는데/지만/고/며/서/는데/면서 등
CONNECTIVE_END = re.compile(r"(는데|지만|면서|으며|며|고|어서|아서|라서|는데도|지요|구요|는데요)$")
TERMINAL = re.compile(r"[.!?。！？…]")
EMOJI = re.compile(r"[\U0001F000-\U0001FAFF\U00002600-\U000027BF]")
LAUGH = re.compile(r"(ㅋ{2,}|ㅎ{2,}|ㅠ{2,}|ㅜ{2,})")
BULLET = re.compile(r"(^|\n)\s*([-*•·]|\d+[.)])\s")


def split_regex(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?。！？])\s+|\n+", text) if s.strip()]


def split_kiwi(kiwi, text: str) -> list[str]:
    return [s.text.strip() for s in kiwi.split_into_sents(text) if s.text.strip()]


def terminal_count(s: str) -> int:
    return len(TERMINAL.findall(s))


def is_suspect(s: str) -> tuple[bool, str]:
    """bad-split 의심 + 사유. 보수적으로 명백한 것만."""
    n = len(s)
    if n < 6:
        return True, "fragment(<6)"
    # 무종결 + 연결어미로 끝 → 잘린/과분할 의심 (다음 절과 이어져야 했을 가능성)
    if not TERMINAL.search(s[-2:]) and CONNECTIVE_END.search(s):
        return True, "connective_end"
    # 한 조각에 종결부호 2개 이상 + 그 뒤로도 텍스트 → 무종결문 병합(under-split)
    if terminal_count(s) >= 2:
        # 종결부호 뒤에 실질 텍스트가 더 있으면 병합 의심
        parts = [p for p in re.split(r"[.!?。！？]", s) if p.strip()]
        if len(parts) >= 2 and all(len(p.strip()) > 8 for p in parts[:2]):
            return True, "under_split(merged)"
    if n > 200:
        return True, "too_long(>200)"
    return False, ""


def audit(name: str, splitter, docs: list[str]) -> dict:
    all_sents: list[str] = []
    per_doc_counts: list[int] = []
    suspects: list[tuple[str, str]] = []
    for text in docs:
        sents = splitter(text)
        per_doc_counts.append(len(sents))
        all_sents.extend(sents)
    lengths = [len(s) for s in all_sents]
    short = [s for s in all_sents if len(s) < 10]
    long_ = [s for s in all_sents if len(s) > 200]
    newline = [s for s in all_sents if "\n" in s]
    emoji = [s for s in all_sents if EMOJI.search(s)]
    laugh = [s for s in all_sents if LAUGH.search(s)]
    for s in all_sents:
        sus, why = is_suspect(s)
        if sus:
            suspects.append((why, s))
    total = len(all_sents)
    by_reason: dict[str, int] = {}
    for why, _ in suspects:
        by_reason[why] = by_reason.get(why, 0) + 1
    return {
        "name": name,
        "total_sents": total,
        "per_doc": per_doc_counts,
        "lengths": lengths,
        "short": short,
        "long": long_,
        "newline": newline,
        "emoji": emoji,
        "laugh": laugh,
        "suspects": suspects,
        "by_reason": by_reason,
    }


def fmt(r: dict) -> str:
    total = r["total_sents"]
    pd = r["per_doc"]
    lg = r["lengths"]

    def pct(xs):
        return f"{100*len(xs)/total:.1f}%" if total else "n/a"

    sus = len(r["suspects"])
    lines = [
        f"### {r['name']}",
        f"- 총 문장 {total} / doc당 mean {statistics.mean(pd):.1f} median {statistics.median(pd):.0f} max {max(pd)}",
        f"- 문장 길이(char) mean {statistics.mean(lg):.0f} median {statistics.median(lg):.0f} max {max(lg)}",
        f"- 10자 미만 조각: {pct(r['short'])}",
        f"- 200자 초과: {pct(r['long'])}",
        f"- 내부 줄바꿈 포함: {pct(r['newline'])}",
        f"- 이모지 포함: {pct(r['emoji'])} / 웃음(ㅋㅎㅠ): {pct(r['laugh'])}",
        f"- **bad-split 의심: {sus} ({100*sus/total:.1f}%)**  사유별 {r['by_reason']}",
    ]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="docs/eval/quality_v1/datasets/festival_sample_50.csv")
    ap.add_argument("--limit", type=int, default=150)
    ap.add_argument("--examples", type=int, default=25)
    ap.add_argument("--out-dir", default="/tmp/splitter_audit")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    docs: list[str] = []
    with Path(args.csv).open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if len(docs) >= args.limit:
                break
            body = (row.get("본문") or "").strip()
            if body:
                docs.append(body)
    print(f"[docs] {len(docs)} docs")

    splitters = [("regex", split_regex)]
    try:
        from kiwipiepy import Kiwi
        kiwi = Kiwi()
        splitters.append(("kiwipiepy", lambda t: split_kiwi(kiwi, t)))
        print("[splitter] kiwipiepy 사용 가능")
    except Exception as e:  # noqa: BLE001
        print(f"[splitter] kiwipiepy 불가({e}) — regex만")

    reports = [audit(name, fn, docs) for name, fn in splitters]

    out = ["# 문장 분리기 audit — 문장 앵커 전제조건", f"- 샘플 {len(docs)} doc (festival)", ""]
    for r in reports:
        out.append(fmt(r))
        out.append("")
    out.append("## 판정 기준")
    out.append("- bad split ≤5% → splitter 단독 / 5~15% → +후처리 / >15% → 문단·슬라이딩 anchor")
    text = "\n".join(out)
    (out_dir / "audit_report.md").write_text(text + "\n", encoding="utf-8")
    print("\n" + text)

    # 사람 눈 리뷰용 최악 예시 덤프
    for r in reports:
        ex_lines = [f"# {r['name']} — bad-split 의심 예시 (사람 리뷰용)\n"]
        # 사유별로 몇 개씩
        from collections import defaultdict
        buckets = defaultdict(list)
        for why, s in r["suspects"]:
            buckets[why].append(s)
        for why, items in buckets.items():
            ex_lines.append(f"\n## {why} ({len(items)}건) — 상위 {args.examples}")
            for s in items[: args.examples]:
                ex_lines.append(f"  · {s!r}")
        (out_dir / f"examples_{r['name']}.txt").write_text("\n".join(ex_lines) + "\n", encoding="utf-8")
    print(f"\n[examples] {out_dir}/examples_*.txt (사람 눈 리뷰)")
    print(f"[report]   {out_dir}/audit_report.md")


if __name__ == "__main__":
    main()
