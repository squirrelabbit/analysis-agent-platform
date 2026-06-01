#!/usr/bin/env python3
"""plan_v2 smoke fixture deterministic builder (Phase 4, 2026-05-27).

[[taxonomy_legacy_policy_2026-05-27]] §7 — 7/30 데모 fixture가
``legacy_missing`` 분기로 떨어지지 않도록 taxonomy metadata sidecar를 포함한
fixture를 결정적으로 재생성한다.

산출물:
  - cleaned.parquet            (4 doc)
  - clause_label.jsonl         (5 clause)
  - doc_genuineness.jsonl      (4 row)
  - clause_label_summary.json  (sidecar — taxonomy_id / taxonomy_hash /
    prompt_version / clause_count / aspect_counts / sentiment_counts)

기존 fixture와 *데이터 동일성*을 보장한다 (test_smoke_5a_aspect_delta /
test_plan_and_execute_analyze가 잠그는 Q2 결과: ambiance_scenery +1 / food
-1 / show_program +1). parquet binary는 pyarrow가 재생성할 때마다 byte 단위
가 다를 수 있지만 schema + row content는 동일.

새 sidecar JSON(``clause_label_summary.json``)이 본 PR의 핵심 산출물.
smoke_analyze_endpoint.sh가 이를 읽어 worker payload의 ``clause_label_
metadata``로 inject한다 → Phase 3-B taxonomy_check가 ``ok`` 분기로 떨어진다.

사용:
  python3 scripts/build_plan_v2_smoke_fixture.py             # 두 위치 모두 생성
  python3 scripts/build_plan_v2_smoke_fixture.py --target tests
  python3 scripts/build_plan_v2_smoke_fixture.py --target data
  python3 scripts/build_plan_v2_smoke_fixture.py --verify     # 생성 안 하고 검증만
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_WORKER_SRC = _REPO_ROOT / "workers" / "python-ai" / "src"
if str(_WORKER_SRC) not in sys.path:
    sys.path.insert(0, str(_WORKER_SRC))


from python_ai_worker.taxonomies import load_taxonomy  # noqa: E402


_TAXONOMY_ID = "festival-v2"
_PROMPT_VERSION = "dataset-clause-label-v3"

# Q2 "작년과 올해의 aspect 증감수치" 시나리오에 맞춘 seed.
# 잠금 (test_smoke_5a_aspect_delta + test_plan_and_execute_analyze):
#   ambiance_scenery +1 / food -1 / show_program +1
# - d1 2025 → ambiance_scenery × 1
# - d2 2025 → food × 1
# - d3 2026 → ambiance_scenery × 2
# - d4 2026 → show_program × 1
_DOCS: list[dict[str, str]] = [
    {
        "doc_id": "d1",
        "row_id": "v1__0",
        "raw_text": "강릉 야행 분위기 좋았어요",
        "cleaned_text": "강릉 야행 분위기 좋았어요",
        "created_at": "2025-08-01T19:00:00",
    },
    {
        "doc_id": "d2",
        "row_id": "v1__1",
        "raw_text": "음식이 정말 맛있었어요",
        "cleaned_text": "음식이 정말 맛있었어요",
        "created_at": "2025-09-10T12:00:00",
    },
    {
        "doc_id": "d3",
        "row_id": "v1__2",
        "raw_text": "올해 야행 분위기는 한층 좋아졌다",
        "cleaned_text": "올해 야행 분위기는 한층 좋아졌다",
        "created_at": "2026-04-05T20:00:00",
    },
    {
        "doc_id": "d4",
        "row_id": "v1__3",
        "raw_text": "프로그램 콘텐츠가 다양해졌다",
        "cleaned_text": "프로그램 콘텐츠가 다양해졌다",
        "created_at": "2026-05-03T11:00:00",
    },
]

_CLAUSES: list[dict[str, str]] = [
    {"doc_id": "d1", "clause": "분위기 좋았어요", "sentiment": "positive", "aspect": "ambiance_scenery"},
    {"doc_id": "d2", "clause": "음식이 맛있었어요", "sentiment": "positive", "aspect": "food"},
    {"doc_id": "d3", "clause": "분위기가 더 좋아졌다", "sentiment": "positive", "aspect": "ambiance_scenery"},
    {"doc_id": "d3", "clause": "야간 조명이 인상적이었다", "sentiment": "positive", "aspect": "ambiance_scenery"},
    {"doc_id": "d4", "clause": "콘텐츠가 다양했다", "sentiment": "positive", "aspect": "show_program"},
]

_GENUINENESS: list[dict[str, str]] = [
    {"doc_id": doc["doc_id"], "genuineness": "genuine_review", "reason": "후기"}
    for doc in _DOCS
]


def _output_dirs(target: str) -> list[Path]:
    dirs: list[Path] = []
    if target in ("both", "tests"):
        dirs.append(_REPO_ROOT / "workers" / "python-ai" / "tests" / "fixtures" / "plan_v2_smoke")
    if target in ("both", "data"):
        dirs.append(_REPO_ROOT / "data" / "plan_v2_smoke")
    return dirs


def _write_parquet(path: Path) -> None:
    # local import — pyarrow는 worker dependency. host에 없을 수도.
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            ("doc_id", pa.string()),
            ("row_id", pa.string()),
            ("raw_text", pa.string()),
            ("cleaned_text", pa.string()),
            ("created_at", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(_DOCS, schema=schema)
    pq.write_table(table, path)


def _write_jsonl(path: Path, rows: list[dict], *, common: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            merged = {**row, **common}
            f.write(json.dumps(merged, ensure_ascii=False))
            f.write("\n")


def _build_summary(taxonomy_hash: str) -> dict:
    aspect_counts = Counter(c["aspect"] for c in _CLAUSES)
    sentiment_counts = Counter(c["sentiment"] for c in _CLAUSES)
    return {
        "taxonomy_id": _TAXONOMY_ID,
        "taxonomy_hash": taxonomy_hash,
        "prompt_version": _PROMPT_VERSION,
        "clause_count": len(_CLAUSES),
        "doc_count": len(_DOCS),
        "aspect_counts": dict(sorted(aspect_counts.items())),
        "sentiment_counts": dict(sorted(sentiment_counts.items())),
        "source": "plan_v2_smoke_fixture_builder",
    }


def _write_summary(path: Path, summary: dict) -> None:
    path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _generate(target: str) -> dict:
    taxonomy = load_taxonomy(_TAXONOMY_ID)
    summary = _build_summary(taxonomy.taxonomy_hash)

    clause_common = {"prompt_version": _PROMPT_VERSION, "source": "lloa"}
    gen_common = {"prompt_version": "v1", "source": "lloa"}

    written: list[Path] = []
    for out_dir in _output_dirs(target):
        out_dir.mkdir(parents=True, exist_ok=True)
        _write_parquet(out_dir / "cleaned.parquet")
        _write_jsonl(out_dir / "clause_label.jsonl", _CLAUSES, common=clause_common)
        _write_jsonl(out_dir / "doc_genuineness.jsonl", _GENUINENESS, common=gen_common)
        _write_summary(out_dir / "clause_label_summary.json", summary)
        written.append(out_dir)

    return {"summary": summary, "written_dirs": [str(p) for p in written]}


def _verify(target: str) -> dict:
    """기존 파일과 비교해서 데이터 동일성 확인. byte diff는 검사 안 함
    (parquet binary 비교는 어려움)."""

    taxonomy = load_taxonomy(_TAXONOMY_ID)
    expected_summary = _build_summary(taxonomy.taxonomy_hash)

    issues: list[str] = []
    for out_dir in _output_dirs(target):
        sidecar = out_dir / "clause_label_summary.json"
        if not sidecar.exists():
            issues.append(f"missing sidecar: {sidecar}")
            continue
        actual = json.loads(sidecar.read_text(encoding="utf-8"))
        if actual.get("taxonomy_id") != expected_summary["taxonomy_id"]:
            issues.append(
                f"taxonomy_id mismatch in {sidecar}: expected {expected_summary['taxonomy_id']}, "
                f"got {actual.get('taxonomy_id')}"
            )
        if actual.get("taxonomy_hash") != expected_summary["taxonomy_hash"]:
            issues.append(
                f"taxonomy_hash mismatch in {sidecar}: expected {expected_summary['taxonomy_hash']}, "
                f"got {actual.get('taxonomy_hash')}"
            )
        if actual.get("aspect_counts") != expected_summary["aspect_counts"]:
            issues.append(
                f"aspect_counts mismatch in {sidecar}: expected {expected_summary['aspect_counts']}, "
                f"got {actual.get('aspect_counts')}"
            )
    return {"issues": issues, "ok": len(issues) == 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=("both", "tests", "data"),
        default="both",
        help="생성 대상 (default: both)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="기존 sidecar 검증만 수행 (생성 안 함)",
    )
    args = parser.parse_args()

    if args.verify:
        result = _verify(args.target)
        if not result["ok"]:
            for issue in result["issues"]:
                print(f"FAIL: {issue}", file=sys.stderr)
            return 1
        print("verify OK")
        return 0

    result = _generate(args.target)
    print(f"generated taxonomy_id={result['summary']['taxonomy_id']}")
    print(f"           taxonomy_hash={result['summary']['taxonomy_hash']}")
    print(f"           clause_count={result['summary']['clause_count']}")
    print(f"           aspect_counts={result['summary']['aspect_counts']}")
    print(f"           sentiment_counts={result['summary']['sentiment_counts']}")
    for d in result["written_dirs"]:
        print(f"  wrote -> {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
