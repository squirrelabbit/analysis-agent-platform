#!/usr/bin/env bash
# silverone 5-A unit smoke 자동화 — Python service (execute_analyze_plan)를
# committed fixture로 직접 호출. compose/dev 없이 deterministic 경로만 검증.
#
# silverone 2026-05-21 5.5단계 결정 — LLM planner (6단계)를 붙이기 전 fixture와
# script를 commit해 deterministic path를 재현 가능하게 잠가둔다.
#
# usage:
#   ./scripts/smoke_analyze_v2_service.sh
#
# exit code:
#   0 — smoke 통과
#   1 — smoke 실패 (plan validation / executor / 결과 불일치 등)
set -euo pipefail

# silverone 2026-06-04 — requires-python >= 3.11. default python3.11, override 가능.
PYTHON="${PYTHON:-python3.11}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="${REPO_ROOT}/workers/python-ai/tests/fixtures/plan_v2_smoke"

if [[ ! -f "${FIXTURES}/cleaned.parquet" ]]; then
  echo "ERROR: fixture not found at ${FIXTURES}" >&2
  exit 1
fi

cd "${REPO_ROOT}"

PYTHONPATH="workers/python-ai/src" "$PYTHON" - "${FIXTURES}" <<'PYEOF'
import json
import sys
from pathlib import Path

from python_ai_worker.executor import ArtifactPaths, execute_analyze_plan


fixtures = Path(sys.argv[1])
artifact_paths = ArtifactPaths(
    docs=fixtures / "cleaned.parquet",
    clauses=fixtures / "clause_label.jsonl",
    genuineness=fixtures / "doc_genuineness.jsonl",
)
plan = json.loads((fixtures / "aspect_delta_plan.json").read_text(encoding="utf-8"))

result = execute_analyze_plan(
    "smoke-service",
    plan,
    artifact_paths=artifact_paths,
)

print(f"plan_version: {result['plan_version']}")
print(f"step_count:   {len(result['steps'])}")
print(f"present.step_id:    {result['present']['step_id']}")
print(f"present.format:     {result['present']['format']}")
print(f"present.title:      {result['present']['title']}")
print(f"present.row_count:  {result['present']['row_count']}")
print()
print("=== aspect_delta (present rows) ===")
by_aspect = {row["aspect"]: row for row in result["present"]["rows"]}
for aspect in ("ambiance_scenery", "food", "show_program"):
    row = by_aspect.get(aspect, {})
    print(
        f"  {aspect:16s}  last={row.get('last_year_count')!s:>5}  "
        f"this={row.get('this_year_count')!s:>5}  "
        f"delta_count={row.get('delta_count')!s:>5}  "
        f"delta_rate={row.get('delta_rate')!s}"
    )
print()

# 기대값 잠금 — committed fixture 형태가 흔들리면 smoke가 실패해야 함.
# silverone 2026-06-04 — taxonomy drift 반영: atmosphere→ambiance_scenery, contents→show_program.
expected = {
    "ambiance_scenery": {"last_year_count": 1, "this_year_count": 2, "delta_count": 1, "delta_rate": 100.0},
    "food":             {"last_year_count": 1, "this_year_count": None, "delta_count": -1, "delta_rate": -100.0},
    "show_program":     {"last_year_count": None, "this_year_count": 1, "delta_count": 1, "delta_rate": None},
}
mismatches: list[str] = []
for aspect, want in expected.items():
    got = by_aspect.get(aspect, {})
    for key, want_value in want.items():
        got_value = got.get(key)
        if isinstance(want_value, float) and isinstance(got_value, (int, float)):
            if abs(got_value - want_value) > 1e-4:
                mismatches.append(f"{aspect}.{key}: want={want_value} got={got_value}")
        elif got_value != want_value:
            mismatches.append(f"{aspect}.{key}: want={want_value!r} got={got_value!r}")

if mismatches:
    print("FAIL — 기대값 불일치:")
    for line in mismatches:
        print(f"  {line}")
    sys.exit(1)

print("PASS — service smoke matches committed expectations")
PYEOF
