#!/usr/bin/env python3
"""dataset build 성능 기준선(baseline) 측정 — 로그/summary 전용(코드 변경 0).

(silverone 2026-06-29) Node 전환/성능 개선의 공통 선행. clean→doc_genuineness→
clause_label→clause_keywords를 순차 실행하며 단계별 wall 시간을 재고, 각 build의 worker
summary(version.metadata)에서 처리수/skip비율/토큰을, control-plane 로그에서 build view
API p95를 모은다. 결과를 마크다운+JSON으로 출력.

한계(로그 전용): LLOA per-call 지연/p95는 현재 worker가 호출당 로그를 안 남겨 측정 불가
(stage wall 시간으로 갈음 — concurrency 튜닝엔 충분). artifact I/O·메모리·stage 내부 분해도 없음.

사용:
  python3 scripts/build_baseline.py \
    --project 5ce18f63-11a3-4638-a6d1-5e101efc70ff \
    --dataset 5a8d6a5a-83b7-4cd9-8fb1-aef685268afb \
    [--version <vid>]   # 생략 시 active version
환경:
  BASE_URL (기본 http://127.0.0.1:18080)
  COMPOSE  (기본 "docker compose -f compose.dev.yml -f compose.local.yml")
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:18080").rstrip("/")
COMPOSE = os.environ.get("COMPOSE", "docker compose -f compose.dev.yml -f compose.local.yml")
GUNSAN_PROJECT = "5ce18f63-11a3-4638-a6d1-5e101efc70ff"
GUNSAN_DATASET = "5a8d6a5a-83b7-4cd9-8fb1-aef685268afb"

STAGES = ["clean", "doc_genuineness", "clause_label", "clause_keywords"]
DONE = {"ready", "completed", "cancelled", "failed", "not_applicable"}
POLL_SEC = 3
POLL_TIMEOUT_SEC = 90 * 60


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE_URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except Exception:
            return exc.code, {"error": raw.decode("utf-8", "replace")}


def resolve_version(pid: str, did: str) -> str:
    _, ds = api("GET", f"/projects/{pid}/datasets/{did}")
    vid = (ds or {}).get("active_dataset_version_id")
    if vid:
        return vid
    _, vs = api("GET", f"/projects/{pid}/datasets/{did}/versions")
    items = vs.get("items", vs) if isinstance(vs, dict) else vs
    if not items:
        sys.exit("no dataset version found")
    return items[0]["dataset_version_id"]


def stage_status(pid: str, did: str, vid: str, stage: str) -> str:
    _, v = api("GET", f"/projects/{pid}/datasets/{did}/versions/{vid}/{stage}?limit=1")
    return str((v or {}).get("status") or "")


def run_stage(pid: str, did: str, vid: str, stage: str) -> dict:
    base = f"/projects/{pid}/datasets/{did}/versions/{vid}/{stage}"
    started = time.monotonic()
    code, resp = api("POST", base, {})
    if code not in (200, 202):
        return {"stage": stage, "ok": False, "error": f"POST {code}: {resp}"}
    last = ""
    deadline = started + POLL_TIMEOUT_SEC
    while time.monotonic() < deadline:
        time.sleep(POLL_SEC)
        st = stage_status(pid, did, vid, stage)
        last = st
        if st in DONE:
            break
    wall = time.monotonic() - started
    return {"stage": stage, "ok": last in ("ready", "completed"), "status": last,
            "wall_sec": round(wall, 1)}


def psql(sql: str) -> str:
    cmd = COMPOSE.split() + ["exec", "-T", "postgres", "psql", "-U", "platform",
                             "-d", "analysis_support", "-t", "-A", "-c", sql]
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception as exc:
        return f"<psql error: {exc}>"


def summary_metrics(vid: str) -> dict:
    """version.metadata의 worker summary에서 단계별 핵심 지표를 뽑는다."""
    def g(expr: str) -> str:
        return psql(
            f"select {expr} from dataset_versions where dataset_version_id='{vid}';"
        ).splitlines()[0] if vid else ""
    return {
        "clean": {
            "input_row_count": g("metadata->'clean_summary'->>'input_row_count'"),
            "output_row_count": g("metadata->'clean_summary'->>'output_row_count'"),
            "deduped_count": g("metadata->'clean_summary'->>'deduped_count'"),
        },
        "doc_genuineness": {
            "processed": g("metadata->'doc_genuineness_summary'->>'processed_row_count'"),
            "input": g("metadata->'doc_genuineness_summary'->>'input_row_count'"),
            "tier_counts": g("metadata->'doc_genuineness_summary'->>'tier_counts'"),
            "prompt_tokens": g("metadata->'doc_genuineness_summary'->>'total_prompt_tokens'"),
            "completion_tokens": g("metadata->'doc_genuineness_summary'->>'total_completion_tokens'"),
            "concurrency": g("metadata->'doc_genuineness_summary'->>'concurrency'"),
        },
        "clause_label": {
            "processed_doc_count": g("metadata->'clause_label_summary'->>'processed_doc_count'"),
            "input_row_count": g("metadata->'clause_label_summary'->>'input_row_count'"),
            "skipped_by_filter": g("metadata->'clause_label_summary'->>'skipped_by_filter'"),
            "clause_count": g("metadata->'clause_label_summary'->>'clause_count'"),
            "concurrency": g("metadata->'clause_label_summary'->>'concurrency'"),
        },
        "clause_keywords": {
            "clause_count": g("metadata->'clause_keywords_summary'->>'clause_count'"),
            "unique_keyword_count": g("metadata->'clause_keywords_summary'->>'unique_keyword_count'"),
        },
    }


def api_p95_from_logs(since_iso: str) -> dict:
    """control-plane http.request.completed 로그에서 build view GET p95(ms)."""
    cmd = COMPOSE.split() + ["logs", "--since", since_iso, "control-plane"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout
    except Exception as exc:
        return {"error": str(exc)}
    durs: list[float] = []
    for line in out.splitlines():
        i = line.find("{")
        if i < 0:
            continue
        try:
            ev = json.loads(line[i:])
        except Exception:
            continue
        if ev.get("event") != "http.request.completed":
            continue
        path = str(ev.get("path") or "")
        if ev.get("method") == "GET" and any(s in path for s in STAGES):
            d = ev.get("duration_ms")
            if isinstance(d, (int, float)):
                durs.append(float(d))
    if not durs:
        return {"samples": 0}
    durs.sort()
    p = lambda q: durs[min(len(durs) - 1, int(q * len(durs)))]
    return {"samples": len(durs), "p50_ms": round(p(0.50), 1),
            "p95_ms": round(p(0.95), 1), "max_ms": round(durs[-1], 1)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=GUNSAN_PROJECT)
    ap.add_argument("--dataset", default=GUNSAN_DATASET)
    ap.add_argument("--version", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    pid, did = args.project, args.dataset
    vid = args.version or resolve_version(pid, did)
    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"# baseline run  project={pid} dataset={did} version={vid}  @ {run_started_iso}\n")

    stage_results = []
    for stage in STAGES:
        print(f"  ▶ {stage} 실행…", flush=True)
        r = run_stage(pid, did, vid, stage)
        print(f"    {stage}: status={r.get('status')} wall={r.get('wall_sec')}s", flush=True)
        stage_results.append(r)
        if not r["ok"] and stage in ("clean", "doc_genuineness", "clause_label"):
            print(f"    ! {stage} 실패 — 후속 단계는 prereq 미충족으로 중단", flush=True)
            break

    metrics = summary_metrics(vid)
    api_p95 = api_p95_from_logs(run_started_iso)

    report = {
        "run_started": run_started_iso, "project": pid, "dataset": did, "version": vid,
        "stages": stage_results, "summary_metrics": metrics, "api_view_latency": api_p95,
        "note": "log-only baseline. LLOA per-call p95 unavailable (no per-call logging); "
                "stage wall time is the comparison signal. artifact I/O / memory / intra-stage "
                "breakdown not captured.",
    }

    # ── 마크다운 리포트 ──
    md = [f"## dataset build baseline ({run_started_iso})",
          f"- project=`{pid}` dataset=`{did}` version=`{vid}`",
          "",
          "| stage | status | wall(s) |", "|---|---|---|"]
    for r in stage_results:
        md.append(f"| {r['stage']} | {r.get('status','-')} | {r.get('wall_sec','-')} |")
    md += ["", "### summary 지표 (worker summary)"]
    dg = metrics["doc_genuineness"]
    cl = metrics["clause_label"]
    md.append(f"- doc_genuineness: processed={dg['processed']}/{dg['input']} "
              f"tier_counts={dg['tier_counts']} tokens(in/out)={dg['prompt_tokens']}/{dg['completion_tokens']} "
              f"concurrency={dg['concurrency']}")
    md.append(f"- clause_label: processed_doc={cl['processed_doc_count']} "
              f"skipped_by_filter={cl['skipped_by_filter']}/{cl['input_row_count']} "
              f"clauses={cl['clause_count']} concurrency={cl['concurrency']}")
    md.append(f"- clean: in={metrics['clean']['input_row_count']} out={metrics['clean']['output_row_count']} "
              f"deduped={metrics['clean']['deduped_count']}")
    md.append(f"- clause_keywords: clauses={metrics['clause_keywords']['clause_count']} "
              f"unique_kw={metrics['clause_keywords']['unique_keyword_count']}")
    md += ["", f"### build view API 지연: {json.dumps(api_p95, ensure_ascii=False)}",
           "", f"> {report['note']}"]
    md_text = "\n".join(md)
    print("\n" + md_text)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\n[json] {args.out}")


if __name__ == "__main__":
    main()
