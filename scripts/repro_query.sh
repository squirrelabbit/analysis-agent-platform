#!/usr/bin/env bash
# scripts/repro_query.sh
#
# 질의 → 결과 재현성 테스트. 같은 자연어 질의를 K회 실행해 결과(표)/plan이
# 얼마나 동일한지 측정한다. recipe(distribution/top_n/event_window_count)가
# "질의 → 결과" 재현성을 얼마나 끌어올렸는지 확인용 (silverone 2026-06-05).
#
# 측정 지표(질의별):
#   - result   : 결과 표(컬럼+행, float는 4자리 반올림) 고유 개수 / K  ← 핵심
#   - shape    : plan 스킬 시퀀스 고유 개수 / K
#   - plan     : plan 전체(step id+skill+params, display 제외) 고유 개수 / K
#
# 사용:
#   ./scripts/repro_query.sh                      # festival / festival_sns, K=5
#   K=10 ./scripts/repro_query.sh                 # 질의당 10회
#   PROJECT_NAME=festival DATASET_NAME=festival_sns ./scripts/repro_query.sh
#   PROJECT_ID=... DATASET_ID=... ./scripts/repro_query.sh   # id 직접 지정
#
# 환경변수: API_BASE(기본 http://127.0.0.1:18080), K(기본 5),
#           PROJECT_ID/PROJECT_NAME, DATASET_ID/DATASET_NAME.
#
# 모든 질의·반복은 단일 분석 thread 안에서 메시지로 실행한다(thread 1개만 생성).
#    → 같은 thread 대화 맥락(직전 동일 Q&A 누적)이 plan에 주는 영향까지 포함한 재현성.
#    테스트 후 정리가 필요하면 출력된 thread_id 하나만 삭제하면 된다.

set -euo pipefail

export API_BASE="${API_BASE:-http://127.0.0.1:18080}"
export K="${K:-5}"
export PROJECT_ID="${PROJECT_ID:-}"
export DATASET_ID="${DATASET_ID:-}"
export PROJECT_NAME="${PROJECT_NAME:-festival}"
export DATASET_NAME="${DATASET_NAME:-festival_sns}"

python3 - <<'PY'
import json, os, sys, time, hashlib, urllib.request, urllib.error

API = os.environ["API_BASE"].rstrip("/")
K = int(os.environ["K"])

# recipe/패턴을 다양하게 자극하는 10개 질의 (festival_sns: SNS 후기 · clauses/genuineness).
QUERIES = [
    ("q01_감성비율",        "전체 문서의 감성(긍정/중립/부정) 비율을 알려줘"),
    ("q02_aspect분포",      "aspect별 문서 수 분포를 보여줘"),
    ("q03_부정aspect_top5", "부정 후기가 가장 많은 aspect 상위 5개를 보여줘"),
    ("q04_최다aspect_top10","가장 많이 언급된 aspect 상위 10개를 빈도순으로 보여줘"),
    ("q05_긍정aspect_top5", "긍정 후기가 가장 많은 aspect 상위 5개를 보여줘"),
    ("q06_진성성_비율",     "문서 진성성(진성/비진성/불확실) 비율을 알려줘"),
    ("q07_진성_감성분포",   "진성으로 분류된 문서만 대상으로 감성 분포를 보여줘"),
    ("q08_부정많은aspect",  "부정 감성 절이 가장 많은 aspect 상위 5개를 건수와 함께 보여줘"),
    ("q09_sentiment_건수",  "감성별 절 개수를 집계해줘"),
    ("q10_비진성_비율",     "비진성(non_review)으로 분류된 문서 수와 전체 대비 비율을 알려줘"),
]


def http(method, path, payload=None, timeout=180):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(API + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def resolve_ids():
    pid = os.environ.get("PROJECT_ID") or ""
    did = os.environ.get("DATASET_ID") or ""
    if pid and did:
        return pid, did
    projects = http("GET", "/projects").get("items", [])
    if not pid:
        pname = os.environ["PROJECT_NAME"]
        match = [p for p in projects if p.get("name") == pname]
        if not match:
            sys.exit(f"프로젝트 '{pname}'를 찾을 수 없습니다. 가능: {[p.get('name') for p in projects]}")
        pid = match[0]["project_id"]
    if not did:
        dname = os.environ["DATASET_NAME"]
        datasets = http("GET", f"/projects/{pid}/datasets").get("items", [])
        match = [d for d in datasets if d.get("name") == dname]
        if not match:
            sys.exit(f"데이터셋 '{dname}'를 찾을 수 없습니다. 가능: {[d.get('name') for d in datasets]}")
        did = match[0]["dataset_id"]
        if not match[0].get("active_dataset_version_id"):
            sys.exit(f"데이터셋 '{dname}'에 활성 버전이 없습니다 — analyze 불가.")
    return pid, did


def create_thread(pid, did, title):
    r = http("POST", f"/projects/{pid}/datasets/{did}/analysis_threads", {"title": title})
    return r.get("thread_id") or r.get("id")


def send_message(pid, did, tid, content):
    # 단일 thread 안에서 후속 메시지로 질의 — analyze와 동일 응답 shape(result.plan/composer).
    return http("POST", f"/projects/{pid}/datasets/{did}/analysis_threads/{tid}/messages",
                {"content": content})


def plan_shape(plan):
    return " → ".join(str(s.get("skill")) for s in plan.get("steps", []))


def plan_full(plan):
    norm = [{"id": s.get("id"), "skill": s.get("skill"), "params": s.get("params")}
            for s in plan.get("steps", [])]
    return json.dumps(norm, sort_keys=True, ensure_ascii=False)


def result_sig(disp):
    if not disp:
        return "<no-display>"
    def rnd(v):
        return round(v, 4) if isinstance(v, float) else v
    rows = sorted(
        json.dumps({k: rnd(v) for k, v in row.items()}, sort_keys=True, ensure_ascii=False)
        for row in disp.get("rows", [])
    )
    return json.dumps({"cols": disp.get("columns", []), "rows": rows}, ensure_ascii=False)


def short(s):
    return hashlib.sha1(s.encode()).hexdigest()[:8]


pid, did = resolve_ids()
tid = create_thread(pid, did, "repro-test")
print(f"# 재현성 테스트  project={pid}  dataset={did}")
print(f"#   thread={tid} (단일 스레드)  K={K}회/질의\n")

result_rates = []
for key, q in QUERIES:
    statuses, shapes, fulls, results = [], [], [], []
    for _ in range(K):
        try:
            d = send_message(pid, did, tid, q)
        except urllib.error.HTTPError as e:
            statuses.append(f"HTTP{e.code}")
            continue
        except Exception as e:  # noqa: BLE001
            statuses.append(f"ERR")
            print(f"  {key}: 호출 실패 {e}", file=sys.stderr)
            continue
        res = d.get("result", {}) or {}
        statuses.append((d.get("run", {}) or {}).get("status", "?"))
        shapes.append(plan_shape(res.get("plan", {}) or {}))
        fulls.append(plan_full(res.get("plan", {}) or {}))
        results.append(result_sig((res.get("composer", {}) or {}).get("display", {})))
        time.sleep(0.2)

    n = len(results)
    ok = sum(1 for s in statuses if s == "completed")
    ru = len(set(results)) if results else 0
    su = len(set(shapes)) if shapes else 0
    fu = len(set(fulls)) if fulls else 0
    rate = (results.count(max(set(results), key=results.count)) / n) if n else 0
    result_rates.append(rate)

    flag = "OK " if ru <= 1 else "⚠️ "
    print(f"## {flag}{key}  «{q}»")
    print(f"   완료 {ok}/{len(statuses)}  status={statuses}")
    print(f"   result 고유 {ru}/{n}   (모달 일치율 {rate*100:.0f}%)")
    print(f"   shape  고유 {su}/{n}")
    for s in sorted(set(shapes)):
        print(f"      [{shapes.count(s)}x] {s}")
    print(f"   plan   고유 {fu}/{n}   hash={[short(x) for x in fulls]}")
    if ru > 1:
        print("      ⚠️ 동일 질의가 서로 다른 결과 표를 냄 — 재현성 실패")
    print()

if result_rates:
    avg = sum(result_rates) / len(result_rates)
    full_repro = sum(1 for r in result_rates if r == 1.0)
    print("=" * 60)
    print(f"요약: 질의 {len(result_rates)}개 · 결과 100% 재현 {full_repro}/{len(result_rates)}개 "
          f"· 평균 모달 일치율 {avg*100:.0f}%")
PY
