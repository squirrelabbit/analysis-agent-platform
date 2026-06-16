# clause_label 문장 앵커 교차모델 측정 (실험 스크립트)

ADR-028 (clause_label sentence-anchor cross-model verify)의 근거 측정 1~6 재현용.
**production path 아님** — 일회성 진단/실험이다. vault 진단 문서:
`검토-raw/clause_label_교차모델_발산_측정_2026-06-16.md`.

## 주의

- **production 코드 아님.** worker/control-plane 런타임에 import되지 않는다.
- **LLOA env 필요** (doc_genuineness / clause_label / 문장 라벨 호출):
  `LLOA_API_KEY`, `LLOA_API_URL`. (effort는 `LLOA_REASONING_EFFORT=low`로 prod 맞춤.)
- **입력 차이 주의** — `raw 본문`과 `clean+non_review 제외 정제본`은 결과가 다르다.
  production clause_label은 `clean → doc_genuineness(non_review 제외)` 이후에 돌므로,
  의미 있는 수치는 **정제본 기준**이다(`prep_cleaned_filtered_docs.py` 산출물 사용).
- **`/tmp` 출력물은 커밋하지 않는다.** 각 스크립트 `--out-dir` 기본값이 `/tmp/...`.
- splitter는 **kiwipiepy** (의존성에 이미 있음). 로컬 재현 시 `pip install
  "kiwipiepy>=0.23,<0.24"` 필요. 현 코드 `_split_sentences`의 `import kss`는 의존성에
  없어 regex로 떨어지는 함정 — audit이 이를 드러낸다.
- 입력 데이터 `data/festival.csv`는 gitignore(2121행). 없으면
  `docs/eval/quality_v1/datasets/festival_sample_50.csv`(50행)로 대체.

## 스크립트

| 파일 | 목적 | 측정 |
|---|---|---|
| `measure_clause_label_cross_model.py` | clause-mode(절 자유) 두 모델 절 발산 | 측정 1·2 |
| `measure_clause_label_sentence_anchor.py` | 문장 앵커 + 단일 aspect | 측정 3 |
| `measure_sentence_anchor_multiaspect.py` | 문장 앵커 + multi-label aspect | 측정 4·6b |
| `audit_sentence_splitter.py` | regex vs kiwipiepy bad-split 감사 | 측정 5·6a |
| `prep_cleaned_filtered_docs.py` | clean+doc_genuineness+non_review 제외 정제본 생성 | 측정 6 전처리 |

## 재현 순서 (정제본 기준 = ADR-028 최종 수치)

```bash
ROOT=...   # repo 루트
cd "$ROOT"
export LLOA_API_KEY=... LLOA_API_URL=... LLOA_REASONING_EFFORT=low
export PYTHONPATH=workers/python-ai/src
PY="python3.11"
DIR=scripts/experiments/clause_label_cross_model

# 1) 정제본 생성 (raw 1000 → clean → doc_genuineness → non_review 제외, ~200 doc)
$PY $DIR/prep_cleaned_filtered_docs.py --csv data/festival.csv --limit 1000 \
    --out-dir /tmp/prep_cleaned_1k

# 2) splitter audit (LLOA 불필요)
$PY $DIR/audit_sentence_splitter.py \
    --csv /tmp/prep_cleaned_1k/cleaned_filtered.csv --limit 202 \
    --out-dir /tmp/splitter_audit_clean

# 3) 문장 앵커 multi-aspect 측정 (Max/Ultra)
$PY $DIR/measure_sentence_anchor_multiaspect.py \
    --models wisenut/wise-lloa-max-v1.2.1,wisenut/wise-lloa-ultra-v1.1.0 \
    --csv /tmp/prep_cleaned_1k/cleaned_filtered.csv --limit 202 \
    --out-dir /tmp/sentence_multiaspect_clean
```

## 측정 결과 요약 (정제본 202 doc, n=5903)

- relevant Cohen's kappa **0.747** (문장 앵커로 비교가능성 회복) — PASS
- sentiment 일치 85.3% / aspect-set Jaccard 0.678 (untuned 측정 프롬프트 = 보수 바닥값)
- 불일치 ~80%가 boundary calibration (sentiment neutral↔pos 81%, aspect add/drop 78%)
- splitter: kiwipiepy 진짜 bad-split ~3% (regex 52% 실격)

## Gate 1 (ADR-028 구현 게이트 — 통과)

production 수준 튜닝 프롬프트(`gate1_tuned_prompt.txt`) + kiwipiepy로 정제본 재측정.

```bash
$PY $DIR/measure_sentence_anchor_multiaspect.py \
    --models wisenut/wise-lloa-max-v1.2.1,wisenut/wise-lloa-ultra-v1.1.0 \
    --csv /tmp/prep_cleaned_1k/cleaned_filtered.csv --limit 202 \
    --splitter kiwipiepy \
    --system-prompt-file $DIR/gate1_tuned_prompt.txt \
    --out-dir /tmp/gate1
```

결과(n≈10000): relevant kappa **0.757** ✅ / sentiment **88.0%** ✅ / aspect-set
Jaccard **0.763**. → 문장 앵커 설계 통과.

- **aspect raw Jaccard 0.80 바는 현 taxonomy서 비현실적**으로 확인(프롬프트 2회 iterate
  plateau ~0.76). aspect는 raw 모델간 일치가 아니라 **judge/union reconciliation 후
  품질**로 평가한다(ADR-028 결정).
- v2(`etc 최후수단` 강제)는 두 모델이 서로 다른 구체 aspect를 골라 **regress** → 폐기.
  같은 시도 반복 금지.
- aspect 천장 원인 = `etc/ambiance_scenery/show_program/experience_booth` 경계 모호
  → **taxonomy 재설계 별도 트랙**.

상세·결론은 ADR-028 + vault 진단 문서 참조.
