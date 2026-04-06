# 시나리오 템플릿 매핑

## 목적

이 문서는 분석팀이 정리한 대표 질문 시나리오를 현재 runtime의 `strict` 시나리오 계약으로 옮길 때 어떤 skill 조합을 쓰는지 정리한 기준 문서다.

- 원본 시나리오 표를 그대로 저장하지 않고, 현재 plan-enabled skill 기준으로 재구성한다.
- `runtime_skill_name`을 명시해 strict 모드에서도 step 해석이 흔들리지 않게 한다.
- 현재 runtime에 없는 전용 기능은 가장 가까운 skill 조합으로 대체하고 그 차이를 적는다.

관련 import fixture:

- [festival_scenarios.import.json](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/testdata/festival_scenarios.import.json)

## 현재 strict 매핑

| 시나리오 | 사용자 질의 | 현재 strict step | runtime skill | 비고 |
| --- | --- | --- | --- | --- |
| `S1` | 이번 벚꽃 축제 반응 어때? | 가비지 필터링 | `garbage_filter` | 원본과 동일 |
| `S1` | 이번 벚꽃 축제 반응 어때? | 질의 문서 필터링 | `document_filter` | `query=벚꽃 축제`, `match_mode=all`로 대표 토큰을 모두 포함한 문서만 추린다 |
| `S1` | 이번 벚꽃 축제 반응 어때? | 감성 비율 집계 | `issue_sentiment_summary` | `document_filter` 결과 subset 기준 |
| `S1` | 이번 벚꽃 축제 반응 어때? | 빈도 기반 키워드 추출 | `keyword_frequency` | `document_filter` 결과 subset 기준 |
| `S1` | 이번 벚꽃 축제 반응 어때? | 대표 문서 샘플링 | `document_sample` | `query=벚꽃 축제` 기준 대표 근거 문서를 추린다 |
| `S1` | 이번 벚꽃 축제 반응 어때? | 전체 담론 요약 | `issue_evidence_summary` | `document_sample` prior artifact를 근거 선택 source로 사용 |
| `S2` | 축제 반응 흐름 어떻게 변했어? | 가비지 필터링 | `garbage_filter` | 원본과 동일 |
| `S2` | 축제 반응 흐름 어떻게 변했어? | 기간별 문서량 추이 | `time_bucket_count` | `bucket=day` |
| `S2` | 축제 반응 흐름 어떻게 변했어? | 기간별 담론 요약 | `issue_trend_summary` | 현재 runtime의 핵심 trend 요약 step |
| `S2` | 축제 반응 흐름 어떻게 변했어? | 변화 근거 요약 | `issue_evidence_summary` | 흐름 설명을 사용자용 근거 요약으로 마무리 |
| `S3` | 축제 전이랑 끝난 후 반응 차이 뭐야? | 가비지 필터링 | `garbage_filter` | 원본과 동일 |
| `S3` | 축제 전이랑 끝난 후 반응 차이 뭐야? | 기간 구간 문서량 비교 | `issue_period_compare` | 현재는 `bucket=day`, `window_size=3` 기준 근사 비교 |
| `S3` | 축제 전이랑 끝난 후 반응 차이 뭐야? | 기간 구간 키워드 비교 | `keyword_frequency` | 현재 runtime에는 전용 기간별 keyword compare skill이 없다 |
| `S3` | 축제 전이랑 끝난 후 반응 차이 뭐야? | 차이 요약 | `issue_evidence_summary` | 비교 결과를 사용자용 텍스트로 마무리 |
| `S4` | 이번 축제 문제 뭐였어? | 가비지 필터링 | `garbage_filter` | 원본과 동일 |
| `S4` | 이번 축제 문제 뭐였어? | 문서 단위 감성 분류 | `issue_sentiment_summary` | 현재는 전체 감성 분포와 대표 예시를 요약 |
| `S4` | 이번 축제 문제 뭐였어? | 감성별 키워드 분석 | `noun_frequency` | 현재 runtime에는 부정 전용 keyword skill이 없어서 명사 중심 집계로 대체 |
| `S4` | 이번 축제 문제 뭐였어? | 감성별 담론 요약 | `issue_evidence_summary` | `query`를 부정 중심으로 덮어 문제 근거를 요약 |
| `S5` | 이번 축제 언제 가장 많이 언급됐어? | 기간별 문서량 추이 | `time_bucket_count` | `bucket=day` |
| `S5` | 이번 축제 언제 가장 많이 언급됐어? | 피크 시점 탐지 | `issue_trend_summary` | 현재 runtime에서 `peak_bucket`을 같이 제공 |
| `S5` | 이번 축제 언제 가장 많이 언급됐어? | 피크 근거 요약 | `issue_evidence_summary` | peak 설명을 사용자용 텍스트로 마무리 |

## 원본 대비 차이

- `S1`은 broad festival corpus에서 `축제` token만 맞는 문서가 과다 유입되는 문제가 있었다.
  - 현재 strict 구성은 `document_filter(query=벚꽃 축제, match_mode=all)`을 추가해 대표 토큰을 모두 포함한 문서만 먼저 추린다.
  - `issue_evidence_summary` 앞에 `document_sample`을 넣어, filter된 문서 subset이 실제 evidence 선택까지 이어지게 했다.
- `S2`의 `기간별 감성 비교`, `감성 비율 변화량`은 현재 runtime에 전용 skill이 없다.
  - 현재 strict 구성은 `time_bucket_count + issue_trend_summary + issue_evidence_summary`로 흐름 요약에 집중한다.
- `S3`의 `전/중/후` 비교는 현재 date range 기반 explicit segmentation이 없다.
  - 현재 strict 구성은 `issue_period_compare`의 `bucket=day`, `window_size=3`로 최근 구간 vs 직전 구간 비교에 가깝다.
- `S4`의 `감성별 키워드 분석`, `감성별 담론 요약`은 현재 부정 라벨만 직접 필터링하는 전용 skill이 없다.
  - 현재 strict 구성은 `issue_sentiment_summary`를 선행하고, `noun_frequency`와 `issue_evidence_summary`의 `query`를 부정 중심으로 조정한다.
- `S5`의 `피크 시점 탐지`는 전용 detector 대신 `time_bucket_count`의 `peak_bucket`과 `issue_trend_summary`를 사용한다.

## 운영 가이드

- strict 시나리오는 가능한 한 모든 step에 `runtime_skill_name`을 넣는다.
- `function_name` 자동 alias는 보조 수단으로만 본다.
- 현재 skill과 1:1로 맞지 않는 질문은 무리하게 alias를 추가하기보다 `runtime_skill_name + parameters + 비고`로 명시하는 편이 안전하다.

## 확인 필요

- `S3`의 전/중/후 비교는 실제 축제 일정 metadata를 dataset version context에 함께 넣어 explicit period segmentation으로 올릴지 결정이 필요하다.
- `S4`의 부정 전용 키워드/담론 분석은 향후 `sentiment-filtered keyword/evidence` skill이 별도로 필요할 수 있다.
