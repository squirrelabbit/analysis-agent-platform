# 전처리 후보와 규칙 설계

## 목적

이 문서는 분석팀 전처리 자산과 현재 runtime 구조를 연결하기 위한 설계 메모다.

- 현재 runtime에 이미 들어간 항목과 아직 후보인 항목을 분리해 적는다.
- 함수 단위 자산을 `prepare 내부 규칙`, `공식 support skill`, `후보 skill`, `backlog`로 다시 분류한다.
- 규칙 명확성, 재사용성, 자동화 가능성을 runtime 관점에서 확인한다.

## 현재 runtime에 반영된 항목

### `garbage_filter`

- 분류: 공식 unstructured support skill
- 목적: 광고, 협찬, 링크 유도, placeholder, noise-only row를 downstream 분석 전에 제거한다.
- 주요 구현:
  - `workers/python-ai/src/python_ai_worker/skills/support.py`
  - `workers/python-ai/src/python_ai_worker/runtime/common.py`
  - `workers/python-ai/src/python_ai_worker/runtime/constants.py`
- 입력:
  - prepared dataset
  - `text_column`
  - `garbage_rule_names`
- 출력:
  - `retained_indices`
  - `removed_indices`
  - `garbage_rule_hits`
  - `removed_samples`
  - execution 경로에서는 `rows.parquet` sidecar와 `artifact_ref`
- 현재 기본 규칙:
  - `ad_marker`
    - 광고, 협찬, 체험단, 원고료, 유료 광고, 소정의 수수료, 쿠팡 파트너스, `sponsored`, `advertisement`
  - `promotion_link`
    - 프로필 링크, 링크 클릭, 구매 링크, 자세한 내용은 링크, 문의/상담 DM
  - `platform_placeholder`
    - 존재하지 않는 이미지입니다, 존재하지 않는 스티커입니다, `Previous imageNext image`
  - `empty_or_noise`
    - prepare regex 정제 후 비어 있거나 token이 남지 않는 텍스트
- 판단:
  - 규칙 명확성: 높음
  - 재사용 가능성: 높음
  - 자동화 가능 여부: YES
- 보완 필요:
  - 채널별 규칙 사전
  - borderline 문서 review 경로
  - rule severity 또는 whitelist

### `dataset_prepare` regex 확장 포인트

- 분류: dataset build 내부 규칙
- 목적: LLM prepare/fallback normalize 전에 명시적인 regex 정제를 먼저 적용한다.
- 주요 구현:
  - `workers/python-ai/src/python_ai_worker/skills/dataset_build.py`
  - `workers/python-ai/src/python_ai_worker/runtime/common.py`
  - `workers/python-ai/src/python_ai_worker/runtime/constants.py`
- 입력:
  - raw text
  - `regex_rule_names`
- 출력:
  - `prepared.parquet`
  - row별 `prepare_regex_applied_rules`
  - artifact summary의 `prepare_regex_rule_hits`
- 현재 기본 규칙:
  - `media_placeholder`
    - 이미지/스티커 placeholder 문구 제거
  - `html_artifact`
    - `<br>`, `&nbsp;` 정리
  - `url_cleanup`
    - `http://`, `https://`, `www.` URL 제거
  - `zero_width_cleanup`
    - zero-width, BOM 문자 제거
- 왜 skill이 아니라 prepare 내부인가:
  - 모든 비정형 row 정제의 공통 선행 단계라서 dataset build에 붙는 편이 자연스럽다.
  - LLM prepare를 켜더라도 deterministic 정제를 먼저 태워 재현성을 높일 수 있다.

## 후보 skill

### `noun_frequency`

- 분류: 후보 unstructured support skill
- 위치: `keyword_frequency` 옆
- 목적: 한국어 VOC/SNS 문서에서 명사 중심 top term을 안정적으로 뽑는다.
- 현재 필요성:
  - `keyword_frequency`는 regex token frequency라 한국어 복합 명사 품질이 제한된다.
  - 분석팀 자산도 Kiwi 기반 명사 추출을 많이 사용한다.
- 예상 입력:
  - prepared dataset
  - `text_column`
  - `top_n`
  - `sample_n`
  - `stopwords`
  - `user_dictionary_path`
  - `min_token_length`
  - `allowed_pos_prefixes` 기본값 `["N"]`
- 예상 처리:
  - Kiwi 같은 형태소 분석기로 품사 태깅
  - 명사만 선택
  - 최소 길이와 불용어 필터 적용
  - 필요하면 사용자 사전 로드
  - 문서 빈도와 전체 토큰 빈도를 함께 집계
- 예상 출력:
  - `top_nouns`
  - `document_frequency`
  - `term_frequency`
  - `sample_rows`
  - 필요 시 `row_noun_tokens`
- 장점:
  - 한국어 이슈명, 기능명, 기관명 추출 품질 개선
  - `keyword_frequency` 대비 의미 있는 명사 집계 강화
- 리스크:
  - 형태소 분석기 의존성 추가
  - 대용량 병렬 처리 전략 필요
  - 사용자 사전 운영 정책 필요
- 판단:
  - 규칙 명확성: 높음
  - 재사용 가능성: 높음
  - 자동화 가능 여부: YES
- 현재 상태:
  - 공식 runtime bundle 미포함
  - 설계 후보

## backlog

### `sentence_split`

- 분류: backlog utility 또는 prepare 옵션 후보
- 목적:
  - 문장 단위 citation
  - sentence-level sentiment
  - 긴 문서에서 evidence ranking 정밀도 향상
- 현재 보류 이유:
  - 이미 chunk 기반 retrieval이 있어 즉시 필수는 아니다.
  - 지금 단계에서는 sentence split보다 retrieval/cluster 품질이 우선순위가 높다.
- 나중에 구현한다면:
  - 입력: prepared dataset, `text_column`, 언어 옵션
  - 출력: `sentences.parquet` 또는 문장 배열 sidecar
  - 후보 기술: `kss` 또는 다른 문장 분리기
- 판단:
  - 규칙 명확성: 중간
  - 재사용 가능성: 중간 이상
  - 자동화 가능 여부: YES, 다만 언어/예외 규칙 검증 필요

## 현재 추천 우선순위

1. `garbage_filter` 규칙 사전 고도화
2. `noun_frequency` 후보를 support skill로 구체화
3. `sentence_split`은 문장 단위 citation 요구가 생길 때 착수

## 확인 필요

- `noun_frequency` 구현 시 Kiwi를 기본 의존성으로 둘지, 선택 플러그인으로 둘지는 아직 결정하지 않았다.
- `sentence_split` 결과를 `prepare` 안에 넣을지, 별도 task로 둘지는 아직 결정하지 않았다.
