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

- 분류: 구현된 unstructured support skill
- 위치: `keyword_frequency` 옆
- 목적: 한국어 VOC/SNS 문서에서 명사 중심 top term을 안정적으로 뽑는다.
- 현재 구현:
  - filtered row를 읽고 `top_nouns(term_frequency, document_frequency)`를 만든다.
  - `kiwipiepy`가 있으면 품사 태깅을 쓰고, 없으면 regex token fallback으로 내려간다.
  - preview row에는 `noun_tokens`를 함께 남긴다.
- 입력:
  - prepared dataset
  - `text_column`
  - `top_n`
  - `sample_n`
  - `stopwords`
  - `user_dictionary_path`
  - `min_token_length`
  - `allowed_pos_prefixes` 기본값 `["N"]`
- 처리:
  - 가능하면 Kiwi 형태소 분석기로 품사 태깅
  - 명사만 선택
  - 최소 길이와 불용어 필터 적용
  - 필요하면 사용자 사전 로드
  - 문서 빈도와 전체 토큰 빈도를 함께 집계
- 출력:
  - `top_nouns`
  - `document_frequency`
  - `term_frequency`
  - `sample_rows`
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
  - 공식 runtime bundle 포함
  - worker task와 planner sequence 연결 완료

## backlog

### `sentence_split`

- 분류: 구현된 support utility skill
- 목적:
  - 문장 단위 citation
  - sentence-level sentiment
  - 긴 문서에서 evidence ranking 정밀도 향상
- 현재 구현:
  - filtered row를 문장 단위 span으로 나누고 `sample_documents` preview를 남긴다.
  - `kss`가 있으면 한국어 문장 분리기를 우선 사용하고, 없으면 regex fallback으로 내려간다.
  - `artifact_output_path`가 있으면 문장 row를 `rows.parquet` sidecar로 저장한다.
- 입력:
  - prepared dataset
  - `text_column`
  - `language`
  - `preview_sentences_per_row`
- 출력:
  - `summary`
  - `sample_documents`
  - 필요 시 `rows.parquet`
- 판단:
  - 규칙 명확성: 중간 이상
  - 재사용 가능성: 중간 이상
  - 자동화 가능 여부: YES

## 현재 추천 우선순위

1. `garbage_filter` 규칙 사전 고도화
2. `noun_frequency` 사용자 사전/stopword 운영 기준 정리
3. `sentence_split` 결과를 sentence-level citation 소비 경로와 연결

## 확인 필요

- `noun_frequency`에서 사용자 사전을 프로젝트 단위 설정으로 둘지, request payload override 중심으로 둘지는 아직 결정하지 않았다.
- `sentence_split` 결과를 `issue_evidence_summary`나 후속 sentence-level sentiment에 바로 연결할지는 아직 결정하지 않았다.
