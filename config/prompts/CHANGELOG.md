# Prompt CHANGELOG

프롬프트 버전 변경 배경 기록. README의 "기본값 변경 절차"에 따라, 새 버전 추가 시
여기에 의도를 남긴다. 파일은 stem = version (예: `clause_label/v5.md` → `v5`).

## 2026-06-25 — festival 통합 base (clause_label v5, doc_genuineness v3)

도메인 fork(`clause_label/v3 일반·v4 군산`, `doc_genuineness/v1 일반·v2 군산`)는 사실
같은 festival 도메인이고, 차이는 예시 몇 개 + 규칙 2~3개뿐이었다. 새 축제가 들어올
때마다 프롬프트를 통째 fork하지 않도록 **task당 festival base 1개**로 통합한다.

- **`clause_label/v5.md` (신설, festival base)** — v3 본문 + v4의 일반화 가능한 규칙
  흡수:
  - 결제·대기줄 혼잡이 운영 연관 시 → `operation_service`
  - 과거 차수 회상·방문 계기만 있고 현재 행사 평가가 없으면 → `neutral`
    (연도/회차 비교가 분석 대상인 데이터셋엔 부적합 → 그런 경우 행사별
    `extra_instructions`로 덮어쓴다)
  - 푸드트럭/부스 먹거리 다양성 → `food`
  군산 고유 예시(백년광장 / 고진포해양테마공원 / 브루스밴드 등)는 base에서 제외하고
  군산 dataset의 `metadata.clause_label.extra_examples`로 옮긴다.
- **`doc_genuineness/v3.md` (신설, festival base)** — v1 본문·예시를 그대로 동결.
  (v1/v2는 본문·Rules·스키마 동일, Examples 도메인만 달랐음.) 군산 예시는
  `metadata.doc_genuineness.extra_examples`로 옮긴다.
- **행사별 추가 슬롯**: 두 base 모두 끝에 `{{#if extra_instructions}}` /
  `{{#if extra_examples}}` 마커를 둔다. 값이 비면 섹션째 생략(append-only). 슬롯은
  **task별 분리** — `metadata.clause_label.extra_*`(문장 배열 스키마) 와
  `metadata.doc_genuineness.extra_*`(문서 3-tier 스키마)는 출력 스키마가 달라 공용 금지.

### deprecated (삭제하지 않음)

- **`clause_label/v4.md`**, **`doc_genuineness/v2.md`** — 군산 특화 fork. v5/v3 base +
  행사별 슬롯으로 대체된다. 과거 artifact 재현(prompt_version 문자열로만 기록되어
  파일이 없으면 재빌드 불가)을 위해 **파일은 보존**한다. 신규 빌드에는 쓰지 않는다.

### default 전환 (parity 게이트)

- **PR2-A** — v5/v3 파일만 추가, default 유지.
- **PR2-B (2026-06-25)** — `doc_genuineness` default를 `v1 → v3`로 전환. v3는 v1
  본문 그대로 + 빈 슬롯이라 **렌더 결과가 v1과 byte-동일**(parity 잠금 테스트
  `test_v3_base_byte_identical_to_v1_when_slot_empty`로 확인) → 기존 dataset
  동작 불변. 슬롯 마커 whitespace도 빈값 시 잔여 줄이 없도록 정리.
- **clause_label default는 v3 유지(보류)** — v5는 v3 대비 흡수 규칙 3줄(결제·대기줄
  →operation, 과거회상→neutral, 푸드트럭→food)이 추가돼 **동작이 바뀔 수 있으므로**,
  강릉/군산 샘플 behavioral parity 측정 후 별도로 전환한다.
