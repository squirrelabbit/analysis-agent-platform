# Prompt Changelog

## dataset-prepare-anthropic-v2

- v1 대비 원문 의미 보존, complaint detail 유지, 과도한 일반화 방지 지시를 강화했다.

## dataset-prepare-anthropic-batch-v2

- v1 대비 issue-specific detail 보존과 불필요한 요약 금지 지시를 강화했다.

## sentiment-anthropic-v2

- neutral 우선 규칙과 ambiguous text 처리 기준을 명시했다.

## sentiment-anthropic-batch-v1

- 감성 라벨링 batch 처리용 최초 버전이다.
- row 순서 유지와 JSON 배열 응답을 명시한다.

## sentiment-anthropic-batch-v2

- `sentiment-anthropic-v2`의 neutral 우선 규칙과 ambiguity 처리 기준을 batch 응답에도 동일하게 적용한다.

## execution-final-answer-v1

- `result_v1`와 evidence만 사용해 grounded final answer를 생성하는 첫 버전이다.
- 새 주장보다 요약/정리/주의사항 표시에 집중하도록 제한한다.
