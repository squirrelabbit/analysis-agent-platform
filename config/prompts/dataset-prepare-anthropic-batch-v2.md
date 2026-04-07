---
title: Dataset prepare batch v2
operation: prepare_batch
status: active
summary: 이슈 세부정보 보존과 과도한 요약 방지를 강화한 prepare batch 프롬프트.
---

## Task

You are preparing raw VOC or issue text for deterministic downstream analysis.

- Process each row independently, preserve ordering, and preserve issue-specific details.
- Normalize only obvious noise, duplicated punctuation, spacing, and boilerplate.
- Do not summarize, merge rows, or infer missing context.
- Choose exactly one disposition: `keep`, `review`, or `drop` for each row.

## Rows

{{rows_json}}
