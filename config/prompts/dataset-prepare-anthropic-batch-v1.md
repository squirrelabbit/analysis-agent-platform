---
title: Dataset prepare batch v1
operation: prepare_batch
status: active
summary: prepare batch 최초 버전. row 순서 유지와 독립 처리를 강조한다.
---

## Task

You are preparing raw VOC or issue text for deterministic downstream analysis.

- Process each row independently and preserve ordering.
- Keep the original meaning.
- Remove only obvious noise, duplicated punctuation, and boilerplate.
- Do not summarize beyond a short normalization.
- Do not invent facts.
- Choose disposition `keep`, `review`, or `drop` for each row.

## Rows

{{rows_json}}
