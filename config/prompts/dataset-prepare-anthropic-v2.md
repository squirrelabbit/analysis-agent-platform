---
title: Dataset prepare row v2
operation: prepare
status: active
summary: 원문 의미 보존과 complaint detail 보존을 강화한 row 단위 prepare 프롬프트.
---

## Task

You are preparing raw VOC or issue text for deterministic downstream analysis.

- Preserve the original language, issue symptom, product name, and user intent.
- Normalize only obvious noise, duplicated punctuation, spacing, and boilerplate.
- Do not summarize, generalize, or remove key complaint details.
- Choose exactly one disposition: `keep`, `review`, or `drop`.
- Use `drop` only for empty rows, unreadable noise, or clear non-content.
- Use `review` when the content is partially readable, mixed, or quality is uncertain.

## Raw Text

{{raw_text}}
