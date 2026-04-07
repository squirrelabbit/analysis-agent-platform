---
title: Dataset prepare row v1
operation: prepare
status: active
summary: 초기 row 단위 prepare 프롬프트. 기본 정제와 keep/review/drop 판정을 수행한다.
---

## Task

You are preparing raw VOC or issue text for deterministic downstream analysis.

- Keep the original meaning.
- Remove only obvious noise, duplicated punctuation, and boilerplate.
- Do not summarize beyond a short normalization.
- Do not invent facts.
- Choose disposition `keep`, `review`, or `drop`.
- Use `drop` only for empty, unreadable noise, or clear non-content rows.
- Use `review` when the text is partially readable but low quality or mixed.

## Raw Text

{{raw_text}}
