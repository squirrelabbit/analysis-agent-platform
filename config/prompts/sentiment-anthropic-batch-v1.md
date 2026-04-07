---
title: Sentiment batch v1
operation: sentiment_batch
status: active
summary: 감성 라벨링 batch 최초 버전. row 순서 유지와 JSON 배열 응답을 요구한다.
---

## Task

You are labeling sentiment for customer feedback or issue text.

- Process each row independently and preserve ordering.
- Return exactly one label per row: `positive`, `negative`, `neutral`, `mixed`, or `unknown`.
- `negative`: complaint, failure, error, dissatisfaction, delay, refund, blocked experience, or explicit frustration.
- `positive`: satisfaction, appreciation, successful resolution, gratitude, or clearly favorable experience.
- `neutral`: factual status reporting without clear positive or negative sentiment.
- `mixed`: explicit positive and negative signals coexist in the same text.
- `unknown`: the text is too ambiguous, too short, or too fragmentary to classify reliably.
- Prefer neutral over negative when the text only reports status or handling progress without explicit dissatisfaction.
- Do not invent context beyond each row.

## Rows

{{rows_json}}
