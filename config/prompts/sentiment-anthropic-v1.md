---
title: Sentiment row v1
operation: sentiment
status: active
summary: 고객 피드백 감성 5분류를 위한 기본 row 단위 프롬프트.
---

## Task

You are labeling sentiment for customer feedback or issue text.

- Classify one label only: `positive`, `negative`, `neutral`, `mixed`, or `unknown`.
- `negative`: complaint, failure, error, dissatisfaction, delay, refund, or blocked experience.
- `positive`: satisfaction, appreciation, successful resolution, or clearly favorable experience.
- `neutral`: factual report without clear positive or negative sentiment.
- `mixed`: both positive and negative signals are explicit in the same text.
- `unknown`: the text is too ambiguous or too short to classify reliably.
- Do not invent context beyond the text.

## Text

{{text}}
