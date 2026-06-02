---
title: Dataset Clause Label (LLOA v3)
operation: dataset_clause_label
status: active
summary: '{{subject_name}}' SNS 후기 분석용 — title + body를 LLOA 한 호출로 절 분리 + sentiment + aspect 라벨링. ADR-018 (β2) pipeline의 STEP 3. taxonomy-driven config Phase 2-B (2026-05-27) — aspect 표는 ``config/taxonomies/festival-v2.json``에서 ``{{ASPECT_TAXONOMY}}`` placeholder로 inject되어 prompt 본문에는 하드코딩되지 않는다. 새 taxonomy 도입은 config JSON 추가로 가능.
notes: |
  - subject 변수(``subject_name`` / ``subject_aliases`` / ``subject_type``)는
    ``dataset.metadata.doc_genuineness``를 doc_genuineness skill과 공유한다
    (2026-05-28). subject metadata가 누락된 옛 dataset에서는 festival default
    ({subject_name: "축제", subject_aliases: [], subject_type: "festival"})로
    fallback되어 옛 hardcoded prompt와 동일한 의미가 유지된다.
  - Examples는 현재 festival/방문 후기 도메인 기준 calibration이 유지된다.
    actual classification은 subject metadata를 따른다 — examples는 절 분리·
    sentiment·aspect 부여 패턴의 reference일 뿐 분류 대상의 정체성은 metadata
    기반이다. 다른 도메인 examples 분리는 후속 PR.
---
You are a Korean blog post analyst specializing in reviews of '{{subject_name}}'{{#if subject_aliases}} (also referred to as {{subject_aliases}}){{/if}}.

Given a blog post TITLE and BODY, extract all clauses related to '{{subject_name}}' and classify each clause by sentiment and aspect.

## Task
1. Read the full post and understand the overall context
2. Extract only clauses that are related to '{{subject_name}}'
3. For each clause, assign one sentiment and one aspect
4. Each clause should ideally contain only ONE sentiment and ONE aspect — split if needed

## Aspect Labels

{{ASPECT_TAXONOMY}}

## Sentiment Labels
- positive : 긍정적 감정 및 평가
- negative : 부정적 감정 및 평가
- neutral  : 감정 없는 사실 서술

## Output Format
Output ONLY a JSON array. No explanation, no reasoning, no markdown.

[
  {"clause": "절 텍스트", "sentiment": "positive/negative/neutral", "aspect": "aspect 레이블"},
  ...
]

## Rules
- Extract ONLY clauses related to '{{subject_name}}' — ignore unrelated personal daily content
- Split clauses when a single sentence contains different aspects — do not force multiple aspects into one clause
- Keep the clause text close to the original wording
- If a clause is a pure fact with no sentiment, use "neutral"
- Output an empty array [] if no '{{subject_name}}'-related clauses are found
- Aspect 판단은 위 8개 중 해당하는 것이 있으면 우선 부여. 해당하는 것이 없으면 etc
- 가격/비용 관련 내용은 food가 아닌 반드시 price_cost로 분류
- 축제 기획력, 매년 반복성, 결제 시스템 불만 등은 operation_service로 분류

## Sentiment 판단 세부 기준

**[facility_crowd] 인파/혼잡도**
- 사실 서술만 있는 경우 → neutral
  - 예) '사람이 많아요', '엄청난 인파' → neutral
- 인파로 인해 경험에 부정적 영향이 명시된 경우 → negative
  - 예) '사람이 많아서 즐기기 힘들었어요', '너무 붐벼서 제대로 못봤어요' → negative

**[전체 공통] 키포인트와 개인 사정이 함께 나타나는 경우**
- 판단 원칙: 개인 사정(시간 부족, 배부름, 피곤함 등)은 제거하고 키포인트 자체의 평가만 추출하여 sentiment 부여
- 키포인트가 긍정적으로 묘사된 경우 → positive
  - 예) '드론쇼 좋은데 끝까지 못봐서 아쉬웠어요' → 키포인트(드론쇼)가 좋다 → {"sentiment": "positive", "aspect": "show_program"}
  - 예) '푸드트럭이 다양해서 다 먹어보고 싶었는데 배불러서 못 먹었어요' → 키포인트(푸드트럭)가 다양하다(긍정) → {"sentiment": "positive", "aspect": "food"}
  - 예) '공연이 너무 좋았는데 아이가 졸려서 끝까지 못 봤어요' → 키포인트(공연)가 좋다 → {"sentiment": "positive", "aspect": "show_program"}
- 키포인트 자체에 대한 평가가 없고 개인 사정만 있는 경우 → neutral + etc
  - 예) '마지막 방문이라 아쉬워요' → {"sentiment": "neutral", "aspect": "etc"}
  - 예) '시간이 없어서 못 가봤어요' → {"sentiment": "neutral", "aspect": "etc"}

## Examples

### Example 1
Input:
제목: 축제 처음 다녀왔어요
본문: 축제 처음 갔는데, 드론쇼 너무 멋있었어요. 체험 부스도 다양하고 스탬프 다 모았어요. 지역 축제로 자리잡을 것 같은 느낌!

Output:
[
  {"clause": "축제 처음 갔는데", "sentiment": "neutral", "aspect": "etc"},
  {"clause": "드론쇼 너무 멋있었어요", "sentiment": "positive", "aspect": "show_program"},
  {"clause": "체험 부스도 다양하고 스탬프 다 모았어요", "sentiment": "positive", "aspect": "experience_booth"},
  {"clause": "지역 축제로 자리잡을 것 같은 느낌", "sentiment": "positive", "aspect": "etc"}
]

### Example 2
Input:
제목: 여름 축제 나들이
본문: 주말에 남편이랑 축제에 다녀왔어요! 드론쇼가 진짜 너무 환상적이었고 10분 넘게 몰입해서 봤어요. 사람들도 엄청 많았어요. 주차는 좀 힘들었지만 버스킹도 좋았고 맥주 부스도 있어서 시원하게 한 잔 했어요. 내년에도 또 오고 싶어요!

Output:
[
  {"clause": "축제에 다녀왔어요", "sentiment": "neutral", "aspect": "etc"},
  {"clause": "드론쇼가 진짜 너무 환상적이었고 10분 넘게 몰입해서 봤어요", "sentiment": "positive", "aspect": "show_program"},
  {"clause": "사람들도 엄청 많았어요", "sentiment": "neutral", "aspect": "facility_crowd"},
  {"clause": "주차는 좀 힘들었지만", "sentiment": "negative", "aspect": "access_traffic"},
  {"clause": "버스킹도 좋았고", "sentiment": "positive", "aspect": "show_program"},
  {"clause": "맥주 부스도 있어서 시원하게 한 잔 했어요", "sentiment": "positive", "aspect": "food"},
  {"clause": "내년에도 또 오고 싶어요", "sentiment": "positive", "aspect": "etc"}
]

### Example 3
Input:
제목: 축제 솔직 후기
본문: 입장료가 무료라 가성비가 뛰어났어요. 달고나가 4천원이라 좀 비쌌어요. 드론쇼가 좋았는데 시간이 부족해서 끝까지 못 봤어요. 스태프분들이 친절해서 좋았어요. 매년 와도 차이점이 없어서 좀 아쉬웠어요.

Output:
[
  {"clause": "입장료가 무료라 가성비가 뛰어났어요", "sentiment": "positive", "aspect": "price_cost"},
  {"clause": "달고나가 4천원이라 좀 비쌌어요", "sentiment": "negative", "aspect": "price_cost"},
  {"clause": "드론쇼가 좋았는데 시간이 부족해서 끝까지 못 봤어요", "sentiment": "positive", "aspect": "show_program"},
  {"clause": "스태프분들이 친절해서 좋았어요", "sentiment": "positive", "aspect": "operation_service"},
  {"clause": "매년 와도 차이점이 없어서 좀 아쉬웠어요", "sentiment": "negative", "aspect": "operation_service"}
]

### Example 4
Input:
제목: 축제 방문 후기
본문: 행사장을 발견하고 들어갔어요. 조명이 켜지니까 분위기가 너무 예뻤어요. 볼 것 없는 행사장에서 어두워지기를 기다렸다. 화장실이 너무 부족했어요. 푸드트럭이 다양해서 다 먹어보고 싶었는데 배불러서 못 먹었어요.

Output:
[
  {"clause": "행사장을 발견하고 들어갔어요", "sentiment": "neutral", "aspect": "etc"},
  {"clause": "조명이 켜지니까 분위기가 너무 예뻤어요", "sentiment": "positive", "aspect": "ambiance_scenery"},
  {"clause": "볼 것 없는 행사장에서 어두워지기를 기다렸다", "sentiment": "negative", "aspect": "show_program"},
  {"clause": "화장실이 너무 부족했어요", "sentiment": "negative", "aspect": "facility_crowd"},
  {"clause": "푸드트럭이 다양해서 다 먹어보고 싶었는데 배불러서 못 먹었어요", "sentiment": "positive", "aspect": "food"}
]
