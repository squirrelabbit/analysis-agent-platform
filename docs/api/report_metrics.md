# 보고서 템플릿 metric 가이드 (운영자용)

기초분석보고서 템플릿(`config/report_templates/*.json`)을 작성할 때, 내부 build/path를
몰라도 **metric 이름**만으로 값을 고를 수 있다. metric 사전은 `config/report_metrics.json`.

## 작성법

### stat (단일 값) — `metric`만
```json
{ "label": "진성 문서수", "metric": "genuine_docs_total" }
```
`format`·`unit`·source·연도 sub는 metric이 자동으로 채운다. `label`만 쓰면 된다(생략 시 metric 기본 라벨).

### 차트 — `view` + `metric` + 옵션
```json
{ "view": "doughnut", "width": "2/3", "metric": "sentiment_distribution", "order": ["positive","neutral","negative"] }
{ "view": "rank", "width": "1/2", "metric": "keywords_positive", "top": 10, "title": "긍정 키워드" }
```
- `view`: stat_grid | bar | doughnut | table | stacked_bar | rank
- `width`: full | 3/4 | 2/3 | 1/2 | 1/3 | 1/4
- 옵션: `order`(고정 순서) · `order_by`(정렬 키) · `top`(상위 N) · `value_format`(metric 기본 덮어쓰기) · `title`

### 고급 — metric에 없으면 source 직접
```json
{ "label": "사용자 정의 값", "source": { "build": "clause_label", "path": "summary.custom.path" } }
```

## metric 목록

| metric | kind | 값 | format/표현 |
|---|---|---|---|
| `genuine_docs_total` | stat | 진성 문서수(전체) | count · 건 |
| `genuine_docs_recent_year` | stat | 진성 문서수(최근연도) | count · 건 (+연도 sub) |
| `clauses_total` | stat | 절 수(전체) | count · 개 |
| `clauses_recent_year` | stat | 절 수(최근연도) | count · 개 (+연도 sub) |
| `sentiment_distribution` | distribution | 절 단위 감성 분포 | percent |
| `aspect_distribution` | distribution | 유형별 절 분포 | count |
| `aspect_sentiment` | stacked | 유형별 감성 구성(stacked) / 유형 순위(rank, order_by positive\|negative) | percent / count |
| `channel_genuine_distribution` | distribution | 채널별 진성 문서 분포 | count |
| `keywords_positive` | rank | 긍정 키워드 순위 | count |
| `keywords_negative` | rank | 부정 키워드 순위 | count |

> 새 metric을 추가하려면 `config/report_metrics.json`의 `metrics`에 항목을 더한다.
> `source.build`는 보고서 엔진이 아는 build root(clean / doc_genuineness / clause_label /
> clause_keywords / channel_breakdown / recent_year_stats / version)여야 한다 — 새 종류의
> 집계가 필요하면 엔진(`report_template.go`)에 build root를 먼저 추가한다.

## 빌드 의존성

각 섹션은 `required_build`가 ready여야 생성된다. metric이 참조하는 build가 미완이면
그 값만 빈칸이 된다(예: clause_label 미빌드 → 절 수 빈칸, 진성 문서수(최근연도)는
doc_genuineness만으로 나옴).

값 렌더 계약(프론트)은 [report_basic_template.sample.md](report_basic_template.sample.md) 참고.
