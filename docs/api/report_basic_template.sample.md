# 데이터 기초 분석 보고서 — 정규화 계약 + 샘플
---
## 1. 요청

```
POST /projects/{project_id}/reports/from_template
Content-Type: application/json

{
  "template_id": "unstructured_basic_v1",
  "dataset_version_id": "ver_abc"
}
```

- `dataset_version_id`는 **clean ready**여야 함. 아니면 `400`(`clean_not_ready`).
- 섹션은 해당 버전에서 **build artifact가 ready인 것만** 생성. 나머지는 `missing_sections`.

---

## 1. 구조

```
report → blocks[] (흰 카드)
         └─ layout[] (행 row)
            └─ panels[] = { view, width, title?, value_format?, data }   ← 자급자족
```
- **패널이 data를 자체 보유** → view별 data 달라도 됨(한 카드에 서로 다른 data/build 혼합 OK).
- `width ∈ "full" | "3/4" | "2/3" | "1/2" | "1/3" | "1/4"`. 한 행 넘으면 다음 줄.
- 블록 optional `scope_label`(예: `"2025년 기준"`) — 템플릿 섹션의 `date_filter:"recent_year"`로
  최신년도 데이터만 집계된 섹션에 붙는다(개요 섹션·날짜 없는 데이터셋엔 없음). 프론트는 제목 옆 배지로 표시.

## 2. 값 표현 = `format` 태그 (템플릿 결정)

```
count → "5,131"(+unit)   percent → "57.4%"   ratio → "0.57"
number → "8940"          code → 모노          text → 그대로(+sub)
```

## 3. view ↔ data 모양

| view | data | 쓰임 (디자인) |
|---|---|---|
| `stat_grid` | `{ items:[{key,label, value, format, unit?, sub?}] }` | 분석개요·문서개요 |
| `bar` / `doughnut` | `{ total, items:[{key,label, count, percent}] }` | 채널/유형(bar), 감성(doughnut) |
| `table` | 위 분포 data를 표로 (또는 `{columns,rows}`) | 차트 옆 표 |
| `stacked_bar` | `{ categories:[{key,label,total}], series:[{key,label, counts[], percents[]}] }` | 유형별 감성(100% 누적) |
| `rank` | `{ items:[{rank,label, value}] }` | 유형별 긍정/부정 순위, 키워드 |
| `text` | `{ markdown }` | 설명 |

- `bar`= 한 방향 막대(값 1개). `rank`= 순위 1열. (좌우 양방향 `diverging`은 이 디자인 미사용 — 필요 시 후속 추가.)
- 색은 프론트가 `key`로 매핑. 분포는 count·percent 둘 다 실어 줌(주 축=`value_format`).

---

## 4. 응답 샘플 (festival_sns 실값)

> 디자인 §2~§8 순서. **채널(§4)은 Phase B**(채널 컬럼 집계 백엔드 필요) — 형태 보여주려 포함.

```jsonc
{
  "report": {
    "report_id":"rpt_sample", "project_id":"proj_festival",
    "dataset_version_id":"ver_festival_v3", "title":"데이터 기초 분석 보고서",
    "blocks": [

      { "block_id":"b1", "section_id":"analysis_overview", "title":"분석 개요",
        "layout":[ {"panels":[ {"view":"stat_grid","width":"full","data":{ "items":[
          {"key":"dataset","label":"데이터셋","value":"festival_sns","format":"text","sub":"v3"},
          {"key":"period","label":"분석 기간","value":"2023.01 ~ 2025.12","format":"text","sub":"3년"},
          {"key":"model","label":"분석 모델","value":"LLOA-MAX","format":"code"},
          {"key":"unit","label":"분석 단위","value":"문서 · 절(clause)","format":"text"},
          {"key":"steps","label":"전처리 단계","value":"정제 · 진성 · 절 라벨링","format":"text"}
        ]}} ]} ]
      },

      { "block_id":"b2", "section_id":"doc_overview", "title":"문서 개요",
        "layout":[ {"panels":[ {"view":"stat_grid","width":"full","data":{ "items":[
          {"key":"genuine_docs","label":"진성 문서수","value":1182,"format":"count","unit":"건"},
          {"key":"genuine_docs","label":"진성 문서수","value":395,"format":"count","unit":"건"},
          {"key":"clauses","label":"절(clause) 수","value":8940,"format":"count","unit":"개"},
          {"key":"clauses","label":"절(clause) 수","value":3072,"format":"count","unit":"개"}
        ]}} ]} ]
      },

      { "block_id":"b3", "section_id":"channel_distribution", "title":"채널별 진성 문서 분포",
        "unit_basis":"doc",
        "layout":[ {"panels":[
          {"view":"bar","width":"2/3","value_format":"count","data":{ "total":1182, "items":[
            {"key":"insta","label":"인스타그램","count":408,"percent":34.5},
            {"key":"blog","label":"블로그","count":291,"percent":24.6},
            {"key":"news","label":"뉴스","count":214,"percent":18.1},
            {"key":"comm","label":"커뮤니티","count":169,"percent":14.3},
            {"key":"youtube","label":"유튜브","count":100,"percent":8.5}
          ]}},
          {"view":"table","width":"1/3","data":{ "total":1182, "items":[
            {"key":"insta","label":"인스타그램","count":408,"percent":34.5},
            {"key":"blog","label":"블로그","count":291,"percent":24.6},
            {"key":"news","label":"뉴스","count":214,"percent":18.1},
            {"key":"comm","label":"커뮤니티","count":169,"percent":14.3},
            {"key":"youtube","label":"유튜브","count":100,"percent":8.5}
          ]}}
        ]} ]
      },

      { "block_id":"b4", "section_id":"sentiment_distribution", "title":"절 단위 감성 분포",
        "unit_basis":"clause",
        "layout":[ {"panels":[
          {"view":"doughnut","width":"2/3","value_format":"percent","data":{ "total":8940, "items":[
            {"key":"positive","label":"긍정","count":5131,"percent":57.4},
            {"key":"neutral","label":"중립","count":1592,"percent":17.8},
            {"key":"negative","label":"부정","count":2217,"percent":24.8}
          ]}},
          {"view":"table","width":"1/3","data":{ "total":8940, "items":[
            {"key":"positive","label":"긍정","count":5131,"percent":57.4},
            {"key":"neutral","label":"중립","count":1592,"percent":17.8},
            {"key":"negative","label":"부정","count":2217,"percent":24.8}
          ]}}
        ]} ]
      },

      { "block_id":"b5", "section_id":"aspect_distribution", "title":"유형별 절 분포",
        "unit_basis":"clause",
        "layout":[ {"panels":[
          {"view":"bar","width":"2/3","value_format":"count","data":{ "total":8940, "items":[
            {"key":"show_program","label":"공연/프로그램","count":2166,"percent":24.2},
            {"key":"ambiance_scenery","label":"분위기/경관","count":1690,"percent":18.9},
            {"key":"operation_service","label":"운영/서비스","count":1502,"percent":16.8},
            {"key":"facility_crowd","label":"편의시설/혼잡도","count":1296,"percent":14.5},
            {"key":"food","label":"음식/먹거리","count":1198,"percent":13.4},
            {"key":"access_traffic","label":"교통/접근성","count":1088,"percent":12.2}
          ]}},
          {"view":"table","width":"1/3","data":{ "total":8940, "items":[
            {"key":"show_program","label":"공연/프로그램","count":2166,"percent":24.2},
            {"key":"ambiance_scenery","label":"분위기/경관","count":1690,"percent":18.9},
            {"key":"operation_service","label":"운영/서비스","count":1502,"percent":16.8},
            {"key":"facility_crowd","label":"편의시설/혼잡도","count":1296,"percent":14.5},
            {"key":"food","label":"음식/먹거리","count":1198,"percent":13.4},
            {"key":"access_traffic","label":"교통/접근성","count":1088,"percent":12.2}
          ]}}
        ]} ]
      },

      { "block_id":"b6", "section_id":"aspect_sentiment", "title":"유형별 감성 구성·대비",
        "unit_basis":"clause",
        "layout":[
          {"panels":[ {"view":"stacked_bar","width":"full","value_format":"percent","title":"긍정·부정 구성비 (100%)","data":{
            "categories":[
              {"key":"show_program","label":"공연/프로그램","total":2166},
              {"key":"ambiance_scenery","label":"분위기/경관","total":1690},
              {"key":"operation_service","label":"운영/서비스","total":1502},
              {"key":"facility_crowd","label":"편의시설/혼잡도","total":1296},
              {"key":"food","label":"음식/먹거리","total":1198},
              {"key":"access_traffic","label":"교통/접근성","total":1088}
            ],
            "series":[
              {"key":"positive","label":"긍정","counts":[1646,1352,541,311,563,218],"percents":[76,80,36,24,47,20]},
              {"key":"neutral", "label":"중립","counts":[282,203,255,194,252,141], "percents":[13,12,17,15,21,13]},
              {"key":"negative","label":"부정","counts":[238,135,706,791,383,729], "percents":[11,8,47,61,32,67]}
            ]}}
          ]},
          {"panels":[
            {"view":"rank","width":"1/2","value_format":"count","title":"긍정 많은 유형","data":{ "items":[
              {"rank":1,"label":"공연/프로그램","value":1646},{"rank":2,"label":"분위기/경관","value":1352},
              {"rank":3,"label":"음식/먹거리","value":563},{"rank":4,"label":"운영/서비스","value":541},
              {"rank":5,"label":"편의시설/혼잡도","value":311},{"rank":6,"label":"교통/접근성","value":218}
            ]}},
            {"view":"rank","width":"1/2","value_format":"count","title":"부정 많은 유형","data":{ "items":[
              {"rank":1,"label":"편의시설/혼잡도","value":791},{"rank":2,"label":"교통/접근성","value":729},
              {"rank":3,"label":"운영/서비스","value":706},{"rank":4,"label":"음식/먹거리","value":383},
              {"rank":5,"label":"공연/프로그램","value":238},{"rank":6,"label":"분위기/경관","value":135}
            ]}}
          ]}
        ]
      },

      { "block_id":"b7", "section_id":"keyword_distribution", "title":"감성별 상위 키워드",
        "unit_basis":"clause",
        "layout":[ {"panels":[
          {"view":"rank","width":"1/2","value_format":"count","title":"긍정 키워드","data":{ "items":[
            {"rank":1,"label":"공연","value":902},{"rank":2,"label":"분위기","value":831},
            {"rank":3,"label":"야경","value":712},{"rank":4,"label":"한복","value":588},
            {"rank":5,"label":"조명","value":521},{"rank":6,"label":"사진","value":463},
            {"rank":7,"label":"포토존","value":402},{"rank":8,"label":"가족","value":357},
            {"rank":9,"label":"추억","value":318},{"rank":10,"label":"재방문","value":276}
          ]}},
          {"view":"rank","width":"1/2","value_format":"count","title":"부정 키워드","data":{ "items":[
            {"rank":1,"label":"대기시간","value":642},{"rank":2,"label":"주차","value":571},
            {"rank":3,"label":"혼잡","value":498},{"rank":4,"label":"화장실","value":417},
            {"rank":5,"label":"셔틀버스","value":368},{"rank":6,"label":"가격","value":329},
            {"rank":7,"label":"안내부족","value":284},{"rank":8,"label":"소음","value":241},
            {"rank":9,"label":"대기열","value":203},{"rank":10,"label":"불친절","value":167}
          ]}}
        ]} ]
      }
    ],
    "created_at":"2026-06-24T06:00:00Z", "updated_at":"2026-06-24T06:00:00Z"
  },

  "included_sections":["analysis_overview","doc_overview","channel_distribution","sentiment_distribution",
                       "aspect_distribution","aspect_sentiment","keyword_distribution"],
  "missing_sections":[]
}
```

