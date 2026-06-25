FROM golang:1.25-bookworm AS build

WORKDIR /src

COPY apps/control-plane/go.mod apps/control-plane/go.sum ./
RUN go mod download

COPY apps/control-plane/ ./

RUN go build -o /out/server ./cmd/server
RUN go build -o /out/temporal-worker ./cmd/temporal-worker

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/temporal-worker /usr/local/bin/temporal-worker
COPY docs/api/openapi.yaml /app/docs/api/openapi.yaml
# ADR-017 (2026-05-19) — internal task routing 카탈로그 (planner와 분리).
# δ-4 (2026-05-21)로 skill_bundle.json은 삭제됐다 — plan은 planner_v2가
# LLM으로 생성하므로 고정 카탈로그가 필요하지 않다.
COPY config/task_registry.json /app/config/task_registry.json
# silverone 2026-06-12 — 전처리 빌드 모델 선택 allowlist (config/*.json 패턴).
COPY config/lloa_models.json /app/config/lloa_models.json
# 보고서 기본 템플릿 정의(2026-06-24). report 엔진이 로드 — 누락 시 unknown template_id.
COPY config/report_templates /app/config/report_templates
# 보고서 metric 별칭 사전(2026-06-25). 템플릿이 metric 이름으로 build/path를 참조.
COPY config/report_metrics.json /app/config/report_metrics.json
# report 엔진의 aspect key→한글 라벨 조회용(없으면 key로 fallback).
COPY config/taxonomies /app/config/taxonomies

EXPOSE 8080

CMD ["/usr/local/bin/server"]
