import {
  mapProgress,
  type BuildBase,
  type BuildBaseDto,
  type PaginatedSummary,
  type PaginatedSummaryDto,
  type VersionBuild,
  type VersionBuildDto,
} from "./base";

// 교차검증(verify, ADR-028) classify 모델 1개의 절 결과 snapshot.
export interface ClauseModelResult {
  relevant?: boolean;
  sentiment?: string;
  aspects?: string[];
}
// judge 결과 — model result + 해소 사유.
export interface ClauseJudgeResult extends ClauseModelResult {
  reason?: string;
}

// ── DTO ──────────────────────────────────────────────────────
export interface ClauseItemDto {
  aspect: string;
  clause: string;
  clause_id: string;
  doc_id: string;
  sentiment: string;
  source: string;
  // silverone 2026-06-11 — 수동 보정 overlay. aspect/sentiment는 effective 값,
  // 아래는 원본/보정 구분용(보정된 행에만).
  original_aspect?: string;
  original_sentiment?: string;
  override_aspect?: string;
  override_sentiment?: string;
  is_overridden?: boolean;
  // 교차검증(verify, ADR-028) 모드 행에만.
  resolution?: string;
  needs_review?: boolean;
  sentence_index?: number | null;
  chunk_index?: number | null;
  model_a_result?: ClauseModelResult | null;
  model_b_result?: ClauseModelResult | null;
  judge_result?: ClauseJudgeResult | null;
}

export interface ClauseSummaryDto {
  aspect: Record<string, number>;
  aspect_sentiment: Record<string, AspectSentiment>;
  sentiment: {
    positive?: number;
    negative?: number;
    neutral?: number;
  };
  total: number;
  // 교차검증(verify) 집계.
  mode?: string;
  resolution?: Record<string, number>;
  resolution_counts?: Record<string, number>;
  models?: { a?: string; b?: string; judge?: string };
}

export type ClauseBuildResponse = BuildBaseDto<
  "clause_label",
  ClauseSummaryDto
> &
  PaginatedSummaryDto<ClauseItemDto>;
export type ClauseVersionBuildDto = VersionBuildDto<ClauseSummaryDto>;

// ── 모델 ─────────────────────────────────────────────────────
export interface ClauseItem {
  aspect: string;
  clause: string;
  clauseId: string;
  docId: string;
  sentiment: string;
  source: string;
  // silverone 2026-06-11 — 수동 보정 overlay (보정된 행에만).
  originalAspect?: string;
  originalSentiment?: string;
  overrideAspect?: string;
  overrideSentiment?: string;
  isOverridden?: boolean;
  // 교차검증(verify) 모드 (ADR-028).
  resolution?: string;
  needsReview?: boolean;
  sentenceIndex?: number | null;
  chunkIndex?: number | null;
  modelAResult?: ClauseModelResult | null;
  modelBResult?: ClauseModelResult | null;
  judgeResult?: ClauseJudgeResult | null;
}

export interface SentimentCount {
  count: number;
  percent: number;
}

// aspect_sentiment는 camelCase 변환 없이 그대로 통과하므로 DTO·모델 공용 타입.
export interface AspectSentiment {
  sentiment: {
    positive: SentimentCount;
    negative: SentimentCount;
    neutral: SentimentCount;
  };
  total: number;
}

export interface ClauseSummary {
  // aspect key(snake_case, taxonomy 기반) → 건수. key 집합은 taxonomy config에
  // 따라 달라지므로 고정 필드가 아닌 동적 맵으로 둔다.
  aspect: Record<string, number>;
  aspectSentiment: Record<string, AspectSentiment>;
  sentiment: {
    positive: number;
    negative: number;
    neutral: number;
  };
  total: number;
  // 교차검증(verify) 집계 (ADR-028).
  mode?: string;
  resolution?: Record<string, number>;
  resolutionCounts?: Record<string, number>;
  models?: { a?: string; b?: string; judge?: string };
}

export type ClauseBuild = BuildBase<"clause_label", ClauseSummary> &
  PaginatedSummary<ClauseItem>;
export type ClauseVersionBuild = VersionBuild<ClauseSummary>;

// ── 매퍼 ─────────────────────────────────────────────────────
const mapClauseItem = (dto: ClauseItemDto): ClauseItem => ({
  aspect: dto.aspect,
  clause: dto.clause,
  clauseId: dto.clause_id,
  docId: dto.doc_id,
  sentiment: dto.sentiment,
  source: dto.source,
  originalAspect: dto.original_aspect,
  originalSentiment: dto.original_sentiment,
  overrideAspect: dto.override_aspect,
  overrideSentiment: dto.override_sentiment,
  isOverridden: dto.is_overridden,
  resolution: dto.resolution,
  needsReview: dto.needs_review,
  sentenceIndex: dto.sentence_index,
  chunkIndex: dto.chunk_index,
  modelAResult: dto.model_a_result,
  modelBResult: dto.model_b_result,
  judgeResult: dto.judge_result,
});

export const mapClauseSummary = (dto: ClauseSummaryDto): ClauseSummary => ({
  // aspect key는 taxonomy(config) 기반이므로 snake_case 그대로 보존한다.
  // item.aspect(raw key) / taxonomy aspectLabels와 동일 기준 → 한글 label 매핑.
  aspect: Object.fromEntries(
    Object.entries(dto.aspect ?? {}).map(([key, value]) => [key, value ?? 0]),
  ),
  aspectSentiment: dto.aspect_sentiment,
  sentiment: {
    positive: dto.sentiment?.positive ?? 0,
    negative: dto.sentiment?.negative ?? 0,
    neutral: dto.sentiment?.neutral ?? 0,
  },
  total: dto.total ?? 0,
  mode: dto.mode,
  resolution: dto.resolution,
  resolutionCounts: dto.resolution_counts,
  models: dto.models,
});

export const mapClauseLabelBuild = (dto: ClauseBuildResponse): ClauseBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at ?? "",
  completedAt: dto.completed_at ?? "",
  durationSeconds: dto.duration_seconds ?? 0,
  errorMessage: dto.error_message ?? "",
  progress: dto.progress ? mapProgress(dto.progress) : undefined,
  summary: dto.summary ? mapClauseSummary(dto.summary) : undefined,
  pagination: dto.pagination,
  applied: dto.applied ?? {},
  items: dto.items?.map(mapClauseItem) ?? [],
});

export const mapClauseLabelVersionBuild = (
  dto: ClauseVersionBuildDto,
): ClauseVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at,
  summary: dto.summary ? mapClauseSummary(dto.summary) : undefined,
});
