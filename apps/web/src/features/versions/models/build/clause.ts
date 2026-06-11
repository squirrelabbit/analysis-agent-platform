import {
  mapProgress,
  type BuildBase,
  type BuildBaseDto,
  type PaginatedSummary,
  type PaginatedSummaryDto,
  type VersionBuild,
  type VersionBuildDto,
} from "./base";

// ── DTO ──────────────────────────────────────────────────────
export interface ClauseItemDto {
  aspect: string;
  clause: string;
  clause_id: string;
  doc_id: string;
  sentiment: string;
  source: string;
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
