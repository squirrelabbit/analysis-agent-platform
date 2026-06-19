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
// 감성별 키워드 raw. 백엔드가 count/weight를 문자열로 보내므로 mapper에서 number 변환.
export interface KeywordSentimentDto {
  keyword: string;
  count: string | number;
  weight: string | number;
}

export interface KeywordItemDto {
  keyword: string;
  count: number;
  document_count: number;
  dominant_sentiment: string;
  dominant_sentiment_ratio: number;
  top_aspect: string;
  representative_clause: string;
}

export interface KeywordSummaryDto {
  total_keyword_count: number;
  unique_keyword_count: number;
  clause_count: number;

  aspect: Record<string, number>;
  sentiment: {
    positive?: number;
    negative?: number;
    neutral?: number;
  };

  top_keywords_positive: Record<string, string>[];
  top_keywords_negative: Record<string, string>[];

  aspect_sentiment_keywords: Record<
    string,
    { positive: KeywordSentimentDto[]; negative: KeywordSentimentDto[] }
  >;

  selected_aspect: string;
  selected_aspect_total: string;
  selected_aspect_sentiment: Record<string, number>;
}

export type KeywordBuildResponse = BuildBaseDto<
  "clause_keywords",
  KeywordSummaryDto
> &
  PaginatedSummaryDto<KeywordItemDto>;
export type KeywordVersionBuildDto = VersionBuildDto<KeywordSummaryDto>;

// ── 모델 ─────────────────────────────────────────────────────
// 감성별 키워드 한 건 (긍/부정 랭킹·aspect별 키워드 공용).
export interface KeywordSentiment {
  keyword: string;
  count: number;
  weight: number;
}

export interface KeywordItem {
  keyword: string;
  count: number;
  documentCount: number;
  dominantSentiment: string;
  dominantSentimentRatio: number;
  topAspect: string;
  representativeClause: string;
}

// 선택 aspect의 긍/부정 키워드 묶음.
export interface AspectSentimentKeywords {
  positive: KeywordSentiment[];
  negative: KeywordSentiment[];
}

export interface KeywordSummary {
  totalKeywordCount: number;
  uniqueKeywordCount: number;
  clauseCount: number;
  // aspect key(snake_case, taxonomy 기반) → 건수. ClauseSummary.aspect와 동일 기준.
  aspect: Record<string, number>;
  sentiment: {
    positive: number;
    negative: number;
    neutral: number;
  };
  topKeywordsPositive: Record<string, string>[];
  topKeywordsNegative: Record<string, string>[];
  // aspect key → 해당 aspect의 긍/부정 키워드.
  aspectSentimentKeywords: Record<string, AspectSentimentKeywords>;
  selectedAspect: string;
  selectedAspectTotal: string;
  selectedAspectSentiment: Record<string, number>;
}

export type KeywordBuild = BuildBase<"clause_keywords", KeywordSummary> &
  PaginatedSummary<KeywordItem>;
export type KeywordVersionBuild = VersionBuild<KeywordSummary>;

// ── 매퍼 ─────────────────────────────────────────────────────
// 백엔드가 count/weight를 문자열로 보내므로 number로 변환.
const mapKeywordSentiment = (dto: KeywordSentimentDto): KeywordSentiment => ({
  keyword: dto.keyword,
  count: Number(dto.count ?? 0),
  weight: Number(dto.weight ?? 0),
});

const mapKeywordItem = (dto: KeywordItemDto): KeywordItem => ({
  keyword: dto.keyword,
  count: Number(dto.count ?? 0),
  documentCount: Number(dto.document_count ?? 0),
  dominantSentiment: dto.dominant_sentiment,
  dominantSentimentRatio: Number(dto.dominant_sentiment_ratio ?? 0),
  topAspect: dto.top_aspect,
  representativeClause: dto.representative_clause,
});

export const mapKeywordSummary = (dto: KeywordSummaryDto): KeywordSummary => ({
  totalKeywordCount: Number(dto.total_keyword_count ?? 0),
  uniqueKeywordCount: Number(dto.unique_keyword_count ?? 0),
  clauseCount: Number(dto.clause_count ?? 0),
  // aspect key는 taxonomy(config) 기반이므로 snake_case 그대로 보존 (ClauseSummary와 동일).
  aspect: Object.fromEntries(
    Object.entries(dto.aspect ?? {}).map(([key, value]) => [
      key,
      Number(value ?? 0),
    ]),
  ),
  sentiment: {
    positive: Number(dto.sentiment?.positive ?? 0),
    negative: Number(dto.sentiment?.negative ?? 0),
    neutral: Number(dto.sentiment?.neutral ?? 0),
  },
  topKeywordsPositive: dto.top_keywords_positive ?? [],
  topKeywordsNegative: dto.top_keywords_negative ?? [],
  aspectSentimentKeywords: Object.fromEntries(
    Object.entries(dto.aspect_sentiment_keywords ?? {}).map(([key, group]) => [
      key,
      {
        positive: (group.positive ?? []).map(mapKeywordSentiment),
        negative: (group.negative ?? []).map(mapKeywordSentiment),
      },
    ]),
  ),
  selectedAspect: dto.selected_aspect ?? "",
  selectedAspectTotal: dto.selected_aspect_total ?? "",
  selectedAspectSentiment: Object.fromEntries(
    Object.entries(dto.selected_aspect_sentiment ?? {}).map(([key, value]) => [
      key,
      Number(value ?? 0),
    ]),
  ),
});

// ── 절 중심(group=clause) view: "절에서 추출된 키워드" 표용 ────────────────
// GET clause_keywords?group=clause → items가 {clause, keywords[]}.
export interface KeywordClauseRow {
  clause: string;
  keywords: string[];
}
export interface KeywordClauseView {
  status?: string;
  items: KeywordClauseRow[];
  total: number;
}
export const mapKeywordClauseView = (dto: {
  status?: string;
  items?: { clause?: string; keywords?: string[] }[];
  pagination?: { total?: number };
}): KeywordClauseView => ({
  status: dto?.status,
  items: (dto?.items ?? []).map((r) => ({
    clause: String(r?.clause ?? ""),
    keywords: Array.isArray(r?.keywords) ? r.keywords.map(String) : [],
  })),
  total: dto?.pagination?.total ?? 0,
});

export const mapKeywordBuild = (dto: KeywordBuildResponse): KeywordBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at ?? "",
  completedAt: dto.completed_at ?? "",
  durationSeconds: dto.duration_seconds ?? 0,
  errorMessage: dto.error_message ?? "",
  progress: dto.progress ? mapProgress(dto.progress) : undefined,
  summary: dto.summary ? mapKeywordSummary(dto.summary) : undefined,
  pagination: dto.pagination,
  applied: dto.applied ?? {},
  items: dto.items?.map(mapKeywordItem) ?? [],
});

export const mapKeywordVersionBuild = (
  dto: KeywordVersionBuildDto,
): KeywordVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at ?? "",
  summary: dto.summary ? mapKeywordSummary(dto.summary) : undefined,
});
