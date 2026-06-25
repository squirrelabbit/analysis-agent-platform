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
  // 검색어/대상명에서 유래한 키워드 (추천 제외어). 자동 제외 아님 — 운영자가 [제외]로 승인.
  suggested_exclude?: boolean;
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
  // 검색어/대상명 유래 추천 제외어 (자동 제외 아님).
  suggestedExclude: boolean;
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
  suggestedExclude: Boolean(dto.suggested_exclude),
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
  // 같은 절 텍스트가 등장한 횟수(리포스트 dedup). 1이면 단일.
  occurrenceCount: number;
}
export interface KeywordClauseView {
  status?: string;
  items: KeywordClauseRow[];
  total: number;
}
export const mapKeywordClauseView = (dto: {
  status?: string;
  items?: { clause?: string; keywords?: string[]; occurrence_count?: number | string }[];
  pagination?: { total?: number };
}): KeywordClauseView => ({
  status: dto?.status,
  items: (dto?.items ?? []).map((r) => ({
    clause: String(r?.clause ?? ""),
    keywords: Array.isArray(r?.keywords) ? r.keywords.map(String) : [],
    occurrenceCount: Number(r?.occurrence_count ?? 1),
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

// ── 키워드 정제 사전 (silverone 2026-06-25) ────────────────────────────────
// dataset 단위 정제 규칙(제외=block, 대표어 지정=synonym) + append-only 이력.
export interface KeywordDictionaryRuleDto {
  id: string;
  rule_type: "block" | "synonym";
  source_term: string;
  target_term?: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

export interface KeywordDictionaryRule {
  id: string;
  ruleType: "block" | "synonym";
  sourceTerm: string;
  targetTerm: string;
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

export const mapKeywordDictionaryRule = (
  dto: KeywordDictionaryRuleDto,
): KeywordDictionaryRule => ({
  id: dto.id,
  ruleType: dto.rule_type,
  sourceTerm: dto.source_term,
  targetTerm: dto.target_term ?? "",
  active: dto.active,
  createdAt: dto.created_at,
  updatedAt: dto.updated_at,
});

export interface KeywordDictionaryEventDto {
  id: string;
  rule_id: string;
  event_type: string;
  before_payload?: string;
  after_payload?: string;
  reason?: string;
  actor_id?: string;
  created_at: string;
}

// payload(JSON 문자열)에서 표시용으로 뽑은 규칙 요약.
export interface KeywordRulePayload {
  ruleType?: string;
  sourceTerm?: string;
  targetTerm?: string;
  active?: boolean;
}

export interface KeywordDictionaryEvent {
  id: string;
  ruleId: string;
  eventType: string;
  before?: KeywordRulePayload;
  after?: KeywordRulePayload;
  reason: string;
  actorId: string;
  createdAt: string;
}

const parseRulePayload = (raw?: string): KeywordRulePayload | undefined => {
  if (!raw) return undefined;
  try {
    const o = JSON.parse(raw) as Record<string, unknown>;
    return {
      ruleType: o.rule_type as string | undefined,
      sourceTerm: o.source_term as string | undefined,
      targetTerm: o.target_term as string | undefined,
      active: o.active as boolean | undefined,
    };
  } catch {
    return undefined;
  }
};

export const mapKeywordDictionaryEvent = (
  dto: KeywordDictionaryEventDto,
): KeywordDictionaryEvent => ({
  id: dto.id,
  ruleId: dto.rule_id,
  eventType: dto.event_type,
  before: parseRulePayload(dto.before_payload),
  after: parseRulePayload(dto.after_payload),
  reason: dto.reason ?? "",
  actorId: dto.actor_id ?? "",
  createdAt: dto.created_at,
});

// 규칙 생성/수정 요청 body.
export interface KeywordDictionaryRuleRequest {
  rule_type: "block" | "synonym";
  source_term: string;
  target_term?: string;
  reason?: string;
}
