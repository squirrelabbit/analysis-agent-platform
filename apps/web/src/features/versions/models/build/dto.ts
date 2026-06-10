import type { Pagination } from "@/shared/models/common";
import type { BuildJobType } from "@/shared/types/common";
import type { AspectSentiment } from "./model";

// 감성별 키워드 raw. 백엔드가 count/weight를 문자열로 보내므로 mapper에서 number 변환.
export interface KeywordSentimentDto {
  keyword: string;
  count: string | number;
  weight: string | number;
}

export interface ProgressDto {
  percent: number;
  processed_rows: number;
  total_rows: number;
  eta_seconds?: number;
  message: string;
  updated_at: string;
}

export interface BuildBaseDto<TType extends BuildJobType, TSummary> {
  build_type: TType;
  status: string;
  job_id: string;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  error_message: string | null;
  progress?: ProgressDto;
  summary?: TSummary;
}

export interface PaginatedSummaryDto<T> {
  items: T[];
  pagination: Pagination;
  // model: 빌드 당시 raw model id snapshot. model_display_name: control-plane이 응답
  // 시점에 env로 입히는 화면 표시명(불일치/미설정 시 생략). 옛 응답엔 없을 수 있어 optional.
  applied: {
    prompt_version: string;
    model?: string;
    model_display_name?: string;
  };
}

export interface CleanSummaryDto {
  clean_reduced_char_count?: number;
  cleaned_input_char_count?: number;
  dropped_count?: number;
  input_row_count?: number;
  kept_count?: number;
  output_row_count?: number;
  source_input_char_count?: number;
  text_column?: string;
  text_columns?: string[];
}

export interface GenuinenessItemDto {
  doc_id: string;
  genuineness: string;
  reason: string;
  source: string;
  cleaned_text: string;
}

export interface GenuinenessSummaryDto {
  genuineness: {
    genuine_review?: number;
    non_review?: number;
    mixed?: number;
    uncertain?: number;
  };
  total: number;
}

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

export type CleanBuildResponse = BuildBaseDto<"clean", CleanSummaryDto>;
export type GenuinenessBuildResponse = BuildBaseDto<
  "doc_genuineness",
  GenuinenessSummaryDto
> &
  PaginatedSummaryDto<GenuinenessItemDto>;
export type ClauseBuildResponse = BuildBaseDto<
  "clause_label",
  ClauseSummaryDto
> &
  PaginatedSummaryDto<ClauseItemDto>;
export type KeywordBuildResponse = BuildBaseDto<
  "clause_keywords",
  KeywordSummaryDto
> &
  PaginatedSummaryDto<KeywordItemDto>;

export type BuildResponse =
  | CleanBuildResponse
  | GenuinenessBuildResponse
  | ClauseBuildResponse
  | KeywordBuildResponse;

export interface VersionBuildDto<T> {
  status: string;
  completed_at?: string;
  summary?: T;
}

export type CleanVersionBuildDto = VersionBuildDto<CleanSummaryDto>;
export type GenuinenessVersionBuildDto = VersionBuildDto<GenuinenessSummaryDto>;
export type ClauseVersionBuildDto = VersionBuildDto<ClauseSummaryDto>;
export type KeywordVersionBuildDto = VersionBuildDto<KeywordSummaryDto>;
