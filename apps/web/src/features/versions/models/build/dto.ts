import type { Pagination } from "@/shared/models/common";
import type { BuildJobType } from "@/shared/types/common";


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
  applied: { prompt_version: string; model?: string; model_display_name?: string };
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
  aspect: {
    show_program?: number;
    experience_booth?: number;
    ambiance_scenery?: number;
    food?: number;
    price_cost?: number;
    facility_crowd?: number;
    access_traffic?: number;
    operation_service?: number;
    etc?: number;
  };
  sentiment: {
    positive?: number;
    negative?: number;
    neutral?: number;
  };
  total: number;
}

/**
| Aspect             | 설명 |
|--------------------|------|
| show_program       | 공연, 퍼레이드, 드론쇼, 불꽃놀이, 버스킹 등 관람형 콘텐츠에 대한 평가 |
| experience_booth   | 체험 부스, 만들기, 스탬프 투어 등 참여형 프로그램에 대한 평가 |
| ambiance_scenery   | 축제장의 분위기, 조명, 포토존, 야경 등 감성적 환경 요소에 대한 평가 | 
| food               | 음식 및 음료의 맛, 메뉴 구성, 다양성에 대한 평가 (가격 제외) |
| price_cost         | 음식, 체험, 기념품 등 축제 내 가격 및 가성비에 대한 평가 | 
| facility_crowd     | 화장실, 쉼터, 청결도, 인파, 혼잡도 등 편의시설 및 환경에 대한 평가 |
| access_traffic     | 교통, 주차, 셔틀버스 등 축제장 접근성에 대한 평가 | 
| operation_service  | 스태프, 안내, 운영 방식, 기획력, 결제 시스템 등 운영 전반에 대한 평가 |
| etc                | 축제와 관련되어 있으나 위 8개 항목에 해당하지 않는 것 | 
 */


/**
- positive : 긍정적 감정 및 평가
- negative : 부정적 감정 및 평가
- neutral  : 감정 없는 사실 서술
 */

export type CleanBuildResponse = BuildBaseDto<"clean", CleanSummaryDto>;
export type GenuinenessBuildResponse = BuildBaseDto<"doc_genuineness", GenuinenessSummaryDto> & PaginatedSummaryDto<GenuinenessItemDto>;
export type ClauseBuildResponse = BuildBaseDto<"clause_label", ClauseSummaryDto> & PaginatedSummaryDto<ClauseItemDto>;

export type BuildResponse = CleanBuildResponse | GenuinenessBuildResponse | ClauseBuildResponse;


export interface VersionBuildDto<T> {
  status: string,
  completed_at?: string,
  summary?: T
}

export type CleanVersionBuildDto = VersionBuildDto<CleanSummaryDto>;
export type GenuinenessVersionBuildDto = VersionBuildDto<GenuinenessSummaryDto>;
export type ClauseVersionBuildDto = VersionBuildDto<ClauseSummaryDto>;
