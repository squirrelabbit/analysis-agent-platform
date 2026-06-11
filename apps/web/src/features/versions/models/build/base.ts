import type { Pagination } from "@/shared/models/common";
import type { BuildJobType } from "@/shared/types/common";

// 모든 빌드 분석이 공유하는 공통 타입·매퍼. 분석별 파일(clean/genuineness/clause/keyword)이
// 이 base를 조합해 각자의 Summary/Item/Build를 정의한다.

// ── DTO (서버 raw, snake_case) ────────────────────────────────
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

// applied는 분석마다 키가 다르다(LLM: prompt_version/model/model_display_name,
// 키워드: extractor_version). 타입을 고정하지 않고 snake_case 그대로 통과시킨 뒤
// 화면(BuildMetaBar)에서 있는 키만 골라 보여준다.
export interface PaginatedSummaryDto<T> {
  items: T[];
  pagination: Pagination;
  applied: Record<string, string>;
}

export interface VersionBuildDto<T> {
  status: string;
  completed_at?: string;
  summary?: T;
}

// ── 도메인 모델 (camelCase) ──────────────────────────────────
export interface ProgressType {
  percent: number;
  processedRows: number;
  totalRows: number;
  etaSeconds?: number;
  message: string;
  updatedAt: string;
}

export interface BuildBase<TType extends BuildJobType, TSummary> {
  buildType: TType;
  status: string;
  jobId: string;
  startedAt: string;
  completedAt: string;
  durationSeconds: number;
  errorMessage: string;
  progress?: ProgressType;
  summary?: TSummary;
}

// applied는 DTO와 동일하게 snake_case Record로 통과시킨다(BuildMetaBar에서 키별 렌더).
export interface PaginatedSummary<T> {
  items: T[];
  pagination: Pagination;
  applied: Record<string, string>;
}

export interface VersionBuild<T> {
  status: string;
  completedAt?: string;
  summary?: T;
}

// ── 공통 매퍼 ────────────────────────────────────────────────
export const mapProgress = (dto: ProgressDto): ProgressType => ({
  percent: dto.percent ?? 0,
  processedRows: dto.processed_rows ?? 0,
  totalRows: dto.total_rows ?? 0,
  etaSeconds: dto.eta_seconds,
  message: dto.message ?? "",
  updatedAt: dto.updated_at,
});
