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

export interface PaginatedSummary<T> {
  items: T[];
  pagination: Pagination;
  // model: 빌드 당시 raw model id. modelDisplayName: 응답 시점 env 기반 화면 표시명.
  // 표시값은 modelDisplayName || model. 옛 응답엔 없을 수 있어 optional.
  applied: { promptVersion: string; model?: string; modelDisplayName?: string };
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

// applied snake_case → camelCase. raw model(snapshot)과 화면 표시명(응답 시점 env)을
// 함께 매핑한다. 옛 응답엔 model/model_display_name이 없을 수 있어 optional.
export const mapApplied = (dto?: {
  prompt_version?: string;
  model?: string;
  model_display_name?: string;
}): { promptVersion: string; model?: string; modelDisplayName?: string } => ({
  promptVersion: dto?.prompt_version ?? "",
  model: dto?.model,
  modelDisplayName: dto?.model_display_name,
});
