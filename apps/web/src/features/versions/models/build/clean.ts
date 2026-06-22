import {
  mapProgress,
  type BuildBase,
  type BuildBaseDto,
  type VersionBuild,
  type VersionBuildDto,
} from "./base";

// ── DTO ──────────────────────────────────────────────────────
export interface CleanSummaryDto {
  clean_reduced_char_count?: number;
  cleaned_input_char_count?: number;
  dropped_count?: number;
  deduped_count?: number;
  input_row_count?: number;
  kept_count?: number;
  output_row_count?: number;
  source_input_char_count?: number;
  text_column?: string;
  text_columns?: string[];
}

export type CleanBuildResponse = BuildBaseDto<"clean", CleanSummaryDto>;
export type CleanVersionBuildDto = VersionBuildDto<CleanSummaryDto>;

// ── 모델 ─────────────────────────────────────────────────────
export interface CleanSummary {
  cleanReducedCharCount: number;
  cleanedInputCharCount: number;
  droppedCount: number;
  dedupedCount: number;
  inputRowCount: number;
  keptCount: number;
  outputRowCount: number;
  sourceInputCharCount: number;
  textColumn: string;
  textColumns: string[];
}

export type CleanBuild = BuildBase<"clean", CleanSummary>;
export type CleanVersionBuild = VersionBuild<CleanSummary>;

// ── 매퍼 ─────────────────────────────────────────────────────
export const mapCleanSummary = (dto: CleanSummaryDto): CleanSummary => ({
  cleanReducedCharCount: dto.clean_reduced_char_count ?? 0,
  cleanedInputCharCount: dto.cleaned_input_char_count ?? 0,
  droppedCount: dto.dropped_count ?? 0,
  dedupedCount: dto.deduped_count ?? 0,
  inputRowCount: dto.input_row_count ?? 0,
  keptCount: dto.kept_count ?? 0,
  outputRowCount: dto.output_row_count ?? 0,
  sourceInputCharCount: dto.source_input_char_count ?? 0,
  textColumn: dto.text_column ?? "",
  textColumns: dto.text_columns ?? [],
});

export const mapCleanBuild = (dto: CleanBuildResponse): CleanBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at ?? "",
  completedAt: dto.completed_at ?? "",
  durationSeconds: dto.duration_seconds ?? 0,
  errorMessage: dto.error_message ?? "",
  progress: dto.progress ? mapProgress(dto.progress) : undefined,
  summary: dto.summary ? mapCleanSummary(dto.summary) : undefined,
});

export const mapCleanVersionBuild = (
  dto: CleanVersionBuildDto,
): CleanVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at ?? "",
  summary: dto.summary ? mapCleanSummary(dto.summary) : undefined,
});
