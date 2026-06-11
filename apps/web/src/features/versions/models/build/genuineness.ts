import {
  mapApplied,
  mapProgress,
  type BuildBase,
  type BuildBaseDto,
  type PaginatedSummary,
  type PaginatedSummaryDto,
  type VersionBuild,
  type VersionBuildDto,
} from "./base";

// ── DTO ──────────────────────────────────────────────────────
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

export type GenuinenessBuildResponse = BuildBaseDto<
  "doc_genuineness",
  GenuinenessSummaryDto
> &
  PaginatedSummaryDto<GenuinenessItemDto>;
export type GenuinenessVersionBuildDto =
  VersionBuildDto<GenuinenessSummaryDto>;

// ── 모델 ─────────────────────────────────────────────────────
export interface GenuinenessItem {
  docId: string;
  genuineness: string;
  reason: string;
  source: string;
  cleanedText: string;
}

export interface GenuinenessSummary {
  genuineness: {
    genuine_review: number;
    non_review: number;
    uncertain: number;
    mixed: number;
  };
  total: number;
}

export type GenuinenessBuild = BuildBase<
  "doc_genuineness",
  GenuinenessSummary
> &
  PaginatedSummary<GenuinenessItem>;
export type GenuinenessVersionBuild = VersionBuild<GenuinenessSummary>;

// ── 매퍼 ─────────────────────────────────────────────────────
const mapGenuinenessItem = (dto: GenuinenessItemDto): GenuinenessItem => ({
  docId: dto.doc_id,
  genuineness: dto.genuineness,
  reason: dto.reason,
  source: dto.source,
  cleanedText: dto.cleaned_text,
});

export const mapGenuinenessSummary = (
  dto: GenuinenessSummaryDto,
): GenuinenessSummary => ({
  genuineness: {
    genuine_review: dto.genuineness?.genuine_review ?? 0,
    non_review: dto.genuineness?.non_review ?? 0,
    mixed: dto.genuineness?.mixed ?? 0,
    uncertain: dto.genuineness?.uncertain ?? 0,
  },
  total: dto.total ?? 0,
});

export const mapGenuinenessBuild = (
  dto: GenuinenessBuildResponse,
): GenuinenessBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at ?? "",
  completedAt: dto.completed_at ?? "",
  durationSeconds: dto.duration_seconds ?? 0,
  errorMessage: dto.error_message ?? "",
  progress: dto.progress ? mapProgress(dto.progress) : undefined,
  summary: dto.summary ? mapGenuinenessSummary(dto.summary) : undefined,
  pagination: dto.pagination,
  applied: mapApplied(dto.applied),
  items: dto.items?.map(mapGenuinenessItem) ?? [],
});

export const mapGenuinenessVersionBuild = (
  dto: GenuinenessVersionBuildDto,
): GenuinenessVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at ?? "",
  summary: dto.summary ? mapGenuinenessSummary(dto.summary) : undefined,
});
