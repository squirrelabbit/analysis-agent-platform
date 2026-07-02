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
export interface GenuinenessItemDto {
  doc_id: string;
  genuineness: string;
  reason: string;
  source: string;
  cleaned_text: string;
  // 원문 URL — clean source_json에서 추출(전용 URL 컬럼). 없으면 빈 문자열/생략.
  source_url?: string;
  // silverone 2026-06-11 — 수동 보정 overlay. genuineness/reason은 effective 값,
  // 아래는 원본/보정 구분용(보정된 행에만).
  original_genuineness?: string;
  original_reason?: string;
  override_genuineness?: string;
  override_reason?: string;
  is_overridden?: boolean;
  // 교차검증(verify, ADR-026) 모드 행에만. genuineness=final_label(effective).
  final_label?: string | null;
  resolution?: string;
  needs_review?: boolean;
  is_disagreement?: boolean;
  model_a_result?: { genuineness?: string; reason?: string } | null;
  model_b_result?: { genuineness?: string; reason?: string } | null;
  judge_result?: {
    decision?: string;
    final_label?: string | null;
    confidence?: number;
    reason?: string;
    judge_model?: string;
  } | null;
}

export interface GenuinenessSummaryDto {
  genuineness: {
    genuine_review?: number;
    non_review?: number;
    uncertain?: number;
  };
  total: number;
  // silverone 2026-06-11 — 수동 보정 메타.
  override_count?: number;
  downstream_rerun_recommended?: boolean;
  // 교차검증(verify) 집계.
  mode?: string;
  agreement_count?: number;
  disagreement_count?: number;
  judge_count?: number;
  review_count?: number;
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
  // 원문 URL(전용 URL 컬럼). 없으면 빈 문자열.
  sourceUrl: string;
  // silverone 2026-06-11 — 수동 보정 overlay (보정된 행에만).
  originalGenuineness?: string;
  originalReason?: string;
  overrideGenuineness?: string;
  overrideReason?: string;
  isOverridden?: boolean;
  // 교차검증(verify) 모드 (ADR-026).
  finalLabel?: string | null;
  resolution?: string;
  needsReview?: boolean;
  isDisagreement?: boolean;
  modelAResult?: { genuineness?: string; reason?: string } | null;
  modelBResult?: { genuineness?: string; reason?: string } | null;
  judgeResult?: {
    decision?: string;
    finalLabel?: string | null;
    confidence?: number;
    reason?: string;
    judgeModel?: string;
  } | null;
}

export interface GenuinenessSummary {
  genuineness: {
    genuine_review: number;
    non_review: number;
    uncertain: number;
  };
  total: number;
  // silverone 2026-06-11 — 수동 보정 메타.
  overrideCount?: number;
  downstreamRerunRecommended?: boolean;
  // 교차검증(verify) 집계 (ADR-026).
  mode?: string;
  agreementCount?: number;
  disagreementCount?: number;
  judgeCount?: number;
  reviewCount?: number;
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
  sourceUrl: dto.source_url ?? "",
  originalGenuineness: dto.original_genuineness,
  originalReason: dto.original_reason,
  overrideGenuineness: dto.override_genuineness,
  overrideReason: dto.override_reason,
  isOverridden: dto.is_overridden,
  finalLabel: dto.final_label,
  resolution: dto.resolution,
  needsReview: dto.needs_review,
  isDisagreement: dto.is_disagreement,
  modelAResult: dto.model_a_result,
  modelBResult: dto.model_b_result,
  judgeResult: dto.judge_result
    ? {
        decision: dto.judge_result.decision,
        finalLabel: dto.judge_result.final_label,
        confidence: dto.judge_result.confidence,
        reason: dto.judge_result.reason,
        judgeModel: dto.judge_result.judge_model,
      }
    : dto.judge_result,
});

export const mapGenuinenessSummary = (
  dto: GenuinenessSummaryDto,
): GenuinenessSummary => ({
  genuineness: {
    genuine_review: dto.genuineness?.genuine_review ?? 0,
    non_review: dto.genuineness?.non_review ?? 0,
    uncertain: dto.genuineness?.uncertain ?? 0,
  },
  total: dto.total ?? 0,
  overrideCount: dto.override_count,
  downstreamRerunRecommended: dto.downstream_rerun_recommended,
  mode: dto.mode,
  agreementCount: dto.agreement_count,
  disagreementCount: dto.disagreement_count,
  judgeCount: dto.judge_count,
  reviewCount: dto.review_count,
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
  applied: dto.applied ?? {},
  items: dto.items?.map(mapGenuinenessItem) ?? [],
});

export const mapGenuinenessVersionBuild = (
  dto: GenuinenessVersionBuildDto,
): GenuinenessVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at ?? "",
  summary: dto.summary ? mapGenuinenessSummary(dto.summary) : undefined,
});
