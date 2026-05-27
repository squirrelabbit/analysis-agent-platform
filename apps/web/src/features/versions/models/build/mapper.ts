import type {
  BuildResponse,
  ClauseBuildResponse,
  ClauseItemDto,
  ClauseSummaryDto,
  ClauseVersionBuildDto,
  CleanBuildResponse,
  CleanSummaryDto,
  CleanVersionBuildDto,
  GenuinenessBuildResponse,
  GenuinenessItemDto,
  GenuinenessSummaryDto,
  GenuinenessVersionBuildDto,
  ProgressDto,
} from "./dto";
import type {
  Build,
  ClauseBuild,
  ClauseItem,
  ClauseSummary,
  ClauseVersionBuild,
  CleanBuild,
  CleanSummary,
  CleanVersionBuild,
  GenuinenessBuild,
  GenuinenessItem,
  GenuinenessSummary,
  GenuinenessVersionBuild,
  ProgressType,
} from "./model";

export const mapProgress = (dto: ProgressDto): ProgressType => ({
  percent: dto.percent ?? 0,
  processedRows: dto.processed_rows ?? 0,
  totalRows: dto.total_rows ?? 0,
  message: dto.message ?? "",
  updatedAt: dto.updated_at,
});

export const mapCleanSummary = (dto: CleanSummaryDto): CleanSummary => ({
  cleanReducedCharCount: dto.clean_reduced_char_count ?? 0,
  cleanedInputCharCount: dto.cleaned_input_char_count ?? 0,
  droppedCount: dto.dropped_count ?? 0,
  inputRowCount: dto.input_row_count ?? 0,
  keptCount: dto.kept_count ?? 0,
  outputRowCount: dto.output_row_count ?? 0,
  sourceInputCharCount: dto.source_input_char_count ?? 0,
  textColumn: dto.text_column ?? "",
  textColumns: dto.text_columns ?? [],
});

const mapGenuinenessItem = (dto: GenuinenessItemDto): GenuinenessItem => ({
  docId: dto.doc_id,
  genuineness: dto.genuineness,
  reason: dto.reason,
  source: dto.source,
});

export const mapGenuinenessSummary = (
  dto: GenuinenessSummaryDto,
): GenuinenessSummary => ({
  // items:  [],
  // pagination:{},
  // applied: { promptVersion: dto.applied.prompt_version ?? "" },
  genuineness: {
    genuineReview: dto.genuineness.genuine_review ?? 0,
    nonReview: dto.genuineness.non_review ?? 0,
    mixed: dto.genuineness.mixed ?? 0,
    uncertain: dto.genuineness.uncertain ?? 0,
  },
  total: dto.total ?? 0,
});

const mapClauseItem = (dto: ClauseItemDto): ClauseItem => ({
  aspect: dto.aspect,
  clause: dto.clause,
  clauseId: dto.clause_id,
  docId: dto.doc_id,
  sentiment: dto.sentiment,
  source: dto.source,
});

export const mapClauseSummary = (dto: ClauseSummaryDto): ClauseSummary => ({
  aspect: {
    showProgram: dto.aspect.show_program ?? 0,
    experienceBooth: dto.aspect.experience_booth ?? 0,
    ambianceScenery: dto.aspect.ambiance_scenery ?? 0,
    food: dto.aspect.food ?? 0,
    priceCost: dto.aspect.price_cost ?? 0,
    facilityCrowd: dto.aspect.facility_crowd ?? 0,
    accessTraffic: dto.aspect.access_traffic ?? 0,
    operationService: dto.aspect.operation_service ?? 0,
    etc: dto.aspect.etc ?? 0,
  },
  sentiment: {
    positive: dto.sentiment.positive ?? 0,
    negative: dto.sentiment.negative ?? 0,
    neutral: dto.sentiment.neutral ?? 0,
  },
  total: dto.total ?? 0,
});

export const mapCleanBuild = (dto: CleanBuildResponse): CleanBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at,
  completedAt: dto.completed_at,
  durationSeconds: dto.duration_seconds,
  errorMessage: dto.error_message,
  progress: mapProgress(dto.progress),
  summary: mapCleanSummary(dto.summary),
});

export const mapGenuinenessBuild = (
  dto: GenuinenessBuildResponse,
): GenuinenessBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at,
  completedAt: dto.completed_at,
  durationSeconds: dto.duration_seconds,
  errorMessage: dto.error_message,
  progress: mapProgress(dto.progress),
  summary: mapGenuinenessSummary(dto.summary),
  pagination: dto.pagination,
  applied: { promptVersion: dto.applied.prompt_version ?? "" },
  items: dto.items.map(mapGenuinenessItem),
});

export const mapClauseLabelBuild = (
  dto: ClauseBuildResponse,
): ClauseBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at,
  completedAt: dto.completed_at,
  durationSeconds: dto.duration_seconds,
  errorMessage: dto.error_message,
  progress: mapProgress(dto.progress),
  summary: mapClauseSummary(dto.summary),
  pagination: dto.pagination,
  applied: { promptVersion: dto.applied.prompt_version ?? "" },
  items: dto.items.map(mapClauseItem),
});

export const mapBuild = (dto: BuildResponse): Build => {
  switch (dto.build_type) {
    case "clean":
      return mapCleanBuild(dto);
    case "doc_genuineness":
      return mapGenuinenessBuild(dto);
    case "clause_label":
      return mapClauseLabelBuild(dto);
  }
};

export const mapCleanVersionBuild = (
  dto: CleanVersionBuildDto,
): CleanVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at ?? "",
  summary: dto.summary ? mapCleanSummary(dto.summary) : undefined,
});

export const mapGenuinenessVersionBuild = (
  dto: GenuinenessVersionBuildDto,
): GenuinenessVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at ?? "",
  summary: dto.summary ? mapGenuinenessSummary(dto.summary) : undefined,
});

export const mapClauseLabelVersionBuild = (
  dto: ClauseVersionBuildDto,
): ClauseVersionBuild => ({
  status: dto.status,
  completedAt: dto.completed_at,
  summary: dto.summary ? mapClauseSummary(dto.summary) : undefined,
});
