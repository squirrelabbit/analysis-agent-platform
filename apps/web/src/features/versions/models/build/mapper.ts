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
  etaSeconds: dto.eta_seconds,
  message: dto.message ?? "",
  updatedAt: dto.updated_at,
});

// applied snake_case → camelCase. raw model(snapshot)과 화면 표시명(응답 시점 env)을
// 함께 매핑한다. 옛 응답엔 model/model_display_name이 없을 수 있어 optional.
const mapApplied = (
  dto?: { prompt_version?: string; model?: string; model_display_name?: string },
): { promptVersion: string; model?: string; modelDisplayName?: string } => ({
  promptVersion: dto?.prompt_version ?? "",
  model: dto?.model,
  modelDisplayName: dto?.model_display_name,
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
  cleanedText: dto.cleaned_text,
});

export const mapGenuinenessSummary = (
  dto: GenuinenessSummaryDto,
): GenuinenessSummary => ({
  // items:  [],
  // pagination:{},
  // applied: { promptVersion: dto.applied.prompt_version ?? "" },
  genuineness: {
    genuineReview: dto.genuineness?.genuine_review ?? 0,
    nonReview: dto.genuineness?.non_review ?? 0,
    mixed: dto.genuineness?.mixed ?? 0,
    uncertain: dto.genuineness?.uncertain ?? 0,
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
  // aspect key는 taxonomy(config) 기반이므로 snake_case 그대로 보존한다.
  // item.aspect(raw key) / taxonomy aspectLabels와 동일 기준 → 한글 label 매핑.
  aspect: Object.fromEntries(
    Object.entries(dto.aspect ?? {}).map(([key, value]) => [key, value ?? 0]),
  ),
  aspectSentiment: dto.aspect_sentiment,
  sentiment: {
    positive: dto.sentiment?.positive ?? 0,
    negative: dto.sentiment?.negative ?? 0,
    neutral: dto.sentiment?.neutral ?? 0,
  },
  total: dto.total ?? 0,
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

export const mapClauseLabelBuild = (
  dto: ClauseBuildResponse,
): ClauseBuild => ({
  buildType: dto.build_type,
  status: dto.status,
  jobId: dto.job_id,
  startedAt: dto.started_at ?? "",
  completedAt: dto.completed_at ?? "",
  durationSeconds: dto.duration_seconds ?? 0,
  errorMessage: dto.error_message ?? "",
  progress: dto.progress ? mapProgress(dto.progress) : undefined,
  summary: dto.summary ? mapClauseSummary(dto.summary) : undefined,
  pagination: dto.pagination,
  applied: mapApplied(dto.applied),
  items: dto.items?.map(mapClauseItem) ?? [],
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
