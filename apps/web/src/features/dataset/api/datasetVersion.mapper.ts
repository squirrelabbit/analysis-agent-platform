import type { UploadVersionFormValues } from "../schcema/dataset.schcema";
import type {
  Artifact,
  BuildStage,
  CleanSummary,
  DatasetVersion,
  Diagnostics,
  PrerpareSummary,
  Progress,
  SourceSummary,
} from "../types/datasetVersion";
import type {
  ArtifactDto,
  BuildStageDto,
  CleanSummaryDto,
  DatasetVersionResponse,
  DiagnosticsDto,
  PrerpareSummaryDto,
  ProgressDto,
  SourceSummaryDto,
  UploadDatasetVersionRequest,
} from "../types/datasetVersion.dto";

export const mapUploadFormToRequest = (
  form: UploadVersionFormValues,
): UploadDatasetVersionRequest => ({
  file: form.file,
  data_type: form.dataType,
  // prepare_required: form.analysisType === "prepare" ? true : undefined,
  // sentiment_required: form.analysisType === "sentiment" ? true : undefined,
  metadata: {
    text_columns: form.text_columns.map((col) => col.value),
    clean_preprocess_options: form.cleanOptions
  }
});

export const mapSourceSummary = (dto: SourceSummaryDto): SourceSummary => ({
  available: dto.available,
  status: dto.status,
  format: dto.format,
  rowCount: dto.row_count,
  columnCount: dto.column_count,
  columns: dto.columns,
  errorMessage: dto.error_message,
  sampleLimit: dto.sample_limit,
  sampleRows: dto.sample_rows,
});

export const mapCleanSummary = (dto: CleanSummaryDto): CleanSummary => ({
  inputRowCount: dto.input_row_count,
  outputRowCount: dto.output_row_count,
  keptCount: dto.kept_count,
  droppedCount: dto.dropped_count,
  textColumns: dto.text_columns,
  textJoiner: dto.text_joiner,
  sourceInputCharCount: dto.source_input_char_count,
  cleanedInputCharCount: dto.cleaned_input_char_count,
  cleanReducedCharCount: dto.clean_reduced_char_count,
});

export const mapPrepareSummary = (
  dto: PrerpareSummaryDto,
): PrerpareSummary => ({
  inputRowCount: dto.input_row_count,
  outputRowCount: dto.output_row_count,
  keptCount: dto.kept_count,
  reviewCount: dto.review_count,
  droppedCount: dto.dropped_count,
  textColumn: dto.text_column,
  textColumns: dto.text_columns,
  textJoiner: dto.text_joiner,
});

export const mapArtifact = (dto: ArtifactDto): Artifact => ({
  artifactId: dto.artifact_id,
  projectId: dto.project_id,
  datasetId: dto.dataset_id,
  datasetVersionId: dto.dataset_version_id,
  artifactType: dto.artifact_type,
  stage: dto.stage,
  status: dto.status,
  uri: dto.uri,
  format: dto.format,
  metadata: dto.metadata,
  createdAt: dto.created_at,
  updatedAt: dto.updated_at,
});

export const mapProgress = (dto: ProgressDto): Progress => ({
  percent: dto.percent,
  processedRows: dto.processed_rows,
  totalRows: dto.total_rows,
  elapsedSeconds: dto.elapsed_seconds,
  message: dto.message,
  updatedAt: dto.updated_at,
});

export const mapDiagnostics = (dto: DiagnosticsDto): Diagnostics => ({
  retryCount: dto.retry_count,
  workflowId: dto.workflow_id,
  workflowRunId: dto.workflow_run_id,
  resumedExecutionCount: dto.resumed_execution_count,
  progress: dto.progress ? mapProgress(dto.progress) : undefined,
});

export const mapBuildStage = (dto: BuildStageDto): BuildStage => ({
  stage: dto.stage,
  status: dto.status,
  applicable: dto.applicable,
  required: dto.required,
  ready: dto.ready,
  dependsOn: dto.depends_on,
  canRun: dto.can_run,
  runGroup: dto.run_group,
  autoRuEligible: dto.auto_run_eligible,
  blockedReason: dto.blocked_reason,
  latestJob: dto.latest_job,
  primaryArtifact: dto.primary_artifact,
  artifacts: dto.artifacts?.map(mapArtifact),
  summary: dto.summary,
  model: dto.model,
  promptVersion: dto.prompt_version,
  diagnostics: dto.diagnostics ? mapDiagnostics(dto.diagnostics) : undefined,
});

export const mapDatasetVersion = (
  dto: DatasetVersionResponse,
): DatasetVersion => ({
  id: dto.dataset_version_id,
  datasetId: dto.dataset_id,
  projectId: dto.project_id,
  metadata: dto.metadata,
  storageUri: dto.storage_uri,
  dataType: dto.data_type,
  recordCount: dto.record_count,
  sourceSummary: mapSourceSummary(dto.source_summary),
  buildStages: dto.build_stages.map(mapBuildStage),
  isActive: dto.is_active,
  cleanStatus: dto.clean_status,
  cleanSummary: dto.clean_summary
    ? mapCleanSummary(dto.clean_summary)
    : undefined,
  prepareStatus: dto.prepare_status,
  prepareSummary: dto.prepare_summary
    ? mapPrepareSummary(dto.prepare_summary)
    : undefined,
  sentimentStatus: dto.sentiment_status,
  embeddingStatus: dto.embedding_status,
});

export const mapVersionList = (
  dto: Omit<DatasetVersionResponse, "source_summary">,
): Omit<DatasetVersion, "sourceSummary"> => ({
  id: dto.dataset_version_id,
  datasetId: dto.dataset_id,
  projectId: dto.project_id,
  metadata: dto.metadata,
  storageUri: dto.storage_uri,
  dataType: dto.data_type,
  recordCount: dto.record_count,
  buildStages: dto.build_stages.map(mapBuildStage),
  isActive: dto.is_active,
  cleanStatus: dto.clean_status,
  cleanSummary: dto.clean_summary
    ? mapCleanSummary(dto.clean_summary)
    : undefined,
  prepareStatus: dto.prepare_status,
  prepareSummary: dto.prepare_summary
    ? mapPrepareSummary(dto.prepare_summary)
    : undefined,
  sentimentStatus: dto.sentiment_status,
  embeddingStatus: dto.embedding_status,
});
