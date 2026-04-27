import type { UploadVersionFormValues } from "../schcema/dataset.schcema";
import type { DatasetVersion, PrepareSummary } from "../types/datasetVersion";
import type { DatasetVersionResponse, PrepareSummaryResponse, UploadDatasetVersionRequest } from "../types/datasetVersion.dto";

export const mapPrepareSummary = (dto: PrepareSummaryResponse): PrepareSummary => ({
  inputRowCount: dto.input_row_count,
  outputRowCount: dto.output_row_count,
  keptCount: dto.kept_count,
  reviewCount: dto.review_count,
  droppedCount: dto.dropped_count,
})

export const mapDatasetVersion = (dto: DatasetVersionResponse): DatasetVersion => ({
  id: dto.dataset_version_id,
  datasetId: dto.dataset_id,
  projectId: dto.project_id,
  storageUri: dto.storage_uri,
  dataType: dto.data_type,
  recordCount: dto.record_count,
  metadata: dto.metadata,
  profile: dto.profile,
  prepareStatus: dto.prepare_status,
  prepareLLMMode: dto.prepare_llm_mode,
  prepareModel: dto.prepare_model,
  preparePromptVersion: dto.prepare_prompt_version,
  prepareUri: dto.prepare_uri,
  preparedAt: dto.prepared_at,
  prepareSummary: mapPrepareSummary(dto.prepare_summary),
  sentimentStatus: dto.sentiment_status,
  sentimentLLMMode: dto.sentiment_llm_mode,
  sentimentModel: dto.sentiment_model,
  sentimentUri: dto.sentiment_uri,
  sentimentLabeledAt: dto.sentiment_labeled_at,
  sentimentPromptVersion: dto.sentiment_prompt_version,
  embeddingStatus: dto.embedding_status,
  embeddingModel: dto.embedding_model,
  embeddingUri: dto.embedding_status,
  isActive: dto.is_active,
  createdAt: dto.created_at,
  readyAt: dto.ready_at,
});

export const mapUploadFormToRequest = (
  form: UploadVersionFormValues,
): UploadDatasetVersionRequest => ({
  file: form.file,
  data_type: form.dataType,
  prepare_required: form.analysisType === "prepare" ? true : undefined,
  sentiment_required: form.analysisType === "sentiment" ? true : undefined,
});